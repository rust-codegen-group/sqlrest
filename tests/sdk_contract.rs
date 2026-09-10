//! Optional cross-project verification; writes only to an owned temp directory.
use serde_json::json;
use sqlrest::{loader::Snapshot, sql::Backend};
use std::{collections::BTreeMap, process::Command};

#[test]
#[ignore = "requires OPENAPI_NEXUS_BIN and tsc on PATH"]
fn recursive_openapi_generates_compiling_typescript() {
    let generator = std::env::var("OPENAPI_NEXUS_BIN").expect("Set OPENAPI_NEXUS_BIN");
    let mut files = BTreeMap::new();
    files.insert("trees/[id]/post.sql".into(), "SELECT ${path.id:int64} AS id, ${body.input.title:string} AS title, ${body.input.completed:boolean} AS completed, ${body.ids:array<int64>} AS children".into());
    files.insert("trees/[id]/post.response.yaml".into(), json!({
        "type":"object","required":["id","title","completed","children"],"additionalProperties":false,
        "properties":{"id":{"type":"integer","format":"int64"},"title":{"type":"string"},
            "completed":{"type":"boolean"},"children":{"type":"array","items":{"$ref":"#/$defs/node"}}},
        "$defs":{"node":{"type":"object","required":["title","children"],"additionalProperties":false,
            "properties":{"title":{"type":"string"},"children":{"type":"array","items":{"$ref":"#/$defs/node"}}}}}
    }).to_string());
    let snapshot = Snapshot::from_files(files, Backend::Turso).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let specification = directory.path().join("openapi.json");
    let output = directory.path().join("sdk");
    std::fs::write(
        &specification,
        serde_json::to_vec_pretty(&snapshot.openapi("/api")).unwrap(),
    )
    .unwrap();
    let result = Command::new(generator)
        .args(["generate", "-i"])
        .arg(&specification)
        .arg("-o")
        .arg(&output)
        .args(["--generators", "typescript-fetch"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "generator failed: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    println!("{}", String::from_utf8_lossy(&result.stdout));
    // Discover the generated TypeScript root without assuming the generator's
    // output layout. Compile all generated source files, not just the entrypoint.
    fn collect(path: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(&path, files);
            } else if path.extension().is_some_and(|e| e == "ts") {
                files.push(path);
            }
        }
    }
    let mut sources = Vec::new();
    collect(&output, &mut sources);
    assert!(
        !sources.is_empty(),
        "generator produced no TypeScript sources"
    );
    let typecheck = directory.path().join("contract-check.ts");
    std::fs::write(
        &typecheck,
        r#"
import type { PostTreesByIdRecordDefsNode } from './sdk/models/PostTreesByIdRecordDefsNode';
const tree: PostTreesByIdRecordDefsNode = {title:'root', children:[{title:'child',children:[]}]};
// @ts-expect-error recursive children must retain their declared field types
const invalid: PostTreesByIdRecordDefsNode = {title:'root', children:[{title:42,children:[]}]};
void tree; void invalid;
"#,
    )
    .unwrap();
    sources.push(typecheck);
    let result = Command::new("tsc")
        .args([
            "--noEmit",
            "--strict",
            "--target",
            "ES2022",
            "--module",
            "ESNext",
            "--moduleResolution",
            "bundler",
            "--lib",
            "ES2022,DOM",
        ])
        .args(&sources)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "TypeScript compile failed: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
