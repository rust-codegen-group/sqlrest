use std::process::Command;

fn run(arguments: &[&str]) {
    let output = Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/e2e.py"))
        .args(["--binary", env!("CARGO_BIN_EXE_sqlrest")])
        .args(arguments)
        .output()
        .expect("Python 3 must be available for delivery E2E");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn turso_examples_and_process_restart() {
    run(&[]);
}

#[test]
#[ignore = "requires empty SQLREST_TEST_POSTGRES, OPENAPI_NEXUS_BIN, Node.js and TypeScript 6"]
fn postgres_examples_and_process_restart() {
    run(&["--backend", "postgres", "--sdk"]);
}

#[test]
#[ignore = "requires OPENAPI_NEXUS_BIN, Node.js and TypeScript 6 on PATH"]
fn generated_sdks_call_real_examples() {
    run(&["--sdk"]);
}
