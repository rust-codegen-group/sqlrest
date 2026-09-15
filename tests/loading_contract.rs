use serde_json::json;
use sqlrest::{
    loader::Snapshot,
    params::Input,
    response::{Cell, Contract},
    sql::{Backend, compile},
};
use std::collections::BTreeMap;

fn files(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(p, s)| ((*p).into(), (*s).into()))
        .collect()
}
const RECORD: &str = "type: object\nproperties:\n  id:\n    type: integer\nrequired: [id]\nadditionalProperties: false\n";

#[test]
fn snapshot_routes_body_and_openapi_are_one_contract() {
    let source = files(&[
        (
            "todos/[id]/patch.sql",
            "UPDATE todos SET title=${body.input.title:string}, completed=${body.input.completed:boolean} WHERE id=${path.id:int64} RETURNING id",
        ),
        ("todos/[id]/patch.response.yaml", RECORD),
        ("todos/latest/get.sql", "SELECT id FROM todos"),
        ("todos/latest/get.response.yaml", RECORD),
    ]);
    let snapshot = Snapshot::from_files(source.clone(), Backend::Turso).unwrap();
    let matched = snapshot.resolve("PATCH", &["todos", "42"]).unwrap();
    let mut input = Input::from_http(
        "",
        br#"{"input":{"title":"hello","completed":true,"ignored":1}}"#,
    )
    .unwrap();
    input.path = matched.path_parameters;
    matched.endpoint.validate_input(&input).unwrap();
    assert_eq!(matched.endpoint.operation_id(), "PatchTodosById");
    assert_eq!(
        snapshot
            .resolve("PATCH", &["todos", "latest"])
            .err()
            .unwrap()
            .status,
        405
    );
    let openapi = snapshot.openapi("/db/personal");
    assert_eq!(openapi["info"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        openapi["components"]["schemas"]["PatchTodosByIdInput"]["properties"]["input"]["properties"]
            ["completed"]["type"],
        "boolean"
    );
    assert_eq!(
        openapi["paths"]["/todos/{id}"]["patch"]["operationId"],
        "PatchTodosById"
    );
    assert_eq!(
        snapshot.openapi("/api")["components"],
        openapi["components"]
    );
    let another = Snapshot::from_files(source, Backend::Turso).unwrap();
    assert_eq!(snapshot.version(), another.version());
}

#[test]
fn definitions_fail_without_database_execution() {
    for source in [
        files(&[("a/get.response.yaml", RECORD)]),
        files(&[("a/[id]/get.sql", "SELECT 1")]),
        files(&[("a/get.sql", "SELECT ${path.id:int64}")]),
        files(&[
            ("a/[id]/get.sql", "SELECT ${path.id:int64}"),
            ("a/[id]/get.response.yaml", RECORD),
            ("a/[name]/post.sql", "SELECT ${path.name:string}"),
            ("a/[name]/post.response.yaml", RECORD),
        ]),
        files(&[
            ("foo-bar/get.sql", "SELECT 1"),
            ("foo-bar/get.response.yaml", RECORD),
            ("foo_bar/get.sql", "SELECT 1"),
            ("foo_bar/get.response.yaml", RECORD),
        ]),
        files(&[("../get.sql", "SELECT 1")]),
        files(&[("get.sql", "SELECT ${body.x:string}, ${body.x.y:string}")]),
        files(&[(
            "get.sql",
            "SELECT ${query.x:string}; SELECT ${query.x:int64}",
        )]),
    ] {
        assert!(Snapshot::from_files(source, Backend::Turso).is_err());
    }
    let conflict = Snapshot::from_files(
        files(&[
            ("foo-bar/get.sql", "SELECT id FROM todos"),
            ("foo-bar/get.response.yaml", RECORD),
            ("foo_bar/get.sql", "SELECT id FROM todos"),
            ("foo_bar/get.response.yaml", RECORD),
        ]),
        Backend::Postgres,
    )
    .err()
    .unwrap();
    assert!(conflict.message.contains("name collision"), "{conflict}");
    let conflict = Snapshot::from_files(
        files(&[
            ("a/[id]/get.sql", "SELECT ${path.id:int64} AS id"),
            ("a/[id]/get.response.yaml", RECORD),
            ("a/[name]/post.sql", "SELECT ${path.name:string} AS id"),
            ("a/[name]/post.response.yaml", RECORD),
        ]),
        Backend::Postgres,
    )
    .err()
    .unwrap();
    assert!(
        conflict.message.contains("dynamic route names"),
        "{conflict}"
    );
}

#[test]
fn loaded_files_are_not_reopened() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("get.sql"), "SELECT 1 AS id").unwrap();
    std::fs::write(directory.path().join("get.response.yaml"), RECORD).unwrap();
    let snapshot = Snapshot::load(directory.path(), Backend::Turso).unwrap();
    let version = snapshot.version().to_owned();
    std::fs::write(directory.path().join("get.sql"), "broken SQL").unwrap();
    assert!(Snapshot::load(directory.path(), Backend::Turso).is_err());
    assert_eq!(snapshot.version(), version);
    assert_eq!(
        snapshot.endpoints()[0].statements()[0].sql,
        "SELECT 1 AS id"
    );
}

#[test]
fn missing_result_schema_and_duplicate_yaml_are_rejected() {
    assert!(
        Snapshot::from_files(
            files(&[("get.sql", "SELECT id FROM todos WHERE false")]),
            Backend::Turso
        )
        .is_err()
    );
    assert!(
        Contract::from_yaml(
            "type: object\nproperties:\n  id: {type: integer}\n  id: {type: string}\n"
        )
        .is_err()
    );
    assert!(compile("SELECT * FROM ${body.table:string}", Backend::Postgres).is_err());
    let source = "CREATE TEMP TABLE sample(id INTEGER); INSERT INTO sample VALUES(${body.id:int64}); SELECT id FROM sample";
    assert_eq!(compile(source, Backend::Turso).unwrap().len(), 3);
}

#[test]
fn schema_ref_siblings_cycles_and_encoded_targets() {
    let contract = Contract::new(json!({
        "type":"object","properties":{"payload":{"$ref":"#/$defs/a%20b","maxProperties":1}},
        "$defs":{"a b":{"type":"object","properties":{"value":{"type":"string"}}}}
    }))
    .unwrap();
    assert!(
        contract
            .record(
                &["payload".into()],
                vec![Cell::Text(r#"{"value":"x"}"#.into())],
                true
            )
            .is_ok()
    );
    assert!(
        contract
            .record(
                &["payload".into()],
                vec![Cell::Text(r#"{"value":"x","extra":2}"#.into())],
                true
            )
            .is_err()
    );
    assert!(Contract::new(json!({"type":"object","$ref":"#"})).is_err());
    assert!(
        Contract::new(
            json!({"type":"object","properties":{},"$ref":"#/examples/0","examples":[true]})
        )
        .is_err()
    );
    let contract = Contract::new(json!({
        "$schema":"https://spec.openapis.org/oas/3.1/dialect/base",
        "type":"object","properties":{"a":{"$ref":"#/$defs/a-b"},"b":{"$ref":"#/$defs/a_b"}},
        "$defs":{"a-b":{"type":"string"},"a_b":{"type":"string"}}
    }))
    .unwrap();
    assert!(contract.openapi_components("Record").is_err());
}

#[test]
fn lifted_recursive_schema_preserves_validation() {
    let contract = Contract::new(json!({
        "type":"object","required":["children"],"additionalProperties":false,
        "properties":{"children":{"type":"array","items":{"$ref":"#"}}}
    }))
    .unwrap();
    let components = contract.openapi_components("Tree").unwrap();
    let exported = json!({"$ref":"#/components/schemas/Tree","components":{"schemas":components}});
    let validator = jsonschema::validator_for(&exported).unwrap();
    for (value, valid) in [
        (json!({"children":[{"children":[]}]}), true),
        (json!({"children":[{"children":0}]}), false),
        (json!({"children":[],"extra":true}), false),
    ] {
        assert_eq!(validator.is_valid(&value), valid);
    }
}

#[test]
fn discriminator_mapping_is_relocated_but_example_data_is_not() {
    let contract = Contract::new(json!({
        "type":"object","properties":{"pet":{"type":"object","oneOf":[{"$ref":"#/$defs/cat"}],
            "discriminator":{"propertyName":"kind","mapping":{"cat":"#/$defs/cat"}}}},
        "$defs":{"cat":{"type":"object","properties":{"kind":{"type":"string","const":"cat"}}}},
        "examples":[{"discriminator":{"mapping":{"cat":"ordinary data"}}}]
    }))
    .unwrap();
    let components = contract.openapi_components("Record").unwrap();
    assert_eq!(
        components["Record"]["properties"]["pet"]["discriminator"]["mapping"]["cat"],
        "#/components/schemas/RecordDefsCat"
    );
    assert_eq!(
        components["Record"]["examples"][0]["discriminator"]["mapping"]["cat"],
        "ordinary data"
    );
}

#[test]
fn explicit_methods_and_acronym_names() {
    let snapshot = Snapshot::from_files(
        files(&[
            ("head.sql", "SELECT id FROM todos"),
            ("head.response.yaml", RECORD),
            ("delete.sql", "DELETE FROM todos"),
        ]),
        Backend::Postgres,
    )
    .unwrap();
    let document = snapshot.openapi("/api");
    assert!(
        document["paths"]["/"]["head"]["responses"]["200"]
            .get("content")
            .is_none()
    );
    assert_eq!(
        document["components"]["schemas"]["DeleteResponse"]["properties"]["records"]["maxItems"],
        0
    );
    assert_eq!(snapshot.resolve("GET", &[]).err().unwrap().status, 405);
    let error = Snapshot::from_files(
        files(&[
            ("fooBAR/get.sql", "SELECT id FROM todos"),
            ("fooBAR/get.response.yaml", RECORD),
            ("fooBar/get.sql", "SELECT id FROM todos"),
            ("fooBar/get.response.yaml", RECORD),
        ]),
        Backend::Turso,
    )
    .err()
    .unwrap();
    assert!(error.message.contains("name collision"));
}

#[test]
fn input_rejects_lossy_encoding_and_keeps_declared_types() {
    for query in ["x=%", "x=%GG", "x=%FF", "x=1&%78=2"] {
        assert!(Input::from_http(query, b"{}").is_err());
    }
    for text in ["1", "\"true\"", "null"] {
        let input = Input::from_http("", format!("{{\"done\":{text}}}").as_bytes()).unwrap();
        assert!(
            sqlrest::params::Parameter::parse("body.done:boolean")
                .unwrap()
                .read(&input)
                .is_err()
        );
    }
}

#[test]
fn schema_annotations_are_data_and_recursive_refs_relocate() {
    let contract = Contract::new(json!({
        "type":"object",
        "properties":{"tree":{"$ref":"#node"}},
        "$defs":{"node":{"$anchor":"node","type":"object","properties":{
            "children":{"type":"array","items":{"$ref":"#node"}}
        },"required":["children"]}},
        "examples":[{"$ref":"this is example data, not a reference"}]
    }))
    .unwrap();
    let relocated = contract
        .relocated("#/components/schemas/TestRecord")
        .unwrap();
    assert_eq!(
        relocated["properties"]["tree"]["$ref"],
        "#/components/schemas/TestRecord/$defs/node"
    );
    assert_eq!(
        relocated["$defs"]["node"]["properties"]["children"]["items"]["$ref"],
        "#/components/schemas/TestRecord/$defs/node"
    );
    assert_eq!(
        relocated["examples"][0]["$ref"],
        "this is example data, not a reference"
    );
    contract
        .record(
            &["tree".into()],
            vec![Cell::Text(r#"{"children":[{"children":[]}]}"#.into())],
            true,
        )
        .unwrap();
}

#[test]
fn compositions_validate_and_ambiguous_decoding_is_rejected() {
    let contract = Contract::new(json!({"allOf":[
        {"type":"object","properties":{"id":{"type":"integer"}}},
        {"type":"object","properties":{"done":{"anyOf":[{"type":"boolean"},{"type":"null"}]}}}
    ]}))
    .unwrap();
    assert_eq!(
        contract
            .record(
                &["done".into(), "id".into()],
                vec![Cell::Integer(1), Cell::Integer(9)],
                true
            )
            .unwrap(),
        json!({"done":true,"id":9})
    );
    for definition in [
        json!({"type":"object","properties":{"x":{"oneOf":[{"type":"string"},{"type":"object"}]}}}),
        json!({"type":"object","properties":{"x":{"type":["integer","boolean"]}}}),
        json!({"type":"object","properties":{"x":{"type":"string","minLenght":5}}}),
        json!({"type":"object","properties":{"x":{"$ref":"file.yaml#/foo"}}}),
        json!({"type":"object","properties":{"x":{"$ref":"#/$defs/missing"}}}),
        json!({"oneOf":[{"type":"object","properties":{"x":{"type":"string"}}},{"type":"object","properties":{"y":{"type":"string"}}}]}),
    ] {
        assert!(Contract::new(definition).is_err());
    }
}

#[test]
fn sql_preserves_quotes_comments_and_statement_bindings() {
    let source = "/* ; ${body.no:string} */ SELECT E'it\\'s;', $tag$;${body.no:string}$tag$, ${query.n:int64}; -- ;\nSELECT ${query.n:int64}, 'it''s'";
    let compiled = compile(source, Backend::Postgres).unwrap();
    assert_eq!(compiled.len(), 2);
    assert_eq!(compiled[0].parameters.len(), 1);
    assert!(compiled[0].sql.ends_with(", $1"));
    assert!(compiled[1].sql.contains("SELECT $1, 'it''s'"));
    for sql in ["SELECT $1", "SELECT :native", "BEGIN", "COMMIT"] {
        assert!(compile(sql, Backend::Postgres).is_err(), "{sql}");
    }
}
