//! Pure interface loading: no database connections and no SQL execution.
use crate::{
    SqlrestError,
    params::{Input, Parameter, identifier, validate_parameters},
    response::Contract,
    sql::{Backend, Statement, compile},
};
use heck::ToUpperCamelCase;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
};

const METHODS: &[&str] = &["get", "head", "post", "put", "patch", "delete", "options"];

pub struct Endpoint {
    backend: Backend,
    method: String,
    path: String,
    segments: Vec<Segment>,
    statements: Vec<Statement>,
    parameters: Vec<Parameter>,
    contract: Option<Contract>,
    operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Static(String),
    Dynamic(String),
}

impl Endpoint {
    pub fn backend(&self) -> Backend {
        self.backend
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }

    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }

    pub fn contract(&self) -> Option<&Contract> {
        self.contract.as_ref()
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Check the entire input contract before a caller starts any SQL.
    pub fn validate_input(&self, input: &Input) -> Result<(), SqlrestError> {
        for parameter in &self.parameters {
            parameter.read(input)?;
        }
        Ok(())
    }
}

/// Owned snapshot. Once returned it never re-reads files or changes in place.
pub struct Snapshot {
    endpoints: Vec<Arc<Endpoint>>,
    openapi: Value,
    version: String,
}

pub struct MatchedEndpoint {
    pub endpoint: Arc<Endpoint>,
    pub path_parameters: BTreeMap<String, String>,
}

impl Snapshot {
    /// The publisher must keep files stable for the duration of this read.
    pub fn load(root: &Path, backend: Backend) -> Result<Self, SqlrestError> {
        let mut files = BTreeMap::new();
        read_tree(root, "", &mut files)?;
        Self::from_files(files, backend)
    }

    /// In-memory equivalent used by embedders and deterministic tests.
    /// Keys are relative, slash-separated paths; values are UTF-8 file contents.
    pub fn from_files(
        files: BTreeMap<String, String>,
        backend: Backend,
    ) -> Result<Self, SqlrestError> {
        let mut endpoints = Vec::new();
        let mut names = BTreeSet::new();
        let mut routes = BTreeSet::new();
        let mut dynamic_prefixes = BTreeMap::new();
        for (file, source) in &files {
            validate_file_path(file)?;
            let filename = file.rsplit('/').next().unwrap();
            if let Some(method) = filename.strip_suffix(".response.yaml") {
                if !METHODS.contains(&method) {
                    return Err(SqlrestError::definition(format!(
                        "Unknown method file: {file}"
                    )));
                }
                let sibling = format!("{}.sql", file.strip_suffix(".response.yaml").unwrap());
                if !files.contains_key(&sibling) {
                    return Err(SqlrestError::definition(format!(
                        "Response schema has no SQL sibling: {file}"
                    )));
                }
                continue;
            }
            let method = filename
                .strip_suffix(".sql")
                .filter(|m| METHODS.contains(m))
                .ok_or_else(|| {
                    SqlrestError::definition(format!("Unexpected interface file: {file}"))
                })?;
            let directory = file.rsplit_once('/').map_or("", |(d, _)| d);
            let mut segments = Vec::new();
            let mut dynamic_names = BTreeSet::new();
            let mut structural = String::new();
            for part in directory.split('/').filter(|s| !s.is_empty()) {
                if let Some(name) = part.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                    if !identifier(name) || !dynamic_names.insert(name.to_owned()) {
                        return Err(SqlrestError::definition(format!(
                            "Invalid or repeated path parameter: {file}"
                        )));
                    }
                    structural.push_str("/{}");
                    if let Some(previous) =
                        dynamic_prefixes.insert(structural.clone(), name.to_owned())
                        && previous != name
                    {
                        return Err(SqlrestError::definition(format!(
                            "Conflicting dynamic route names: {file}"
                        )));
                    }
                    segments.push(Segment::Dynamic(name.into()));
                } else {
                    if !part
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
                    {
                        return Err(SqlrestError::definition(format!(
                            "Invalid route segment: {part}"
                        )));
                    }
                    structural.push('/');
                    structural.push_str(part);
                    segments.push(Segment::Static(part.into()));
                }
            }
            if !routes.insert((method, structural)) {
                return Err(SqlrestError::definition(format!("Duplicate route: {file}")));
            }
            let path = if segments.is_empty() {
                "/".into()
            } else {
                segments
                    .iter()
                    .map(|s| match s {
                        Segment::Static(s) => format!("/{s}"),
                        Segment::Dynamic(s) => format!("/{{{s}}}"),
                    })
                    .collect::<String>()
            };
            let statements = compile(source, backend)?;
            let mut parameters: Vec<_> = statements
                .iter()
                .flat_map(|s| s.parameters.clone())
                .collect();
            validate_parameters(&parameters)?;
            parameters.sort_by_key(Parameter::path);
            parameters.dedup();
            let referenced: BTreeSet<_> = parameters
                .iter()
                .filter(|p| p.source == "path")
                .map(|p| p.fields[0].clone())
                .collect();
            if referenced != dynamic_names {
                return Err(SqlrestError::definition(format!(
                    "Every dynamic segment needs a typed path reference, and vice versa: {file}"
                )));
            }
            let schema_path = format!("{}.response.yaml", file.strip_suffix(".sql").unwrap());
            let contract = files
                .get(&schema_path)
                .map(|s| Contract::from_yaml(s))
                .transpose()?;
            if statements.last().unwrap().returns_rows && contract.is_none() {
                return Err(SqlrestError::definition(format!(
                    "Final result columns require a response schema: {file}"
                )));
            }
            let operation_id = operation_name(method, &segments);
            // One registry for generated names, including suffixes, avoids
            // collisions between an operation and another operation's model.
            for name in [
                &operation_id,
                &format!("{operation_id}Input"),
                &format!("{operation_id}Record"),
                &format!("{operation_id}Response"),
            ] {
                if !names.insert(name.to_owned()) {
                    return Err(SqlrestError::definition(format!(
                        "Generated name collision at {file}: {name}"
                    )));
                }
            }
            endpoints.push(Arc::new(Endpoint {
                backend,
                method: method.into(),
                path,
                segments,
                statements,
                parameters,
                contract,
                operation_id,
            }));
        }
        // First differing static segment wins; method matching happens only
        // after choosing a path, so a dynamic route cannot bypass a static one.
        endpoints.sort_by_key(|e| {
            e.segments
                .iter()
                .map(|s| match s {
                    Segment::Static(_) => 0,
                    Segment::Dynamic(_) => 1,
                })
                .collect::<Vec<_>>()
        });
        let openapi = build_openapi(&endpoints)?;
        let mut digest = Sha256::new();
        digest.update(match backend {
            Backend::Turso => b"turso".as_slice(),
            Backend::Postgres => b"postgres".as_slice(),
        });
        for (path, content) in files {
            for item in [path, content] {
                digest.update((item.len() as u64).to_be_bytes());
                digest.update(item.as_bytes());
            }
        }
        Ok(Self {
            endpoints,
            openapi,
            version: format!("{:x}", digest.finalize()),
        })
    }

    pub fn endpoints(&self) -> &[Arc<Endpoint>] {
        &self.endpoints
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// `segments` are already percent-decoded by the transport, exactly once.
    pub fn resolve(
        &self,
        method: &str,
        segments: &[&str],
    ) -> Result<MatchedEndpoint, SqlrestError> {
        let first = self
            .endpoints
            .iter()
            .find(|e| captures(e, segments).is_some())
            .ok_or_else(|| SqlrestError::new(404, "route_not_found", "No matching route"))?;
        let endpoint = self
            .endpoints
            .iter()
            .find(|e| e.path == first.path && e.method.eq_ignore_ascii_case(method))
            .ok_or_else(|| {
                SqlrestError::new(
                    405,
                    "method_not_allowed",
                    "Method not defined for this route",
                )
            })?;
        Ok(MatchedEndpoint {
            endpoint: endpoint.clone(),
            path_parameters: captures(endpoint, segments).unwrap(),
        })
    }

    pub fn openapi(&self, server_url: &str) -> Value {
        let mut document = self.openapi.clone();
        document["servers"] = json!([{"url":server_url}]);
        document
    }
}

fn captures(endpoint: &Endpoint, values: &[&str]) -> Option<BTreeMap<String, String>> {
    if endpoint.segments.len() != values.len() {
        return None;
    }
    let mut result = BTreeMap::new();
    for (segment, value) in endpoint.segments.iter().zip(values) {
        if value.is_empty() || value.contains('/') {
            return None;
        }
        match segment {
            Segment::Static(s) if s != value => return None,
            Segment::Dynamic(name) => {
                result.insert(name.clone(), (*value).into());
            }
            _ => {}
        }
    }
    Some(result)
}

fn validate_file_path(path: &str) -> Result<(), SqlrestError> {
    if path.is_empty()
        || path.contains('\\')
        || path.split('/').any(|s| matches!(s, "" | "." | ".."))
    {
        return Err(SqlrestError::definition(format!(
            "Invalid relative interface path: {path}"
        )));
    }
    Ok(())
}

fn read_tree(
    root: &Path,
    prefix: &str,
    output: &mut BTreeMap<String, String>,
) -> Result<(), SqlrestError> {
    for entry in fs::read_dir(root)
        .map_err(|e| SqlrestError::definition(format!("Cannot read interface directory: {e}")))?
    {
        let entry = entry.map_err(|e| SqlrestError::definition(e.to_string()))?;
        let filename = entry
            .file_name()
            .into_string()
            .map_err(|_| SqlrestError::definition("Interface filename must be UTF-8"))?;
        let relative = if prefix.is_empty() {
            filename
        } else {
            format!("{prefix}/{filename}")
        };
        let kind = entry
            .file_type()
            .map_err(|e| SqlrestError::definition(e.to_string()))?;
        if kind.is_dir() {
            read_tree(&entry.path(), &relative, output)?;
        } else if kind.is_file() {
            output.insert(
                relative,
                fs::read_to_string(entry.path())
                    .map_err(|e| SqlrestError::definition(e.to_string()))?,
            );
        } else {
            return Err(SqlrestError::definition(
                "Interface tree must contain regular files and directories, not symlinks or devices",
            ));
        }
    }
    Ok(())
}

fn operation_name(method: &str, segments: &[Segment]) -> String {
    let mut result = title_case(method);
    for segment in segments {
        match segment {
            Segment::Static(s) => result.push_str(&title_case(s)),
            Segment::Dynamic(s) => {
                result.push_str("By");
                result.push_str(&title_case(s));
            }
        }
    }
    result
}

fn title_case(text: &str) -> String {
    text.to_upper_camel_case()
}

fn insert_body(schema: &mut Value, path: &[String], leaf: Value) {
    let name = &path[0];
    let required = schema["required"].as_array_mut().unwrap();
    if !required.iter().any(|s| s == name) {
        required.push(json!(name));
    }
    if path.len() == 1 {
        schema["properties"][name] = leaf;
    } else {
        let child = schema["properties"]
            .as_object_mut()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| json!({"type":"object","properties":{},"required":[]}));
        insert_body(child, &path[1..], leaf);
    }
}

fn build_openapi(endpoints: &[Arc<Endpoint>]) -> Result<Value, SqlrestError> {
    let mut document = json!({
        "openapi": "3.1.0",
        "info": {"title": "SQLRest", "version": "0.1.0"},
        "paths": {},
        "components": {"schemas": {}}
    });
    for endpoint in endpoints {
        let id = &endpoint.operation_id;
        let mut parameters = Vec::new();
        let mut body = json!({"type":"object","properties":{},"required":[]});
        for parameter in &endpoint.parameters {
            if parameter.source == "body" {
                insert_body(&mut body, &parameter.fields, parameter.ty.schema());
            } else {
                parameters.push(json!({
                    "in": parameter.source,
                    "name": parameter.fields[0],
                    "required": true,
                    "schema": parameter.ty.schema()
                }));
            }
        }
        let response = if let Some(contract) = &endpoint.contract {
            let record_name = format!("{id}Record");
            for (name, schema) in contract.openapi_components(&record_name)? {
                if document["components"]["schemas"].get(&name).is_some() {
                    return Err(SqlrestError::definition(format!(
                        "Generated schema name collision: {name}"
                    )));
                }
                document["components"]["schemas"][&name] = schema;
            }
            json!({
                "type": "object",
                "required": ["records"],
                "additionalProperties": false,
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {"$ref": format!("#/components/schemas/{record_name}")}
                    }
                }
            })
        } else {
            // Actual column metadata is checked at execution. Missing schema
            // means the only declared success result is an empty record array.
            json!({
                "type": "object",
                "required": ["records"],
                "additionalProperties": false,
                "properties": {
                    "records": {"type": "array", "maxItems": 0, "items": {}}
                }
            })
        };
        let response_name = format!("{id}Response");
        if document["components"]["schemas"]
            .get(&response_name)
            .is_some()
        {
            return Err(SqlrestError::definition(format!(
                "Generated schema name collision: {response_name}"
            )));
        }
        document["components"]["schemas"][&response_name] = response;
        let mut operation = json!({
            "operationId": id,
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": "Successful execution",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": format!("#/components/schemas/{response_name}")}
                        }
                    }
                }
            }
        });
        operation["responses"]["default"] = json!({
            "description": "Execution or request error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "required": ["error"],
                        "properties": {
                            "error": {
                                "type": "object",
                                "required": ["code", "message"],
                                "properties": {
                                    "code": {"type": "string"},
                                    "message": {"type": "string"},
                                    "parameter": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
        });
        if endpoint.method == "head" {
            operation["responses"]["200"] =
                json!({"description":"Successful execution; HTTP HEAD sends no response body"});
            operation["responses"]["default"] =
                json!({"description":"Request failed; HTTP HEAD sends no response body"});
        }
        if !body["required"].as_array().unwrap().is_empty() {
            let input_name = format!("{id}Input");
            if document["components"]["schemas"].get(&input_name).is_some() {
                return Err(SqlrestError::definition(format!(
                    "Generated schema name collision: {input_name}"
                )));
            }
            document["components"]["schemas"][&input_name] = body;
            operation["requestBody"] = json!({
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {"$ref": format!("#/components/schemas/{input_name}")}
                    }
                }
            });
        }
        if document["paths"].get(&endpoint.path).is_none() {
            document["paths"][&endpoint.path] = json!({});
        }
        document["paths"][&endpoint.path][&endpoint.method] = operation;
    }
    Ok(document)
}
