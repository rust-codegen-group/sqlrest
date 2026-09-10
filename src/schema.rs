use crate::SqlrestError;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
const FRAGMENT_ENCODING: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'~')
    .remove(b'$')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.');

const MAPS: &[&str] = &[
    "properties",
    "patternProperties",
    "$defs",
    "dependentSchemas",
];
const LISTS: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];
const SINGLE: &[&str] = &[
    "items",
    "contains",
    "additionalProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
    "propertyNames",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
];
const LEAVES: &[&str] = &[
    "$schema",
    "$ref",
    "$anchor",
    "$comment",
    "type",
    "enum",
    "const",
    "title",
    "description",
    "default",
    "examples",
    "example",
    "deprecated",
    "readOnly",
    "writeOnly",
    "format",
    "contentEncoding",
    "contentMediaType",
    "required",
    "dependentRequired",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "pattern",
    "minItems",
    "maxItems",
    "uniqueItems",
    "minContains",
    "maxContains",
    "minProperties",
    "maxProperties",
    "discriminator",
    "xml",
    "externalDocs",
];

/// Walk schema locations, never instance-valued annotations (examples, defaults,
/// enum, const) or property names. Those can legitimately contain "$ref" keys.
pub fn walk(
    value: &mut Value,
    pointer: &str,
    visit: &mut impl FnMut(&mut Map<String, Value>, &str) -> Result<(), SqlrestError>,
) -> Result<(), SqlrestError> {
    if value.is_boolean() {
        return visit(&mut Map::new(), pointer);
    }
    let map = value
        .as_object_mut()
        .ok_or_else(|| SqlrestError::definition("Schema must be an object or boolean"))?;
    visit(map, pointer)?;
    let keys: Vec<_> = map.keys().cloned().collect();
    for key in keys {
        let path = format!("{pointer}/{}", escape(&key));
        if MAPS.contains(&key.as_str()) {
            let children = map[&key]
                .as_object_mut()
                .ok_or_else(|| SqlrestError::definition(format!("{key} must be an object")))?;
            for (name, child) in children {
                walk(child, &format!("{path}/{}", escape(name)), visit)?;
            }
        } else if LISTS.contains(&key.as_str()) {
            let children = map[&key]
                .as_array_mut()
                .ok_or_else(|| SqlrestError::definition(format!("{key} must be an array")))?;
            for (index, child) in children.iter_mut().enumerate() {
                walk(child, &format!("{path}/{index}"), visit)?;
            }
        } else if SINGLE.contains(&key.as_str()) {
            walk(&mut map[&key], &path, visit)?;
        }
    }
    Ok(())
}

fn escape(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

/// Reference-valued keywords only; annotations containing instance data are
/// intentionally not traversed.
pub fn rewrite_refs(
    map: &mut Map<String, Value>,
    rewrite: &mut impl FnMut(&str) -> Result<String, SqlrestError>,
) -> Result<(), SqlrestError> {
    if let Some(reference) = map.get_mut("$ref") {
        let text = reference
            .as_str()
            .ok_or_else(|| SqlrestError::definition("$ref must be a string"))?;
        *reference = Value::String(rewrite(text)?);
    }
    if let Some(discriminator) = map.get_mut("discriminator") {
        let object = discriminator
            .as_object_mut()
            .ok_or_else(|| SqlrestError::definition("discriminator must be an object"))?;
        if !object.get("propertyName").is_some_and(Value::is_string) {
            return Err(SqlrestError::definition("discriminator needs propertyName"));
        }
        if let Some(mapping) = object.get_mut("mapping") {
            for reference in mapping
                .as_object_mut()
                .ok_or_else(|| SqlrestError::definition("discriminator mapping must be an object"))?
                .values_mut()
            {
                let text = reference.as_str().ok_or_else(|| {
                    SqlrestError::definition(
                        "discriminator mapping targets must be local references",
                    )
                })?;
                *reference = Value::String(rewrite(text)?);
            }
        }
    }
    Ok(())
}

/// Resolve local anchors to document pointers, preserving recursive graphs.
pub fn normalize(mut schema: Value) -> Result<Value, SqlrestError> {
    let mut anchors = BTreeMap::new();
    let mut locations = BTreeSet::new();
    walk(&mut schema, "", &mut |map, pointer| {
        locations.insert(pointer.to_owned());
        for key in map.keys() {
            if !MAPS.contains(&key.as_str())
                && !LISTS.contains(&key.as_str())
                && !SINGLE.contains(&key.as_str())
                && !LEAVES.contains(&key.as_str())
                && !key.starts_with("x-")
            {
                return Err(SqlrestError::definition(format!(
                    "Unsupported schema keyword: {key}"
                )));
            }
        }
        if let Some(value) = map.get("$schema") {
            let uri = value.as_str().unwrap_or("");
            if uri != "https://json-schema.org/draft/2020-12/schema"
                && uri != "https://spec.openapis.org/oas/3.1/dialect/base"
            {
                return Err(SqlrestError::definition(
                    "Response schema must use the 2020-12 / OpenAPI 3.1 dialect",
                ));
            }
            // OAS 3.1 adds annotation keywords to 2020-12. Those are handled as
            // annotations here; use the bundled metaschema, never fetch one.
            map.insert(
                "$schema".into(),
                json!("https://json-schema.org/draft/2020-12/schema"),
            );
        }
        if let Some(anchor) = map.get("$anchor") {
            let anchor = anchor
                .as_str()
                .ok_or_else(|| SqlrestError::definition("Invalid schema anchor"))?;
            if anchor.is_empty()
                || !anchor
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
                || anchors
                    .insert(anchor.to_owned(), pointer.to_owned())
                    .is_some()
            {
                return Err(SqlrestError::definition(
                    "Invalid or duplicate schema anchor",
                ));
            }
        }
        Ok(())
    })?;
    // Boolean schema locations are valid ref targets too.
    fn is_schema(root: &Value, pointer: &str, locations: &BTreeSet<String>) -> bool {
        locations.contains(pointer) && root.pointer(pointer).is_some()
    }
    let root = schema.clone();
    walk(&mut schema, "", &mut |map, _| {
        rewrite_refs(map, &mut |reference| {
            let fragment = reference.strip_prefix('#').ok_or_else(|| {
                SqlrestError::definition("External schema references are prohibited")
            })?;
            let fragment = percent_encoding::percent_decode_str(fragment)
                .decode_utf8()
                .map_err(|_| SqlrestError::definition("Invalid UTF-8 in schema reference"))?;
            let pointer = if fragment.is_empty() || fragment.starts_with('/') {
                fragment.to_string()
            } else {
                anchors
                    .get(fragment.as_ref())
                    .cloned()
                    .ok_or_else(|| SqlrestError::definition("Unknown local schema anchor"))?
            };
            if root.pointer(&pointer).is_none() || !is_schema(&root, &pointer, &locations) {
                return Err(SqlrestError::definition(format!(
                    "Reference does not target a schema: {reference}"
                )));
            }
            let encoded =
                percent_encoding::utf8_percent_encode(&pointer, FRAGMENT_ENCODING).to_string();
            Ok(format!("#{encoded}"))
        })?;
        map.remove("$anchor");
        Ok(())
    })?;
    reject_nonprogressing_cycles(&mut schema)?;
    Ok(schema)
}

pub fn target<'a>(root: &'a Value, reference: &str) -> Result<&'a Value, SqlrestError> {
    let pointer = percent_encoding::percent_decode_str(&reference[1..])
        .decode_utf8()
        .map_err(|_| SqlrestError::definition("Invalid reference encoding"))?;
    root.pointer(&pointer)
        .ok_or_else(|| SqlrestError::definition("Unresolved schema reference"))
}

fn reject_nonprogressing_cycles(root: &mut Value) -> Result<(), SqlrestError> {
    let mut graph = BTreeMap::<String, Vec<String>>::new();
    walk(root, "", &mut |map, pointer| {
        let mut edges = Vec::new();
        if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
            edges.push(
                percent_encoding::percent_decode_str(&reference[1..])
                    .decode_utf8()
                    .map_err(|_| SqlrestError::definition("Invalid reference encoding"))?
                    .into_owned(),
            );
        }
        for keyword in ["allOf", "anyOf", "oneOf"] {
            if let Some(children) = map.get(keyword).and_then(Value::as_array) {
                for i in 0..children.len() {
                    edges.push(format!("{pointer}/{keyword}/{i}"));
                }
            }
        }
        for keyword in ["not", "if", "then", "else"] {
            if map.contains_key(keyword) {
                edges.push(format!("{pointer}/{keyword}"));
            }
        }
        if let Some(children) = map.get("dependentSchemas").and_then(Value::as_object) {
            for key in children.keys() {
                edges.push(format!("{pointer}/dependentSchemas/{}", escape(key)));
            }
        }
        graph.insert(pointer.to_owned(), edges);
        Ok(())
    })?;
    let mut complete = BTreeSet::new();
    for start in graph.keys() {
        let mut stack = vec![(start.clone(), false)];
        let mut active = BTreeSet::new();
        while let Some((node, leaving)) = stack.pop() {
            if leaving {
                active.remove(&node);
                complete.insert(node);
                continue;
            }
            if complete.contains(&node) {
                continue;
            }
            if !active.insert(node.clone()) {
                return Err(SqlrestError::definition(
                    "Schema reference cycle must descend through an object property or array item",
                ));
            }
            stack.push((node.clone(), true));
            for next in graph.get(&node).into_iter().flatten() {
                stack.push((next.clone(), false));
            }
        }
    }
    Ok(())
}

pub const STRING: u8 = 1;
pub const BOOLEAN: u8 = 2;
pub const NUMBER: u8 = 4;
pub const OBJECT: u8 = 8;
pub const ARRAY: u8 = 16;
pub const NULL: u8 = 32;
const ALL: u8 = STRING | BOOLEAN | NUMBER | OBJECT | ARRAY | NULL;

/// Conservative set of possible JSON types; allOf intersects, unions combine.
pub fn types(
    root: &Value,
    node: &Value,
    active: &mut BTreeSet<String>,
) -> Result<u8, SqlrestError> {
    if node == &Value::Bool(false) {
        return Ok(0);
    }
    if node == &Value::Bool(true) {
        return Ok(ALL);
    }
    let mut result = ALL;
    if let Some(ty) = node.get("type") {
        let items = if let Some(s) = ty.as_str() {
            vec![s]
        } else {
            ty.as_array()
                .ok_or_else(|| SqlrestError::definition("Invalid schema type"))?
                .iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| SqlrestError::definition("Invalid schema type"))
                })
                .collect::<Result<_, SqlrestError>>()?
        };
        result = 0;
        for item in items {
            result |= match item {
                "string" => STRING,
                "boolean" => BOOLEAN,
                "number" | "integer" => NUMBER,
                "object" => OBJECT,
                "array" => ARRAY,
                "null" => NULL,
                _ => return Err(SqlrestError::definition("Unknown schema type")),
            };
        }
    }
    if let Some(reference) = node.get("$ref").and_then(Value::as_str)
        && active.insert(reference.to_owned())
    {
        result &= types(root, target(root, reference)?, active)?;
        active.remove(reference);
    }
    if let Some(branches) = node.get("allOf").and_then(Value::as_array) {
        for branch in branches {
            result &= types(root, branch, active)?;
        }
    }
    for keyword in ["anyOf", "oneOf"] {
        if let Some(branches) = node.get(keyword).and_then(Value::as_array) {
            let mut union = 0;
            for branch in branches {
                union |= types(root, branch, active)?;
            }
            result &= union;
        }
    }
    Ok(result)
}

pub fn properties(root: &Value) -> Result<BTreeMap<String, Value>, SqlrestError> {
    fn merge(target: &mut BTreeMap<String, Value>, source: BTreeMap<String, Value>) {
        for (key, value) in source {
            if let Some(previous) = target.remove(&key) {
                target.insert(key, json!({"allOf":[previous,value]}));
            } else {
                target.insert(key, value);
            }
        }
    }

    fn collect(
        root: &Value,
        node: &Value,
        active: &mut BTreeSet<String>,
    ) -> Result<BTreeMap<String, Value>, SqlrestError> {
        let mut fields: BTreeMap<_, _> = node
            .get("properties")
            .and_then(Value::as_object)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        if let Some(reference) = node.get("$ref").and_then(Value::as_str)
            && active.insert(reference.into())
        {
            merge(
                &mut fields,
                collect(root, target(root, reference)?, active)?,
            );
            active.remove(reference);
        }
        if let Some(branches) = node.get("allOf").and_then(Value::as_array) {
            for branch in branches {
                merge(&mut fields, collect(root, branch, active)?);
            }
        }
        for keyword in ["anyOf", "oneOf"] {
            if let Some(branches) = node.get(keyword).and_then(Value::as_array) {
                let mut alternatives = Vec::new();
                for branch in branches {
                    alternatives.push(collect(root, branch, active)?);
                }
                if let Some(first) = alternatives.first() {
                    let expected: BTreeSet<_> = fields.keys().chain(first.keys()).collect();
                    if alternatives
                        .iter()
                        .any(|a| fields.keys().chain(a.keys()).collect::<BTreeSet<_>>() != expected)
                    {
                        return Err(SqlrestError::definition(
                            "Record schema unions must have a fixed set of column names",
                        ));
                    }
                    let keys: BTreeSet<_> = alternatives
                        .iter()
                        .flat_map(|a| a.keys().cloned())
                        .collect();
                    let mut union = BTreeMap::new();
                    for key in keys {
                        let branches: Vec<_> = alternatives
                            .iter()
                            .map(|a| a.get(&key).cloned().unwrap_or(Value::Bool(true)))
                            .collect();
                        union.insert(key, json!({"anyOf":branches}));
                    }
                    merge(&mut fields, union);
                }
            }
        }
        Ok(fields)
    }
    if types(root, root, &mut BTreeSet::new())? != OBJECT {
        return Err(SqlrestError::definition(
            "Record schema must explicitly describe objects",
        ));
    }
    collect(root, root, &mut BTreeSet::new())
}
