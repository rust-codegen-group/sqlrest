use crate::SqlrestError;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// Driver values before schema-directed decoding. Binary data must be explicitly
/// encoded by SQL; text is never guessed to be JSON without a structural schema.
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(String),
    Json(Value),
}

pub struct Contract {
    schema: Value,
    validator: jsonschema::Validator,
}

impl Contract {
    pub fn from_yaml(source: &str) -> Result<Self, SqlrestError> {
        let schema = serde_norway::from_str(source)
            .map_err(|e| SqlrestError::definition(format!("Invalid response schema: {e}")))?;
        Self::new(schema)
    }

    pub fn new(schema: Value) -> Result<Self, SqlrestError> {
        check_references(&schema)?;
        let root = resolve(&schema, &schema)?;
        if root.get("type").and_then(Value::as_str) != Some("object")
            || !root.get("properties").is_some_and(Value::is_object)
        {
            return Err(SqlrestError::definition(
                "Record schema needs explicit object properties",
            ));
        }
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
            .map_err(|e| SqlrestError::definition(format!("Invalid response schema: {e}")))?;
        Ok(Self { schema, validator })
    }

    pub fn schema(&self) -> &Value {
        &self.schema
    }

    pub fn check_columns(&self, names: &[String]) -> Result<(), SqlrestError> {
        let properties = resolve(&self.schema, &self.schema)?["properties"]
            .as_object()
            .unwrap();
        let unique: BTreeSet<_> = names.iter().collect();
        if unique.len() != names.len() || unique != properties.keys().collect() {
            return Err(SqlrestError::contract(
                "Result columns must exactly match record properties",
            ));
        }
        Ok(())
    }

    pub fn record(
        &self,
        names: &[String],
        cells: Vec<Cell>,
        turso: bool,
    ) -> Result<Value, SqlrestError> {
        self.check_columns(names)?;
        if cells.len() != names.len() {
            return Err(SqlrestError::contract("Invalid result width"));
        }
        let properties = resolve(&self.schema, &self.schema)?["properties"]
            .as_object()
            .unwrap();
        let mut record = Map::new();
        for (name, cell) in names.iter().zip(cells) {
            let field = resolve(&self.schema, &properties[name])?;
            let structural = has_type(field, "object") || has_type(field, "array");
            let boolean = has_type(field, "boolean");
            let value = match cell {
                Cell::Null => Value::Null,
                Cell::Boolean(v) => Value::Bool(v),
                Cell::Integer(v) if turso && boolean => match v {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => return Err(SqlrestError::contract("Boolean column must contain 0 or 1")),
                },
                Cell::Integer(v) => v.into(),
                Cell::Real(v) => serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .ok_or_else(|| SqlrestError::contract("Non-finite result number"))?,
                Cell::Text(v) if structural => serde_json::from_str(&v)
                    .map_err(|_| SqlrestError::contract("Invalid JSON result column"))?,
                Cell::Text(v) => Value::String(v),
                Cell::Json(v) => v,
            };
            record.insert(name.clone(), value);
        }
        let record = Value::Object(record);
        if !self.validator.is_valid(&record) {
            return Err(SqlrestError::contract(
                "Result does not satisfy response schema",
            ));
        }
        Ok(record)
    }
}

fn has_type(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(s)) => s == expected,
        Some(Value::Array(items)) => items.iter().any(|v| v.as_str() == Some(expected)),
        _ => false,
    }
}

fn resolve<'a>(root: &'a Value, mut node: &'a Value) -> Result<&'a Value, SqlrestError> {
    let mut seen = BTreeSet::new();
    while let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        if !seen.insert(reference) {
            return Err(SqlrestError::definition(
                "Cyclic schema alias without a concrete type",
            ));
        }
        let pointer = reference
            .strip_prefix('#')
            .ok_or_else(|| SqlrestError::definition("External schema reference"))?;
        node = root
            .pointer(pointer)
            .ok_or_else(|| SqlrestError::definition("Unresolved local schema reference"))?;
    }
    Ok(node)
}

fn check_references(value: &Value) -> Result<(), SqlrestError> {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                // Resource identifiers change reference scope; do not let the
                // validator retrieve resources outside this self-contained file.
                if key == "$id" || key == "$dynamicRef" {
                    return Err(SqlrestError::definition(
                        "Schema resource IDs and dynamic references are not supported",
                    ));
                }
                if key == "$ref"
                    && !value
                        .as_str()
                        .is_some_and(|s| s == "#" || s.starts_with("#/"))
                {
                    return Err(SqlrestError::definition(
                        "Only local JSON Pointer schema references are supported",
                    ));
                }
                check_references(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                check_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn columns_are_checked_even_without_rows() {
        let contract =
            Contract::new(json!({"type":"object","properties":{"id":{"type":"integer"}}})).unwrap();
        assert!(contract.check_columns(&["id".into()]).is_ok());
        for names in [vec![], vec!["other".into()], vec!["id".into(), "id".into()]] {
            assert!(contract.check_columns(&names).is_err());
        }
    }

    #[test]
    fn recursive_json_and_explicit_bool() {
        let contract = Contract::new(json!({
            "type":"object", "properties":{
                "done":{"type":"boolean"}, "tree":{"$ref":"#/$defs/node"}
            }, "required":["done","tree"],
            "$defs":{"node":{"type":"object","properties":{
                "children":{"type":"array","items":{"$ref":"#/$defs/node"}}
            },"required":["children"],"additionalProperties":false}}
        }))
        .unwrap();
        let names = vec!["tree".into(), "done".into()];
        let cells = vec![
            Cell::Text(r#"{"children":[{"children":[]}]}"#.into()),
            Cell::Integer(1),
        ];
        assert!(contract.record(&names, cells.clone(), true).is_ok());
        assert!(contract.record(&names, cells, false).is_err());
        assert!(Contract::new(json!({"$ref":"https://example.com/schema"})).is_err());
    }
}
