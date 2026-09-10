use crate::{SqlrestError, schema};
use heck::ToUpperCamelCase;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

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
    fields: BTreeMap<String, Decode>,
}

#[derive(Clone, Copy)]
struct Decode {
    json: bool,
    boolean: bool,
}

impl Contract {
    pub fn from_yaml(source: &str) -> Result<Self, SqlrestError> {
        let yaml: serde_norway::Value = serde_norway::from_str(source)
            .map_err(|e| SqlrestError::definition(format!("Invalid response schema: {e}")))?;
        let schema = serde_json::to_value(yaml).map_err(|e| {
            SqlrestError::definition(format!(
                "Response schema must use JSON-compatible values: {e}"
            ))
        })?;
        Self::new(schema)
    }

    pub fn new(schema: Value) -> Result<Self, SqlrestError> {
        let schema = schema::normalize(schema)?;
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
            .map_err(|e| SqlrestError::definition(format!("Invalid response schema: {e}")))?;
        let mut fields = BTreeMap::new();
        for (name, definition) in schema::properties(&schema)? {
            let types = schema::types(&schema, &definition, &mut BTreeSet::new())?;
            let structural = types & (schema::OBJECT | schema::ARRAY) != 0;
            let boolean = types & schema::BOOLEAN != 0;
            if structural && types & schema::STRING != 0 || boolean && types & schema::NUMBER != 0 {
                return Err(SqlrestError::definition(format!(
                    "Ambiguous database decoding for field {name}; string/JSON and number/boolean unions require an unambiguous contract"
                )));
            }
            fields.insert(
                name,
                Decode {
                    json: structural,
                    boolean,
                },
            );
        }
        Ok(Self {
            schema,
            validator,
            fields,
        })
    }

    pub fn schema(&self) -> &Value {
        &self.schema
    }

    pub fn relocated(&self, base: &str) -> Result<Value, SqlrestError> {
        let mut schema = self.schema.clone();
        schema::walk(&mut schema, "", &mut |map, _| {
            schema::rewrite_refs(map, &mut |reference| {
                Ok(format!("{base}{}", &reference[1..]))
            })
        })?;
        Ok(schema)
    }

    /// Lift referenced nodes to named components without expanding recursive
    /// graphs. This also works for consumers that only resolve named models.
    pub fn openapi_components(&self, name: &str) -> Result<BTreeMap<String, Value>, SqlrestError> {
        let mut targets = BTreeSet::from(["#".to_owned()]);
        let mut schema = self.schema.clone();
        schema::walk(&mut schema, "", &mut |map, _| {
            schema::rewrite_refs(map, &mut |reference| {
                targets.insert(reference.to_owned());
                Ok(reference.to_owned())
            })
        })?;
        let mut names = BTreeMap::new();
        let mut used = BTreeSet::new();
        for target in &targets {
            let pointer = percent_encoding::percent_decode_str(&target[1..])
                .decode_utf8()
                .map_err(|_| SqlrestError::definition("Invalid reference encoding"))?;
            let suffix = pointer.to_upper_camel_case();
            let generated = format!("{name}{suffix}");
            if !generated.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return Err(SqlrestError::definition(
                    "Referenced schema nodes need names that normalize to ASCII component identifiers",
                ));
            }
            if !used.insert(generated.clone()) {
                return Err(SqlrestError::definition(format!(
                    "Generated schema name collision: {generated}"
                )));
            }
            names.insert(target.clone(), generated);
        }
        let mut components = BTreeMap::new();
        for target in targets {
            let pointer = percent_encoding::percent_decode_str(&target[1..])
                .decode_utf8()
                .map_err(|_| SqlrestError::definition("Invalid reference encoding"))?;
            let mut node = self.schema.pointer(&pointer).unwrap().clone();
            schema::walk(&mut node, "", &mut |map, _| {
                schema::rewrite_refs(map, &mut |reference| {
                    let generated = &names[reference];
                    Ok(format!("#/components/schemas/{generated}"))
                })
            })?;
            components.insert(names[&target].clone(), node);
        }
        Ok(components)
    }

    pub fn check_columns(&self, names: &[String]) -> Result<(), SqlrestError> {
        let unique: BTreeSet<_> = names.iter().collect();
        if unique.len() != names.len() || unique != self.fields.keys().collect() {
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
        let mut record = Map::new();
        for (name, cell) in names.iter().zip(cells) {
            let structural = self.fields[name].json;
            let boolean = self.fields[name].boolean;
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
