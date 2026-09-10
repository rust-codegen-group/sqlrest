use crate::SqlrestError;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    String,
    Boolean,
    Int64,
    Float64,
    Array(Box<Type>),
    Nullable(Box<Type>),
}

impl Type {
    pub fn parse(s: &str) -> Result<Self, SqlrestError> {
        Ok(match s {
            "string" => Self::String,
            "boolean" => Self::Boolean,
            "int64" => Self::Int64,
            "float64" => Self::Float64,
            _ if s.starts_with("array<") && s.ends_with('>') => {
                let inner = Self::parse(&s[6..s.len() - 1])?;
                if !inner.scalar() {
                    return Err(SqlrestError::definition(
                        "array elements must be non-null scalars",
                    ));
                }
                Self::Array(Box::new(inner))
            }
            _ if s.starts_with("nullable<") && s.ends_with('>') => {
                let inner = Self::parse(&s[9..s.len() - 1])?;
                if matches!(inner, Self::Nullable(_)) {
                    return Err(SqlrestError::definition("nested nullable is not supported"));
                }
                Self::Nullable(Box::new(inner))
            }
            _ => {
                return Err(SqlrestError::definition(format!(
                    "unknown parameter type: {s}"
                )));
            }
        })
    }

    fn scalar(&self) -> bool {
        matches!(
            self,
            Self::String | Self::Boolean | Self::Int64 | Self::Float64
        )
    }

    pub fn schema(&self) -> Value {
        match self {
            Self::String => json!({"type":"string"}),
            Self::Boolean => json!({"type":"boolean"}),
            Self::Int64 => {
                json!({"type":"integer","format":"int64","minimum":i64::MIN,"maximum":i64::MAX})
            }
            Self::Float64 => json!({"type":"number","format":"double"}),
            Self::Array(t) => json!({"type":"array","items":t.schema()}),
            Self::Nullable(t) => json!({"anyOf":[t.schema(),{"type":"null"}]}),
        }
    }

    fn check(&self, value: &Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Boolean => value.is_boolean(),
            Self::Int64 => value.as_i64().is_some(),
            Self::Float64 => value.as_f64().is_some_and(f64::is_finite),
            Self::Array(t) => value
                .as_array()
                .is_some_and(|a| a.iter().all(|v| t.check(v))),
            Self::Nullable(t) => value.is_null() || t.check(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    pub source: String,
    pub fields: Vec<String>,
    pub ty: Type,
}

impl Parameter {
    pub fn parse(s: &str) -> Result<Self, SqlrestError> {
        let (path, ty) = s
            .split_once(':')
            .ok_or_else(|| SqlrestError::definition("parameter type is required"))?;
        let mut parts = path.split('.');
        let source = parts.next().unwrap_or_default().to_string();
        let fields: Vec<_> = parts.map(str::to_owned).collect();
        let ty = Type::parse(ty)?;
        if !matches!(source.as_str(), "path" | "query" | "body")
            || fields.is_empty()
            || fields.iter().any(|s| !identifier(s))
        {
            return Err(SqlrestError::definition(format!(
                "invalid parameter path: {path}"
            )));
        }
        if source != "body" && (fields.len() != 1 || !ty.scalar()) {
            return Err(SqlrestError::definition(
                "path/query require one scalar field",
            ));
        }
        Ok(Self { source, fields, ty })
    }

    pub fn path(&self) -> String {
        format!("{}.{}", self.source, self.fields.join("."))
    }

    pub fn read(&self, request: &Input) -> Result<Value, SqlrestError> {
        let path = self.path();
        let missing =
            || SqlrestError::parameter("parameter_missing", &path, "Required parameter is missing");
        let mismatch = || {
            SqlrestError::parameter(
                "parameter_type_mismatch",
                &path,
                "Parameter does not match its declared type",
            )
        };
        if self.source == "body" {
            let mut value = &request.body;
            for field in &self.fields {
                let object = value.as_object().ok_or_else(mismatch)?;
                value = object.get(field).ok_or_else(missing)?;
            }
            return if self.ty.check(value) {
                Ok(value.clone())
            } else {
                Err(mismatch())
            };
        }
        let map = if self.source == "path" {
            &request.path
        } else {
            &request.query
        };
        let text = map.get(&self.fields[0]).ok_or_else(missing)?;
        let value = match self.ty {
            Type::String => json!(text),
            Type::Boolean => match text.as_str() {
                "true" => json!(true),
                "false" => json!(false),
                _ => return Err(mismatch()),
            },
            Type::Int64 => {
                let digits = text.strip_prefix('-').unwrap_or(text);
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(mismatch());
                }
                json!(text.parse::<i64>().map_err(|_| mismatch())?)
            }
            Type::Float64 => {
                // Use JSON number grammar: no whitespace, NaN, infinity or leading '+'.
                if text.trim() != text {
                    return Err(mismatch());
                }
                let v: Value = serde_json::from_str(text).map_err(|_| mismatch())?;
                if !v.is_number() || !self.ty.check(&v) {
                    return Err(mismatch());
                }
                v
            }
            _ => unreachable!(),
        };
        Ok(value)
    }
}

pub fn identifier(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Debug, Default)]
pub struct Input {
    pub path: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub body: Value,
}

impl Input {
    pub fn from_http(query: &str, body: &[u8]) -> Result<Self, SqlrestError> {
        // Reject malformed percent encoding before the form parser can replace
        // invalid UTF-8 with U+FFFD or preserve an invalid '%' literally.
        let bytes = query.as_bytes();
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && (i + 2 >= bytes.len()
                    || !bytes[i + 1].is_ascii_hexdigit()
                    || !bytes[i + 2].is_ascii_hexdigit())
            {
                return Err(SqlrestError::new(
                    400,
                    "invalid_query",
                    "Invalid query percent encoding",
                ));
            }
        }
        percent_encoding::percent_decode_str(query)
            .decode_utf8()
            .map_err(|_| SqlrestError::new(400, "invalid_query", "Query is not valid UTF-8"))?;
        let mut fields = BTreeMap::new();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if fields
                .insert(key.into_owned(), value.into_owned())
                .is_some()
            {
                return Err(SqlrestError::new(
                    400,
                    "duplicate_parameter",
                    "Duplicate query parameter",
                ));
            }
        }
        let body = if body.is_empty() {
            json!({})
        } else {
            serde_json::from_slice::<UniqueJson>(body)
                .map_err(|_| {
                    SqlrestError::new(400, "invalid_json", "Invalid JSON or duplicate object key")
                })?
                .0
        };
        Ok(Self {
            path: BTreeMap::new(),
            query: fields,
            body,
        })
    }
}

struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = UniqueJson;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON with unique object keys")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(json!(v)))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(json!(v)))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(json!(v)))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(json!(v)))
            }

            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(v) = a.next_element::<UniqueJson>()? {
                    items.push(v.0);
                }
                Ok(UniqueJson(Value::Array(items)))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut obj = serde_json::Map::new();
                while let Some((k, v)) = a.next_entry::<String, UniqueJson>()? {
                    if obj.insert(k, v.0).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(UniqueJson(Value::Object(obj)))
            }
        }
        d.deserialize_any(V)
    }
}

pub fn validate_parameters(params: &[Parameter]) -> Result<(), SqlrestError> {
    for (i, a) in params.iter().enumerate() {
        for b in &params[..i] {
            if a.source != b.source {
                continue;
            }
            if a.fields == b.fields && a.ty != b.ty {
                return Err(SqlrestError::definition("conflicting parameter types"));
            }
            if a.fields != b.fields
                && (a.fields.starts_with(&b.fields) || b.fields.starts_with(&a.fields))
            {
                return Err(SqlrestError::definition(
                    "parameter is both a value and an object parent",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicates_rejected() {
        assert!(Input::from_http("a=1&a=2", b"{}").is_err());
        assert!(Input::from_http("", br#"{"nested":{"a":1,"a":2}}"#).is_err());
    }

    #[test]
    fn typed_paths() {
        let input = Input::from_http(
            "id=9223372036854775807",
            br#"{"input":{"done":true},"ids":[1,2],"note":null}"#,
        )
        .unwrap();
        assert_eq!(
            Parameter::parse("query.id:int64")
                .unwrap()
                .read(&input)
                .unwrap(),
            json!(i64::MAX)
        );
        for spec in [
            "body.input.done:boolean",
            "body.ids:array<int64>",
            "body.note:nullable<string>",
        ] {
            assert!(Parameter::parse(spec).unwrap().read(&input).is_ok());
        }
        assert!(
            Parameter::parse("body.note:string")
                .unwrap()
                .read(&input)
                .is_err()
        );
        assert!(
            Parameter::parse("body.input.done:string")
                .unwrap()
                .read(&input)
                .is_err()
        );
        assert!(
            Parameter::parse("body.absent:nullable<string>")
                .unwrap()
                .read(&input)
                .is_err()
        );
    }

    #[test]
    fn strict_arrays_and_conflicts() {
        let input = Input::from_http("", br#"{"ids":[1,"2"]}"#).unwrap();
        assert!(
            Parameter::parse("body.ids:array<int64>")
                .unwrap()
                .read(&input)
                .is_err()
        );
        for spec in [
            "body.ids:array<array<int64>>",
            "body.ids:array<nullable<int64>>",
            "query.ids:array<int64>",
            "path.id",
        ] {
            assert!(Parameter::parse(spec).is_err());
        }
        assert!(
            validate_parameters(&[
                Parameter::parse("body.a:string").unwrap(),
                Parameter::parse("body.a.b:string").unwrap()
            ])
            .is_err()
        );
    }
}
