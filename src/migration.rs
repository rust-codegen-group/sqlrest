//! Forward-only, one-file-per-transaction migrations and recoverable SQL history.
#![doc = include_str!("../docs/migrations.md")]
use crate::{
    SqlrestError,
    execution::{Executor, Limits},
    loader::{Endpoint, Snapshot},
    params::Input,
    sql::{self, Backend},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlparser::tokenizer::{Token, Tokenizer};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

const TABLE: &str = "__sqlrest_migrations";
const RECORD: &str = r#"{"type":"object","properties":{"version":{"type":"integer"},"filename":{"type":"string"},"source":{"type":"string"},"checksum":{"type":"string"}},"required":["version","filename","source","checksum"],"additionalProperties":false}"#;
const COUNT: &str = r#"{"type":"object","properties":{"total":{"type":"integer"}},"required":["total"],"additionalProperties":false}"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: i64,
    pub filename: String,
    pub source: String,
    pub checksum: String,
}

pub(crate) struct File {
    pub record: AppliedMigration,
    endpoint: Arc<Endpoint>,
    input: Input,
}

pub(crate) struct Plan {
    files: BTreeMap<i64, File>,
}

#[derive(Clone)]
pub(crate) struct Migrator {
    pub executor: Executor,
    pub backend: Backend,
    pub timeout: Duration,
}

impl Plan {
    /// Exactly one read of each file; all later steps use these owned bytes.
    pub fn load(root: &Path, backend: Backend) -> Result<Self, SqlrestError> {
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(root).map_err(|_| invalid("Cannot read migration directory"))? {
            let entry = entry.map_err(|_| invalid("Cannot read migration entry"))?;
            if !entry
                .file_type()
                .map_err(|_| invalid("Cannot inspect migration entry"))?
                .is_file()
            {
                return Err(invalid(
                    "Migrations must be regular files, not directories or symlinks",
                ));
            }
            let filename = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("Migration filename must be UTF-8"))?;
            let version = version(&filename)?;
            let source = fs::read_to_string(entry.path())
                .map_err(|_| invalid("Cannot read migration SQL as UTF-8"))?;
            let statements = sql::compile(&source, backend)
                .map_err(|_| invalid("Unsupported or invalid transactional migration SQL"))?;
            if statements.iter().any(|s| !s.parameters.is_empty()) {
                return Err(invalid("Migration SQL cannot contain request parameters"));
            }
            // Reserve the metadata namespace; SQL is still trusted configuration,
            // not a sandbox for functions with external/dynamic side effects.
            let dialect = backend.dialect();
            if Tokenizer::new(dialect.as_ref(), &source).tokenize()
                .map_err(|_| invalid("Invalid migration SQL"))?
                .iter().any(|t| matches!(t, Token::Word(w) if w.value.to_ascii_lowercase().starts_with("__sqlrest_")))
            {
                return Err(invalid("Migration SQL cannot reference reserved metadata identifiers"));
            }
            let record = AppliedMigration {
                version,
                filename,
                checksum: checksum(&source),
                source,
            };
            let table = table(backend);
            // The original SQL is preserved. A newline before the separator
            // prevents a trailing line comment from swallowing the history insert.
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {table} (version BIGINT NOT NULL PRIMARY KEY, filename TEXT NOT NULL UNIQUE, source TEXT NOT NULL, checksum TEXT NOT NULL);\n{}\n;\nINSERT INTO {table}(version,filename,source,checksum) VALUES(${{body.version:int64}},${{body.filename:string}},${{body.source:string}},${{body.checksum:string}})",
                record.source,
            );
            let endpoint = endpoint("post", &sql, None, backend)?;
            let body = serde_json::to_vec(&json!(&record))
                .map_err(|_| invalid("Cannot encode migration record"))?;
            let input = Input::from_http("", &body)?;
            if files
                .insert(
                    version,
                    File {
                        record,
                        endpoint,
                        input,
                    },
                )
                .is_some()
            {
                return Err(invalid("Duplicate numeric migration version"));
            }
        }
        Ok(Self { files })
    }

    pub fn validate(&self, history: &[AppliedMigration]) -> Result<(), SqlrestError> {
        let highest = history.last().map_or(0, |r| r.version);
        let versions: BTreeSet<_> = history.iter().map(|r| r.version).collect();
        for record in history {
            if self.files.get(&record.version).map(|f| &f.record) != Some(record) {
                return Err(SqlrestError::new(
                    409,
                    "migration_history_mismatch",
                    "An applied migration is missing or changed; export and restore its original file",
                ));
            }
        }
        for file in self.files.values() {
            if versions.contains(&file.record.version) {
                continue;
            }
            if file.record.version <= highest {
                return Err(invalid(
                    "New migrations must have versions above all applied versions",
                ));
            }
        }
        Ok(())
    }

    pub fn into_pending(self, history: &[AppliedMigration]) -> Result<Vec<File>, SqlrestError> {
        self.validate(history)?;
        let highest = history.last().map_or(0, |r| r.version);
        Ok(self
            .files
            .into_values()
            .filter(|file| file.record.version > highest)
            .collect())
    }
}

impl Migrator {
    pub async fn history(&self) -> Result<Vec<AppliedMigration>, SqlrestError> {
        let exists = match self.backend {
            Backend::Turso => {
                "SELECT count(*) AS total FROM main.sqlite_schema WHERE name='__sqlrest_migrations'"
            }
            Backend::Postgres => {
                "SELECT count(*) AS total FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='__sqlrest_migrations'"
            }
        };
        let bytes = self
            .executor
            .execute(
                endpoint("get", exists, Some(COUNT), self.backend)?,
                Input::default(),
                self.limits(),
            )
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| corrupt())?;
        if value["records"][0]["total"] == 0 {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT version,filename,source,checksum FROM {} ORDER BY version",
            table(self.backend)
        );
        let bytes = self
            .executor
            .execute(
                endpoint("get", &sql, Some(RECORD), self.backend)?,
                Input::default(),
                self.limits(),
            )
            .await?;
        tokio::task::spawn_blocking(move || decode_history(&bytes))
            .await
            .map_err(|_| corrupt())?
    }

    pub async fn apply(&self, file: File) -> Result<(), SqlrestError> {
        self.executor
            .execute(
                file.endpoint,
                file.input,
                Limits {
                    timeout: self.timeout,
                    max_rows: 0,
                },
            )
            .await?;
        Ok(())
    }

    fn limits(&self) -> Limits {
        // Business response limits must not silently truncate durable history.
        Limits {
            timeout: self.timeout,
            max_rows: usize::MAX,
        }
    }
}

fn decode_history(bytes: &[u8]) -> Result<Vec<AppliedMigration>, SqlrestError> {
    #[derive(Deserialize)]
    struct Response {
        records: Vec<AppliedMigration>,
    }
    let records = serde_json::from_slice::<Response>(bytes)
        .map_err(|_| corrupt())?
        .records;
    let mut previous = 0;
    for record in &records {
        if version(&record.filename).ok() != Some(record.version)
            || record.version <= previous
            || record.checksum != checksum(&record.source)
        {
            return Err(corrupt());
        }
        previous = record.version;
    }
    Ok(records)
}

fn endpoint(
    method: &str,
    sql: &str,
    schema: Option<&str>,
    backend: Backend,
) -> Result<Arc<Endpoint>, SqlrestError> {
    let mut files = BTreeMap::from([(format!("{method}.sql"), sql.to_owned())]);
    if let Some(schema) = schema {
        files.insert(format!("{method}.response.yaml"), schema.to_owned());
    }
    Ok(Snapshot::from_files(files, backend)?.endpoints()[0].clone())
}

fn table(backend: Backend) -> String {
    let schema = match backend {
        Backend::Turso => "main",
        Backend::Postgres => "public",
    };
    format!("{schema}.\"{TABLE}\"")
}

fn version(filename: &str) -> Result<i64, SqlrestError> {
    let (number, name) = filename
        .strip_suffix(".sql")
        .and_then(|s| s.split_once('_'))
        .ok_or_else(|| invalid("Expected a migration filename such as 0001_create_todos.sql"))?;
    if number.is_empty()
        || !number.bytes().all(|b| b.is_ascii_digit())
        || name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(invalid("Invalid migration filename"));
    }
    number
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("Migration version must be a positive int64"))
}

fn checksum(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn invalid(message: &str) -> SqlrestError {
    SqlrestError::new(400, "invalid_migration", message)
}

fn corrupt() -> SqlrestError {
    SqlrestError::new(
        500,
        "migration_history_corrupt",
        "Migration history cannot be trusted",
    )
}
