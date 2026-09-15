//! Durable fixed-layout workspace configuration.
use crate::{SqlrestError, execution::Limits};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DatabaseConfig {
    Turso {},
    PostgresUnencrypted { connection: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLimits {
    #[serde(default = "request_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default = "max_rows")]
    pub max_rows: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            request_timeout_ms: request_timeout(),
            max_rows: max_rows(),
        }
    }
}

impl RequestLimits {
    pub(crate) fn validate(self) -> Result<Limits, SqlrestError> {
        let timeout = duration(self.request_timeout_ms)?;
        if self.max_rows == 0
            || self.max_rows as u128 > i64::MAX as u128
            || self.request_timeout_ms > i64::MAX as u64
        {
            return Err(invalid(
                "Limits must be positive integers representable in TOML",
            ));
        }
        Ok(Limits {
            timeout,
            max_rows: self.max_rows,
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishRequest {
    #[serde(default, deserialize_with = "database_present")]
    pub database: Option<DatabaseConfig>,
    #[serde(default)]
    pub limits: RequestLimits,
    #[serde(default = "migration_timeout")]
    pub migration_timeout_ms: u64,
}

impl Default for PublishRequest {
    fn default() -> Self {
        Self {
            database: None,
            limits: RequestLimits::default(),
            migration_timeout_ms: migration_timeout(),
        }
    }
}

impl PublishRequest {
    pub(crate) fn validate(&self) -> Result<(), SqlrestError> {
        self.limits.validate()?;
        duration(self.migration_timeout_ms)?;
        if let Some(database) = &self.database {
            validate_database(database)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Recovery {
    None,
    Migration,
    Reload,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistentState {
    pub recovery: Recovery,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub database: DatabaseConfig,
    pub state: PersistentState,
    #[serde(deserialize_with = "persisted_limits")]
    pub limits: RequestLimits,
}

impl Record {
    pub fn validate(&self) -> Result<(), SqlrestError> {
        validate_database(&self.database)?;
        self.limits.validate()?;
        Ok(())
    }
}

pub(crate) struct Workspace {
    root: PathBuf,
    // Kept until all registry clones, workers and requests have released ownership.
    _lock: File,
    #[cfg(test)]
    pub(crate) persist_fault: std::sync::Mutex<Option<PersistFault>>,
    #[cfg(test)]
    pub(crate) remove_fault: std::sync::Mutex<Option<RemoveFault>>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistFault {
    BeforeReplace,
    AfterReplace,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoveFault {
    BeforeRemove,
    AfterRemove,
}

impl Workspace {
    pub fn open(root: &Path) -> Result<Self, SqlrestError> {
        let root = std::path::absolute(root).map_err(io_error)?;
        let mut missing = Vec::new();
        let mut ancestor = root.as_path();
        while !ancestor.try_exists().map_err(io_error)? {
            missing.push(ancestor.to_owned());
            ancestor = ancestor
                .parent()
                .ok_or_else(|| invalid("Invalid workspace root"))?;
        }
        fs::create_dir_all(&root).map_err(io_error)?;
        for path in missing.iter().rev() {
            sync_directory(path)?;
            sync_directory(path.parent().unwrap())?;
        }
        let root = fs::canonicalize(root).map_err(io_error)?;
        let lock_path = root.join(".sqlrest.lock");
        reject_symlink(&lock_path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(io_error)?;
        lock.try_lock().map_err(|error| {
            SqlrestError::new(
                409,
                "workspace_locked",
                "Workspace is already owned or cannot be locked",
            )
            .with_diagnostic("workspace", error)
        })?;
        directory(&root.join("databases"))?;
        sync_directory(&root)?;
        Ok(Self {
            root,
            _lock: lock,
            #[cfg(test)]
            persist_fault: std::sync::Mutex::new(None),
            #[cfg(test)]
            remove_fault: std::sync::Mutex::new(None),
        })
    }

    pub fn directory(&self, name: &str) -> PathBuf {
        self.root.join("databases").join(name)
    }

    pub fn prepare(&self, name: &str) -> Result<(), SqlrestError> {
        validate_name(name)?;
        let root = self.directory(name);
        directory(&root)?;
        directory(&root.join("interfaces"))?;
        directory(&root.join("migrations"))?;
        reject_symlink(&root.join("data.db"))?;
        reject_symlink(&root.join("database.toml"))?;
        sync_directory(&root)?;
        sync_directory(&self.root.join("databases"))
    }

    /// Validate managed paths without creating directories during recovery.
    pub fn validate_layout(&self, name: &str) -> Result<(), SqlrestError> {
        validate_name(name)?;
        let root = self.directory(name);
        reject_symlink(&root)?;
        for child in ["interfaces", "migrations", "data.db", "database.toml"] {
            reject_symlink(&root.join(child))?;
        }
        Ok(())
    }

    pub fn names(&self) -> Result<Vec<String>, SqlrestError> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.root.join("databases")).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let kind = entry.file_type().map_err(io_error)?;
            if !kind.is_dir() && !kind.is_symlink() {
                continue;
            }
            let config = entry.path().join("database.toml");
            match fs::symlink_metadata(config) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                // The individual directory may be unreadable. Keep its name so
                // read() can attach the failure without suppressing healthy DBs.
                _ => {}
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("Invalid database directory name"))?;
            names.push(name);
        }
        names.sort();
        Ok(names)
    }

    pub fn read(&self, name: &str) -> Result<Record, SqlrestError> {
        validate_name(name)?;
        let root = self.directory(name);
        reject_symlink(&root)?;
        let path = root.join("database.toml");
        reject_symlink(&path)?;
        let contents = fs::read_to_string(path).map_err(io_error)?;
        let record: Record = toml::from_str(&contents).map_err(|error| {
            // TOML errors can quote credentials; never expose the source.
            invalid("Cannot parse database.toml").with_diagnostic("toml", error)
        })?;
        record.validate()?;
        Ok(record)
    }

    pub fn require_data(&self, name: &str) -> Result<(), SqlrestError> {
        let path = self.directory(name).join("data.db");
        reject_symlink(&path)?;
        if !fs::metadata(&path)
            .map_err(|error| {
                SqlrestError::new(
                    503,
                    "database_file_missing",
                    "Registered database file is unavailable",
                )
                .with_diagnostic("workspace", error)
            })?
            .is_file()
        {
            return Err(invalid("data.db must be a regular file"));
        }
        Ok(())
    }

    pub fn persist(&self, name: &str, record: &Record) -> Result<(), SqlrestError> {
        #[cfg(test)]
        let fault = self.persist_fault.lock().unwrap().take();
        record.validate()?;
        let root = self.directory(name);
        reject_symlink(&root)?;
        let target = root.join("database.toml");
        reject_symlink(&target)?;
        let bytes = toml::to_string_pretty(record)
            .map_err(|_| invalid("Cannot serialize configuration"))?;
        let temporary = root.join(format!(".database-{}.tmp", uuid::Uuid::new_v4().simple()));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary).map_err(io_error)?;
            file.write_all(bytes.as_bytes()).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            #[cfg(test)]
            if fault == Some(PersistFault::BeforeReplace) {
                return Err(io_error(std::io::Error::other(
                    "injected pre-replacement failure",
                )));
            }
            fs::rename(&temporary, &target).map_err(io_error)?;
            #[cfg(test)]
            if fault == Some(PersistFault::AfterReplace) {
                return Err(uncertain(io_error(std::io::Error::other(
                    "injected directory sync failure",
                ))));
            }
            sync_directory(&root).map_err(uncertain)
        })();
        // This is our uniquely named temporary file, never a user database.
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub fn remove(&self, name: &str) -> Result<(), SqlrestError> {
        #[cfg(test)]
        let fault = self.remove_fault.lock().unwrap().take();
        let root = self.directory(name);
        reject_symlink(&root)?;
        #[cfg(test)]
        if fault == Some(RemoveFault::BeforeRemove) {
            return Err(io_error(std::io::Error::other(
                "injected configuration removal failure",
            )));
        }
        match fs::remove_file(root.join("database.toml")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        #[cfg(test)]
        if fault == Some(RemoveFault::AfterRemove) {
            return Err(io_error(std::io::Error::other(
                "injected removal directory sync failure",
            )));
        }
        sync_directory(&root)
    }

    pub fn sync_data(&self, name: &str) -> Result<(), SqlrestError> {
        File::open(self.directory(name).join("data.db"))
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
        sync_directory(&self.directory(name))
    }
}

pub(crate) fn validate_name(name: &str) -> Result<(), SqlrestError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
    {
        return Err(invalid("Invalid database name"));
    }
    Ok(())
}

pub(crate) fn duration(ms: u64) -> Result<Duration, SqlrestError> {
    let duration = Duration::from_millis(ms);
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        return Err(invalid(
            "Timeout must be a positive finite number of milliseconds",
        ));
    }
    Ok(duration)
}

fn validate_database(database: &DatabaseConfig) -> Result<(), SqlrestError> {
    if let DatabaseConfig::PostgresUnencrypted { connection } = database {
        connection
            .parse::<tokio_postgres::Config>()
            .map_err(|_| invalid("Invalid PostgreSQL connection configuration"))?;
    }
    Ok(())
}

fn database_present<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<DatabaseConfig>, D::Error> {
    DatabaseConfig::deserialize(d).map(Some)
}

fn persisted_limits<'de, D: serde::Deserializer<'de>>(d: D) -> Result<RequestLimits, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CompleteLimits {
        request_timeout_ms: u64,
        max_rows: usize,
    }
    let value = CompleteLimits::deserialize(d)?;
    Ok(RequestLimits {
        request_timeout_ms: value.request_timeout_ms,
        max_rows: value.max_rows,
    })
}

fn uncertain(error: SqlrestError) -> SqlrestError {
    SqlrestError::new(
        500,
        "workspace_commit_uncertain",
        "Configuration replacement completed but durability could not be confirmed",
    )
    .with_diagnostic("workspace", error)
}

fn request_timeout() -> u64 {
    5000
}

fn migration_timeout() -> u64 {
    60000
}

fn max_rows() -> usize {
    1000
}

fn directory(path: &Path) -> Result<(), SqlrestError> {
    reject_symlink(path)?;
    fs::create_dir_all(path).map_err(io_error)
}

fn reject_symlink(path: &Path) -> Result<(), SqlrestError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_symlink() => {
            Err(invalid("Workspace paths must not be symlinks"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), SqlrestError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}

fn invalid(message: &str) -> SqlrestError {
    SqlrestError::new(400, "invalid_configuration", message)
}

fn io_error(error: std::io::Error) -> SqlrestError {
    SqlrestError::new(
        500,
        "workspace_io_failed",
        "Workspace filesystem operation failed",
    )
    .with_diagnostic("workspace", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_defaults_are_independent_and_strict() {
        let request: PublishRequest = serde_json::from_str(r#"{"limits":{"max_rows":7}}"#).unwrap();
        assert_eq!(request.limits.request_timeout_ms, 5000);
        assert_eq!(request.limits.max_rows, 7);
        assert_eq!(request.migration_timeout_ms, 60000);
        for value in [
            r#"{"limits":null}"#,
            r#"{"limits":{"max_rows":null}}"#,
            r#"{"database":null}"#,
            r#"{"migration_timeout_ms":null}"#,
            r#"{"interfaces":"x"}"#,
            r#"{"database":{"kind":"turso","path":"elsewhere"}}"#,
        ] {
            assert!(
                serde_json::from_str::<PublishRequest>(value).is_err(),
                "{value}"
            );
        }
        for value in [
            r#"{"limits":{"max_rows":0}}"#,
            r#"{"migration_timeout_ms":0}"#,
        ] {
            assert!(
                serde_json::from_str::<PublishRequest>(value)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }

    #[test]
    fn durable_records_and_exclusive_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = Workspace::open(temp.path()).unwrap();
        assert!(Workspace::open(temp.path()).is_err());
        workspace.prepare("app").unwrap();
        assert!(workspace.names().unwrap().is_empty());
        let record = Record {
            database: DatabaseConfig::Turso {},
            state: PersistentState {
                recovery: Recovery::Reload,
            },
            limits: RequestLimits::default(),
        };
        workspace.persist("app", &record).unwrap();
        assert_eq!(workspace.names().unwrap(), ["app"]);
        assert_eq!(
            workspace.read("app").unwrap().state.recovery,
            Recovery::Reload
        );
        assert!(workspace.require_data("app").is_err());
        workspace.remove("app").unwrap();
        assert!(workspace.directory("app").join("interfaces").is_dir());
        drop(workspace);
        assert!(Workspace::open(temp.path()).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn unregistered_non_utf8_directory_is_ignored() {
        use std::os::unix::ffi::OsStringExt;

        let temp = tempfile::tempdir().unwrap();
        let workspace = Workspace::open(temp.path()).unwrap();
        fs::create_dir(
            temp.path()
                .join("databases")
                .join(std::ffi::OsString::from_vec(vec![0xff])),
        )
        .unwrap();
        assert!(workspace.names().unwrap().is_empty());
    }
}
