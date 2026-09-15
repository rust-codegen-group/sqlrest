//! Durable database ownership, unified publication and operation tracking.
#![doc = include_str!("../docs/registry.md")]

pub use crate::workspace::{DatabaseConfig, PublishRequest, Recovery, RequestLimits};
use crate::{
    SqlrestError,
    execution::{Executor, Limits},
    loader::{MatchedEndpoint, Snapshot},
    migration::{AppliedMigration, Migrator, Plan},
    params::Input,
    sql::Backend,
    workspace::{PersistentState, Record, Workspace, duration, validate_name},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Clone, PartialEq, Eq)]
enum Target {
    Turso(PathBuf),
    PostgresUnencrypted(Box<tokio_postgres::Config>),
}

#[derive(Clone)]
struct Configuration {
    target: Target,
    interfaces: PathBuf,
    migrations: PathBuf,
    limits: Limits,
}

impl Configuration {
    fn from_record(
        workspace: &Workspace,
        name: &str,
        record: &Record,
    ) -> Result<Self, SqlrestError> {
        let root = workspace.directory(name);
        let target = match &record.database {
            DatabaseConfig::Turso {} => Target::Turso(root.join("data.db")),
            DatabaseConfig::PostgresUnencrypted { connection } => {
                Target::PostgresUnencrypted(Box::new(connection.parse().map_err(|_| {
                    SqlrestError::definition("Invalid PostgreSQL connection configuration")
                })?))
            }
        };
        Ok(Self {
            target,
            interfaces: root.join("interfaces"),
            migrations: root.join("migrations"),
            limits: record.limits.validate()?,
        })
    }

    fn backend(&self) -> Backend {
        match self.target {
            Target::Turso(_) => Backend::Turso,
            Target::PostgresUnencrypted(_) => Backend::Postgres,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Publishing,
    Ready,
    RecoveryRequired,
    Unregistering,
    Unregistered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationId {
    kind: OperationKind,
    value: u128,
}

impl OperationId {
    fn new(kind: OperationKind) -> Self {
        Self {
            kind,
            value: uuid::Uuid::new_v4().as_u128(),
        }
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut value = self.value;
        let mut bytes = [b'0'; 25];
        let mut start = bytes.len();
        loop {
            start -= 1;
            bytes[start] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(value % 36) as usize];
            value /= 36;
            if value == 0 {
                break;
            }
        }
        write!(
            formatter,
            "{}-{}",
            self.kind.prefix(),
            std::str::from_utf8(&bytes[start..]).unwrap()
        )
    }
}

impl Serialize for OperationId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for OperationId {
    type Err = SqlrestError;

    fn from_str(value: &str) -> Result<Self, SqlrestError> {
        let invalid = || {
            SqlrestError::new(
                400,
                "invalid_operation_id",
                "Expected an operation prefix and lowercase base36 UUID",
            )
        };
        let (prefix, encoded) = value.split_once('-').ok_or_else(invalid)?;
        let kind = match prefix {
            "publish" => OperationKind::Publish,
            "unregister" => OperationKind::Unregister,
            _ => return Err(invalid()),
        };
        if encoded.is_empty()
            || encoded.len() > 25
            || !encoded
                .bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
            || (encoded.len() > 1 && encoded.starts_with('0'))
        {
            return Err(invalid());
        }
        let value = u128::from_str_radix(encoded, 36).map_err(|_| invalid())?;
        Ok(Self { kind, value })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Publish,
    Unregister,
}

impl OperationKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Unregister => "unregister",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Operation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub outcome: Outcome,
    pub error: Option<SqlrestError>,
    pub publish: Option<PublishProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishStep {
    Connecting,
    Preflight,
    Draining,
    Applying,
    Loading,
    Complete,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishProgress {
    pub step: PublishStep,
    pub applied_versions: Vec<i64>,
    pub current_version: Option<i64>,
    pub failed_version: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub phase: Phase,
    pub version: Option<String>,
    pub active_requests: usize,
    pub current_operation: Option<Operation>,
    pub last_operation: Option<Operation>,
    pub recovery: Option<Recovery>,
    pub limits: Option<RequestLimits>,
    pub error: Option<SqlrestError>,
}

#[derive(Clone)]
pub struct Registry {
    entries: Arc<Mutex<BTreeMap<String, Arc<Database>>>>,
    workspace: Arc<Workspace>,
    shutdown: Arc<Shutdown>,
}

#[derive(Default)]
struct Shutdown {
    closed: AtomicBool,
    result: Mutex<Option<Result<(), SqlrestError>>>,
    changed: Notify,
}

struct Database {
    name: String,
    workspace: Arc<Workspace>,
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    phase: Phase,
    config: Option<Configuration>,
    record: Option<Record>,
    invalid_record: bool,
    resources: Option<Resources>,
    snapshot: Option<Arc<Snapshot>>,
    active: usize,
    current: Option<Operation>,
    last: Option<Operation>,
    error: Option<SqlrestError>,
}

impl Database {
    fn new(name: String, workspace: Arc<Workspace>) -> Self {
        Self {
            name,
            workspace,
            state: Mutex::new(State {
                phase: Phase::RecoveryRequired,
                config: None,
                record: None,
                invalid_record: false,
                resources: None,
                snapshot: None,
                active: 0,
                current: None,
                last: None,
                error: None,
            }),
            changed: Notify::new(),
        }
    }
}

// Release the database before its file claim.
#[derive(Clone)]
struct Resources {
    executor: Executor,
    _claim: Option<Arc<FileClaim>>,
}

struct FileClaim {
    path: PathBuf,
    handle: same_file::Handle,
}

static FILE_CLAIMS: OnceLock<Mutex<Vec<Weak<FileClaim>>>> = OnceLock::new();

impl Registry {
    /// Acquire the workspace and restore each persisted database independently.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, SqlrestError> {
        let root = root.as_ref().to_owned();
        let workspace = Arc::new(blocking(move || Workspace::open(&root)).await?);
        let registry = Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            workspace,
            shutdown: Arc::new(Shutdown::default()),
        };
        tokio::spawn(async move {
            let workspace = registry.workspace.clone();
            let names = blocking(move || workspace.names()).await?;
            for name in names {
                let database = Arc::new(Database::new(name.clone(), registry.workspace.clone()));
                registry
                    .entries
                    .lock()
                    .unwrap()
                    .insert(name, database.clone());
                if let Err(error) = restore(&database).await {
                    let mut state = database.state.lock().unwrap();
                    state.error = Some(error);
                    state.phase = Phase::RecoveryRequired;
                }
            }
            Ok::<_, SqlrestError>(registry)
        })
        .await
        .map_err(|_| worker_failed())?
    }

    /// Accept one detached publication; disconnecting never cancels accepted work.
    pub fn publish(
        &self,
        name: &str,
        request: PublishRequest,
    ) -> Result<OperationId, SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| worker_failed())?;
        validate_name(name)?;
        request.validate()?;
        let (database, id) = {
            let mut entries = self.entries.lock().unwrap();
            self.check_open()?;
            if !entries.contains_key(name) && request.database.is_none() {
                return Err(SqlrestError::new(
                    400,
                    "database_configuration_required",
                    "First publish requires database configuration",
                ));
            }
            let database = entries
                .entry(name.into())
                .or_insert_with(|| Arc::new(Database::new(name.into(), self.workspace.clone())))
                .clone();
            let mut state = database.state.lock().unwrap();
            if state.current.is_some() {
                return Err(busy());
            }
            if state.invalid_record {
                return Err(SqlrestError::new(
                    409,
                    "invalid_persisted_configuration",
                    "Repair database.toml and restart before publishing",
                ));
            }
            if request.database.is_none() && state.record.is_none() {
                return Err(SqlrestError::new(
                    400,
                    "database_configuration_required",
                    "First publish requires database configuration",
                ));
            }
            let id = begin(&mut state, OperationKind::Publish);
            drop(state);
            (database, id)
        };
        runtime.spawn(async move {
            let working = database.clone();
            let result = tokio::spawn(async move { run_publish(&working, request).await })
                .await
                .unwrap_or_else(|_| Err(worker_failed()));
            let mut state = database.state.lock().unwrap();
            if result.is_err() && state.phase == Phase::Publishing {
                state.phase = Phase::RecoveryRequired;
            }
            finish(&mut state, result);
            drop(state);
            database.changed.notify_waiters();
        });
        Ok(id)
    }

    pub fn status(&self, name: &str) -> Result<Status, SqlrestError> {
        let database = self.database(name)?;
        let state = database.state.lock().unwrap();
        Ok(status(&state))
    }

    /// Includes failed startup entries, never connection credentials.
    pub fn statuses(&self) -> BTreeMap<String, Status> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .map(|(name, database)| (name.clone(), status(&database.state.lock().unwrap())))
            .collect()
    }

    pub fn openapi(&self, name: &str, server_url: &str) -> Result<Value, SqlrestError> {
        let database = self.database(name)?;
        let snapshot = database
            .state
            .lock()
            .unwrap()
            .snapshot
            .clone()
            .ok_or_else(unavailable)?;
        Ok(snapshot.openapi(server_url))
    }

    /// Durable original SQL, without overwriting interface/migration files.
    pub async fn export_migrations(
        &self,
        name: &str,
    ) -> Result<Vec<AppliedMigration>, SqlrestError> {
        let (migrator, lifetime) = {
            let entries = self.entries.lock().unwrap();
            self.check_open()?;
            let database = lookup(&entries, name)?;
            let mut state = database.state.lock().unwrap();
            if state.current.is_some() || state.phase == Phase::Unregistered {
                return Err(unavailable());
            }
            let resources = state.resources.as_ref().ok_or_else(unavailable)?;
            let config = state.config.as_ref().ok_or_else(unavailable)?;
            let migrator = Migrator {
                executor: resources.executor.clone(),
                backend: config.backend(),
                timeout: Duration::from_secs(60),
            };
            state.active += 1;
            (
                migrator,
                RequestLifetime {
                    database: database.clone(),
                    _snapshot: state.snapshot.clone(),
                },
            )
        };
        tokio::spawn(async move {
            let _lifetime = lifetime;
            let result = migrator.history().await;
            // Release executor ownership before announcing drain completion.
            drop(migrator);
            result
        })
        .await
        .map_err(|_| worker_failed())?
    }

    /// Delete only the registration, after draining actual request cleanup.
    pub fn unregister(&self, name: &str) -> Result<OperationId, SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| worker_failed())?;
        let (database, id) = {
            let entries = self.entries.lock().unwrap();
            self.check_open()?;
            let database = lookup(&entries, name)?;
            let mut state = database.state.lock().unwrap();
            if state.current.is_some() {
                return Err(busy());
            }
            let id = begin(&mut state, OperationKind::Unregister);
            state.phase = Phase::Unregistering;
            drop(state);
            (database, id)
        };
        runtime.spawn(async move {
            let working = database.clone();
            let result = tokio::spawn(async move {
                drain(&working).await;
                let db = working.clone();
                blocking(move || db.workspace.remove(&db.name)).await?;
                let retired = {
                    let mut state = working.state.lock().unwrap();
                    state.record = None;
                    state.config = None;
                    state.invalid_record = false;
                    (state.resources.take(), state.snapshot.take())
                };
                blocking(move || {
                    drop(retired);
                    Ok(())
                })
                .await
            })
            .await
            .unwrap_or_else(|_| Err(worker_failed()));
            let mut state = database.state.lock().unwrap();
            state.phase = if result.is_ok() {
                Phase::Unregistered
            } else {
                Phase::RecoveryRequired
            };
            finish(&mut state, result);
            drop(state);
            database.changed.notify_waiters();
        });
        Ok(id)
    }

    pub fn operation(&self, name: &str, id: OperationId) -> Result<Operation, SqlrestError> {
        let database = self.database(name)?;
        let state = database.state.lock().unwrap();
        operation(&state, id)
    }

    pub async fn wait_operation(
        &self,
        name: &str,
        id: OperationId,
    ) -> Result<Operation, SqlrestError> {
        let database = self.database(name)?;
        loop {
            let changed = database.changed.notified();
            {
                let state = database.state.lock().unwrap();
                let operation = operation(&state, id)?;
                if operation.outcome != Outcome::Running {
                    return Ok(operation);
                }
            }
            changed.await;
        }
    }

    pub async fn execute(
        &self,
        name: &str,
        method: &str,
        segments: &[&str],
        input: Input,
    ) -> Result<Vec<u8>, SqlrestError> {
        self.admit(name, method, segments)?
            .execute(input, None)
            .await
    }

    pub(crate) fn admit(
        &self,
        name: &str,
        method: &str,
        segments: &[&str],
    ) -> Result<AdmittedRequest, SqlrestError> {
        let entries = self.entries.lock().unwrap();
        self.check_open()?;
        let database = lookup(&entries, name)?;
        let mut state = database.state.lock().unwrap();
        if state.phase != Phase::Ready {
            return Err(unavailable());
        }
        let snapshot = state.snapshot.as_ref().unwrap().clone();
        let matched = snapshot.resolve(method, segments)?;
        let executor = state.resources.as_ref().unwrap().executor.clone();
        let limits = state.config.as_ref().unwrap().limits;
        state.active += 1;
        let lifetime = RequestLifetime {
            database: database.clone(),
            _snapshot: Some(snapshot),
        };
        Ok(AdmittedRequest {
            executor,
            matched,
            limits,
            lifetime,
        })
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.closed.load(Ordering::Acquire)
    }

    /// Drain accepted work without changing persisted registrations.
    pub async fn shutdown(&self) -> Result<(), SqlrestError> {
        self.start_shutdown()?;
        loop {
            let changed = self.shutdown.changed.notified();
            if let Some(result) = self.shutdown.result.lock().unwrap().clone() {
                return result;
            }
            changed.await;
        }
    }

    pub(crate) fn start_shutdown(&self) -> Result<(), SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| worker_failed())?;
        let databases = {
            let entries = self.entries.lock().unwrap();
            if self.shutdown.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            entries.values().cloned().collect::<Vec<_>>()
        };
        let shutdown = self.shutdown.clone();
        runtime.spawn(async move {
            let result = tokio::spawn(async move {
                for database in databases {
                    loop {
                        let changed = database.changed.notified();
                        {
                            let state = database.state.lock().unwrap();
                            if state.current.is_none() && state.active == 0 {
                                break;
                            }
                        }
                        changed.await;
                    }
                    let retired = {
                        let mut state = database.state.lock().unwrap();
                        state.phase = Phase::Unregistered;
                        (state.resources.take(), state.snapshot.take())
                    };
                    blocking(move || {
                        drop(retired);
                        Ok(())
                    })
                    .await?;
                    database.changed.notify_waiters();
                }
                Ok::<_, SqlrestError>(())
            })
            .await
            .unwrap_or_else(|_| Err(worker_failed()));
            *shutdown.result.lock().unwrap() = Some(result);
            shutdown.changed.notify_waiters();
        });
        Ok(())
    }

    fn check_open(&self) -> Result<(), SqlrestError> {
        if self.is_shutting_down() {
            Err(shutting_down())
        } else {
            Ok(())
        }
    }

    fn database(&self, name: &str) -> Result<Arc<Database>, SqlrestError> {
        lookup(&self.entries.lock().unwrap(), name)
    }
}

async fn restore(database: &Arc<Database>) -> Result<(), SqlrestError> {
    let db = database.clone();
    let record = match blocking(move || db.workspace.read(&db.name)).await {
        Ok(record) => record,
        Err(error) => {
            database.state.lock().unwrap().invalid_record = true;
            return Err(error);
        }
    };
    let config = Configuration::from_record(&database.workspace, &database.name, &record)?;
    {
        let mut state = database.state.lock().unwrap();
        state.record = Some(record.clone());
        state.config = Some(config.clone());
    }
    let db = database.clone();
    let turso = matches!(config.target, Target::Turso(_));
    blocking(move || {
        db.workspace.validate_layout(&db.name)?;
        if turso {
            db.workspace.require_data(&db.name)?;
        }
        Ok(())
    })
    .await?;
    let resources = open_resources(config.clone(), false).await?;
    database.state.lock().unwrap().resources = Some(resources);
    if record.state.recovery == Recovery::None {
        let snapshot =
            blocking(move || Snapshot::load(&config.interfaces, config.backend()).map(Arc::new))
                .await?;
        let mut state = database.state.lock().unwrap();
        state.snapshot = Some(snapshot);
        state.phase = Phase::Ready;
    }
    Ok(())
}

async fn run_publish(
    database: &Arc<Database>,
    request: PublishRequest,
) -> Result<(), SqlrestError> {
    let (old_record, old_config, old_resources) = {
        let state = database.state.lock().unwrap();
        (
            state.record.clone(),
            state.config.clone(),
            state.resources.clone(),
        )
    };
    let mut candidate = Record {
        database: request
            .database
            .or_else(|| old_record.as_ref().map(|r| r.database.clone()))
            .ok_or_else(|| SqlrestError::definition("Database configuration required"))?,
        state: PersistentState {
            recovery: Recovery::Reload,
        },
        limits: request.limits,
    };
    let config = Configuration::from_record(&database.workspace, &database.name, &candidate)?;
    let same_target = old_config
        .as_ref()
        .is_some_and(|old| old.target == config.target);
    let db = database.clone();
    let must_exist = old_record
        .as_ref()
        .is_some_and(|r| r.database == DatabaseConfig::Turso {})
        && candidate.database == DatabaseConfig::Turso {};
    blocking(move || {
        if must_exist {
            db.workspace.require_data(&db.name)?;
        }
        db.workspace.prepare(&db.name)
    })
    .await?;
    let resources = if same_target {
        match old_resources {
            Some(resources) => resources,
            None => open_resources(config.clone(), !must_exist).await?,
        }
    } else {
        open_resources(config.clone(), !must_exist).await?
    };

    // Changed target: close/drain, then durably adopt before touching its schema.
    if !same_target || old_record.is_none() {
        let previous_phase = close_admission(database);
        drain(database).await;
        let db = database.clone();
        let turso = matches!(config.target, Target::Turso(_));
        blocking(move || {
            if turso {
                db.workspace.sync_data(&db.name)?;
            }
            Ok(())
        })
        .await
        .map_err(|error| restore_admission(database, previous_phase, error))?;
        persist(database, candidate.clone())
            .await
            .map_err(|error| restore_admission(database, previous_phase, error))?;
        let retired = {
            let mut state = database.state.lock().unwrap();
            state.config = Some(config.clone());
            state.record = Some(candidate.clone());
            (
                state.resources.replace(resources.clone()),
                state.snapshot.take(),
            )
        };
        blocking(move || {
            drop(retired);
            Ok(())
        })
        .await?;
    } else {
        database.state.lock().unwrap().resources = Some(resources.clone());
    }

    progress(database, |p| p.step = PublishStep::Preflight);
    let root = config.migrations.clone();
    let backend = config.backend();
    let plan = blocking(move || Plan::load(&root, backend)).await?;
    let timeout = duration(request.migration_timeout_ms)?;
    let mut migrator = Migrator {
        executor: resources.executor.clone(),
        backend,
        timeout,
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let history = migrator.history().await?;
    let (pending, expected_history) =
        blocking(move || Ok((plan.into_pending(&history)?, history))).await?;
    let recovering_migration = database
        .state
        .lock()
        .unwrap()
        .record
        .as_ref()
        .is_some_and(|r| r.state.recovery == Recovery::Migration);
    if !pending.is_empty() || recovering_migration {
        let budget = remaining(deadline)?;
        let previous_phase = close_admission(database);
        drain(database).await;
        // Preserve effective limits until the candidate interfaces succeed.
        let mut blocked = database.state.lock().unwrap().record.clone().unwrap();
        blocked.state.recovery = Recovery::Migration;
        persist(database, blocked.clone())
            .await
            .map_err(|error| restore_admission(database, previous_phase, error))?;
        database.state.lock().unwrap().record = Some(blocked.clone());
        // Draining existing requests and persisting the blocker do not consume
        // the remaining migration budget. Never reset it per file.
        let deadline = tokio::time::Instant::now() + budget;
        migrator.timeout = remaining(deadline)?;
        let history = migrator.history().await?;
        if history != expected_history {
            return Err(SqlrestError::new(
                409,
                "migration_history_changed",
                "Migration history changed during publication; retry publish",
            ));
        }
        for file in pending {
            let version = file.record.version;
            progress(database, |p| {
                p.step = PublishStep::Applying;
                p.current_version = Some(version);
            });
            migrator.timeout = remaining(deadline).inspect_err(|_| {
                progress(database, |p| {
                    p.failed_version = Some(version);
                    p.current_version = None;
                });
            })?;
            if let Err(error) = migrator.apply(file).await {
                progress(database, |p| {
                    p.failed_version = Some(version);
                    p.current_version = None;
                });
                return Err(error);
            }
            progress(database, |p| {
                p.applied_versions.push(version);
                p.current_version = None;
            });
        }
        blocked.state.recovery = Recovery::Reload;
        persist(database, blocked.clone()).await?;
        database.state.lock().unwrap().record = Some(blocked);
    }
    progress(database, |p| p.step = PublishStep::Loading);
    let root = config.interfaces.clone();
    let snapshot = blocking(move || Snapshot::load(&root, backend).map(Arc::new)).await?;
    candidate.state.recovery = Recovery::None;
    persist(database, candidate.clone()).await?;
    let retired = {
        let mut state = database.state.lock().unwrap();
        state.record = Some(candidate);
        state.config = Some(config);
        state.resources = Some(resources);
        state.phase = Phase::Ready;
        state.error = None;
        let old = state.snapshot.replace(snapshot);
        state
            .current
            .as_mut()
            .unwrap()
            .publish
            .as_mut()
            .unwrap()
            .step = PublishStep::Complete;
        old
    };
    blocking(move || {
        drop(retired);
        Ok(())
    })
    .await
}

fn remaining(deadline: tokio::time::Instant) -> Result<Duration, SqlrestError> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(crate::turso_driver::timeout)
}

async fn persist(database: &Arc<Database>, record: Record) -> Result<(), SqlrestError> {
    let db = database.clone();
    let result = blocking(move || db.workspace.persist(&db.name, &record)).await;
    if result
        .as_ref()
        .is_err_and(|error| error.code == "workspace_commit_uncertain")
    {
        // rename may precede a failed directory sync. Do not claim rollback.
        let mut state = database.state.lock().unwrap();
        state.phase = Phase::RecoveryRequired;
        // Reload the authoritative file on restart before using either target.
        state.invalid_record = true;
    }
    result
}

fn close_admission(database: &Database) -> Phase {
    let mut state = database.state.lock().unwrap();
    let previous_phase = state.phase;
    state.phase = Phase::Publishing;
    state
        .current
        .as_mut()
        .unwrap()
        .publish
        .as_mut()
        .unwrap()
        .step = PublishStep::Draining;
    previous_phase
}

// Only used before adopting a new target or mutating its schema. A definite
// persistence failure leaves the old service intact; an uncertain commit does not.
fn restore_admission(
    database: &Database,
    previous_phase: Phase,
    error: SqlrestError,
) -> SqlrestError {
    let mut state = database.state.lock().unwrap();
    if !state.invalid_record && state.phase == Phase::Publishing {
        state.phase = previous_phase;
    }
    error
}

async fn drain(database: &Database) {
    loop {
        let changed = database.changed.notified();
        if database.state.lock().unwrap().active == 0 {
            return;
        }
        changed.await;
    }
}

fn progress(database: &Database, update: impl FnOnce(&mut PublishProgress)) {
    let mut state = database.state.lock().unwrap();
    update(state.current.as_mut().unwrap().publish.as_mut().unwrap());
}

fn begin(state: &mut State, kind: OperationKind) -> OperationId {
    let id = OperationId::new(kind);
    state.current = Some(Operation {
        id,
        kind,
        outcome: Outcome::Running,
        error: None,
        publish: (kind == OperationKind::Publish).then_some(PublishProgress {
            step: PublishStep::Connecting,
            applied_versions: Vec::new(),
            current_version: None,
            failed_version: None,
        }),
    });
    id
}

fn finish(state: &mut State, result: Result<(), SqlrestError>) {
    let mut operation = state
        .current
        .take()
        .expect("operation owns management slot");
    operation.outcome = if result.is_ok() {
        Outcome::Succeeded
    } else {
        Outcome::Failed
    };
    operation.error = result.err();
    state.error = operation.error.clone();
    state.last = Some(operation);
}

fn operation(state: &State, id: OperationId) -> Result<Operation, SqlrestError> {
    state
        .current
        .iter()
        .chain(state.last.iter())
        .find(|op| op.id == id)
        .cloned()
        .ok_or_else(|| {
            SqlrestError::new(
                404,
                "operation_not_found",
                "Operation record is unavailable; inspect database state before retrying",
            )
        })
}

fn status(state: &State) -> Status {
    Status {
        phase: state.phase,
        version: state.snapshot.as_ref().map(|s| s.version().into()),
        active_requests: state.active,
        current_operation: state.current.clone(),
        last_operation: state.last.clone(),
        recovery: state.record.as_ref().map(|r| r.state.recovery),
        limits: state.record.as_ref().map(|r| r.limits),
        error: state.error.clone(),
    }
}

pub(crate) struct AdmittedRequest {
    executor: Executor,
    matched: MatchedEndpoint,
    pub limits: Limits,
    lifetime: RequestLifetime,
}

impl AdmittedRequest {
    pub async fn execute(
        self,
        mut input: Input,
        remaining: Option<Duration>,
    ) -> Result<Vec<u8>, SqlrestError> {
        let Self {
            executor,
            matched,
            mut limits,
            lifetime,
        } = self;
        if let Some(remaining) = remaining {
            limits.timeout = limits.timeout.min(remaining);
        }
        input.path = matched.path_parameters;
        executor
            .execute_tracked(matched.endpoint, input, limits, lifetime)
            .await
    }
}

struct RequestLifetime {
    database: Arc<Database>,
    _snapshot: Option<Arc<Snapshot>>,
}

impl Drop for RequestLifetime {
    fn drop(&mut self) {
        self.database.state.lock().unwrap().active -= 1;
        self.database.changed.notify_waiters();
    }
}

fn lookup(
    entries: &BTreeMap<String, Arc<Database>>,
    name: &str,
) -> Result<Arc<Database>, SqlrestError> {
    entries.get(name).cloned().ok_or_else(|| {
        SqlrestError::new(
            404,
            "database_not_found",
            "Database configuration not found",
        )
    })
}

pub(crate) fn shutting_down() -> SqlrestError {
    SqlrestError::new(503, "server_shutting_down", "Server is shutting down")
}

fn unavailable() -> SqlrestError {
    SqlrestError::new(
        503,
        "database_unavailable",
        "Database interfaces are not accepting requests",
    )
}

fn busy() -> SqlrestError {
    SqlrestError::new(
        409,
        "operation_in_progress",
        "A database operation is in progress",
    )
}

fn worker_failed() -> SqlrestError {
    SqlrestError::new(
        500,
        "management_task_failed",
        "Database management worker failed",
    )
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, SqlrestError> + Send + 'static,
) -> Result<T, SqlrestError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| worker_failed())?
}

async fn open_resources(
    config: Configuration,
    allow_create: bool,
) -> Result<Resources, SqlrestError> {
    match config.target {
        Target::Turso(path) => {
            blocking(move || {
                let claim = claim_file(&path, allow_create)?;
                let database = crate::turso_driver::open(&claim.path)?;
                let after = same_file::Handle::from_path(&claim.path).map_err(|_| file_error())?;
                if after != claim.handle {
                    return Err(file_error());
                }
                Ok(Resources {
                    executor: Executor::turso(database),
                    _claim: Some(claim),
                })
            })
            .await
        }
        Target::PostgresUnencrypted(config) => {
            let (client, connection) = tokio::time::timeout(
                Duration::from_secs(5),
                config.connect(tokio_postgres::NoTls),
            )
            .await
            .map_err(|_| crate::turso_driver::timeout())?
            .map_err(|error| {
                SqlrestError::new(500, "database_error", "Cannot connect to database")
                    .with_diagnostic("postgres", error)
            })?;
            drop(client);
            drop(connection);
            Ok(Resources {
                executor: Executor::postgres_unencrypted(*config),
                _claim: None,
            })
        }
    }
}

fn claim_file(path: &Path, allow_create: bool) -> Result<Arc<FileClaim>, SqlrestError> {
    let mut claims = FILE_CLAIMS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    claims.retain(|claim| claim.strong_count() != 0);
    if let Ok(metadata) = fs::metadata(path)
        && !metadata.is_file()
    {
        return Err(file_error());
    }
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(allow_create)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| file_error())?,
        Err(_) => return Err(file_error()),
    };
    if !file.metadata().map_err(|_| file_error())?.is_file() {
        return Err(file_error());
    }
    let path = fs::canonicalize(path).map_err(|_| file_error())?;
    let handle = same_file::Handle::from_file(file).map_err(|_| file_error())?;
    if claims
        .iter()
        .filter_map(Weak::upgrade)
        .any(|claim| claim.path == path || claim.handle == handle)
    {
        return Err(SqlrestError::new(
            409,
            "database_already_registered",
            "Database file is already owned by this process",
        ));
    }
    let claim = Arc::new(FileClaim { path, handle });
    claims.push(Arc::downgrade(&claim));
    Ok(claim)
}

fn file_error() -> SqlrestError {
    SqlrestError::new(
        400,
        "invalid_database_path",
        "Cannot open a stable regular database file",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::{PersistFault, RemoveFault};

    async fn fixture() -> (tempfile::TempDir, Registry, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("databases/app");
        fs::create_dir_all(root.join("interfaces")).unwrap();
        fs::create_dir_all(root.join("migrations")).unwrap();
        fs::write(root.join("interfaces/get.sql"), "SELECT 'old' AS value").unwrap();
        fs::write(root.join("interfaces/get.response.yaml"),
            r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#).unwrap();
        let registry = Registry::open(directory.path()).await.unwrap();
        let id = registry
            .publish(
                "app",
                PublishRequest {
                    database: Some(DatabaseConfig::Turso {}),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            registry.wait_operation("app", id).await.unwrap().outcome,
            Outcome::Succeeded
        );
        (directory, registry, root)
    }

    #[tokio::test]
    async fn failed_replacement_keeps_old_snapshot_and_limits() {
        let (_directory, registry, root) = fixture().await;
        let original = fs::read(root.join("database.toml")).unwrap();
        let version = registry.status("app").unwrap().version;
        fs::write(root.join("interfaces/get.sql"), "SELECT 'new' AS value").unwrap();
        *registry.workspace.persist_fault.lock().unwrap() = Some(PersistFault::BeforeReplace);
        let id = registry
            .publish(
                "app",
                PublishRequest {
                    limits: RequestLimits {
                        max_rows: 2,
                        request_timeout_ms: 15,
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            registry.wait_operation("app", id).await.unwrap().outcome,
            Outcome::Failed
        );
        let state = registry.status("app").unwrap();
        assert_eq!(state.phase, Phase::Ready);
        assert_eq!(state.version, version);
        assert_eq!(state.limits, Some(RequestLimits::default()));
        assert_eq!(fs::read(root.join("database.toml")).unwrap(), original);
        let result = registry
            .execute("app", "get", &[], Input::default())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&result).unwrap()["records"][0]["value"],
            "old"
        );
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_blocker_persistence_prevents_database_mutation() {
        let (_directory, registry, root) = fixture().await;
        let original = fs::read(root.join("database.toml")).unwrap();
        let version = registry.status("app").unwrap().version;
        fs::write(root.join("interfaces/get.sql"), "SELECT 'new' AS value").unwrap();
        fs::write(
            root.join("migrations/0001_new.sql"),
            "CREATE TABLE new_table(id BIGINT)",
        )
        .unwrap();
        *registry.workspace.persist_fault.lock().unwrap() = Some(PersistFault::BeforeReplace);
        let id = registry
            .publish(
                "app",
                PublishRequest {
                    limits: RequestLimits {
                        max_rows: 2,
                        request_timeout_ms: 15,
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            registry.wait_operation("app", id).await.unwrap().outcome,
            Outcome::Failed
        );
        let state = registry.status("app").unwrap();
        assert_eq!(state.phase, Phase::Ready);
        assert_eq!(state.version, version);
        assert_eq!(state.limits, Some(RequestLimits::default()));
        assert_eq!(fs::read(root.join("database.toml")).unwrap(), original);
        let result = registry
            .execute("app", "get", &[], Input::default())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&result).unwrap()["records"][0]["value"],
            "old"
        );
        assert!(registry.export_migrations("app").await.unwrap().is_empty());
        // Successful CREATE TABLE proves the failed publish never ran that DDL.
        let id = registry.publish("app", PublishRequest::default()).unwrap();
        assert_eq!(
            registry.wait_operation("app", id).await.unwrap().outcome,
            Outcome::Succeeded
        );
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unregister_persistence_failures_close_admission_and_allow_retry() {
        for fault in [RemoveFault::BeforeRemove, RemoveFault::AfterRemove] {
            let (_directory, registry, root) = fixture().await;
            let source = fs::read(root.join("interfaces/get.sql")).unwrap();
            *registry.workspace.remove_fault.lock().unwrap() = Some(fault);
            let id = registry.unregister("app").unwrap();
            let operation = registry.wait_operation("app", id).await.unwrap();
            assert_eq!(operation.outcome, Outcome::Failed);
            assert!(operation.error.is_some());
            assert_eq!(
                registry.status("app").unwrap().phase,
                Phase::RecoveryRequired
            );
            assert!(
                registry
                    .execute("app", "get", &[], Input::default())
                    .await
                    .is_err()
            );
            assert_eq!(
                root.join("database.toml").exists(),
                fault == RemoveFault::BeforeRemove
            );
            assert!(root.join("data.db").is_file());
            assert_eq!(fs::read(root.join("interfaces/get.sql")).unwrap(), source);

            let id = registry.unregister("app").unwrap();
            assert_eq!(
                registry.wait_operation("app", id).await.unwrap().outcome,
                Outcome::Succeeded
            );
            assert_eq!(registry.status("app").unwrap().phase, Phase::Unregistered);
            assert!(!root.join("database.toml").exists());
            assert!(root.join("data.db").is_file());
            assert_eq!(fs::read(root.join("interfaces/get.sql")).unwrap(), source);
            registry.shutdown().await.unwrap();
        }
    }

    mod postgres_tests {
        use super::*;

        #[tokio::test]
        #[ignore = "requires disposable SQLREST_TEST_POSTGRES"]
        async fn target_adoption_persistence_failures_preserve_or_block_old_service() {
            let connection = std::env::var("SQLREST_TEST_POSTGRES").unwrap();
            let (directory, registry, root) = fixture().await;
            let original = fs::read(root.join("database.toml")).unwrap();
            let version = registry.status("app").unwrap().version;
            fs::write(root.join("interfaces/get.sql"), "SELECT 'new' AS value").unwrap();
            let request = PublishRequest {
                database: Some(DatabaseConfig::PostgresUnencrypted { connection }),
                limits: RequestLimits {
                    max_rows: 2,
                    request_timeout_ms: 5000,
                },
                ..Default::default()
            };
            *registry.workspace.persist_fault.lock().unwrap() = Some(PersistFault::BeforeReplace);
            let id = registry.publish("app", request.clone()).unwrap();
            assert_eq!(
                registry.wait_operation("app", id).await.unwrap().outcome,
                Outcome::Failed
            );
            let state = registry.status("app").unwrap();
            assert_eq!(state.phase, Phase::Ready);
            assert_eq!(state.version, version);
            assert_eq!(state.limits, Some(RequestLimits::default()));
            assert_eq!(fs::read(root.join("database.toml")).unwrap(), original);
            let result = registry
                .execute("app", "get", &[], Input::default())
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&result).unwrap()["records"][0]["value"],
                "old"
            );

            *registry.workspace.persist_fault.lock().unwrap() = Some(PersistFault::AfterReplace);
            let id = registry.publish("app", request).unwrap();
            let operation = registry.wait_operation("app", id).await.unwrap();
            assert_eq!(operation.error.unwrap().code, "workspace_commit_uncertain");
            assert_eq!(
                registry.status("app").unwrap().phase,
                Phase::RecoveryRequired
            );
            assert!(
                registry
                    .execute("app", "get", &[], Input::default())
                    .await
                    .is_err()
            );
            assert!(registry.publish("app", PublishRequest::default()).is_err());
            registry.shutdown().await.unwrap();
            drop(registry);

            let registry = Registry::open(directory.path()).await.unwrap();
            let database = registry.database("app").unwrap();
            {
                let state = database.state.lock().unwrap();
                assert!(matches!(
                    state.config.as_ref().unwrap().target,
                    Target::PostgresUnencrypted(_)
                ));
                assert_eq!(state.phase, Phase::RecoveryRequired);
                assert_eq!(
                    state.record.as_ref().unwrap().state.recovery,
                    Recovery::Reload
                );
                assert_eq!(state.record.as_ref().unwrap().limits.max_rows, 2);
            }
            assert!(root.join("data.db").exists());
            registry.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn uncertain_config_commit_requires_restart_and_never_reports_success() {
        let (directory, registry, root) = fixture().await;
        fs::write(root.join("interfaces/get.sql"), "SELECT 'new' AS value").unwrap();
        *registry.workspace.persist_fault.lock().unwrap() = Some(PersistFault::AfterReplace);
        let id = registry
            .publish(
                "app",
                PublishRequest {
                    limits: RequestLimits {
                        max_rows: 2,
                        request_timeout_ms: 5000,
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        let op = registry.wait_operation("app", id).await.unwrap();
        assert_eq!(op.error.unwrap().code, "workspace_commit_uncertain");
        assert_eq!(
            registry.status("app").unwrap().phase,
            Phase::RecoveryRequired
        );
        assert!(registry.publish("app", PublishRequest::default()).is_err());
        registry.shutdown().await.unwrap();
        drop(registry);
        let registry = Registry::open(directory.path()).await.unwrap();
        assert_eq!(registry.status("app").unwrap().limits.unwrap().max_rows, 2);
        assert_eq!(registry.status("app").unwrap().phase, Phase::Ready);
        registry.shutdown().await.unwrap();
    }

    #[test]
    fn ids_are_typed_canonical_and_round_trip() {
        for kind in [OperationKind::Publish, OperationKind::Unregister] {
            let id = OperationId::new(kind);
            assert_eq!(id.to_string().parse::<OperationId>().unwrap(), id);
            assert!(id.to_string().starts_with(kind.prefix()));
            assert_eq!(
                serde_json::from_str::<OperationId>(&serde_json::to_string(&id).unwrap()).unwrap(),
                id
            );
            assert_ne!(id, OperationId::new(kind));
        }
        for invalid in [
            "1",
            "op-123",
            "publish-ABC",
            "publish-01",
            "publish-",
            "publish-zzzzzzzzzzzzzzzzzzzzzzzzz",
        ] {
            assert!(invalid.parse::<OperationId>().is_err());
        }
        let max = OperationId {
            kind: OperationKind::Publish,
            value: u128::MAX,
        };
        assert_eq!(max.to_string().parse::<OperationId>().unwrap(), max);
    }
}
