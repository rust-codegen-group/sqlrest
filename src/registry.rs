//! In-memory database ownership, publication and operation tracking.
#![doc = include_str!("../docs/registry.md")]

use crate::{
    SqlrestError,
    execution::{Executor, Limits},
    loader::Snapshot,
    migration::{AppliedMigration, Migrator, Plan},
    params::Input,
    sql::Backend,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::Notify;

#[derive(Clone, PartialEq, Eq)]
pub enum Target {
    Turso(PathBuf),
    /// Explicitly unencrypted, with the same transport contract as Executor.
    PostgresUnencrypted(Box<tokio_postgres::Config>),
}

#[derive(Clone, PartialEq, Eq)]
pub struct Configuration {
    pub target: Target,
    pub interfaces: PathBuf,
    pub migrations: PathBuf,
    pub limits: Limits,
}

impl Configuration {
    fn normalize(mut self) -> Result<Self, SqlrestError> {
        self.interfaces = absolute(&self.interfaces)?;
        self.migrations = absolute(&self.migrations)?;
        if let Target::Turso(path) = &mut self.target {
            *path = absolute(path)?;
        }
        if self.limits.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.limits.timeout)
                .is_none()
        {
            return Err(SqlrestError::definition(
                "Execution timeout must be positive and finite",
            ));
        }
        Ok(self)
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
    Registering,
    Unloaded,
    Ready,
    Migrating,
    Paused,
    Unregistering,
    Unregistered,
    RegistrationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OperationId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Reload,
    Migrate,
    Unregister,
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
    pub migration: Option<MigrationProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    MigrationFailed,
    ReloadFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStep {
    Preflight,
    Draining,
    Applying,
    Reloading,
    Complete,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationProgress {
    pub step: MigrationStep,
    pub applied_versions: Vec<i64>,
    pub current_version: Option<i64>,
    pub failed_version: Option<i64>,
    pub interfaces_reloaded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub phase: Phase,
    pub version: Option<String>,
    pub active_requests: usize,
    pub current_operation: Option<Operation>,
    pub last_operation: Option<Operation>,
    pub pause_reason: Option<PauseReason>,
}

#[derive(Clone, Default)]
pub struct Registry {
    entries: Arc<Mutex<BTreeMap<String, Arc<Database>>>>,
}

struct Database {
    config: Configuration,
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    phase: Phase,
    resources: Option<Resources>,
    snapshot: Option<Arc<Snapshot>>,
    active: usize,
    current: Option<Operation>,
    last: Option<Operation>,
    registration_error: Option<SqlrestError>,
    pause_reason: Option<PauseReason>,
}

// Field order matters: drop the database before releasing its file claim.
struct Resources {
    executor: Executor,
    _claim: Option<Arc<FileClaim>>,
}

struct FileClaim {
    path: PathBuf,
    handle: same_file::Handle,
}

// Even independent Registry instances cannot open the same Turso file twice.
static FILE_CLAIMS: OnceLock<Mutex<Vec<Weak<FileClaim>>>> = OnceLock::new();
static NEXT_OPERATION: AtomicU64 = AtomicU64::new(1);

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registration opens the database but never publishes interfaces or runs migrations.
    /// Dropping this future does not cancel registration after it has been reserved.
    pub async fn register(
        &self,
        name: &str,
        config: Configuration,
    ) -> Result<Status, SqlrestError> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        {
            return Err(SqlrestError::definition(
                "Invalid database configuration name",
            ));
        }
        let config = config.normalize()?;
        let (database, new) = {
            let mut entries = self.entries.lock().unwrap();
            let existing = entries
                .get(name)
                .filter(|db| db.state.lock().unwrap().phase != Phase::Unregistered);
            if let Some(existing) = existing {
                if existing.config != config {
                    return Err(SqlrestError::new(
                        409,
                        "configuration_conflict",
                        "Database name has a different configuration",
                    ));
                }
                (existing.clone(), false)
            } else {
                let database = Arc::new(Database {
                    config,
                    state: Mutex::new(State {
                        phase: Phase::Registering,
                        resources: None,
                        snapshot: None,
                        active: 0,
                        current: None,
                        last: None,
                        registration_error: None,
                        pause_reason: None,
                    }),
                    changed: Notify::new(),
                });
                entries.insert(name.into(), database.clone());
                (database, true)
            }
        };
        if new {
            let registry = self.clone();
            let name = name.to_owned();
            let database = database.clone();
            tokio::spawn(async move {
                let config = database.config.clone();
                // A separate join boundary converts worker panics to a terminal result.
                let result = tokio::spawn(open_resources(config))
                    .await
                    .unwrap_or_else(|_| Err(worker_failed()));
                let failed = result.is_err();
                {
                    let mut state = database.state.lock().unwrap();
                    match result {
                        Ok(resources) => {
                            state.resources = Some(resources);
                            state.phase = Phase::Unloaded;
                        }
                        Err(error) => {
                            state.registration_error = Some(error);
                            state.phase = Phase::RegistrationFailed;
                        }
                    }
                }
                if failed {
                    let mut entries = registry.entries.lock().unwrap();
                    if entries
                        .get(&name)
                        .is_some_and(|db| Arc::ptr_eq(db, &database))
                    {
                        entries.remove(&name);
                    }
                }
                database.changed.notify_waiters();
            });
        }
        loop {
            let changed = database.changed.notified();
            {
                let state = database.state.lock().unwrap();
                if let Some(error) = &state.registration_error {
                    return Err(error.clone());
                }
                if state.phase != Phase::Registering {
                    return Ok(status(&state));
                }
            }
            changed.await;
        }
    }

    pub fn status(&self, name: &str) -> Result<Status, SqlrestError> {
        let database = self.database(name)?;
        let state = database.state.lock().unwrap();
        Ok(status(&state))
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

    /// Start a detached publication. Only the fully compiled candidate is swapped in.
    pub fn reload(&self, name: &str) -> Result<OperationId, SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            SqlrestError::definition("Management operations require a Tokio runtime")
        })?;
        let database = self.database(name)?;
        let id = begin(&database, OperationKind::Reload)?;
        runtime.spawn(async move {
            let config = database.config.clone();
            let result = tokio::task::spawn_blocking(move || {
                Snapshot::load(&config.interfaces, config.backend()).map(Arc::new)
            })
            .await
            .unwrap_or_else(|_| Err(worker_failed()));
            let mut state = database.state.lock().unwrap();
            let mut old_snapshot = None;
            let result = match result {
                Ok(snapshot) => {
                    old_snapshot = state.snapshot.replace(snapshot);
                    state.phase = Phase::Ready;
                    state.pause_reason = None;
                    Ok(())
                }
                Err(error) => Err(error),
            };
            finish(&mut state, result);
            drop(state);
            database.changed.notify_waiters();
            // A large retired schema must not be destroyed under the status lock.
            drop(old_snapshot);
        });
        Ok(id)
    }

    /// Preflight before pausing; accepted migrations survive caller disconnects.
    pub fn migrate(&self, name: &str) -> Result<OperationId, SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            SqlrestError::definition("Management operations require a Tokio runtime")
        })?;
        let database = self.database(name)?;
        let id = begin(&database, OperationKind::Migrate)?;
        runtime.spawn(async move {
            let working = database.clone();
            let result = tokio::spawn(async move { run_migration(&working).await })
                .await
                .unwrap_or_else(|_| Err(worker_failed()));
            let mut state = database.state.lock().unwrap();
            if result.is_err() && state.phase == Phase::Migrating {
                state.phase = Phase::Paused;
                state.pause_reason = Some(PauseReason::MigrationFailed);
                let progress = state.current.as_mut().unwrap().migration.as_mut().unwrap();
                if progress.failed_version.is_none() {
                    progress.failed_version = progress.current_version.take();
                }
            }
            finish(&mut state, result);
            drop(state);
            database.changed.notify_waiters();
        });
        Ok(id)
    }

    /// Return durable original SQL, without writing or overwriting runtime files.
    pub async fn export_migrations(
        &self,
        name: &str,
    ) -> Result<Vec<AppliedMigration>, SqlrestError> {
        let database = self.database(name)?;
        let (migrator, lifetime) = {
            let mut state = database.state.lock().unwrap();
            if !matches!(state.phase, Phase::Ready | Phase::Unloaded | Phase::Paused) {
                return Err(unavailable());
            }
            let migrator = migrator(&database, &state);
            state.active += 1;
            (
                migrator,
                RequestLifetime {
                    database: database.clone(),
                    _snapshot: state.snapshot.clone(),
                },
            )
        };
        // Export is a read-only management task. Keep its drain lease until the
        // executor has finished even if its caller drops this future.
        tokio::spawn(async move {
            let _lifetime = lifetime;
            let result = migrator.history().await;
            drop(migrator);
            result
        })
        .await
        .map_err(|_| worker_failed())?
    }

    /// Close admission immediately; completion waits for actual request cleanup.
    pub fn unregister(&self, name: &str) -> Result<OperationId, SqlrestError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            SqlrestError::definition("Management operations require a Tokio runtime")
        })?;
        let database = self.database(name)?;
        let id = begin(&database, OperationKind::Unregister)?;
        runtime.spawn(async move {
            loop {
                let changed = database.changed.notified();
                if database.state.lock().unwrap().active == 0 {
                    break;
                }
                changed.await;
            }
            let (resources, snapshot) = {
                let mut state = database.state.lock().unwrap();
                (state.resources.take(), state.snapshot.take())
            };
            let result = tokio::task::spawn_blocking(move || drop((resources, snapshot)))
                .await
                .map_err(|_| worker_failed());
            let mut state = database.state.lock().unwrap();
            state.phase = Phase::Unregistered;
            state.pause_reason = None;
            finish(&mut state, result);
            drop(state);
            database.changed.notify_waiters();
        });
        Ok(id)
    }

    /// Only current and most recent results are retained, including after unregister.
    pub fn operation(&self, name: &str, id: OperationId) -> Result<Operation, SqlrestError> {
        let database = self.database(name)?;
        let state = database.state.lock().unwrap();
        operation(&state, id)
    }

    /// Waiting is optional; dropping this future never cancels the operation.
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

    /// Segments must already be percent-decoded once by the transport.
    pub async fn execute(
        &self,
        name: &str,
        method: &str,
        segments: &[&str],
        mut input: Input,
    ) -> Result<Vec<u8>, SqlrestError> {
        let database = self.database(name)?;
        let (executor, matched, lifetime) = {
            let mut state = database.state.lock().unwrap();
            if state.phase != Phase::Ready {
                return Err(unavailable());
            }
            let snapshot = state.snapshot.as_ref().unwrap().clone();
            let matched = snapshot.resolve(method, segments)?;
            let executor = state.resources.as_ref().unwrap().executor.clone();
            state.active += 1;
            let lifetime = RequestLifetime {
                database: database.clone(),
                _snapshot: Some(snapshot),
            };
            (executor, matched, lifetime)
        };
        // Never trust caller-supplied path parameters over the resolved route.
        input.path = matched.path_parameters;
        executor
            .execute_tracked(matched.endpoint, input, database.config.limits, lifetime)
            .await
    }

    fn database(&self, name: &str) -> Result<Arc<Database>, SqlrestError> {
        self.entries
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| {
                SqlrestError::new(
                    404,
                    "database_not_found",
                    "Database configuration not found",
                )
            })
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

fn begin(database: &Database, kind: OperationKind) -> Result<OperationId, SqlrestError> {
    let mut state = database.state.lock().unwrap();
    if state.current.is_some() || state.phase == Phase::Registering {
        return Err(SqlrestError::new(
            409,
            "operation_in_progress",
            "A database operation is in progress",
        ));
    }
    if matches!(state.phase, Phase::Unregistered | Phase::RegistrationFailed) {
        return Err(SqlrestError::new(
            404,
            "database_not_found",
            "Database configuration is not registered",
        ));
    }
    if kind == OperationKind::Reload && state.pause_reason == Some(PauseReason::MigrationFailed) {
        return Err(SqlrestError::new(
            409,
            "migration_recovery_required",
            "Retry migrate successfully before reloading interfaces",
        ));
    }
    let id = NEXT_OPERATION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .map(OperationId)
        .map_err(|_| worker_failed())?;
    state.current = Some(Operation {
        id,
        kind,
        outcome: Outcome::Running,
        error: None,
        migration: (kind == OperationKind::Migrate).then_some(MigrationProgress {
            step: MigrationStep::Preflight,
            applied_versions: Vec::new(),
            current_version: None,
            failed_version: None,
            interfaces_reloaded: false,
        }),
    });
    if kind == OperationKind::Unregister {
        state.phase = Phase::Unregistering;
    }
    Ok(id)
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
                "Operation result is no longer retained",
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
        pause_reason: state.pause_reason,
    }
}

fn migrator(database: &Database, state: &State) -> Migrator {
    Migrator {
        executor: state
            .resources
            .as_ref()
            .expect("registered resources")
            .executor
            .clone(),
        backend: database.config.backend(),
        timeout: database.config.limits.timeout,
    }
}

fn progress(database: &Database, update: impl FnOnce(&mut MigrationProgress)) {
    let mut state = database.state.lock().unwrap();
    update(state.current.as_mut().unwrap().migration.as_mut().unwrap());
}

async fn run_migration(database: &Database) -> Result<(), SqlrestError> {
    let root = database.config.migrations.clone();
    let backend = database.config.backend();
    let plan = tokio::task::spawn_blocking(move || Plan::load(&root, backend))
        .await
        .map_err(|_| worker_failed())??;
    let migrator = migrator(database, &database.state.lock().unwrap());
    let history = migrator.history().await?;
    let plan = tokio::task::spawn_blocking(move || {
        plan.validate(&history)?;
        Ok::<_, SqlrestError>(plan)
    })
    .await
    .map_err(|_| worker_failed())??;
    let published = {
        let mut state = database.state.lock().unwrap();
        let published = state.snapshot.is_some();
        state.phase = Phase::Migrating;
        state
            .current
            .as_mut()
            .unwrap()
            .migration
            .as_mut()
            .unwrap()
            .step = MigrationStep::Draining;
        published
    };
    loop {
        let changed = database.changed.notified();
        if database.state.lock().unwrap().active == 0 {
            break;
        }
        changed.await;
    }
    // Recheck history after drain, but never re-read the deployed files.
    let history = migrator.history().await?;
    let pending = tokio::task::spawn_blocking(move || plan.into_pending(&history))
        .await
        .map_err(|_| worker_failed())??;
    for file in pending {
        let version = file.record.version;
        progress(database, |p| {
            p.step = MigrationStep::Applying;
            p.current_version = Some(version);
        });
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
    let snapshot = if published {
        progress(database, |p| p.step = MigrationStep::Reloading);
        let root = database.config.interfaces.clone();
        match tokio::task::spawn_blocking(move || Snapshot::load(&root, backend).map(Arc::new))
            .await
            .unwrap_or_else(|_| Err(worker_failed()))
        {
            Ok(snapshot) => Some(snapshot),
            Err(_) => {
                let mut state = database.state.lock().unwrap();
                state.phase = Phase::Paused;
                state.pause_reason = Some(PauseReason::ReloadFailed);
                return Err(SqlrestError::new(
                    500,
                    "migration_reload_failed",
                    "Migrations are committed but interface reload failed; fix interfaces and reload",
                ));
            }
        }
    } else {
        None
    };
    let mut state = database.state.lock().unwrap();
    let old = std::mem::replace(&mut state.snapshot, snapshot);
    state.phase = if published {
        Phase::Ready
    } else {
        Phase::Unloaded
    };
    state.pause_reason = None;
    let progress = state.current.as_mut().unwrap().migration.as_mut().unwrap();
    progress.step = MigrationStep::Complete;
    progress.interfaces_reloaded = published;
    drop(state);
    drop(old);
    Ok(())
}

async fn open_resources(config: Configuration) -> Result<Resources, SqlrestError> {
    let timeout = config.limits.timeout;
    match config.target {
        Target::Turso(path) => tokio::task::spawn_blocking(move || {
            let claim = claim_file(&path)?;
            let database = crate::turso_driver::open(&claim.path)?;
            // Runtime owns file stability; detect replacement during opening.
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
        .unwrap_or_else(|_| Err(worker_failed())),
        Target::PostgresUnencrypted(config) => {
            let (client, connection) =
                tokio::time::timeout(timeout, config.connect(tokio_postgres::NoTls))
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

fn claim_file(path: &Path) -> Result<Arc<FileClaim>, SqlrestError> {
    let claims = FILE_CLAIMS.get_or_init(|| Mutex::new(Vec::new()));
    // Serialize only identity reservation/creation, never database opening or SQL.
    let mut claims = claims.lock().unwrap();
    claims.retain(|claim| claim.strong_count() != 0);
    if let Ok(metadata) = fs::metadata(path)
        && !metadata.is_file()
    {
        return Err(file_error());
    }
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
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

fn absolute(path: &Path) -> Result<PathBuf, SqlrestError> {
    if path.as_os_str().is_empty() {
        return Err(SqlrestError::definition(
            "Configuration paths cannot be empty",
        ));
    }
    std::path::absolute(path).map_err(|_| file_error())
}

fn unavailable() -> SqlrestError {
    SqlrestError::new(
        503,
        "database_unavailable",
        "Database interfaces are not accepting requests",
    )
}

fn file_error() -> SqlrestError {
    SqlrestError::new(
        400,
        "invalid_database_path",
        "Cannot open a stable regular database file",
    )
}

fn worker_failed() -> SqlrestError {
    SqlrestError::new(
        500,
        "management_task_failed",
        "Database management worker failed",
    )
}
