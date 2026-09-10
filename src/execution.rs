//! One buffered response per transaction. No registry, pool, or HTTP dependency.
use crate::{
    SqlrestError,
    loader::Endpoint,
    params::{Input, Type},
    response::Cell,
    sql::Backend,
};
use serde_json::Value;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use turso_sdk_kit::rsapi::TursoDatabase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub timeout: Duration,
    pub max_rows: usize,
}

#[derive(Clone)]
pub struct Executor {
    database: Database,
}

#[derive(Clone)]
enum Database {
    Turso(Arc<TursoDatabase>),
    Postgres(Arc<tokio_postgres::Config>),
}

// Drop database ownership before announcing that request cleanup has completed,
// including failures during input validation.
struct TrackedDatabase<L> {
    database: Option<Database>,
    _lifetime: L,
}

impl Executor {
    pub fn turso(database: Arc<TursoDatabase>) -> Self {
        Self {
            database: Database::Turso(database),
        }
    }

    /// Explicitly unencrypted transport, for a trusted local connection/proxy.
    /// sslmode=require fails rather than being silently ignored.
    pub fn postgres_unencrypted(config: tokio_postgres::Config) -> Self {
        Self {
            database: Database::Postgres(Arc::new(config)),
        }
    }

    pub async fn execute(
        &self,
        endpoint: Arc<Endpoint>,
        input: Input,
        limits: Limits,
    ) -> Result<Vec<u8>, SqlrestError> {
        self.clone()
            .execute_tracked(endpoint, input, limits, ())
            .await
    }

    /// The lifetime token belongs to the cleanup supervisor, not the caller.
    pub(crate) async fn execute_tracked(
        self,
        endpoint: Arc<Endpoint>,
        input: Input,
        limits: Limits,
        lifetime: impl Send + 'static,
    ) -> Result<Vec<u8>, SqlrestError> {
        let backend = match self.database {
            Database::Turso(_) => Backend::Turso,
            Database::Postgres(_) => Backend::Postgres,
        };
        if endpoint.backend() != backend {
            return Err(SqlrestError::definition(
                "Endpoint and executor database backends differ",
            ));
        }
        let deadline = Instant::now()
            .checked_add(limits.timeout)
            .ok_or_else(|| SqlrestError::definition("Execution timeout is too large"))?;
        let control = Control {
            deadline,
            cancel: CancellationToken::new(),
        };
        let _cancel_on_drop = CancelOnDrop(control.cancel.clone());
        let database = self.database;
        // The worker owns cleanup even if the caller disconnects/drops this future.
        tokio::spawn(async move {
            let mut tracked = TrackedDatabase {
                database: Some(database),
                _lifetime: lifetime,
            };
            let checking_endpoint = endpoint.clone();
            let checking_control = control.clone();
            let validation = tokio::task::spawn_blocking(move || {
                prepare(&checking_endpoint, &input, &checking_control)
            });
            // No database resources exist yet; timeout may stop waiting for
            // this pure CPU task without allowing any SQL to run later.
            let prepared = control
                .wait(async {
                    validation.await.map_err(|_| {
                        SqlrestError::new(
                            500,
                            "execution_task_failed",
                            "Input validation worker failed",
                        )
                    })?
                })
                .await?;
            match tracked.database.take().unwrap() {
                Database::Turso(database) => {
                    crate::turso_driver::run_request(
                        database,
                        endpoint,
                        prepared,
                        control,
                        limits.max_rows,
                    )
                    .await
                }
                Database::Postgres(config) => {
                    crate::postgres_driver::run_request(
                        (*config).clone(),
                        endpoint,
                        prepared,
                        control,
                        limits.max_rows,
                    )
                    .await
                }
            }
        })
        .await
        .map_err(|_| SqlrestError::new(500, "execution_task_failed", "Execution worker failed"))?
    }
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Clone)]
pub(crate) struct Control {
    pub deadline: Instant,
    pub cancel: CancellationToken,
}

impl Control {
    pub fn check(&self) -> Result<(), SqlrestError> {
        if self.cancel.is_cancelled() {
            return Err(SqlrestError::new(
                499,
                "request_cancelled",
                "Request execution was cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(crate::turso_driver::timeout());
        }
        Ok(())
    }

    pub fn remaining(&self) -> Result<Duration, SqlrestError> {
        self.check()?;
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(crate::turso_driver::timeout)
    }

    pub async fn wait<T>(
        &self,
        future: impl Future<Output = Result<T, SqlrestError>>,
    ) -> Result<T, SqlrestError> {
        self.check()?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(SqlrestError::new(
                499,
                "request_cancelled",
                "Request execution was cancelled",
            )),
            _ = tokio::time::sleep_until(self.deadline.into()) => Err(crate::turso_driver::timeout()),
            result = future => result,
        }
    }
}

pub(crate) struct BoundValue {
    pub ty: Type,
    pub value: Value,
}
pub(crate) type Prepared = Vec<Vec<BoundValue>>;

fn prepare(
    endpoint: &Endpoint,
    input: &Input,
    control: &Control,
) -> Result<Prepared, SqlrestError> {
    let mut output = Vec::new();
    for statement in endpoint.statements() {
        control.check()?;
        let mut values = Vec::new();
        for parameter in &statement.parameters {
            control.check()?;
            values.push(BoundValue {
                ty: parameter.ty.clone(),
                value: parameter.read(input)?,
            });
        }
        output.push(values);
    }
    control.check()?;
    Ok(output)
}

pub(crate) fn response(
    endpoint: &Endpoint,
    names: &[String],
    rows: Vec<Vec<Cell>>,
    turso: bool,
    control: &Control,
) -> Result<Vec<u8>, SqlrestError> {
    control.check()?;
    let mut records = Vec::with_capacity(rows.len());
    if !names.is_empty() {
        let contract = endpoint
            .contract()
            .ok_or_else(|| SqlrestError::contract("Result columns require a response schema"))?;
        contract.check_columns(names)?;
        for cells in rows {
            control.check()?;
            records.push(contract.record(names, cells, turso)?);
        }
    } else if !rows.is_empty() {
        return Err(SqlrestError::contract(
            "Rows returned without column metadata",
        ));
    }
    control.check()?;
    let bytes = serde_json::to_vec(&serde_json::json!({"records":records}))
        .map_err(|_| SqlrestError::contract("Response cannot be encoded as JSON"))?;
    control.check()?;
    Ok(bytes)
}

pub(crate) fn row_limit() -> SqlrestError {
    SqlrestError::new(
        500,
        "row_limit_exceeded",
        "Result exceeds maximum returned rows",
    )
}

pub(crate) fn commit_unknown() -> SqlrestError {
    SqlrestError::new(
        500,
        "commit_outcome_unknown",
        "Commit outcome is unknown; do not blindly retry writes",
    )
}

pub(crate) fn cleanup_failed() -> SqlrestError {
    SqlrestError::new(
        500,
        "transaction_cleanup_failed",
        "Could not confirm transaction cleanup; connection discarded",
    )
}
