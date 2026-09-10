//! Official SDK adapter. Database work is synchronous and belongs on blocking workers.
use crate::{
    SqlrestError,
    execution::{self, BoundValue, Control, Prepared},
    loader::Endpoint,
    params::Type,
    response::Cell,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use turso_core::{Numeric, Value};
use turso_sdk_kit::rsapi::{
    TursoConnection, TursoDatabase, TursoDatabaseConfig, TursoError, TursoStatusCode,
};

pub fn open(path: &Path) -> Result<Arc<TursoDatabase>, SqlrestError> {
    let database = TursoDatabase::new(TursoDatabaseConfig {
        path: path
            .to_str()
            .ok_or_else(|| SqlrestError::definition("Database path must be UTF-8"))?
            .into(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: Default::default(),
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: Default::default(),
    });
    while let Some(completion) = database.open().map_err(database_error)?.io() {
        completion
            .wait(database.io().map_err(database_error)?.as_ref())
            .map_err(|error| database_error_generic().with_diagnostic("turso", error))?;
    }
    Ok(database)
}

/// Small synchronous primitive, also used by capability tests.
pub fn execute(
    connection: &TursoConnection,
    sql: &str,
    deadline: Instant,
) -> Result<(), SqlrestError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(timeout)?;
    connection.set_query_timeout(remaining);
    connection.set_busy_timeout(remaining);
    let mut statement = connection.prepare_single(sql).map_err(database_error)?;
    loop {
        match statement.step(None) {
            Ok(TursoStatusCode::Done) => return Ok(()),
            Ok(TursoStatusCode::Row) => {}
            Ok(TursoStatusCode::Io) => return Err(database_error_generic()),
            Err(error) if Instant::now() >= deadline => {
                return Err(timeout().with_diagnostic("turso", error));
            }
            Err(error) => return Err(database_error(error)),
        }
        if Instant::now() >= deadline {
            return Err(timeout());
        }
    }
}

pub fn timeout() -> SqlrestError {
    SqlrestError::new(
        504,
        "execution_timeout",
        "Request execution deadline exceeded",
    )
}

fn database_error_generic() -> SqlrestError {
    SqlrestError::new(500, "database_error", "Database operation failed")
}

fn database_error(error: TursoError) -> SqlrestError {
    // This pinned SDK reports query_only rejection during prepare as a generic
    // parse error rather than its structured Readonly variant.
    if matches!(&error, TursoError::Error(message) if message == "Parse error: Cannot execute write statement in query_only mode")
    {
        return SqlrestError::new(
            403,
            "read_only_violation",
            "Read-only requests cannot modify the database",
        )
        .with_diagnostic("turso", error);
    }
    let public = match &error {
        TursoError::Interrupt(_) => timeout(),
        TursoError::Readonly(_) => SqlrestError::new(
            403,
            "read_only_violation",
            "Read-only requests cannot modify the database",
        ),
        TursoError::Constraint(_) => SqlrestError::new(
            409,
            "constraint_violation",
            "Database constraint rejected the operation",
        ),
        TursoError::Busy(_) | TursoError::BusySnapshot(_) => {
            SqlrestError::new(409, "database_busy", "Database is busy")
        }
        _ => database_error_generic(),
    };
    public.with_diagnostic("turso", error)
}

struct CloseConnection(Arc<TursoConnection>);

impl Drop for CloseConnection {
    fn drop(&mut self) {
        let _ = self.0.close();
    }
}

pub(crate) async fn run_request(
    database: Arc<TursoDatabase>,
    endpoint: Arc<Endpoint>,
    prepared: Prepared,
    control: Control,
    max_rows: usize,
) -> Result<Vec<u8>, SqlrestError> {
    let connection_slot = Arc::new(Mutex::new(None::<Arc<TursoConnection>>));
    let watched_slot = connection_slot.clone();
    let watched_control = control.clone();
    let watchdog = tokio::spawn(async move {
        tokio::select! {
            _ = watched_control.cancel.cancelled() => {},
            _ = tokio::time::sleep_until(watched_control.deadline.into()) => {},
        }
        loop {
            if let Some(connection) = watched_slot.lock().unwrap().as_ref() {
                connection.interrupt();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let worker_control = control.clone();
    let started = Arc::new(Mutex::new(false));
    let worker_started = started.clone();
    let mut worker = tokio::task::spawn_blocking(move || {
        *worker_started.lock().unwrap() = true;
        request(
            &database,
            &endpoint,
            prepared,
            &worker_control,
            max_rows,
            &connection_slot,
        )
    });
    let finished = tokio::select! {
        result = &mut worker => Some(result),
        _ = control.cancel.cancelled() => None,
        _ = tokio::time::sleep_until(control.deadline.into()) => None,
    };
    let result = if let Some(result) = finished {
        result
    } else {
        // This cancels a queued blocking job. Running jobs cannot be aborted:
        // they are interrupted above and must finish transaction cleanup.
        worker.abort();
        {
            let started = started.lock().unwrap();
            if !*started {
                // A queued spawn_blocking JoinHandle may not resolve until a
                // worker becomes available. Under this gate no DB work has
                // started; cancel before releasing it and return immediately.
                let error = control.check().err().unwrap_or_else(timeout);
                control.cancel.cancel();
                watchdog.abort();
                return Err(error);
            }
        }
        worker.await
    }
    .map_err(|error| {
        if error.is_cancelled() {
            control.check().err().unwrap_or_else(timeout)
        } else {
            SqlrestError::new(500, "execution_task_failed", "Database worker failed")
        }
    });
    watchdog.abort();
    result?
}

fn request(
    database: &TursoDatabase,
    endpoint: &Endpoint,
    prepared: Prepared,
    control: &Control,
    max_rows: usize,
    slot: &Mutex<Option<Arc<TursoConnection>>>,
) -> Result<Vec<u8>, SqlrestError> {
    control.check()?;
    let connection = database.connect().map_err(database_error)?;
    let _close = CloseConnection(connection.clone());
    *slot.lock().unwrap() = Some(connection.clone());
    control.check()?;
    let result = (|| {
        execute(&connection, "PRAGMA foreign_keys=1", control.deadline)?;
        if matches!(endpoint.method(), "get" | "head") {
            execute(&connection, "PRAGMA query_only=1", control.deadline)?;
        }
        control.check()?;
        execute(&connection, "BEGIN", control.deadline)?;
        let mut final_names = Vec::new();
        let mut final_rows = Vec::new();
        for (index, (sql, values)) in endpoint.statements().iter().zip(prepared).enumerate() {
            let remaining = control.remaining()?;
            connection.set_query_timeout(remaining);
            connection.set_busy_timeout(remaining);
            let mut statement = connection
                .prepare_single(&sql.sql)
                .map_err(database_error)?;
            for (index, value) in values.into_iter().enumerate() {
                statement
                    .bind_positional(index + 1, bind(value))
                    .map_err(database_error)?;
            }
            let last = index + 1 == endpoint.statements().len();
            let columns = statement.column_count();
            if last {
                final_names = (0..columns)
                    .map(|i| statement.column_name(i).map_err(database_error))
                    .collect::<Result<_, SqlrestError>>()?;
                if !final_names.is_empty() {
                    endpoint
                        .contract()
                        .ok_or_else(|| {
                            SqlrestError::contract("Result columns require a response schema")
                        })?
                        .check_columns(&final_names)?;
                }
            }
            loop {
                control.check()?;
                match statement.step(None).map_err(database_error)? {
                    TursoStatusCode::Done => break,
                    TursoStatusCode::Row if last => {
                        if final_rows.len() >= max_rows {
                            return Err(execution::row_limit());
                        }
                        let cells = (0..columns)
                            .map(|i| cell(statement.row_value(i).map_err(database_error)?))
                            .collect::<Result<_, SqlrestError>>()?;
                        final_rows.push(cells);
                    }
                    TursoStatusCode::Row => {}
                    TursoStatusCode::Io => return Err(database_error_generic()),
                }
            }
        }
        execution::response(endpoint, &final_names, final_rows, true, control)
    })();
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(error) => {
            let original = control.check().err().unwrap_or(error);
            *slot.lock().unwrap() = None;
            cleanup(&connection)?;
            return Err(original);
        }
    };
    if let Err(error) = control.check() {
        *slot.lock().unwrap() = None;
        cleanup(&connection)?;
        return Err(error);
    }
    // Prepare/check before attempting COMMIT: a deadline discovered here is a
    // definite pre-commit failure, not an unknown commit outcome.
    let commit = (|| {
        let remaining = control.remaining()?;
        connection.set_query_timeout(remaining);
        connection.set_busy_timeout(remaining);
        let statement = connection
            .prepare_single("COMMIT")
            .map_err(database_error)?;
        control.check()?;
        Ok(statement)
    })();
    let mut commit = match commit {
        Ok(statement) => statement,
        Err(error) => {
            *slot.lock().unwrap() = None;
            cleanup(&connection)?;
            return Err(error);
        }
    };
    let committed = match commit.step(None) {
        Ok(TursoStatusCode::Done) => Ok(()),
        result => Err(execution::commit_unknown().with_diagnostic("turso", result)),
    };
    drop(commit);
    if let Err(error) = committed {
        *slot.lock().unwrap() = None;
        let _ = cleanup(&connection);
        return Err(error);
    }
    Ok(bytes)
}

fn cleanup(connection: &TursoConnection) -> Result<(), SqlrestError> {
    if !connection.get_auto_commit() {
        execute(
            connection,
            "ROLLBACK",
            Instant::now() + Duration::from_secs(5),
        )
        .map_err(|_| execution::cleanup_failed())?;
    }
    if !connection.get_auto_commit() {
        return Err(execution::cleanup_failed());
    }
    Ok(())
}

fn bind(bound: BoundValue) -> Value {
    fn value(ty: &Type, v: serde_json::Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        match ty {
            Type::Nullable(inner) => value(inner, v),
            Type::String => Value::Text(v.as_str().unwrap().to_owned().into()),
            Type::Boolean => Value::from_i64(i64::from(v.as_bool().unwrap())),
            Type::Int64 => Value::from_i64(v.as_i64().unwrap()),
            Type::Float64 => Value::from_f64(v.as_f64().unwrap()),
            Type::Array(_) => Value::Text(v.to_string().into()),
        }
    }
    value(&bound.ty, bound.value)
}

fn cell(value: Value) -> Result<Cell, SqlrestError> {
    Ok(match value {
        Value::Null => Cell::Null,
        Value::Numeric(Numeric::Integer(v)) => Cell::Integer(v),
        Value::Numeric(Numeric::Float(v)) => Cell::Real(f64::from(v)),
        Value::Text(v) => Cell::Text(v.as_str().into()),
        Value::Blob(_) => {
            return Err(SqlrestError::contract(
                "Binary columns require explicit SQL encoding",
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn real_driver_error_retains_private_diagnostics() {
        let directory = tempfile::tempdir().unwrap();
        let database = super::open(&directory.path().join("diagnostics.db")).unwrap();
        let connection = database.connect().unwrap();
        let error = super::execute(
            &connection,
            "SELECT private_missing_column",
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .unwrap_err();
        assert_eq!(error.code, "database_error");
        assert!(
            error
                .diagnostic()
                .unwrap()
                .contains("private_missing_column")
        );
        assert!(
            !serde_json::to_string(&error)
                .unwrap()
                .contains("private_missing_column")
        );
        connection.close().unwrap();
    }
}
