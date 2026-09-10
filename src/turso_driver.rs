//! Small adapter to the official SDK, whose interruption API is not exposed by
//! the high-level Rust binding. Synchronous operations belong on blocking workers.
use crate::SqlrestError;
use std::{path::Path, sync::Arc, time::Instant};
use turso_sdk_kit::rsapi::{TursoConnection, TursoDatabase, TursoDatabaseConfig, TursoStatusCode};

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
            .map_err(database_error)?;
    }
    Ok(database)
}

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
    // The SDK query timer does not cap its busy-handler sleep. Both budgets
    // must be bounded by the same remaining request deadline.
    connection.set_busy_timeout(remaining);
    let mut statement = connection.prepare_single(sql).map_err(database_error)?;
    loop {
        match statement.step(None) {
            Ok(TursoStatusCode::Done) => return Ok(()),
            Ok(TursoStatusCode::Row) => {}
            Ok(TursoStatusCode::Io) => {
                return Err(SqlrestError::new(
                    500,
                    "database_error",
                    "Unexpected asynchronous database operation",
                ));
            }
            Err(turso_sdk_kit::rsapi::TursoError::Interrupt(_)) => return Err(timeout()),
            Err(_) if Instant::now() >= deadline => return Err(timeout()),
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

pub(crate) fn database_error(_: impl std::fmt::Display) -> SqlrestError {
    SqlrestError::new(500, "database_error", "Database operation failed")
}

pub fn request(
    database: &TursoDatabase,
    statements: &[crate::sql::Statement],
    input: &crate::params::Input,
    contract: Option<&crate::response::Contract>,
    readonly: bool,
    max_rows: usize,
    deadline: Instant,
) -> Result<Vec<u8>, SqlrestError> {
    use crate::response::Cell;
    use turso_core::{Numeric, Value};
    // Validate every parameter before acquiring a connection or executing SQL.
    let parameters = statements
        .iter()
        .map(|s| {
            s.parameters
                .iter()
                .map(|p| p.read(input))
                .collect::<Result<Vec<_>, SqlrestError>>()
        })
        .collect::<Result<Vec<_>, SqlrestError>>()?;
    let connection = database.connect().map_err(database_error)?;
    if readonly {
        execute(&connection, "PRAGMA query_only=1", deadline)?;
    }
    execute(&connection, "BEGIN", deadline)?;
    let result = (|| {
        let mut records = Vec::new();
        for (index, (sql, values)) in statements.iter().zip(parameters).enumerate() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(timeout)?;
            connection.set_query_timeout(remaining);
            let mut statement = connection
                .prepare_single(&sql.sql)
                .map_err(database_error)?;
            for (index, value) in values.into_iter().enumerate() {
                let value = match value {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::Bool(v) => Value::from_i64(i64::from(v)),
                    serde_json::Value::Number(n) => match n.as_i64() {
                        Some(v) => Value::from_i64(v),
                        None => Value::from_f64(n.as_f64().unwrap()),
                    },
                    serde_json::Value::String(s) => Value::Text(s.into()),
                    value => Value::Text(value.to_string().into()),
                };
                statement
                    .bind_positional(index + 1, value)
                    .map_err(database_error)?;
            }
            let last = index + 1 == statements.len();
            let names = (0..statement.column_count())
                .map(|i| statement.column_name(i).map_err(database_error))
                .collect::<Result<Vec<_>, SqlrestError>>()?;
            if last && !names.is_empty() {
                contract
                    .ok_or_else(|| {
                        SqlrestError::contract("Result columns require a response schema")
                    })?
                    .check_columns(&names)?;
            }
            loop {
                if Instant::now() >= deadline {
                    return Err(timeout());
                }
                match statement.step(None) {
                    Ok(TursoStatusCode::Done) => break,
                    Ok(TursoStatusCode::Row) if last => {
                        if records.len() >= max_rows {
                            return Err(SqlrestError::new(
                                500,
                                "row_limit_exceeded",
                                "Result exceeds maximum rows",
                            ));
                        }
                        let cells = (0..names.len())
                            .map(|i| {
                                Ok(match statement.row_value(i).map_err(database_error)? {
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
                            })
                            .collect::<Result<Vec<_>, SqlrestError>>()?;
                        records.push(
                            contract
                                .ok_or_else(|| SqlrestError::contract("Missing response schema"))?
                                .record(&names, cells, true)?,
                        );
                    }
                    Ok(TursoStatusCode::Row) => {}
                    Ok(TursoStatusCode::Io) => {
                        return Err(database_error("unexpected asynchronous operation"));
                    }
                    Err(turso_sdk_kit::rsapi::TursoError::Interrupt(_)) => return Err(timeout()),
                    Err(error) => return Err(database_error(error)),
                }
            }
        }
        let response =
            serde_json::to_vec(&serde_json::json!({"records":records})).map_err(database_error)?;
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        Ok(response)
    })();
    match result {
        Ok(response) => {
            // Once commit starts, an error cannot be advertised as a guaranteed rollback.
            execute(&connection, "COMMIT", deadline).map_err(|_| {
                SqlrestError::new(
                    500,
                    "commit_outcome_unknown",
                    "Commit outcome is unknown; do not blindly retry writes",
                )
            })?;
            Ok(response)
        }
        Err(error) => {
            if !connection.get_auto_commit() {
                // Cleanup has its own internal budget and is never returned to a pool.
                let _ = execute(
                    &connection,
                    "ROLLBACK",
                    Instant::now() + std::time::Duration::from_secs(5),
                );
            }
            let _ = connection.close();
            Err(error)
        }
    }
}
