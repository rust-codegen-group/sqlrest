use crate::{
    SqlrestError,
    execution::{self, BoundValue, Control, Prepared},
    loader::Endpoint,
    params::Type as InputType,
    response::Cell,
};
use futures_util::{TryStreamExt, pin_mut};
use std::{sync::Arc, time::Duration};
use tokio_postgres::{
    Client, NoTls, Row,
    types::{ToSql, Type},
};

struct ConnectionTask(tokio::task::JoinHandle<()>);

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn run_request(
    config: tokio_postgres::Config,
    endpoint: Arc<Endpoint>,
    prepared: Prepared,
    control: Control,
    max_rows: usize,
) -> Result<Vec<u8>, SqlrestError> {
    let (client, connection) = control
        .wait(async { config.connect(NoTls).await.map_err(database_error) })
        .await?;
    let _connection_task = ConnectionTask(tokio::spawn(async move {
        if let Err(error) = connection.await {
            let _ = database_error(error);
        }
    }));
    let result = control
        .wait(async {
            let begin = if matches!(endpoint.method(), "get" | "head") {
                "BEGIN READ ONLY"
            } else {
                "BEGIN"
            };
            client.batch_execute(begin).await.map_err(database_error)?;
            let mut final_names = Vec::new();
            let mut final_rows = Vec::new();
            for (index, (statement, values)) in
                endpoint.statements().iter().zip(prepared).enumerate()
            {
                set_deadline(&client, &control).await?;
                let types: Vec<_> = values.iter().map(|v| parameter_type(&v.ty)).collect();
                let params: Vec<_> = values.into_iter().map(bind).collect();
                let statement = client
                    .prepare_typed(&statement.sql, &types)
                    .await
                    .map_err(database_error)?;
                let last = index + 1 == endpoint.statements().len();
                if last {
                    if statement.columns().iter().any(|c| !supported(c.type_())) {
                        return Err(SqlrestError::contract(
                            "Unsupported result type; use explicit SQL conversion",
                        ));
                    }
                    final_names = statement
                        .columns()
                        .iter()
                        .map(|c| c.name().into())
                        .collect();
                    if !final_names.is_empty() {
                        endpoint
                            .contract()
                            .ok_or_else(|| {
                                SqlrestError::contract("Result columns require a response schema")
                            })?
                            .check_columns(&final_names)?;
                    }
                }
                set_deadline(&client, &control).await?;
                let stream = client
                    .query_raw(
                        &statement,
                        params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)),
                    )
                    .await
                    .map_err(database_error)?;
                pin_mut!(stream);
                while let Some(row) = stream.try_next().await.map_err(database_error)? {
                    control.check()?;
                    if last {
                        if final_rows.len() >= max_rows {
                            return Err(execution::row_limit());
                        }
                        final_rows.push(decode(&row)?);
                    }
                }
            }
            // CPU validation/serialization must not block the async cancellation timer.
            let endpoint = endpoint.clone();
            let control = control.clone();
            tokio::task::spawn_blocking(move || {
                execution::response(&endpoint, &final_names, final_rows, false, &control)
            })
            .await
            .map_err(|_| {
                SqlrestError::new(500, "execution_task_failed", "Response worker failed")
            })?
        })
        .await;
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(error) => {
            cleanup(&client).await?;
            return Err(error);
        }
    };
    if let Err(error) = control.check() {
        cleanup(&client).await?;
        return Err(error);
    }
    if let Err(error) = control.wait(set_deadline(&client, &control)).await {
        cleanup(&client).await?;
        return Err(error);
    }
    // Past this point transport failure may mean the server committed but its
    // acknowledgement was lost. Never label it as a guaranteed rollback.
    let mut attempted = false;
    if let Err(error) = control
        .wait(async {
            attempted = true;
            client.batch_execute("COMMIT").await.map_err(database_error)
        })
        .await
    {
        let _ = cleanup(&client).await;
        return Err(if attempted {
            execution::commit_unknown().with_diagnostic("postgres", error)
        } else {
            error
        });
    }
    Ok(bytes)
}

async fn set_deadline(client: &Client, control: &Control) -> Result<(), SqlrestError> {
    let milliseconds = control
        .remaining()?
        .as_millis()
        .clamp(1, i32::MAX as u128)
        .to_string();
    client
        .query_one(
            "SELECT set_config('statement_timeout',$1,true)",
            &[&milliseconds],
        )
        .await
        .map_err(database_error)?;
    Ok(())
}

async fn cleanup(client: &Client) -> Result<(), SqlrestError> {
    // A single cancel can arrive while the backend is between protocol
    // messages and miss a query already queued by the driver. Keep cancelling
    // until ROLLBACK is acknowledged. Only ROLLBACK itself may be retried.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    loop {
        let rollback = client.batch_execute("ROLLBACK");
        tokio::pin!(rollback);
        loop {
            tokio::select! {
                biased;
                result = &mut rollback => match result {
                    Ok(()) => return Ok(()),
                    Err(error) if error.code().is_some_and(|c| c.code() == "57014") => break,
                    Err(error) => {
                        return Err(execution::cleanup_failed().with_diagnostic("postgres", error));
                    }
                },
                _ = tokio::time::sleep_until(deadline) => return Err(execution::cleanup_failed()),
                _ = tick.tick() => {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(100),
                        client.cancel_token().cancel_query(NoTls),
                    )
                    .await;
                }
            }
        }
    }
}

fn database_error(error: tokio_postgres::Error) -> SqlrestError {
    let public = match error.code().map(|s| s.code()) {
        Some("57014") => crate::turso_driver::timeout(),
        Some("25006") => SqlrestError::new(
            403,
            "read_only_violation",
            "Read-only requests cannot modify the database",
        ),
        Some("23505" | "23503" | "23502" | "23514" | "23P01") => SqlrestError::new(
            409,
            "constraint_violation",
            "Database constraint rejected the operation",
        ),
        Some("40001" | "40P01" | "55P03") => SqlrestError::new(
            409,
            "database_busy",
            "Concurrent database operation conflicted",
        ),
        _ => SqlrestError::new(500, "database_error", "Database operation failed"),
    };
    public.with_diagnostic("postgres", error)
}

fn parameter_type(ty: &InputType) -> Type {
    match ty {
        InputType::Nullable(inner) => parameter_type(inner),
        InputType::String => Type::TEXT,
        InputType::Boolean => Type::BOOL,
        InputType::Int64 => Type::INT8,
        InputType::Float64 => Type::FLOAT8,
        InputType::Array(_) => Type::JSONB,
    }
}

fn bind(bound: BoundValue) -> Box<dyn ToSql + Send + Sync> {
    let mut ty = &bound.ty;
    while let InputType::Nullable(inner) = ty {
        ty = inner;
    }
    match ty {
        InputType::String => Box::new(bound.value.as_str().map(str::to_owned)),
        InputType::Boolean => Box::new(bound.value.as_bool()),
        InputType::Int64 => Box::new(bound.value.as_i64()),
        InputType::Float64 => Box::new(bound.value.as_f64()),
        InputType::Array(_) => Box::new(if bound.value.is_null() {
            None
        } else {
            Some(bound.value)
        }),
        InputType::Nullable(_) => unreachable!(),
    }
}

fn decode(row: &Row) -> Result<Vec<Cell>, SqlrestError> {
    let mut values = Vec::new();
    for (i, column) in row.columns().iter().enumerate() {
        macro_rules! read {
            ($ty:ty,$map:expr) => {
                row.try_get::<_, Option<$ty>>(i)
                    .map_err(|error| {
                        SqlrestError::contract("Cannot decode database result type")
                            .with_diagnostic("postgres", error)
                    })?
                    .map($map)
                    .unwrap_or(Cell::Null)
            };
        }
        values.push(match *column.type_() {
            Type::BOOL => read!(bool, Cell::Boolean),
            Type::INT2 => read!(i16, |v| Cell::Integer(i64::from(v))),
            Type::INT4 => read!(i32, |v| Cell::Integer(i64::from(v))),
            Type::INT8 => read!(i64, Cell::Integer),
            Type::FLOAT4 => read!(f32, |v| Cell::Real(f64::from(v))),
            Type::FLOAT8 => read!(f64, Cell::Real),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => read!(String, Cell::Text),
            Type::JSON | Type::JSONB => read!(serde_json::Value, Cell::Json),
            _ => {
                return Err(SqlrestError::contract(
                    "Unsupported result type; use explicit SQL conversion",
                ));
            }
        });
    }
    Ok(values)
}

fn supported(ty: &Type) -> bool {
    matches!(
        *ty,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::JSON
            | Type::JSONB
    )
}

#[cfg(test)]
mod diagnostic_tests {
    #[tokio::test]
    #[ignore = "requires SQLREST_TEST_POSTGRES disposable PostgreSQL"]
    async fn real_driver_error_retains_private_diagnostics() {
        let (client, connection) = tokio_postgres::connect(
            &std::env::var("SQLREST_TEST_POSTGRES").unwrap(),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        let task = tokio::spawn(connection);
        for (sql, code, secret) in [
            (
                "SELECT private_missing_column",
                "database_error",
                "private_missing_column",
            ),
            (
                "DO $$ BEGIN RAISE EXCEPTION 'private_constraint_value' USING ERRCODE = '23514'; END $$",
                "constraint_violation",
                "private_constraint_value",
            ),
        ] {
            let error = super::database_error(client.batch_execute(sql).await.unwrap_err());
            assert_eq!(error.code, code);
            assert!(error.diagnostic().unwrap().contains(secret));
            assert!(!serde_json::to_string(&error).unwrap().contains(secret));
            assert!(!error.to_string().contains(secret));
        }
        drop(client);
        task.await.unwrap().unwrap();
    }
}
