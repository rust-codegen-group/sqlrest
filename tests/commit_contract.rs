//! Real wire fault: forward COMMIT, observe its server acknowledgement, then
//! close the client socket before delivering that acknowledgement.
use sqlrest::{
    execution::{Executor, Limits},
    loader::Snapshot,
    params::Input,
    sql::Backend,
};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::NoTls;

async fn message(reader: &mut (impl AsyncRead + Unpin)) -> std::io::Result<(u8, Vec<u8>)> {
    let tag = reader.read_u8().await?;
    let length = reader.read_u32().await?;
    if !(4..1_048_576).contains(&length) {
        return Err(std::io::Error::other("invalid test protocol message"));
    }
    let mut body = vec![0; length as usize - 4];
    reader.read_exact(&mut body).await?;
    Ok((tag, body))
}

async fn send(writer: &mut (impl AsyncWrite + Unpin), tag: u8, body: &[u8]) -> std::io::Result<()> {
    writer.write_u8(tag).await?;
    writer.write_u32(body.len() as u32 + 4).await?;
    writer.write_all(body).await
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; injects a connection loss after COMMIT"]
async fn committed_write_with_lost_ack_is_unknown_not_rollback() {
    let url = std::env::var("SQLREST_TEST_POSTGRES").expect("Set SQLREST_TEST_POSTGRES");
    let (observer, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let table = format!("sqlrest_commit_probe_{}", std::process::id());
    observer
        .batch_execute(&format!("CREATE TABLE {table}(id BIGINT PRIMARY KEY)"))
        .await
        .unwrap();
    let target = url::Url::parse(&url).unwrap();
    let address = (
        target.host_str().unwrap().to_owned(),
        target.port().unwrap_or(5432),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (mut frontend, _) = listener.accept().await.unwrap();
        let mut backend = TcpStream::connect(address).await.unwrap();
        // NoTLS sends StartupMessage, which has no leading tag byte.
        let length = frontend.read_u32().await.unwrap();
        assert!((8..1_048_576).contains(&length));
        let mut startup = vec![0; length as usize - 4];
        frontend.read_exact(&mut startup).await.unwrap();
        backend.write_u32(length).await.unwrap();
        backend.write_all(&startup).await.unwrap();
        let (mut front_read, mut front_write) = frontend.into_split();
        let (mut back_read, mut back_write) = backend.into_split();
        let mut forward = tokio::spawn(async move {
            while let Ok((tag, body)) = message(&mut front_read).await {
                if send(&mut back_write, tag, &body).await.is_err() {
                    break;
                }
            }
        });
        let mut committed_tx = Some(committed_tx);
        loop {
            tokio::select! {
                _ = &mut forward => break,
                result = message(&mut back_read) => {
                    let Ok((tag,body)) = result else { break; };
                    if tag == b'C' && body == b"COMMIT\0" {
                        committed_tx.take().unwrap().send(()).unwrap();
                        break; // The server committed; deliberately lose its acknowledgement.
                    }
                    if send(&mut front_write,tag,&body).await.is_err() { break; }
                }
            }
        }
        forward.abort();
    });
    let mut proxy_url = target;
    proxy_url.set_host(Some("127.0.0.1")).unwrap();
    proxy_url.set_port(Some(port)).unwrap();
    let executor = Executor::postgres_unencrypted(proxy_url.as_str().parse().unwrap());
    let snapshot = Snapshot::from_files(
        BTreeMap::from([("post.sql".into(), format!("INSERT INTO {table} VALUES(1)"))]),
        Backend::Postgres,
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        executor.execute(
            snapshot.endpoints()[0].clone(),
            Input::default(),
            Limits {
                timeout: Duration::from_secs(3),
                max_rows: 0,
            },
        ),
    )
    .await
    .unwrap();
    committed_rx.await.unwrap();
    assert_eq!(result.unwrap_err().code, "commit_outcome_unknown");
    assert_eq!(
        observer
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    observer
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    proxy.await.unwrap();
}

#[tokio::test]
async fn connection_handshake_is_in_the_request_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let executor = Executor::postgres_unencrypted(
        format!("host=127.0.0.1 port={port} user=test sslmode=disable")
            .parse()
            .unwrap(),
    );
    let snapshot = Snapshot::from_files(
        BTreeMap::from([("post.sql".into(), "DELETE FROM items".into())]),
        Backend::Postgres,
    )
    .unwrap();
    let error = executor
        .execute(
            snapshot.endpoints()[0].clone(),
            Input::default(),
            Limits {
                timeout: Duration::from_millis(30),
                max_rows: 0,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "execution_timeout");
    server.abort();
}
