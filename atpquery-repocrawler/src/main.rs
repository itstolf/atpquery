use clap::Parser;
use futures::TryStreamExt;
use prost::Message;
use prost_reflect::ReflectMessage;
use sqlx::{Connection, Executor};
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    bigquery_project: String,

    #[arg(long, default_value = "atpquery")]
    bigquery_dataset: String,

    #[arg(long, default_value = "atpquery.db")]
    db_path: String,

    #[arg(long, default_value = "https://bsky.network")]
    relay_host: String,

    #[arg(long, default_value_t = 8)]
    num_workers: usize,

    #[arg(long, default_value_t = false)]
    only_crawl_queued_repos: bool,
}

fn rewrite_tags(v: ciborium::Value) -> Result<ciborium::Value, cid::Error> {
    Ok(match v {
        ciborium::Value::Integer(_)
        | ciborium::Value::Bytes(_)
        | ciborium::Value::Float(_)
        | ciborium::Value::Text(_)
        | ciborium::Value::Bool(_)
        | ciborium::Value::Null => v,
        ciborium::Value::Tag(42, v) => match v.as_ref() {
            ciborium::Value::Bytes(v) if v.first() == Some(&0x00) => {
                ciborium::Value::Text(cid::Cid::read_bytes(&v[1..])?.to_string())
            }
            _ => ciborium::Value::Tag(42, v),
        },
        ciborium::Value::Array(vs) => ciborium::Value::Array(
            vs.into_iter()
                .map(rewrite_tags)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ciborium::Value::Map(vs) => ciborium::Value::Map(
            vs.into_iter()
                .map(|(k, v)| Ok::<_, cid::Error>((k, rewrite_tags(v)?)))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => unreachable!(),
    })
}

use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_request::{ProtoData, Rows};
use gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{AppendRowsRequest, ProtoRows, ProtoSchema};

const TABLE_NAME: &str = "raw_records";

async fn worker_main(
    relay_host: String,
    bigquery_project: String,
    bigquery_dataset: String,
    client: reqwest::Client,
    bigquery_write_client: gcloud_sdk::GoogleApi<
        BigQueryWriteClient<gcloud_sdk::GoogleAuthMiddleware>,
    >,
    rl: std::sync::Arc<governor::DefaultDirectRateLimiter>,
    rx: async_channel::Receiver<String>,
    db_conn: std::sync::Arc<tokio::sync::Mutex<sqlx::sqlite::SqliteConnection>>,
) -> Result<(), anyhow::Error> {
    loop {
        let did = rx.recv().await?;

        for attempt in 0..5 {
            if let Err(err) = {
                use gcloud_sdk::google::cloud::bigquery::storage::v1::{
                    write_stream, CreateWriteStreamRequest, WriteStream,
                };
                let write_stream = bigquery_write_client
                    .get()
                    .create_write_stream(CreateWriteStreamRequest {
                        parent: format!(
                            "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                            project = bigquery_project,
                            dataset = bigquery_dataset,
                        ),
                        write_stream: Some(WriteStream {
                            r#type: write_stream::Type::Pending as i32,
                            ..Default::default()
                        }),
                    })
                    .await?
                    .get_ref()
                    .name
                    .clone();

                let mut blockstore_loader = atproto_repo::blockstore::Loader::new();
                blockstore_loader.mst_ignore_missing(true);

                let rl = &rl;
                let relay_host = &relay_host;
                let client = &client;
                let did = did.as_str();
                let write_stream = &write_stream;
                let db_conn = std::sync::Arc::clone(&db_conn);
                let bigquery_project = bigquery_project.as_str();
                let bigquery_dataset = bigquery_dataset.as_str();
                let bigquery_write_client = bigquery_write_client.clone();

                (move || async move {
                    rl.until_ready().await;
                    let repo = tokio::time::timeout(
                        std::time::Duration::from_secs(30 * 60),
                        blockstore_loader.load(
                            &mut tokio::time::timeout(
                                std::time::Duration::from_secs(10 * 60),
                                client
                                    .get(format!(
                                        "{}/xrpc/com.atproto.sync.getRepo?did={}",
                                        relay_host, did
                                    ))
                                    .send(),
                            )
                            .await??
                            .error_for_status()?
                            .bytes_stream()
                            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                            .into_async_read(),
                        ),
                    )
                    .await??;

                    let (bqw_tx, bqw_rx) = tokio::sync::mpsc::channel(1);

                    {
                        let mut bqw_stream = bigquery_write_client
                            .get()
                            .append_rows(tokio_stream::wrappers::ReceiverStream::new(bqw_rx))
                            .await?
                            .into_inner();

                        let mut n = 0;
                        for (key, cid) in repo.key_and_cids() {
                            let key = String::from_utf8_lossy(key);
                            let parts = key.splitn(2, '/').collect::<Vec<_>>();
                            let (collection, rkey) = match parts[..] {
                                [collection, rkey] => (collection, rkey),
                                _ => {
                                    continue;
                                }
                            };

                            let block = if let Some(block) = repo.get_by_cid(cid) {
                                block
                            } else {
                                continue;
                            };

                            let row = atpquery_protos::Row {
                                repo: Some(did.to_string()),
                                collection: Some(collection.to_string()),
                                rkey: Some(rkey.to_string()),
                                rev: Some(repo.commit().rev.0 as i64),
                                record: Some(serde_json::to_string(&rewrite_tags(
                                    ciborium::from_reader::<ciborium::Value, _>(
                                        &mut std::io::Cursor::new(block),
                                    )?,
                                )?)?),
                            };

                            bqw_tx
                                .send(AppendRowsRequest {
                                    write_stream: write_stream.to_string(),
                                    rows: Some(Rows::ProtoRows(ProtoData {
                                        writer_schema: Some(ProtoSchema {
                                            proto_descriptor: Some(
                                                row.descriptor().descriptor_proto().clone(),
                                            ),
                                        }),
                                        rows: Some(ProtoRows {
                                            serialized_rows: vec![row.encode_to_vec()],
                                        }),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                })
                                .await?;

                            n += 1;

                            const BATCH_SIZE: usize = 100;
                            if n % BATCH_SIZE == 0 {
                                while n > 0 {
                                    bqw_stream.message().await?;
                                    n -= 1;
                                }
                            }
                        }

                        while n > 0 {
                            bqw_stream.message().await?;
                            n -= 1;
                        }
                    }

                    use gcloud_sdk::google::cloud::bigquery::storage::v1::{
                        BatchCommitWriteStreamsRequest, FinalizeWriteStreamRequest,
                    };
                    bigquery_write_client
                        .get()
                        .finalize_write_stream(FinalizeWriteStreamRequest {
                            name: write_stream.clone(),
                            ..Default::default()
                        })
                        .await?;
                    tracing::info!(action = "finalize", did = did);

                    {
                        let mut db_conn = db_conn.lock().await;
                        let mut tx = db_conn.begin().await?;

                        sqlx::query!(
                            r#"--sql
                            DELETE FROM pending WHERE did = ?
                            "#,
                            did
                        )
                        .execute(&mut *tx)
                        .await?;

                        sqlx::query!(
                            r#"--sql
                            INSERT INTO committing (write_stream) VALUES (?)
                            "#,
                            write_stream
                        )
                        .execute(&mut *tx)
                        .await?;

                        tx.commit().await?;
                    }

                    bigquery_write_client
                        .get()
                        .batch_commit_write_streams(BatchCommitWriteStreamsRequest {
                            parent: format!(
                                "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                                project = bigquery_project,
                                dataset = bigquery_dataset,
                            ),
                            write_streams: vec![write_stream.clone()],
                            ..Default::default()
                        })
                        .await?;

                    {
                        let mut db_conn = db_conn.lock().await;

                        sqlx::query!(
                            r#"--sql
                            DELETE FROM committing WHERE write_stream = ?
                            "#,
                            write_stream
                        )
                        .execute(&mut *db_conn)
                        .await?;
                    }

                    tracing::info!(action = "commit", write_stream = write_stream);
                    Ok::<_, anyhow::Error>(())
                })()
                .await
            } {
                let why = format!("{:?}", err);
                tracing::error!(did, why, attempt);

                if matches!(
                    err.downcast_ref::<atproto_repo::blockstore::Error>(),
                    Some(atproto_repo::blockstore::Error::MissingRootCid(_))
                ) {
                    // Try again.
                    continue;
                }

                let mut db_conn = db_conn.lock().await;
                sqlx::query!(
                    r#"--sql
                    INSERT INTO errors (did, why)
                    VALUES ($1, $2)
                    "#,
                    did,
                    why
                )
                .execute(&mut *db_conn)
                .await?;
            }
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let db_conn = std::sync::Arc::new(tokio::sync::Mutex::new(
        sqlx::sqlite::SqliteConnection::connect(&args.db_path).await?,
    ));
    {
        let mut db_conn = db_conn.lock().await;
        db_conn
            .execute(
                r#"--sql
            PRAGMA journal_mode = wal;

            CREATE TABLE IF NOT EXISTS pending (
                did TEXT NOT NULL PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS cursor (
                cursor TEXT NOT NULL PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS committing (
                write_stream TEXT NOT NULL PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS errors (
                did TEXT NOT NULL,
                why TEXT NOT NULL
            );
            "#,
            )
            .await?;
    }

    let bigquery_write_client = gcloud_sdk::GoogleApi::from_function(
            gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient::new,
            "https://bigquerystorage.googleapis.com",
            None,
        )
            .await?;

    let rl = std::sync::Arc::new(governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    )));

    let client = reqwest::Client::new();

    let (pending_tx, pending_rx) = async_channel::bounded(args.num_workers * 20);

    let workers = (0..args.num_workers)
        .map(|i| {
            tokio::spawn({
                let db_conn = std::sync::Arc::clone(&db_conn);
                let relay_host = args.relay_host.clone();
                let bigquery_project = args.bigquery_project.clone();
                let bigquery_dataset = args.bigquery_dataset.clone();
                let client = client.clone();
                let rl = std::sync::Arc::clone(&rl);
                let pending_rx = pending_rx.clone();
                let bigquery_write_client = bigquery_write_client.clone();
                async move {
                    worker_main(
                        relay_host,
                        bigquery_project,
                        bigquery_dataset,
                        client,
                        bigquery_write_client,
                        rl,
                        pending_rx,
                        db_conn,
                    )
                    .instrument(tracing::info_span!("worker", i))
                    .await
                }
            })
        })
        .collect::<Vec<_>>();

    {
        use gcloud_sdk::google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsRequest;

        let mut db_conn = db_conn.lock().await;
        let mut tx = db_conn.begin().await?;

        for write_stream in sqlx::query!(
            r#"--sql
            DELETE FROM committing RETURNING write_stream
            "#
        )
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|v| v.write_stream)
        {
            bigquery_write_client
                .get()
                .batch_commit_write_streams(BatchCommitWriteStreamsRequest {
                    parent: format!(
                        "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                        project = args.bigquery_project,
                        dataset = args.bigquery_dataset,
                    ),
                    write_streams: vec![write_stream.clone()],
                    ..Default::default()
                })
                .await?;
            tracing::info!(action = "commit", write_stream = write_stream);
        }

        tx.commit().await?;
    }

    // Write all pending stuff to queue.
    for did in {
        let mut db_conn = db_conn.lock().await;
        sqlx::query!(
            r#"--sql
            SELECT did FROM pending
            "#
        )
        .fetch_all(&mut *db_conn)
        .await?
        .into_iter()
        .map(|r| r.did)
        .collect::<Vec<_>>()
    } {
        pending_tx.send(did).await?;
    }

    'top: {
        if !args.only_crawl_queued_repos {
            let cursor = if let Some(cursor) = {
                let mut db_conn = db_conn.lock().await;
                sqlx::query!("SELECT cursor FROM cursor")
                    .fetch_optional(&mut *db_conn)
                    .await?
                    .map(|v| v.cursor)
            } {
                cursor
            } else {
                break 'top;
            };

            loop {
                let mut url = format!(
                    "{}/xrpc/com.atproto.sync.listRepos?limit=1000",
                    args.relay_host
                );
                url.push_str(&format!("&cursor={}", cursor));

                #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                #[serde(rename_all = "camelCase")]
                struct Output {
                    cursor: Option<String>,
                    repos: Vec<Repo>,
                }

                #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                #[serde(rename_all = "camelCase")]
                struct Repo {
                    did: String,
                    head: String,
                }

                rl.until_ready().await;
                let output: Output = serde_json::from_slice(
                    &client
                        .get(url)
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?,
                )?;

                let mut db_conn = db_conn.lock().await;
                let mut tx = db_conn.begin().await?;
                for repo in output.repos {
                    sqlx::query!(
                        r#"--sql
                        INSERT OR IGNORE INTO pending (did)
                        VALUES (?)
                        "#,
                        repo.did
                    )
                    .execute(&mut *tx)
                    .await?;
                    pending_tx.send(repo.did).await?;
                }

                if let Some(c) = output.cursor.as_ref() {
                    sqlx::query!(
                        r#"--sql
                        UPDATE cursor
                        SET cursor = ?
                        "#,
                        c
                    )
                    .execute(&mut *tx)
                    .await?;
                } else {
                    sqlx::query!(
                        r#"--sql
                        DELETE FROM cursor
                        "#
                    )
                    .execute(&mut *tx)
                    .await?;
                }

                tx.commit().await?;

                if output.cursor.is_none() {
                    break;
                }
            }
        }
    }

    futures::future::join_all(workers)
        .await
        .into_iter()
        .flatten()
        .collect::<Result<_, _>>()?;
    Ok(())
}
