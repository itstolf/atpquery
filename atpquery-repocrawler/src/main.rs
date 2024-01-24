use clap::Parser;
use futures::TryStreamExt;
use sqlx::Connection;
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

async fn worker_main(
    relay_host: String,
    bigquery_project: String,
    bigquery_dataset: String,
    client: reqwest::Client,
    bigquery_write_client: gcloud_sdk::GoogleApi<gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient<gcloud_sdk::GoogleAuthMiddleware>>,
    rl: std::sync::Arc<governor::DefaultDirectRateLimiter>,
    queued_notify: std::sync::Arc<tokio::sync::Notify>,
    db_pool: sqlx::sqlite::SqlitePool,
) -> Result<(), anyhow::Error> {
    loop {
        loop {
            const TABLE_NAME: &str = "raw_records";
            let write_stream = bigquery_write_client.get().create_write_stream(
                gcloud_sdk::google::cloud::bigquery::storage::v1::CreateWriteStreamRequest {
                    parent: format!(
                        "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                        project = bigquery_project,
                        dataset = bigquery_dataset,
                    ),
                    write_stream: Some(
                        gcloud_sdk::google::cloud::bigquery::storage::v1::WriteStream {
                            r#type: gcloud_sdk::google::cloud::bigquery::storage::v1::write_stream::Type::Pending as i32,
                            ..Default::default()
                        },
                    ),
                },
            ).await?.get_ref().name.clone();
            let mut tx = db_pool.begin().await?;
            // let did = if let Some(did) = sqlx::query!(
            //     r#"--sql
            //     DELETE FROM pending
            //     WHERE
            //         did = (
            //             SELECT did
            //             FROM pending
            //             FOR UPDATE
            //             SKIP LOCKED
            //             LIMIT 1
            //         )
            //     RETURNING did
            //     "#
            // )
            // .fetch_optional(&mut *tx)
            // .await?
            // .map(|r| r.did)
            // {
            //     did
            // } else {
            //     queued_notify.notified().await;
            //     tracing::info!("wakeup");
            //     continue;
            // };
            let did = "a";

            for attempt in 0..5 {
                if let Err(err) = {
                    let mut blockstore_loader = atproto_repo::blockstore::Loader::new();
                    blockstore_loader.mst_ignore_missing(true);

                    let rl = &rl;
                    let relay_host = &relay_host;
                    let client = &client;
                    let did = did.clone();

                    let tx = &mut tx;
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

                        for (key, cid) in repo.key_and_cids() {
                            let key = String::from_utf8_lossy(key);
                            let (collection, rkey) = match key.splitn(2, '/').collect::<Vec<_>>()[..] {
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

                            let record =
                            serde_json::to_string(&rewrite_tags(ciborium::from_reader::<
                                ciborium::Value,
                                _,
                            >(
                                &mut std::io::Cursor::new(block),
                            )?)?)?;

                            // TODO: Write records.
                        }

                        Ok::<_, anyhow::Error>(())
                    })()
                    .await
                } {
                    let why = format!("{:?}", err);
                    tracing::error!(did, why, attempt);
                    match err.downcast_ref::<atproto_repo::blockstore::Error>() {
                        Some(atproto_repo::blockstore::Error::MissingRootCid(_)) => {
                            // Try again.
                            continue;
                        }
                        _ => {}
                    }
                    // sqlx::query!(
                    //     r#"--sql
                    //     INSERT INTO followscrawler.errors (did, why)
                    //     VALUES ($1, $2)
                    //     "#,
                    //     did,
                    //     why
                    // )
                    // .execute(&mut *tx)
                    // .await?;
                }
                break;
            }
            bigquery_write_client
                .get()
                .finalize_write_stream(
                    gcloud_sdk::google::cloud::bigquery::storage::v1::FinalizeWriteStreamRequest {
                        name: write_stream.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            // TODO: Save write stream name.
            tx.commit().await?;

            let mut tx = db_pool.begin().await?;
            bigquery_write_client
                .get()
                .batch_commit_write_streams(
                    gcloud_sdk::google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsRequest {
                        parent: format!(
                            "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                            project = bigquery_project,
                            dataset = bigquery_dataset,
                        ),
                        write_streams: vec![write_stream],
                        ..Default::default()
                    },
                )
                .await?;
            // TODO: Flush write stream.
            tx.commit().await?;
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

    let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&args.db_path)
        .await?;

    let bigquery_write_client = gcloud_sdk::GoogleApi::from_function(
            gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient::new,
            "https://bigquerystorage.googleapis.com",
            None,
        )
            .await?;

    let rl = std::sync::Arc::new(governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    )));

    let queued_notify = std::sync::Arc::new(tokio::sync::Notify::new());

    let client = reqwest::Client::new();

    let workers = (0..args.num_workers)
        .map(|i| {
            tokio::spawn({
                let db_pool = db_pool.clone();
                let relay_host = args.relay_host.clone();
                let bigquery_project = args.bigquery_project.clone();
                let bigquery_dataset = args.bigquery_dataset.clone();
                let client = client.clone();
                let rl = std::sync::Arc::clone(&rl);
                let queued_notify = std::sync::Arc::clone(&queued_notify);
                let bigquery_write_client = bigquery_write_client.clone();
                async move {
                    worker_main(
                        relay_host,
                        bigquery_project,
                        bigquery_dataset,
                        client,
                        bigquery_write_client,
                        rl,
                        queued_notify,
                        db_pool,
                    )
                    .instrument(tracing::info_span!("worker", i))
                    .await
                }
            })
        })
        .collect::<Vec<_>>();
    futures::future::join_all(workers)
        .await
        .into_iter()
        .flatten()
        .collect::<Result<_, _>>()?;
    Ok(())
}
