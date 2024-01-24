mod firehose;

use clap::Parser;
use futures::{SinkExt, StreamExt};
use prost::Message;
use prost_reflect::ReflectMessage;
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    bigquery_project: String,

    #[arg(long, default_value = "atpquery")]
    bigquery_dataset: String,

    #[arg(long, default_value = "wss://bsky.network")]
    firehose_host: String,

    #[arg(long, default_value = "127.0.0.1:9000")]
    prometheus_listen: std::net::SocketAddr,
}

const CHECKPOINT_FILE_NAME: &str = "atpquery-firehoseingester.checkpoint";

#[derive(serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    write_stream: String,
    bq_seq: i64,
    firehose_seq: i64,
}

fn write_checkpoint(checkpoint: &Checkpoint) -> Result<(), anyhow::Error> {
    let mut f = std::fs::File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(format!("{CHECKPOINT_FILE_NAME}.tmp"))?;
    serde_json::to_writer(&mut f, checkpoint)?;
    f.sync_data()?;
    std::fs::rename(format!("{CHECKPOINT_FILE_NAME}.tmp"), CHECKPOINT_FILE_NAME)?;
    Ok(())
}

fn read_checkpoint() -> Result<Option<Checkpoint>, anyhow::Error> {
    let mut f = match std::fs::File::open(CHECKPOINT_FILE_NAME) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(e) => {
            return Err(e.into());
        }
    };
    Ok(serde_json::from_reader(&mut f)?)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(args.prometheus_listen)
        .install()?;

    metrics::describe_histogram!(
        "atpbq-firehoseingester.ingest_delay",
        metrics::Unit::Seconds,
        "ingestion delay"
    );

    let checkpoint = read_checkpoint()?;

    let mut url = format!(
        "{}/xrpc/com.atproto.sync.subscribeRepos",
        args.firehose_host
    );
    if let Some(cursor) = checkpoint.as_ref().map(|c| c.firehose_seq) {
        tracing::info!(cursor = cursor);
        url.push_str(&format!("?cursor={cursor}"));
    } else {
        tracing::info!("no cursor");
    }

    let mut bq_seq = checkpoint.as_ref().map(|c| c.bq_seq).unwrap_or(0);

    let bigquery_write_client = gcloud_sdk::GoogleApi::from_function(
        gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient::new,
        "https://bigquerystorage.googleapis.com",
        None,
    )
        .await?;

    let write_stream =
        if let Some(write_stream) = checkpoint.as_ref().map(|c| c.write_stream.to_string()) {
            match bigquery_write_client
                .get()
                .get_write_stream(
                    gcloud_sdk::google::cloud::bigquery::storage::v1::GetWriteStreamRequest {
                        name: write_stream.clone(),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => Some(write_stream),
                Err(e) if e.code() == tonic::Code::NotFound => None,
                Err(e) => {
                    return Err(e.into());
                }
            }
        } else {
            None
        };

    let write_stream = if let Some(write_stream) = write_stream {
        write_stream
    } else {
        const TABLE_NAME: &str = "raw_records";
        bigquery_write_client.get().create_write_stream(
            gcloud_sdk::google::cloud::bigquery::storage::v1::CreateWriteStreamRequest {
                parent: format!(
                    "projects/{project}/datasets/{dataset}/tables/{TABLE_NAME}",
                    project = args.bigquery_project,
                    dataset = args.bigquery_dataset,
                ),
                write_stream: Some(
                    gcloud_sdk::google::cloud::bigquery::storage::v1::WriteStream {
                        r#type: gcloud_sdk::google::cloud::bigquery::storage::v1::write_stream::Type::Committed as i32,
                        ..Default::default()
                    },
                ),
            },
        ).await?.get_ref().name.clone()
    };

    let (stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut tx, mut rx) = stream.split();

    let (mut bqw_tx, bqw_rx) = tokio::sync::mpsc::channel(1);
    let seqs = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::BTreeMap::new()));

    let mut task: tokio::task::JoinHandle<Result<(), anyhow::Error>> = tokio::task::spawn({
        let write_stream = write_stream.clone();
        let seqs = std::sync::Arc::clone(&seqs);

        async move {
            use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_response::Response;
            let mut stream = bigquery_write_client
                .get()
                .append_rows(tokio_stream::wrappers::ReceiverStream::new(bqw_rx))
                .await?
                .into_inner();

            while let Some(v) = stream.message().await? {
                let r = match v.response.unwrap() {
                    Response::AppendResult(r) => r,
                    Response::Error(status) => {
                        if status.code == tonic::Code::AlreadyExists as i32
                            || status.code == tonic::Code::Aborted as i32
                        {
                            continue;
                        }
                        return Err(tonic::Status::new(status.code.into(), status.message).into());
                    }
                };
                let bq_seq = r.offset.unwrap();
                let firehose_seq = {
                    let mut seqs = seqs.lock().await;
                    *seqs = seqs.split_off(&bq_seq);
                    seqs.remove(&bq_seq).unwrap()
                };
                write_checkpoint(&Checkpoint {
                    bq_seq,
                    firehose_seq,
                    write_stream: write_stream.clone(),
                })?;
            }
            Ok(())
        }
    });

    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    tx.send(tokio_tungstenite::tungstenite::Message::Ping(vec![]))
                ).await??;
            }

            r = &mut task => {
                return r?.map_err(|e| e.into());
            }

            msg = tokio::time::timeout(std::time::Duration::from_secs(60), rx.next()) => {
                let msg = if let Some(msg) = msg? {
                    msg
                } else {
                    break;
                };

                let msg = if let tokio_tungstenite::tungstenite::Message::Binary(msg) = msg? {
                    msg
                } else {
                    continue;
                };

                process_message(&msg, &write_stream, &mut bqw_tx, &mut bq_seq, &seqs)
                    .instrument(tracing::info_span!("process_message"))
                    .await?;
            }
        }
    }

    Ok(())
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

async fn process_message(
    message: &[u8],
    write_stream: &str,
    req_tx: &mut tokio::sync::mpsc::Sender<
        gcloud_sdk::google::cloud::bigquery::storage::v1::AppendRowsRequest,
    >,
    bq_seq: &mut i64,
    seqs: &std::sync::Arc<tokio::sync::Mutex<std::collections::BTreeMap<i64, i64>>>,
) -> Result<(), anyhow::Error> {
    let (_seq, time) = match firehose::Message::parse(message)? {
        firehose::Message::Info(info) => {
            tracing::info!(name = info.name, message = info.message);
            return Ok(());
        }
        firehose::Message::Commit(commit) => {
            let items = match rs_car::car_read_all(&mut &commit.blocks[..], true).await {
                Ok((parsed, _)) => parsed
                    .into_iter()
                    .collect::<std::collections::HashMap<_, _>>(),
                Err(e) => {
                    tracing::error!(error = format!("rs_car::car_read_all: {e:?}"));
                    return Ok(());
                }
            };

            let rev = commit.rev.to_u64() as i64;

            let mut rows = vec![];
            let offset = *bq_seq;

            for op in commit.ops {
                let (collection, rkey) = match op.path.splitn(2, '/').collect::<Vec<_>>()[..] {
                    [collection, rkey] => (collection, rkey),
                    _ => {
                        continue;
                    }
                };

                match op.action.as_str() {
                    "create" | "update" => {
                        let block =
                            if let Some(block) = op.cid.and_then(|cid| items.get(&cid.into())) {
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
                        rows.push(atpquery_protos::Row {
                            collection: Some(collection.to_string()),
                            repo: Some(commit.repo.clone()),
                            rkey: Some(rkey.to_string()),
                            rev: Some(rev),
                            record: Some(record.clone()),
                        });
                        tracing::info!(
                            action = op.action,
                            seq = commit.seq,
                            actor_did = commit.repo,
                            collection = collection,
                            rkey = rkey,
                            rev = ?commit.rev,
                            record = record
                        );
                    }
                    "delete" => {
                        rows.push(atpquery_protos::Row {
                            collection: Some(collection.to_string()),
                            repo: Some(commit.repo.clone()),
                            rkey: Some(rkey.to_string()),
                            rev: Some(rev),
                            record: None,
                        });
                        tracing::info!(
                            action = op.action,
                            seq = commit.seq,
                            actor_did = commit.repo,
                            collection = collection,
                            rev = ?commit.rev,
                            rkey = rkey
                        );
                    }
                    _ => {
                        continue;
                    }
                }
            }

            if let Some(first_row) = rows.first() {
                {
                    let mut seqs = seqs.lock().await;
                    seqs.insert(offset, commit.seq);
                    *bq_seq += 1;
                }

                {
                    use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_request::{
                        ProtoData, Rows,
                    };
                    use gcloud_sdk::google::cloud::bigquery::storage::v1::{
                        AppendRowsRequest, ProtoRows, ProtoSchema,
                    };

                    req_tx
                        .send(AppendRowsRequest {
                            write_stream: write_stream.to_string(),
                            offset: Some(offset),
                            rows: Some(Rows::ProtoRows(ProtoData {
                                writer_schema: Some(ProtoSchema {
                                    proto_descriptor: Some(
                                        first_row.descriptor().descriptor_proto().clone(),
                                    ),
                                }),
                                rows: Some(ProtoRows {
                                    serialized_rows: rows
                                        .into_iter()
                                        .map(|v| v.encode_to_vec())
                                        .collect(),
                                }),
                                ..Default::default()
                            })),
                            ..Default::default()
                        })
                        .await?;
                }
            }

            (commit.seq, commit.time)
        }
        firehose::Message::Tombstone(tombstone) => {
            // Delete tombstone.did from Bigquery.
            (tombstone.seq, tombstone.time)
        }
        firehose::Message::Handle(handle) => (handle.seq, handle.time),
        firehose::Message::Migrate(migrate) => (migrate.seq, migrate.time),
    };

    let now = time::OffsetDateTime::now_utc();
    metrics::histogram!(
        "atpbq-firehoseingester.ingest_delay",
        (now - time).as_seconds_f64()
    );

    Ok(())
}
