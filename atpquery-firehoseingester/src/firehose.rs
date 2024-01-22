use std::io::Read;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagCborCidGeneric<const S: usize>(cid::CidGeneric<S>);

pub type DagCborCid = DagCborCidGeneric<64>;

impl<'de, const S: usize> serde::Deserialize<'de> for DagCborCidGeneric<S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut r = std::io::Cursor::new(
            ciborium::tag::Required::<Vec<u8>, 42>::deserialize(deserializer)?.0,
        );
        let mut prefix = [0u8; 1];
        r.read_exact(&mut prefix[..])
            .map_err(serde::de::Error::custom)?;
        let [prefix] = prefix;
        if prefix != 0x00 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(prefix as u64),
                &"expected multibase identity (0x00) prefix",
            ));
        }
        Ok(DagCborCidGeneric(
            cid::CidGeneric::<S>::read_bytes(r).map_err(serde::de::Error::custom)?,
        ))
    }
}

impl<const S: usize> From<cid::CidGeneric<S>> for DagCborCidGeneric<S> {
    fn from(value: cid::CidGeneric<S>) -> Self {
        Self(value)
    }
}

impl<const S: usize> From<DagCborCidGeneric<S>> for cid::CidGeneric<S> {
    fn from(value: DagCborCidGeneric<S>) -> Self {
        value.0
    }
}

impl<'a, const S: usize> From<&'a DagCborCidGeneric<S>> for &'a cid::CidGeneric<S> {
    fn from(value: &'a DagCborCidGeneric<S>) -> Self {
        &value.0
    }
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub blobs: Vec<DagCborCid>,
    #[serde(with = "serde_bytes", default)]
    pub blocks: Vec<u8>,
    pub commit: Option<DagCborCid>,
    pub ops: Vec<RepoOp>,
    pub prev: Option<DagCborCid>,
    pub rebase: bool,
    pub repo: String,
    pub seq: i64,
    #[serde(deserialize_with = "time::serde::rfc3339::deserialize")]
    pub time: time::OffsetDateTime,
    pub too_big: bool,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Handle {
    pub did: String,
    pub handle: String,
    pub seq: i64,
    #[serde(deserialize_with = "time::serde::rfc3339::deserialize")]
    pub time: time::OffsetDateTime,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub message: Option<String>,
    pub name: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Migrate {
    pub did: String,
    pub migrate_to: Option<String>,
    pub seq: i64,
    #[serde(deserialize_with = "time::serde::rfc3339::deserialize")]
    pub time: time::OffsetDateTime,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RepoOp {
    pub action: String,
    pub cid: Option<DagCborCid>,
    pub path: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Tombstone {
    pub did: String,
    pub seq: i64,
    #[serde(deserialize_with = "time::serde::rfc3339::deserialize")]
    pub time: time::OffsetDateTime,
}
pub enum Message {
    Commit(Commit),
    Handle(Handle),
    Info(Info),
    Migrate(Migrate),
    Tombstone(Tombstone),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("firehose: {error}: {message:?}")]
    Firehose {
        error: String,
        message: Option<String>,
    },

    #[error("ciborium: {0}")]
    Ciborium(#[from] ciborium::de::Error<std::io::Error>),

    #[error("unknown operation: {0}")]
    UnknownOperation(i8),

    #[error("unknown type: {0}")]
    UnknownType(String),
}

impl Message {
    pub fn parse(buf: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(buf);

        #[derive(serde::Deserialize)]
        struct Header {
            #[serde(rename = "op")]
            operation: i8,

            #[serde(rename = "t")]
            r#type: Option<String>,
        }
        let Header { operation, r#type } = ciborium::from_reader(&mut cursor)?;

        if operation == -1 {
            #[derive(serde::Deserialize)]
            struct ErrorBody {
                error: String,
                message: Option<String>,
            }
            let ErrorBody { error, message } = ciborium::from_reader(&mut cursor)?;
            return Err(Error::Firehose { error, message });
        }

        if operation != 1 {
            return Err(Error::UnknownOperation(operation));
        }

        Ok(match r#type.unwrap_or_else(|| "".to_string()).as_str() {
            "#commit" => Self::Commit(ciborium::from_reader(&mut cursor)?),
            "#handle" => Self::Handle(ciborium::from_reader(&mut cursor)?),
            "#info" => Self::Info(ciborium::from_reader(&mut cursor)?),
            "#migrate" => Self::Migrate(ciborium::from_reader(&mut cursor)?),
            "#tombstone" => Self::Tombstone(ciborium::from_reader(&mut cursor)?),
            t => return Err(Error::UnknownType(t.to_string())),
        })
    }
}
