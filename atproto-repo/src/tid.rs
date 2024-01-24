use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("time: {0}")]
    Time(#[from] time::Error),

    #[error("fast32: {0}")]
    Fast32(fast32::DecodeError),

    #[error("non-zero top bit")]
    NonZeroTopBit,

    #[error("invalid length")]
    InvalidLength,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Tid {
    pub time: time::OffsetDateTime,
    pub clock_identifier: u16,
}

impl Tid {
    pub fn from_u64(v: u64) -> Result<Self, Error> {
        if v >> 63 != 0 {
            return Err(Error::NonZeroTopBit);
        }

        Ok(Self {
            time: time::OffsetDateTime::from_unix_timestamp_nanos((v >> 10) as i128 * 1000)
                .map_err(time::Error::from)?,
            clock_identifier: (v & 0b1111111111) as u16,
        })
    }

    pub fn to_u64(&self) -> u64 {
        self.clock_identifier as u64 | (((self.time.unix_timestamp_nanos() / 1000) as u64) << 10)
    }
}

fast32::make_base32_alpha!(
    BASE32_SORTABLE,
    DEC_BASE32_SORTABLE,
    b"234567abcdefghijklmnopqrstuvwxyz"
);

impl std::str::FromStr for Tid {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_u64(
            BASE32_SORTABLE.decode_u64_str(s).map_err(Error::Fast32)?,
        )?)
    }
}

impl<'de> serde::Deserialize<'de> for Tid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Tid::from_str(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            Tid::from_str("3kjphmpjr5o2e").unwrap(),
            Tid {
                time: time::OffsetDateTime::from_unix_timestamp_nanos(1706078674345076 * 1000)
                    .unwrap(),
                clock_identifier: 10
            }
        );
    }

    #[test]
    fn test_to_u64() {
        assert_eq!(
            Tid {
                time: time::OffsetDateTime::from_unix_timestamp_nanos(1706078674345076 * 1000)
                    .unwrap(),
                clock_identifier: 10
            }
            .to_u64(),
            1747024562529357834
        );
    }
}
