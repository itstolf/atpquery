use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("fast32: {0}")]
    Fast32(fast32::DecodeError),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Tid(pub u64);

impl Tid {
    pub fn is_strictly_valid(&self) -> bool {
        (self.0 >> 63) == 0
    }

    pub fn extract_time(&self) -> Result<time::OffsetDateTime, time::error::ComponentRange> {
        time::OffsetDateTime::from_unix_timestamp_nanos((self.0 >> 10) as i128 * 1000)
    }

    pub fn clock_identifier(&self) -> u16 {
        (self.0 & 0b1111111111) as u16
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
        Ok(Self(
            BASE32_SORTABLE.decode_u64_str(s).map_err(Error::Fast32)?,
        ))
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
        let tid = Tid::from_str("3kjphmpjr5o2e").unwrap();
        assert_eq!(
            tid.extract_time().unwrap(),
            time::OffsetDateTime::from_unix_timestamp_nanos(1706078674345076 * 1000).unwrap(),
        );
        assert_eq!(tid.clock_identifier(), 10,);
    }
}
