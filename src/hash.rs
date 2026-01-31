use rogue_logging::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fmt::{Debug, Display, Formatter, Write};

const HEXADECIMAL_RADIX: u32 = 16;

/// Fixed-size byte array hash.
///
/// Serializes to and from hexadecimal strings.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Hash<const N: usize> {
    bytes: [u8; N],
}

impl<const N: usize> Hash<N> {
    /// Create a new hash from a byte array
    #[must_use]
    pub fn new(bytes: [u8; N]) -> Self {
        Self { bytes }
    }

    /// Create a [`Hash`] from a hexadecimal string.
    pub fn from_string(hex: &str) -> Result<Self, Error> {
        let bytes = to_bytes(hex)?;
        Ok(Hash::new(bytes))
    }

    /// Hexadecimal string representation.
    #[must_use]
    pub fn to_hex(&self) -> String {
        self.bytes.iter().fold(String::new(), |mut acc, &b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Underlying byte array.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.bytes
    }

    /// Truncated hash with `M` bytes.
    ///
    /// Returns `None` if `M > N`.
    #[must_use]
    pub fn truncate<const M: usize>(&self) -> Option<Hash<M>> {
        let bytes: [u8; M] = self.bytes[..M].try_into().ok()?;
        Some(Hash::new(bytes))
    }
}

impl<const N: usize> Debug for Hash<N> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.to_hex())
    }
}

impl<const N: usize> Default for Hash<N> {
    fn default() -> Self {
        Self::new([0; N])
    }
}

impl<const N: usize> Display for Hash<N> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.to_hex())
    }
}

impl<const N: usize> Serialize for Hash<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de, const N: usize> Deserialize<'de> for Hash<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str = String::deserialize(deserializer)?;
        #[allow(clippy::absolute_paths)]
        Hash::from_string(&hex_str).map_err(serde::de::Error::custom)
    }
}

/// Convert a hexadecimal string to a 20-byte array.
#[allow(clippy::needless_range_loop)]
#[allow(clippy::indexing_slicing)]
fn to_bytes<const N: usize>(hex: &str) -> Result<[u8; N], Error> {
    let length = hex.len();
    if length != N * 2 {
        return Err(Error {
            action: "convert hash".to_owned(),
            message: format!("Length was not {}: {length}", N * 2),
            ..Error::default()
        });
    }
    let mut bytes = [0_u8; N];
    for i in 0..N {
        let start = i * 2;
        let byte_str = &hex[start..start + 2];
        bytes[i] = to_byte(byte_str)?;
    }
    Ok(bytes)
}

/// Convert a 2-character hexadecimal string to a byte.
fn to_byte(hex: &str) -> Result<u8, Error> {
    u8::from_str_radix(hex, HEXADECIMAL_RADIX).map_err(|_| Error {
        action: "convert hash".to_owned(),
        message: format!("Invalid hex character: {hex}"),
        ..Error::default()
    })
}
