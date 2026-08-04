use std::fmt;

use serde::de::{self, DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Value;

use crate::{Extensions, ProtocolError, ProtocolResult};

pub(crate) const CONTENT_DIGEST_PATTERN: &str = r"^[0-9a-f]{64}$";

pub(crate) fn parse_unique_json(bytes: &[u8]) -> ProtocolResult<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let UniqueJsonValue(value) = UniqueJsonValue::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

/// Decodes one bounded JSON value while recursively rejecting duplicate object members.
///
/// The byte limit is checked before parsing, allowing transport adapters to preserve duplicate-key
/// detection without first materializing an untrusted `serde_json::Value`.
pub fn decode_bounded_unique_json<T>(bytes: &[u8], max_bytes: usize) -> ProtocolResult<T>
where
    T: DeserializeOwned,
{
    if bytes.len() > max_bytes {
        return Err(ProtocolError::LimitExceeded {
            limit_name: "JSON message bytes",
            limit: max_bytes,
            actual: bytes.len(),
        });
    }
    let value = parse_unique_json(bytes)?;
    Ok(serde_json::from_value(value)?)
}

struct UniqueJsonValue(Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJsonValue)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(UniqueJsonValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(UniqueJsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object member {key:?}"
                )));
            }
            let UniqueJsonValue(value) = object.next_value()?;
            values.insert(key, value);
        }
        Ok(UniqueJsonValue(Value::Object(values)))
    }
}

pub(crate) fn validate_extension_keys(
    extensions: &Extensions,
    reserved: &[&str],
) -> ProtocolResult<()> {
    if let Some(key) = extensions
        .keys()
        .find(|key| reserved.contains(&key.as_str()))
    {
        return Err(ProtocolError::InvalidField {
            field: "extensions",
            reason: format!("extension key {key:?} collides with a protocol member"),
        });
    }
    Ok(())
}

pub(crate) fn validate_collection_limit(
    field: &'static str,
    actual: usize,
    limit: usize,
) -> ProtocolResult<()> {
    if actual > limit {
        Err(ProtocolError::LimitExceeded {
            limit_name: field,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_nonempty_limited(
    field: &'static str,
    value: &str,
    limit: usize,
) -> ProtocolResult<()> {
    let length = value.chars().count();
    if length == 0 {
        Err(ProtocolError::InvalidField {
            field,
            reason: "must not be empty".to_owned(),
        })
    } else if length > limit {
        Err(ProtocolError::LimitExceeded {
            limit_name: field,
            limit,
            actual: length,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_positive(field: &'static str, value: u64) -> ProtocolResult<()> {
    if value == 0 {
        Err(ProtocolError::InvalidField {
            field,
            reason: "must be greater than zero".to_owned(),
        })
    } else {
        Ok(())
    }
}
