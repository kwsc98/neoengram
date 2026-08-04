use std::str::FromStr;

use neoengram_core::ContentDigest;
use serde::Serialize;
use serde_json::Value;

use crate::{ProtocolError, ProtocolResult};

/// Serializes a value using the RFC 8785 JSON Canonicalization Scheme.
pub fn jcs_bytes<T: Serialize>(value: &T) -> ProtocolResult<Vec<u8>> {
    let materialized = serde_json::to_value(value)?;
    validate_binary64_numbers(&materialized)?;
    Ok(serde_json_canonicalizer::to_vec(value)?)
}

fn validate_binary64_numbers(value: &Value) -> ProtocolResult<()> {
    match value {
        Value::Number(number) => {
            let lossless = number.as_i64().map_or_else(
                || {
                    number.as_u64().map_or_else(
                        || number.as_f64().is_some_and(f64::is_finite),
                        |integer| (integer as f64) as u128 == u128::from(integer),
                    )
                },
                |integer| (integer as f64) as i128 == i128::from(integer),
            );
            if !lossless {
                return Err(ProtocolError::InvalidField {
                    field: "canonical_json_number",
                    reason:
                        "integer must round-trip through IEEE 754 binary64 without changing value"
                            .to_owned(),
                });
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_binary64_numbers(item)?;
            }
        }
        Value::Object(object) => {
            for item in object.values() {
                validate_binary64_numbers(item)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

/// Prefixes canonical JSON with an unambiguous protocol domain for signing.
///
/// The encoded form is the non-empty printable ASCII domain, one NUL byte, then the RFC 8785
/// representation of `value`. Domains containing NUL are rejected so two protocols cannot produce
/// the same byte sequence by shifting data across the separator.
pub fn domain_separated_jcs_bytes<T: Serialize>(
    domain: &str,
    value: &T,
) -> ProtocolResult<Vec<u8>> {
    if domain.is_empty()
        || !domain
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != 0)
    {
        return Err(ProtocolError::InvalidField {
            field: "signing_domain",
            reason: "must be non-empty printable ASCII without NUL".to_owned(),
        });
    }
    let canonical = jcs_bytes(value)?;
    let mut signing_bytes = Vec::with_capacity(domain.len() + 1 + canonical.len());
    signing_bytes.extend_from_slice(domain.as_bytes());
    signing_bytes.push(0);
    signing_bytes.extend_from_slice(&canonical);
    Ok(signing_bytes)
}

/// Returns BLAKE3 over the RFC 8785 canonical JSON representation.
pub fn jcs_blake3<T: Serialize>(value: &T) -> ProtocolResult<ContentDigest> {
    let canonical = jcs_bytes(value)?;
    let encoded = blake3::hash(&canonical).to_hex().to_string();
    ContentDigest::from_str(&encoded)
        .map_err(|error| ProtocolError::InvalidDigest(error.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn jcs_sorts_nested_object_keys_and_preserves_array_order() {
        let value = json!({"z": 0, "a": {"b": 2, "a": 1}, "items": [3, 2, 1]});
        assert_eq!(
            jcs_bytes(&value).unwrap(),
            br#"{"a":{"a":1,"b":2},"items":[3,2,1],"z":0}"#
        );
    }

    #[test]
    fn jcs_uses_utf16_property_order() {
        let value = json!({"\u{1f600}": 1, "\u{fffd}": 2});
        assert_eq!(
            String::from_utf8(jcs_bytes(&value).unwrap()).unwrap(),
            "{\"😀\":1,\"�\":2}"
        );
    }

    #[test]
    fn jcs_matches_the_rfc_8785_serialization_sample() {
        let value: Value = serde_json::from_str(
            r#"{
                "numbers": [333333333.33333329, 1E30, 4.50, 2e-3, 1e-27],
                "string": "\u20ac$\u000f\nA'B\"\\\\\"/",
                "literals": [null, true, false]
            }"#,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(jcs_bytes(&value).unwrap()).unwrap(),
            r#"{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}"#
        );
    }

    #[test]
    fn jcs_matches_rfc_8785_appendix_b_number_vectors() {
        const VECTORS: &[(u64, &str)] = &[
            (0x0000_0000_0000_0000, "0"),
            (0x8000_0000_0000_0000, "0"),
            (0x0000_0000_0000_0001, "5e-324"),
            (0x8000_0000_0000_0001, "-5e-324"),
            (0x7fef_ffff_ffff_ffff, "1.7976931348623157e+308"),
            (0xffef_ffff_ffff_ffff, "-1.7976931348623157e+308"),
            (0x4340_0000_0000_0000, "9007199254740992"),
            (0xc340_0000_0000_0000, "-9007199254740992"),
            (0x4430_0000_0000_0000, "295147905179352830000"),
            (0x44b5_2d02_c7e1_4af5, "9.999999999999997e+22"),
            (0x44b5_2d02_c7e1_4af6, "1e+23"),
            (0x44b5_2d02_c7e1_4af7, "1.0000000000000001e+23"),
            (0x444b_1ae4_d6e2_ef4e, "999999999999999700000"),
            (0x444b_1ae4_d6e2_ef4f, "999999999999999900000"),
            (0x444b_1ae4_d6e2_ef50, "1e+21"),
            (0x3eb0_c6f7_a0b5_ed8c, "9.999999999999997e-7"),
            (0x3eb0_c6f7_a0b5_ed8d, "0.000001"),
            (0x41b3_de43_5555_5553, "333333333.3333332"),
            (0x41b3_de43_5555_5554, "333333333.33333325"),
            (0x41b3_de43_5555_5555, "333333333.3333333"),
            (0x41b3_de43_5555_5556, "333333333.3333334"),
            (0x41b3_de43_5555_5557, "333333333.33333343"),
            (0xbecb_f647_612f_3696, "-0.0000033333333333333333"),
            (0x4314_3ff3_c1cb_0959, "1424953923781206.2"),
        ];

        for &(bits, expected) in VECTORS {
            let value = f64::from_bits(bits);
            assert_eq!(
                String::from_utf8(jcs_bytes(&value).unwrap()).unwrap(),
                expected,
                "unexpected NumberToString output for {bits:016x}"
            );
        }
    }

    #[test]
    fn jcs_uses_ecmascript_rounding_for_extension_numbers() {
        let value: Value = serde_json::from_str(r#"{"n":-201562347225087.62}"#).unwrap();
        assert_eq!(
            String::from_utf8(jcs_bytes(&value).unwrap()).unwrap(),
            r#"{"n":-201562347225087.62}"#
        );
    }

    #[test]
    fn jcs_rejects_non_finite_numbers() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(jcs_bytes(&value).is_err());
        }
    }

    #[test]
    fn jcs_rejects_integers_that_binary64_would_round_to_another_value() {
        const MAX_CONSECUTIVE_INTEGER: u64 = 1_u64 << 53;

        assert_eq!(
            jcs_bytes(&json!({"nested": [MAX_CONSECUTIVE_INTEGER]})).unwrap(),
            br#"{"nested":[9007199254740992]}"#
        );
        assert!(matches!(
            jcs_bytes(&json!({"nested": [MAX_CONSECUTIVE_INTEGER + 1]})),
            Err(ProtocolError::InvalidField {
                field: "canonical_json_number",
                ..
            })
        ));
        assert_eq!(
            jcs_bytes(&json!({"nested": [MAX_CONSECUTIVE_INTEGER + 2]})).unwrap(),
            br#"{"nested":[9007199254740994]}"#
        );
    }

    #[test]
    fn signing_bytes_are_domain_separated() {
        let value = json!({"request_id": "request-a"});
        let first = domain_separated_jcs_bytes("neoengram.test.first.v1", &value).unwrap();
        let second = domain_separated_jcs_bytes("neoengram.test.second.v1", &value).unwrap();

        assert_ne!(first, second);
        assert_eq!(
            first,
            b"neoengram.test.first.v1\0{\"request_id\":\"request-a\"}"
        );
        assert!(domain_separated_jcs_bytes("", &value).is_err());
        assert!(domain_separated_jcs_bytes("invalid\0domain", &value).is_err());
    }
}
