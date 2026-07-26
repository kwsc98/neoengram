use std::str::FromStr;

use neoengram_core::ContentDigest;
use serde::Serialize;

use crate::{ProtocolError, ProtocolResult};

/// Serializes a value using the RFC 8785 JSON Canonicalization Scheme.
pub fn jcs_bytes<T: Serialize>(value: &T) -> ProtocolResult<Vec<u8>> {
    Ok(serde_json_canonicalizer::to_vec(value)?)
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
}
