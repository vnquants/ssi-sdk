//! Lenient serde helpers for SSI payloads.
//!
//! SSI sends numbers as strings (`"45500"`, `"1250000"`, `"1234.0"`, sometimes with thousands
//! separators `"2,493,089.5"`), sometimes as JSON numbers, and uses `""` for "no value".
//! Error messages never echo the value: payloads can carry account numbers and balances.
//!
//! Scalars are read through [`RawValue`], so a JSON number reaches [`Decimal`] as its literal
//! text and never passes through `f64`. Typed payloads must therefore be decoded from JSON text
//! (`serde_json::from_str` / `from_slice`), not from an already parsed `Value`.

use std::str::FromStr as _;

#[cfg(feature = "data")]
use chrono::NaiveDate;
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, prelude::ToPrimitive as _};
use serde::{
    Deserialize as _, Deserializer,
    de::{DeserializeOwned, Error as _},
};
use serde_json::value::RawValue;

#[cfg(feature = "data")]
use crate::time::{parse_vn_date, parse_vn_datetime};

/// Classifies the text of one JSON value: `None` for `null`, `""` or whitespace, the unquoted
/// text for strings, the literal for numbers and booleans, an error for arrays and objects.
pub(crate) fn raw_scalar(raw: &str) -> Result<Option<String>, &'static str> {
    let text = raw.trim();
    match text.as_bytes().first() {
        None => Ok(None),
        Some(b'"') => {
            let unquoted: String = serde_json::from_str(text).map_err(|_| "invalid JSON string")?;
            let trimmed = unquoted.trim();
            Ok((!trimmed.is_empty()).then(|| trimmed.to_owned()))
        }
        Some(b'[' | b'{') => Err("expected a scalar value"),
        _ if text == "null" => Ok(None),
        _ => Ok(Some(text.to_owned())),
    }
}

/// Reads a JSON scalar as trimmed text; `null` and `""` become `None`.
fn scalar_text<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    raw_scalar(raw.get()).map_err(D::Error::custom)
}

pub(crate) fn parse_decimal(text: &str) -> Option<Decimal> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(stripped) = strip_thousands_commas(text) {
        return Decimal::from_str(&stripped).ok();
    }
    Decimal::from_str(text)
        .or_else(|_| Decimal::from_scientific(text))
        .ok()
}

/// Strips ASCII thousands separators from strictly grouped decimal strings such as `"2,493,089"`
/// or `"-1,234.50"`, returning `None` when no comma is present or the grouping is malformed.
fn strip_thousands_commas(text: &str) -> Option<String> {
    if !text.contains(',') {
        return None;
    }
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((int, frac)) => {
            if frac.is_empty() || !frac.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            (int, Some(frac))
        }
        None => (unsigned, None),
    };
    let mut groups = integer.split(',');
    let first = groups.next()?;
    if first.is_empty() || first.len() > 3 || !first.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut out = String::with_capacity(text.len());
    if negative {
        out.push('-');
    }
    out.push_str(first);
    let mut group_count = 0_usize;
    for group in groups {
        if group.len() != 3 || !group.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        out.push_str(group);
        group_count += 1;
    }
    if group_count == 0 {
        return None;
    }
    if let Some(frac) = fraction {
        out.push('.');
        out.push_str(frac);
    }
    Some(out)
}

pub(crate) fn parse_quantity(text: &str) -> Option<u64> {
    let value = parse_decimal(text)?;
    if value.fract().is_zero() {
        value.to_u64()
    } else {
        None
    }
}

pub(crate) fn parse_i64(text: &str) -> Option<i64> {
    let value = parse_decimal(text)?;
    if value.fract().is_zero() {
        value.to_i64()
    } else {
        None
    }
}

#[cfg(feature = "data")]
pub(crate) fn decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    let text = scalar_text(deserializer)?.ok_or_else(|| D::Error::custom("missing decimal"))?;
    parse_decimal(&text).ok_or_else(|| D::Error::custom("invalid decimal"))
}

pub(crate) fn opt_decimal<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Decimal>, D::Error> {
    scalar_text(deserializer)?
        .map(|text| parse_decimal(&text).ok_or_else(|| D::Error::custom("invalid decimal")))
        .transpose()
}

pub(crate) fn decimal_or_zero<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Decimal, D::Error> {
    Ok(opt_decimal(deserializer)?.unwrap_or_default())
}

#[cfg(feature = "data")]
pub(crate) fn quantity<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = scalar_text(deserializer)?.ok_or_else(|| D::Error::custom("missing quantity"))?;
    parse_quantity(&text).ok_or_else(|| D::Error::custom("invalid quantity"))
}

pub(crate) fn quantity_or_zero<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    Ok(opt_quantity(deserializer)?.unwrap_or_default())
}

pub(crate) fn opt_quantity<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    scalar_text(deserializer)?
        .map(|text| parse_quantity(&text).ok_or_else(|| D::Error::custom("invalid quantity")))
        .transpose()
}

pub(crate) fn opt_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
    scalar_text(deserializer)?
        .map(|text| parse_i64(&text).ok_or_else(|| D::Error::custom("invalid integer")))
        .transpose()
}

pub(crate) fn i64_or_zero<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    Ok(opt_i64(deserializer)?.unwrap_or_default())
}

#[cfg(feature = "trading")]
pub(crate) fn bool_lenient<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    let Some(text) = scalar_text(deserializer)? else {
        return Ok(false);
    };
    if text.eq_ignore_ascii_case("true") || text == "1" {
        Ok(true)
    } else if text.eq_ignore_ascii_case("false") || text == "0" {
        Ok(false)
    } else {
        Err(D::Error::custom("invalid boolean"))
    }
}

#[cfg(feature = "data")]
pub(crate) fn vn_date<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveDate, D::Error> {
    let text = scalar_text(deserializer)?.ok_or_else(|| D::Error::custom("missing date"))?;
    parse_vn_date(&text).ok_or_else(|| D::Error::custom("invalid date"))
}

#[cfg(feature = "data")]
pub(crate) fn opt_vn_date<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<NaiveDate>, D::Error> {
    scalar_text(deserializer)?
        .map(|text| parse_vn_date(&text).ok_or_else(|| D::Error::custom("invalid date")))
        .transpose()
}

#[cfg(feature = "data")]
pub(crate) fn vn_datetime<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error> {
    let text = scalar_text(deserializer)?.ok_or_else(|| D::Error::custom("missing time"))?;
    parse_vn_datetime(&text).ok_or_else(|| D::Error::custom("invalid time"))
}

#[cfg(feature = "data")]
pub(crate) fn opt_vn_datetime<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    scalar_text(deserializer)?
        .map(|text| parse_vn_datetime(&text).ok_or_else(|| D::Error::custom("invalid time")))
        .transpose()
}

pub(crate) fn opt_epoch_millis<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    Ok(opt_i64(deserializer)?.and_then(DateTime::from_timestamp_millis))
}

pub(crate) fn string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Ok(scalar_text(deserializer)?.unwrap_or_default())
}

pub(crate) fn opt_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    scalar_text(deserializer)
}

pub(crate) fn null_as_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Decodes an optional JSON object while treating `null` and an empty `{}` as `None`.
///
/// Reads via [`RawValue`] so nested numeric fields keep their original JSON text.
pub(crate) fn opt_non_empty_object<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let raw = Option::<Box<RawValue>>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.get().trim();
    if trimmed == "null" {
        return Ok(None);
    }
    if let Some(inner) = trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}'))
        && inner.trim().is_empty()
    {
        return Ok(None);
    }
    serde_json::from_str(trimmed)
        .map(Some)
        .map_err(|_| D::Error::custom("invalid nested object"))
}

/// Formats a `Decimal` as a plain decimal string without exponent notation or unnecessary
/// trailing fractional zeros (`25.50` -> `"25.5"`, `26000.0` -> `"26000"`).
#[cfg(feature = "trading")]
pub(crate) fn format_plain_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct Sample {
        #[serde(default, deserialize_with = "decimal_or_zero")]
        price: Decimal,
        #[serde(default, deserialize_with = "opt_decimal")]
        ratio: Option<Decimal>,
        #[serde(default, deserialize_with = "quantity_or_zero")]
        volume: u64,
        #[serde(default, deserialize_with = "i64_or_zero")]
        net: i64,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct InnerObj {
        #[serde(default, deserialize_with = "i64_or_zero")]
        signed: i64,
    }

    #[derive(Debug, Deserialize)]
    struct Wrapper {
        #[serde(default, deserialize_with = "opt_non_empty_object")]
        inner: Option<InnerObj>,
    }

    #[test]
    fn strings_commas_numbers_and_blanks() {
        let sample: Sample = serde_json::from_str(
            r#"{"price":"2,493,089.5","ratio":"","volume":"1234.0","net":-2}"#,
        )
        .expect("decodes");
        assert_eq!(sample.price, dec!(2493089.5));
        assert_eq!(sample.ratio, None);
        assert_eq!(sample.volume, 1234);
        assert_eq!(sample.net, -2);

        let numeric: Sample =
            serde_json::from_str(r#"{"price":85907.33212345678912,"volume":12345678901234567890}"#)
                .expect("decodes");
        assert_eq!(numeric.price.to_string(), "85907.33212345678912");
        assert_eq!(numeric.volume, 12_345_678_901_234_567_890);
    }

    #[test]
    fn thousands_separators_are_accepted_only_when_strictly_grouped() {
        assert_eq!(parse_decimal("2,493,089"), Some(dec!(2493089)));
        assert_eq!(parse_decimal("-1,234.56"), Some(dec!(-1234.56)));
        assert_eq!(parse_quantity("1,000,000"), Some(1_000_000));
        for bad in ["1,5", "1234,567", ",123", "123,", "1,234.5,6", "1,234."] {
            assert_eq!(parse_decimal(bad), None, "{bad} must be rejected");
        }
    }

    #[test]
    fn empty_objects_become_none() {
        let empty: Wrapper = serde_json::from_str(r#"{"inner":{}}"#).expect("empty");
        assert_eq!(empty.inner, None);
        let null_val: Wrapper = serde_json::from_str(r#"{"inner":null}"#).expect("null");
        assert_eq!(null_val.inner, None);
        let filled: Wrapper =
            serde_json::from_str(r#"{"inner":{"signed":"-2,500"}}"#).expect("filled");
        assert_eq!(filled.inner, Some(InnerObj { signed: -2_500 }));
    }

    #[test]
    fn errors_never_echo_values() {
        let error = serde_json::from_str::<Sample>(r#"{"price":"003CSECRET","volume":"1"}"#)
            .expect_err("invalid");
        assert!(!error.to_string().contains("SECRET"));
        assert!(serde_json::from_str::<Sample>(r#"{"price":"1","volume":"1.5"}"#).is_err());
    }
}
