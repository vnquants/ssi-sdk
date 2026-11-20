//! Exchange time handling.
//!
//! SSI sends local `Asia/Ho_Chi_Minh` wall-clock strings without an offset
//! (`2026/07/29 09:15:32`, `2026/07/29`). This module converts them to UTC and back.
//!
//! The live stream (2026-11-20) sends trade times with milliseconds
//! (`2026/11/20 09:53:33.771`) although the reference documents `YYYY/MM/DD HH:mi:ss`; quote
//! times are whole seconds. Both forms parse, and a fraction keeps its full precision.

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone as _, Utc};
use chrono_tz::Tz;

/// The exchange time zone.
pub const VN_TZ: Tz = chrono_tz::Asia::Ho_Chi_Minh;

/// Source of the current UTC time.
///
/// Token expiry checks and receive timestamps go through this trait so tests can drive time.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Current UTC time.
    fn now(&self) -> DateTime<Utc>;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

const DATE_TIME_FORMATS: [&str; 7] = [
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
];
const DATE_FORMATS: [&str; 4] = ["%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"];

/// Converts an exchange-local wall-clock time to UTC.
pub fn vn_local_to_utc(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    VN_TZ
        .from_local_datetime(&local)
        .single()
        .map(|time| time.with_timezone(&Utc))
}

/// Start of an exchange-local trading date (00:00 `Asia/Ho_Chi_Minh`) in UTC.
pub fn vn_date_start_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    vn_local_to_utc(date.and_time(NaiveTime::MIN))
}

/// The exchange-local date of a UTC instant.
pub fn vn_date_of(time: DateTime<Utc>) -> NaiveDate {
    time.with_timezone(&VN_TZ).date_naive()
}

/// Parses an SSI local date-time string (`YYYY/MM/DD HH:MM:SS`, `YYYY/MM/DD HH:MM:SS.mmm` and
/// close variants) to UTC.
///
/// Strings that carry an explicit offset (`...Z`, `...+07:00`) are honoured as such. A bare
/// date is read as 00:00 local time.
pub fn parse_vn_datetime(text: &str) -> Option<DateTime<Utc>> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(time) = DateTime::parse_from_rfc3339(text) {
        return Some(time.with_timezone(&Utc));
    }
    for format in DATE_TIME_FORMATS {
        if let Ok(local) = NaiveDateTime::parse_from_str(text, format) {
            return vn_local_to_utc(local);
        }
    }
    parse_vn_date(text).and_then(vn_date_start_utc)
}

/// Parses an SSI date string (`YYYY/MM/DD`, `YYYY-MM-DD`, `DD/MM/YYYY`).
///
/// A date-time string is accepted and its date part returned.
pub fn parse_vn_date(text: &str) -> Option<NaiveDate> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    for format in DATE_FORMATS {
        if let Ok(date) = NaiveDate::parse_from_str(text, format) {
            return Some(date);
        }
    }
    for format in DATE_TIME_FORMATS {
        if let Ok(local) = NaiveDateTime::parse_from_str(text, format) {
            return Some(local.date());
        }
    }
    None
}

/// Formats a date the way SSI query parameters expect it: `YYYY/MM/DD`.
pub fn format_vn_date(date: NaiveDate) -> String {
    date.format("%Y/%m/%d").to_string()
}

/// Formats a UTC instant as an exchange-local `YYYY/MM/DD HH:MM:SS` query parameter.
pub fn format_vn_datetime(time: DateTime<Utc>) -> String {
    time.with_timezone(&VN_TZ)
        .format("%Y/%m/%d %H:%M:%S")
        .to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike as _, Timelike as _};

    use super::*;

    #[test]
    fn local_time_is_seven_hours_ahead_of_utc() {
        let utc = parse_vn_datetime("2026/07/29 09:15:32").expect("parses");
        assert_eq!(
            (utc.year(), utc.month(), utc.day()),
            (2026, 7, 29),
            "same day"
        );
        assert_eq!((utc.hour(), utc.minute(), utc.second()), (2, 15, 32));
        assert_eq!(format_vn_datetime(utc), "2026/07/29 09:15:32");
    }

    #[test]
    fn accepts_variants_and_offsets() {
        let expected = parse_vn_datetime("2026/07/29 09:15:32");
        assert_eq!(parse_vn_datetime("2026-07-29 09:15:32"), expected);
        assert_eq!(parse_vn_datetime("29/07/2026 09:15:32"), expected);
        assert_eq!(parse_vn_datetime("2026-07-29T02:15:32Z"), expected);
        assert_eq!(parse_vn_datetime("2026-07-29T09:15:32+07:00"), expected);
        assert_eq!(parse_vn_datetime(""), None);
        assert_eq!(parse_vn_datetime("garbage"), None);
    }

    #[test]
    fn fractional_seconds_parse() {
        let utc = parse_vn_datetime("2026/11/20 09:53:33.771").expect("parses");
        assert_eq!(utc.to_rfc3339(), "2026-11-20T02:53:33.771+00:00");
        assert_eq!(
            parse_vn_datetime("2026/11/20 09:53:33.000"),
            parse_vn_datetime("2026/11/20 09:53:33")
        );
        assert_eq!(
            parse_vn_datetime("2026/11/20 09:53:33.5").map(|time| time.nanosecond()),
            Some(500_000_000)
        );
        assert_eq!(
            parse_vn_date("2026/11/20 09:53:33.771"),
            NaiveDate::from_ymd_opt(2026, 11, 20)
        );
        for bad in [
            "2026/11/20 09:53:33.",
            "2026/11/20 09:53:33.77x",
            "2026/11/20 09:53:33,771",
            "2026/11/20 24:00:00.000",
        ] {
            assert_eq!(parse_vn_datetime(bad), None, "{bad}");
        }
    }

    #[test]
    fn dates_and_day_starts() {
        let date = parse_vn_date("2026/07/29").expect("parses");
        assert_eq!(parse_vn_date("29/07/2026"), Some(date));
        assert_eq!(parse_vn_date("2026/07/29 14:45:00"), Some(date));
        assert_eq!(format_vn_date(date), "2026/07/29");
        let start = vn_date_start_utc(date).expect("valid");
        assert_eq!(start.to_rfc3339(), "2026-07-28T17:00:00+00:00");
        assert_eq!(vn_date_of(start), date);
    }
}
