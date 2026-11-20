//! OHLCV bars: `GET /api/v3/data/ohlc`
//! (<https://developers.ssi.com.vn/docs/api-reference/data-ohlc>).
//!
//! Observed behaviour (live check 2026-11-20):
//!
//! - `from`/`to` must carry a time (`YYYY/MM/DD HH:mm:ss`) for every timeframe, including `1d`;
//!   a bare date is rejected with `400213` although the reference says daily takes `YYYY/MM/DD`.
//! - Bars come newest first. The response has `pageIndex`/`pageSize` but no page count, so the
//!   walker stops at the first short page.
//! - Daily prices are adjusted (they carry fractions of a dong, e.g. `85907.332`); intraday
//!   prices are raw integers in dong.
//! - Intraday history covers the most recent year (an older intraday day returns no bars, not
//!   an error); daily history starts at listing (VNM: 5189 bars from 2006-01-19).
//! - `30m` is rejected with `400210` (`Invalid Timeframe`) although the reference lists it.
//! - Intraday bars start at 09:15 local (the opening auction match); `1h` bars are stamped on the
//!   hour (09:00); the last bar of the day is the 14:45 closing auction.

use std::fmt;

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::decode;
use crate::{
    SsiClient,
    error::{Error, Result},
    http::Sensitivity,
    model::check_symbol,
    time::{VN_TZ, vn_date_of, vn_local_to_utc},
};

const ENDPOINT: &str = "/api/v3/data/ohlc";
/// Largest page size this crate requests (1000 was accepted live).
pub const MAX_PAGE_SIZE: u32 = 1000;
/// Guard against a server that ignores `pageIndex`.
const MAX_PAGES: u32 = 10_000;
/// Intraday history depth documented by SSI.
pub const INTRADAY_HISTORY_DAYS: i64 = 365;

/// Bar timeframe, as documented. Production rejected `30m` with `400210` on 2026-11-20; the
/// other six were accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    /// 1 minute.
    #[serde(rename = "1m")]
    Minute1,
    /// 3 minutes.
    #[serde(rename = "3m")]
    Minute3,
    /// 5 minutes.
    #[serde(rename = "5m")]
    Minute5,
    /// 15 minutes.
    #[serde(rename = "15m")]
    Minute15,
    /// 30 minutes (documented, but rejected by production with `400210`).
    #[serde(rename = "30m")]
    Minute30,
    /// 1 hour.
    #[serde(rename = "1h")]
    Hour1,
    /// 1 day.
    #[serde(rename = "1d")]
    Day1,
}

impl Timeframe {
    /// Every timeframe.
    pub const ALL: [Self; 7] = [
        Self::Minute1,
        Self::Minute3,
        Self::Minute5,
        Self::Minute15,
        Self::Minute30,
        Self::Hour1,
        Self::Day1,
    ];

    /// The `timeFrame` query value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minute1 => "1m",
            Self::Minute3 => "3m",
            Self::Minute5 => "5m",
            Self::Minute15 => "15m",
            Self::Minute30 => "30m",
            Self::Hour1 => "1h",
            Self::Day1 => "1d",
        }
    }

    /// Whether this is an intraday timeframe (limited to the most recent year).
    pub const fn is_intraday(self) -> bool {
        !matches!(self, Self::Day1)
    }

    /// Whether live SSI V3 servers accept this timeframe (`30m` is documented in the API
    /// reference but rejected live with `400210`).
    pub const fn is_supported_live(self) -> bool {
        !matches!(self, Self::Minute30)
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Timeframe {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|timeframe| timeframe.as_str().eq_ignore_ascii_case(text.trim()))
            .ok_or_else(|| Error::InvalidRequest(format!("unknown timeframe {text:?}")))
    }
}

/// One OHLCV bar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bar {
    /// Symbol.
    pub symbol: String,
    /// Bar time as sent (`tradingDate`), converted from `Asia/Ho_Chi_Minh` to UTC. For daily
    /// bars this is 00:00 local time of the trading date.
    pub time: DateTime<Utc>,
    /// Exchange-local trading date.
    pub trading_date: NaiveDate,
    /// Open.
    pub open: Decimal,
    /// High.
    pub high: Decimal,
    /// Low.
    pub low: Decimal,
    /// Close.
    pub close: Decimal,
    /// Volume in shares.
    pub volume: u64,
    /// Traded value in dong.
    pub value: Decimal,
}

#[derive(Deserialize)]
struct WireBar {
    #[serde(deserialize_with = "crate::de::string")]
    symbol: String,
    #[serde(rename = "tradingDate", deserialize_with = "crate::de::vn_datetime")]
    time: DateTime<Utc>,
    #[serde(deserialize_with = "crate::de::decimal")]
    open: Decimal,
    #[serde(deserialize_with = "crate::de::decimal")]
    high: Decimal,
    #[serde(deserialize_with = "crate::de::decimal")]
    low: Decimal,
    #[serde(deserialize_with = "crate::de::decimal")]
    close: Decimal,
    #[serde(deserialize_with = "crate::de::quantity")]
    volume: u64,
    #[serde(default, deserialize_with = "crate::de::opt_decimal")]
    value: Option<Decimal>,
}

impl From<WireBar> for Bar {
    fn from(wire: WireBar) -> Self {
        Self {
            symbol: wire.symbol,
            trading_date: vn_date_of(wire.time),
            time: wire.time,
            open: wire.open,
            high: wire.high,
            low: wire.low,
            close: wire.close,
            volume: wire.volume,
            value: wire.value.unwrap_or_default(),
        }
    }
}

#[derive(Deserialize)]
struct WirePage {
    #[serde(rename = "pageIndex", default, deserialize_with = "crate::de::opt_i64")]
    page_index: Option<i64>,
    #[serde(rename = "pageSize", default, deserialize_with = "crate::de::opt_i64")]
    page_size: Option<i64>,
    #[serde(default)]
    data: Option<Vec<WireBar>>,
}

/// One page of bars, in server order (newest first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OhlcPage {
    /// Bars of this page.
    pub bars: Vec<Bar>,
    /// `pageIndex` echoed by the server.
    pub page_index: Option<i64>,
    /// `pageSize` echoed by the server.
    pub page_size: Option<i64>,
}

/// An OHLC query: symbol, timeframe and an exchange-local time range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OhlcQuery {
    symbol: String,
    timeframe: Timeframe,
    from: NaiveDateTime,
    to: NaiveDateTime,
    page_size: u32,
}

impl OhlcQuery {
    /// Whole exchange-local days `from..=to` (00:00:00 to 23:59:59 `Asia/Ho_Chi_Minh`).
    pub fn days(
        symbol: &str,
        timeframe: Timeframe,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Self> {
        let end = NaiveTime::from_hms_opt(23, 59, 59).unwrap_or(NaiveTime::MIN);
        Self::local(
            symbol,
            timeframe,
            from.and_time(NaiveTime::MIN),
            to.and_time(end),
        )
    }

    /// A UTC range, converted to exchange-local time.
    pub fn between(
        symbol: &str,
        timeframe: Timeframe,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Self> {
        Self::local(
            symbol,
            timeframe,
            from.with_timezone(&VN_TZ).naive_local(),
            to.with_timezone(&VN_TZ).naive_local(),
        )
    }

    fn local(
        symbol: &str,
        timeframe: Timeframe,
        from: NaiveDateTime,
        to: NaiveDateTime,
    ) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "OHLC range: from is after to".to_owned(),
            ));
        }
        Ok(Self {
            symbol: check_symbol(symbol)?,
            timeframe,
            from,
            to,
            page_size: MAX_PAGE_SIZE,
        })
    }

    /// Bars per page, `1..=1000` (default 1000).
    pub fn with_page_size(mut self, page_size: u32) -> Result<Self> {
        if !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(Error::InvalidRequest(format!(
                "page size must be 1..={MAX_PAGE_SIZE}"
            )));
        }
        self.page_size = page_size;
        Ok(self)
    }

    /// Symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Timeframe.
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Range start (exchange-local).
    pub const fn from(&self) -> NaiveDateTime {
        self.from
    }

    /// Range end (exchange-local).
    pub const fn to(&self) -> NaiveDateTime {
        self.to
    }

    /// Fits the range to what SSI serves at `now`: an intraday start older than one year is
    /// moved to one year ago, and an intraday end in the future to the end of today (SSI
    /// returns nothing at all for a future intraday end). `None` when nothing is left.
    pub fn clamped(&self, now: DateTime<Utc>) -> Option<Self> {
        if !self.timeframe.is_intraday() {
            return Some(self.clone());
        }
        let local_now = now.with_timezone(&VN_TZ).naive_local();
        let today_end = local_now
            .date()
            .and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap_or(NaiveTime::MIN));
        let earliest = (local_now - ChronoDuration::days(INTRADAY_HISTORY_DAYS))
            .date()
            .and_time(NaiveTime::MIN);
        let from = self.from.max(earliest);
        let to = self.to.min(today_end);
        (from <= to).then(|| Self {
            from,
            to,
            ..self.clone()
        })
    }

    fn params(&self, page_index: u32) -> Vec<(&'static str, String)> {
        vec![
            ("symbol", self.symbol.clone()),
            ("from", self.from.format("%Y/%m/%d %H:%M:%S").to_string()),
            ("to", self.to.format("%Y/%m/%d %H:%M:%S").to_string()),
            ("timeFrame", self.timeframe.as_str().to_owned()),
            ("pageIndex", page_index.to_string()),
            ("pageSize", self.page_size.to_string()),
        ]
    }

    /// Range start in UTC.
    pub fn from_utc(&self) -> Option<DateTime<Utc>> {
        vn_local_to_utc(self.from)
    }
}

pub(crate) fn parse_page(body: Option<&[u8]>) -> Result<OhlcPage> {
    let Some(body) = body else {
        return Ok(OhlcPage {
            bars: Vec::new(),
            page_index: None,
            page_size: None,
        });
    };
    let page: WirePage = decode(ENDPOINT, body, Sensitivity::Public)?;
    Ok(OhlcPage {
        bars: page
            .data
            .unwrap_or_default()
            .into_iter()
            .map(Bar::from)
            .collect(),
        page_index: page.page_index,
        page_size: page.page_size,
    })
}

impl SsiClient {
    /// One page of bars (1-based `page_index`), exactly as the server pages them.
    pub async fn ohlc_page(&self, query: &OhlcQuery, page_index: u32) -> Result<OhlcPage> {
        if page_index == 0 {
            return Err(Error::InvalidRequest("pageIndex starts at 1".to_owned()));
        }
        let body = self
            .get_body(ENDPOINT, &query.params(page_index), Sensitivity::Public)
            .await?;
        parse_page(body.as_deref())
    }

    /// Every bar in the query range, oldest first, walking all pages.
    ///
    /// The endpoint reports no page count. The walker stops at an empty page, or at a page
    /// shorter than the page size the server echoes (`pageSize`; production echoes it, and a
    /// server that caps the size below the request is followed). Without an echoed size it
    /// walks until an empty page.
    ///
    /// The range is first fitted with [`OhlcQuery::clamped`] (intraday: last year, not past
    /// today); a warning is logged when that cuts the requested range.
    pub async fn ohlc(&self, query: &OhlcQuery) -> Result<Vec<Bar>> {
        let Some(fitted) = query.clamped(self.now()) else {
            tracing::warn!(
                symbol = %query.symbol,
                timeframe = %query.timeframe,
                "OHLC range is outside the intraday history SSI serves; nothing to fetch"
            );
            return Ok(Vec::new());
        };
        if fitted != *query {
            tracing::warn!(
                symbol = %query.symbol,
                timeframe = %query.timeframe,
                from = %fitted.from,
                to = %fitted.to,
                "OHLC range fitted to the history SSI serves"
            );
        }
        let mut bars: Vec<Bar> = Vec::new();
        let mut previous_first: Option<DateTime<Utc>> = None;
        for page_index in 1..=MAX_PAGES {
            let page = self.ohlc_page(&fitted, page_index).await?;
            let count = page.bars.len();
            let first = page.bars.first().map(|bar| bar.time);
            if first.is_some() && first == previous_first {
                return Err(Error::Decode {
                    endpoint: ENDPOINT.to_owned(),
                    detail: format!("page {page_index} repeated the previous page"),
                });
            }
            previous_first = first;
            bars.extend(page.bars);
            tracing::debug!(symbol = %fitted.symbol, page_index, count, "OHLC page");
            let echoed = page
                .page_size
                .filter(|size| *size > 0)
                .and_then(|size| usize::try_from(size).ok());
            if count == 0 || echoed.is_some_and(|size| count < size) {
                break;
            }
        }
        bars.sort_by_key(|bar| bar.time);
        bars.dedup_by_key(|bar| bar.time);
        Ok(bars)
    }

    /// Convenience helper: every bar for `symbol` on today's `Asia/Ho_Chi_Minh` trading date.
    pub async fn ohlc_today(&self, symbol: &str, timeframe: Timeframe) -> Result<Vec<Bar>> {
        let today = vn_date_of(self.now());
        let query = OhlcQuery::days(symbol, timeframe, today, today)?;
        self.ohlc(&query).await
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn query_parameters_carry_times() {
        let from = NaiveDate::from_ymd_opt(2025, 1, 1).expect("date");
        let to = NaiveDate::from_ymd_opt(2025, 12, 31).expect("date");
        let query = OhlcQuery::days("vnm", Timeframe::Day1, from, to).expect("valid");
        let params = query.params(2);
        assert!(params.contains(&("from", "2025/01/01 00:00:00".to_owned())));
        assert!(params.contains(&("to", "2025/12/31 23:59:59".to_owned())));
        assert!(params.contains(&("symbol", "VNM".to_owned())));
        assert!(params.contains(&("pageIndex", "2".to_owned())));
        assert!(OhlcQuery::days("VNM", Timeframe::Day1, to, from).is_err());
    }

    #[test]
    fn clamps_intraday_ranges() {
        let now = DateTime::parse_from_rfc3339("2026-11-20T03:00:00Z")
            .expect("time")
            .with_timezone(&Utc);
        let query = OhlcQuery::days(
            "VNM",
            Timeframe::Minute1,
            NaiveDate::from_ymd_opt(2024, 1, 1).expect("date"),
            NaiveDate::from_ymd_opt(2027, 1, 1).expect("date"),
        )
        .expect("valid");
        let fitted = query.clamped(now).expect("overlap");
        assert_eq!(fitted.from().to_string(), "2025-11-20 00:00:00");
        assert_eq!(fitted.to().to_string(), "2026-11-20 23:59:59");
        let daily = OhlcQuery::days(
            "VNM",
            Timeframe::Day1,
            NaiveDate::from_ymd_opt(2006, 1, 1).expect("date"),
            NaiveDate::from_ymd_opt(2027, 1, 1).expect("date"),
        )
        .expect("valid");
        assert_eq!(daily.clamped(now).as_ref(), Some(&daily));
    }

    #[test]
    fn parses_pages() {
        let text = json!({
            "code": 200, "msg": "", "pageIndex": 1, "pageSize": 1000,
            "data": [{"close": "51100", "high": "51100", "low": "51100", "open": "51100",
                      "symbol": "VNM", "tradingDate": "2026/11/19 14:45:00",
                      "value": "13745900000", "volume": "269000"}]
        })
        .to_string();
        let page = parse_page(Some(text.as_bytes())).expect("decodes");
        let bar = page.bars.first().expect("one bar");
        assert_eq!(bar.close, dec!(51100));
        assert_eq!(bar.time.to_rfc3339(), "2026-11-19T07:45:00+00:00");
        assert_eq!(
            bar.trading_date,
            NaiveDate::from_ymd_opt(2026, 11, 19).expect("date")
        );
        assert_eq!(parse_page(None).expect("empty").bars, Vec::new());
        assert_eq!(
            "15M".parse::<Timeframe>().expect("parses"),
            Timeframe::Minute15
        );
        assert!(Timeframe::Minute15.is_supported_live());
        assert!(!Timeframe::Minute30.is_supported_live());
    }
}
