//! Bulk historical Parquet market-data files.
//!
//! - `GET /api/v3/data/file`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-marketFile>): returns signed
//!   Parquet download links (`{date, dataLink}`) for `1m` (weekly files, max 2-month span),
//!   `15m` (monthly files, unlimited span) and `1d` (single full-history file; `fromDate` and
//!   `toDate` must **not** be sent). Requires scope `data:file:get`.
//! - `GET /api/v3/data/download?token=<token>`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-marketFileDownload>): downloads raw
//!   Parquet bytes (`application/octet-stream`). Must send the **same Bearer token** used to
//!   call `/api/v3/data/file`. Each link expires in 30 minutes and is invalidated after one
//!   successful download (429 / 503 / interrupted downloads do not consume it).

use std::fmt;

use chrono::{Months, NaiveDate};
use serde::Deserialize;
use url::Url;

use super::decode;
use crate::{
    SsiClient,
    error::{Error, Result},
    http::Sensitivity,
    secret::Masked,
    time::{format_vn_date, vn_date_of},
};

const FILE_LIST: &str = "/api/v3/data/file";
const FILE_DOWNLOAD: &str = "/api/v3/data/download";
const MAX_SYMBOL_LEN: usize = 12;
const MINUTE1_MAX_MONTHS: u32 = 2;

/// Timeframe supported by `GET /api/v3/data/file`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketFileTimeframe {
    /// 1-minute bars (weekly Parquet files; max 2-month query range).
    Minute1,
    /// 15-minute bars (monthly Parquet files; unlimited query range).
    Minute15,
    /// 1-day bars (single full-history Parquet file; no date parameters allowed).
    Day1,
}

impl MarketFileTimeframe {
    /// Every supported market-file timeframe.
    pub const ALL: [Self; 3] = [Self::Minute1, Self::Minute15, Self::Day1];

    /// The `timeFrame` query value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minute1 => "1m",
            Self::Minute15 => "15m",
            Self::Day1 => "1d",
        }
    }
}

impl fmt::Display for MarketFileTimeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MarketFileTimeframe {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|timeframe| timeframe.as_str().eq_ignore_ascii_case(text.trim()))
            .ok_or_else(|| Error::InvalidRequest(format!("unknown market file timeframe {text:?}")))
    }
}

/// Validated query for `GET /api/v3/data/file`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketFileQuery {
    symbol: String,
    timeframe: MarketFileTimeframe,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
}

impl MarketFileQuery {
    /// Full-history daily (`1d`) Parquet file for `symbol`.
    ///
    /// SSI rejects `fromDate` / `toDate` for `1d`, so no date bounds are sent.
    pub fn daily(symbol: &str) -> Result<Self> {
        Ok(Self {
            symbol: check_file_symbol(symbol)?,
            timeframe: MarketFileTimeframe::Day1,
            from_date: None,
            to_date: None,
        })
    }

    /// Weekly 1-minute (`1m`) Parquet files overlapping `from..=to`.
    ///
    /// Enforces `from <= to` and SSI's 2-calendar-month maximum span.
    pub fn minute1(symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "market file 1m query: fromDate is after toDate".to_owned(),
            ));
        }
        let max_to = from
            .checked_add_months(Months::new(MINUTE1_MAX_MONTHS))
            .ok_or_else(|| {
                Error::InvalidRequest("market file 1m query: date out of range".to_owned())
            })?;
        if to > max_to {
            return Err(Error::InvalidRequest(format!(
                "market file 1m query spans more than {MINUTE1_MAX_MONTHS} months"
            )));
        }
        Ok(Self {
            symbol: check_file_symbol(symbol)?,
            timeframe: MarketFileTimeframe::Minute1,
            from_date: Some(from),
            to_date: Some(to),
        })
    }

    /// Monthly 15-minute (`15m`) Parquet files overlapping `from..=to` (no span limit).
    pub fn minute15(symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "market file 15m query: fromDate is after toDate".to_owned(),
            ));
        }
        Ok(Self {
            symbol: check_file_symbol(symbol)?,
            timeframe: MarketFileTimeframe::Minute15,
            from_date: Some(from),
            to_date: Some(to),
        })
    }

    /// Normalized uppercase symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Requested timeframe.
    pub const fn timeframe(&self) -> MarketFileTimeframe {
        self.timeframe
    }

    /// `fromDate` (always `None` for `1d`).
    pub const fn from_date(&self) -> Option<NaiveDate> {
        self.from_date
    }

    /// `toDate` (always `None` for `1d`).
    pub const fn to_date(&self) -> Option<NaiveDate> {
        self.to_date
    }

    fn params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("symbol", self.symbol.clone()),
            ("timeFrame", self.timeframe.as_str().to_owned()),
        ];
        if let Some(from) = self.from_date {
            params.push(("fromDate", format_vn_date(from)));
        }
        if let Some(to) = self.to_date {
            params.push(("toDate", format_vn_date(to)));
        }
        params
    }
}

/// One Parquet file entry returned by `GET /api/v3/data/file`.
///
/// `data_link` carries a single-use, 30-minute download token, so `Debug` masks the `token=`
/// parameter.
#[derive(Clone, PartialEq, Eq)]
pub struct MarketFile {
    /// First trading date of the week (`1m`), first calendar day of the month (`15m`), or `None`
    /// for the single full-history daily file (`1d`).
    pub date: Option<NaiveDate>,
    data_link: String,
}

impl MarketFile {
    /// Constructs a `MarketFile` from an optional period date and `dataLink`.
    pub fn new(date: Option<NaiveDate>, data_link: impl Into<String>) -> Self {
        Self {
            date,
            data_link: data_link.into().trim().to_owned(),
        }
    }

    /// Raw `dataLink` returned by SSI (contains a sensitive one-time token; never log directly).
    pub fn data_link(&self) -> &str {
        &self.data_link
    }

    /// Extracts the `token` query parameter from `dataLink`.
    pub fn download_token(&self) -> Result<String> {
        extract_download_token(&self.data_link, None)
    }
}

impl fmt::Debug for MarketFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketFile")
            .field("date", &self.date)
            .field("data_link", &mask_data_link(&self.data_link))
            .finish()
    }
}

#[derive(Deserialize)]
struct WireMarketFile {
    #[serde(default, deserialize_with = "crate::de::opt_vn_date")]
    date: Option<NaiveDate>,
    #[serde(rename = "dataLink", deserialize_with = "crate::de::string")]
    data_link: String,
}

#[derive(Deserialize)]
struct WireMarketFileList {
    #[serde(default)]
    files: Option<Vec<WireMarketFile>>,
    #[serde(default)]
    data: Option<WireMarketFileData>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireMarketFileData {
    Wrapped { files: Vec<WireMarketFile> },
    List(Vec<WireMarketFile>),
}

pub(crate) fn parse_market_files(body: Option<&[u8]>) -> Result<Vec<MarketFile>> {
    let Some(body) = body else {
        return Ok(Vec::new());
    };
    let wire: WireMarketFileList = decode(FILE_LIST, body, Sensitivity::Public)?;
    let items = if let Some(files) = wire.files {
        files
    } else {
        match wire.data {
            Some(WireMarketFileData::Wrapped { files } | WireMarketFileData::List(files)) => files,
            None => Vec::new(),
        }
    };
    Ok(items
        .into_iter()
        .map(|item| MarketFile::new(item.date, item.data_link))
        .collect())
}

fn check_file_symbol(symbol: &str) -> Result<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > MAX_SYMBOL_LEN
        || !symbol.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        return Err(Error::InvalidRequest(format!(
            "market file symbol must be 1-{MAX_SYMBOL_LEN} ASCII letters or digits, got {symbol:?}"
        )));
    }
    Ok(symbol)
}

fn mask_data_link(link: &str) -> String {
    let base = Url::parse("https://placeholder.invalid").ok();
    let parsed = Url::parse(link)
        .ok()
        .or_else(|| base.and_then(|base| base.join(link).ok()));
    let Some(url) = parsed else {
        return Masked::new(link).to_string();
    };
    let mut masked_query = Vec::new();
    let mut saw_token = false;
    for (key, value) in url.query_pairs() {
        if key.eq_ignore_ascii_case("token") {
            saw_token = true;
            masked_query.push(format!("{key}={}", Masked::new(&value)));
        } else {
            masked_query.push(format!("{key}={value}"));
        }
    }
    if !saw_token {
        return Masked::new(link).to_string();
    }
    let prefix = if link.starts_with('/') {
        url.path().to_owned()
    } else {
        format!("{}://{}{}", url.scheme(), url.authority(), url.path())
    };
    format!("{prefix}?{}", masked_query.join("&"))
}

fn extract_download_token(link: &str, expected_origin: Option<&Url>) -> Result<String> {
    let link = link.trim();
    if link.is_empty() {
        return Err(Error::InvalidRequest(
            "empty market file dataLink".to_owned(),
        ));
    }
    let parsed = if link.starts_with('/') {
        let base = expected_origin
            .cloned()
            .or_else(|| Url::parse("https://api.ssi.com.vn").ok())
            .ok_or_else(|| Error::InvalidRequest("invalid base URL".to_owned()))?;
        base.join(link)
            .map_err(|_| Error::InvalidRequest("invalid relative dataLink".to_owned()))?
    } else {
        let url = Url::parse(link)
            .map_err(|_| Error::InvalidRequest("invalid dataLink URL".to_owned()))?;
        if let Some(origin) = expected_origin
            && (url.scheme() != origin.scheme()
                || url.host() != origin.host()
                || url.port_or_known_default() != origin.port_or_known_default())
        {
            return Err(Error::InvalidRequest(
                "dataLink origin does not match configured api_url".to_owned(),
            ));
        }
        url
    };
    if parsed.path() != FILE_DOWNLOAD {
        return Err(Error::InvalidRequest(format!(
            "dataLink path must be {FILE_DOWNLOAD}"
        )));
    }
    let token = parsed
        .query_pairs()
        .find(|(key, _)| key == "token")
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::InvalidRequest("dataLink is missing a token parameter".to_owned()))?;
    validate_download_token(&token)?;
    Ok(token)
}

fn validate_download_token(token: &str) -> Result<()> {
    if token.is_empty()
        || token
            .chars()
            .any(|ch| ch.is_ascii_control() || ch.is_ascii_whitespace())
    {
        return Err(Error::InvalidRequest("invalid download token".to_owned()));
    }
    Ok(())
}

impl SsiClient {
    /// Lists Parquet market-data download links (`GET /api/v3/data/file`, scope `data:file:get`).
    pub async fn market_files(&self, query: &MarketFileQuery) -> Result<Vec<MarketFile>> {
        if let Some(to) = query.to_date {
            let today = vn_date_of(self.now());
            if to > today {
                return Err(Error::InvalidRequest(
                    "market file query: toDate cannot be in the future".to_owned(),
                ));
            }
        }
        let params = query.params();
        parse_market_files(
            self.get_body(FILE_LIST, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// Downloads the raw Parquet bytes for one [`MarketFile`] returned by
    /// [`SsiClient::market_files`].
    ///
    /// Verifies that an absolute `dataLink` shares the configured `api_url` origin and points to
    /// `/api/v3/data/download`, then sends `GET /api/v3/data/download?token=...` with the same
    /// Bearer token and `Accept: application/octet-stream`.
    pub async fn download_market_file(&self, file: &MarketFile) -> Result<Vec<u8>> {
        let token = extract_download_token(file.data_link(), Some(self.config().api_url()))?;
        self.download_market_file_by_token(&token).await
    }

    /// Downloads raw Parquet bytes directly by `token` (`GET /api/v3/data/download?token=...`).
    pub async fn download_market_file_by_token(&self, token: &str) -> Result<Vec<u8>> {
        validate_download_token(token)?;
        self.get_bytes(FILE_DOWNLOAD, &[("token", token.to_owned())])
            .await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("date")
    }

    #[test]
    fn enforces_symbol_and_timeframe_rules() {
        let daily = MarketFileQuery::daily(" vnm ").expect("valid");
        assert_eq!(daily.symbol(), "VNM");
        assert_eq!(
            daily.params(),
            vec![("symbol", "VNM".to_owned()), ("timeFrame", "1d".to_owned())]
        );

        // 12 chars accepted, 13 rejected.
        assert!(MarketFileQuery::daily("VN30F2612000").is_ok());
        assert!(MarketFileQuery::daily("VN30F26120001").is_err());
        assert!(MarketFileQuery::daily("VN M").is_err());

        // 1m: up to 2 calendar months ok, 2 months + 1 day rejected.
        let m1_ok =
            MarketFileQuery::minute1("VNM", date("2026-01-05"), date("2026-03-05")).expect("2m ok");
        assert_eq!(
            m1_ok.params(),
            vec![
                ("symbol", "VNM".to_owned()),
                ("timeFrame", "1m".to_owned()),
                ("fromDate", "2026/01/05".to_owned()),
                ("toDate", "2026/03/05".to_owned()),
            ]
        );
        assert!(MarketFileQuery::minute1("VNM", date("2026-01-05"), date("2026-03-06")).is_err());
        assert!(MarketFileQuery::minute1("VNM", date("2026-03-05"), date("2026-01-05")).is_err());

        // 15m: multi-year span allowed.
        assert!(MarketFileQuery::minute15("VNM", date("2020-01-01"), date("2025-12-31")).is_ok());
        assert!(MarketFileQuery::minute15("VNM", date("2025-12-31"), date("2025-01-01")).is_err());
    }

    #[test]
    fn parses_files_and_masks_download_token_in_debug() {
        let body = json!({
            "files": [
                {
                    "date": "2026-01-05",
                    "dataLink": "https://api.ssi.com.vn/api/v3/data/download?token=SECRET_ONE_TIME_TOKEN_999"
                },
                {
                    "date": "",
                    "dataLink": "/api/v3/data/download?token=ANOTHER_SECRET_123"
                }
            ],
            "code": 200,
            "msg": "OK"
        })
        .to_string();
        let files = parse_market_files(Some(body.as_bytes())).expect("decodes");
        assert_eq!(files.len(), 2);
        let first = files.first().expect("first");
        assert_eq!(first.date, Some(date("2026-01-05")));
        assert_eq!(
            first.download_token().expect("token"),
            "SECRET_ONE_TIME_TOKEN_999"
        );
        let debug_text = format!("{first:?}");
        assert!(!debug_text.contains("SECRET_ONE_TIME_TOKEN_999"));
        assert!(debug_text.contains("/api/v3/data/download?token=***("));

        let second = files.get(1).expect("second");
        assert_eq!(second.date, None);
        assert_eq!(
            second.download_token().expect("token"),
            "ANOTHER_SECRET_123"
        );
    }

    #[test]
    fn rejects_off_origin_or_wrong_path_data_links() {
        let api = Url::parse("https://api.ssi.com.vn").expect("url");
        assert!(
            extract_download_token(
                "https://evil.example/api/v3/data/download?token=abc",
                Some(&api)
            )
            .is_err()
        );
        assert!(
            extract_download_token("https://api.ssi.com.vn/api/v3/other?token=abc", Some(&api))
                .is_err()
        );
        assert!(
            extract_download_token(
                "https://api.ssi.com.vn/api/v3/data/download?foo=bar",
                Some(&api)
            )
            .is_err()
        );
        assert_eq!(
            extract_download_token(
                "https://api.ssi.com.vn/api/v3/data/download?token=tok123",
                Some(&api)
            )
            .expect("ok"),
            "tok123"
        );
    }
}
