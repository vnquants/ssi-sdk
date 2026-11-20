//! Corporate actions: `GET /api/v3/data/corporateActions`
//! (<https://developers.ssi.com.vn/docs/api-reference/data-corporateActions>).
//!
//! One request covers at most a 5-day span (`to - from <= 5` days, verified live); the
//! [`SsiClient::corporate_actions`] walker splits a longer range into such windows and walks all
//! pages of each. Events that already have an ex-right date are filtered by it; newly announced
//! events are filtered by execution or announcement date and then have empty `exrightDate` and
//! `ratio`. This endpoint is limited to one request per second per key (`X-RATELIMIT-LIMIT: 1`);
//! the client paces itself from the headers.

use chrono::{Duration as ChronoDuration, NaiveDate};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::decode;
use crate::{
    SsiClient,
    error::{Error, Result},
    http::Sensitivity,
    model::{Exchange, check_symbol},
    time::format_vn_date,
};

const ENDPOINT: &str = "/api/v3/data/corporateActions";
/// Longest `to - from` span, in days, that one request may cover.
pub const MAX_SPAN_DAYS: i64 = 5;
const MAX_PAGES: u32 = 1000;

/// Language of `eventTitle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Language {
    /// Vietnamese (`vn`, the server default).
    #[default]
    Vietnamese,
    /// English (`en`).
    English,
}

impl Language {
    /// Query value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vietnamese => "vn",
            Self::English => "en",
        }
    }
}

/// One corporate-action event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "WireCorporateAction")]
pub struct CorporateAction {
    /// Symbol.
    pub symbol: String,
    /// Event title (`eventTitle`), in the requested language. SSI gives no event-type code.
    #[serde(rename = "eventTitle")]
    pub title: String,
    /// Ex-right date; empty for events not yet scheduled.
    #[serde(rename = "exrightDate")]
    pub ex_right_date: Option<NaiveDate>,
    /// Record date.
    #[serde(rename = "recordDate")]
    pub record_date: Option<NaiveDate>,
    /// Issue / execution date.
    #[serde(rename = "issueDate")]
    pub issue_date: Option<NaiveDate>,
    /// Exchange; often empty in production.
    pub exchange: Option<Exchange>,
    /// Numeric price adjustment ratio when SSI sends a decimal (for example `0.9355` for a cash
    /// dividend); `None` when empty or non-decimal (such as `"10:1"` for rights issues / bonus
    /// shares—see [`Self::ratio_text`]).
    pub ratio: Option<Decimal>,
    /// Raw trimmed `ratio` text exactly as sent (for example `"0.9355"` or `"10:1"`), `None`
    /// when blank or absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio_text: Option<String>,
}

#[derive(Deserialize)]
struct WireCorporateAction {
    #[serde(deserialize_with = "crate::de::string")]
    symbol: String,
    #[serde(rename = "eventTitle", deserialize_with = "crate::de::string")]
    title: String,
    #[serde(
        rename = "exrightDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    ex_right_date: Option<NaiveDate>,
    #[serde(
        rename = "recordDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    record_date: Option<NaiveDate>,
    #[serde(
        rename = "issueDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    issue_date: Option<NaiveDate>,
    #[serde(default, deserialize_with = "opt_exchange")]
    exchange: Option<Exchange>,
    #[serde(default, deserialize_with = "crate::de::opt_string")]
    ratio: Option<String>,
    #[serde(default, deserialize_with = "crate::de::opt_string")]
    ratio_text: Option<String>,
}

impl From<WireCorporateAction> for CorporateAction {
    fn from(wire: WireCorporateAction) -> Self {
        let ratio_text = wire.ratio_text.or_else(|| wire.ratio.clone());
        let ratio = wire
            .ratio
            .as_deref()
            .or(ratio_text.as_deref())
            .and_then(crate::de::parse_decimal);
        Self {
            symbol: wire.symbol,
            title: wire.title,
            ex_right_date: wire.ex_right_date,
            record_date: wire.record_date,
            issue_date: wire.issue_date,
            exchange: wire.exchange,
            ratio,
            ratio_text,
        }
    }
}

fn opt_exchange<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Exchange>, D::Error> {
    Ok(crate::de::opt_string(deserializer)?.map(|text| Exchange::normalize(&text)))
}

#[derive(Deserialize)]
struct WirePage {
    #[serde(rename = "pageIndex", default, deserialize_with = "crate::de::opt_i64")]
    page_index: Option<i64>,
    #[serde(rename = "pageSize", default, deserialize_with = "crate::de::opt_i64")]
    page_size: Option<i64>,
    #[serde(
        rename = "pagesCount",
        alias = "pageCount",
        default,
        deserialize_with = "crate::de::opt_i64"
    )]
    pages_count: Option<i64>,
    #[serde(
        rename = "itemsCount",
        default,
        deserialize_with = "crate::de::opt_i64"
    )]
    items_count: Option<i64>,
    #[serde(default)]
    data: Option<Vec<CorporateAction>>,
}

/// One page of events.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CorporateActionsPage {
    /// Events, ordered by ex-right date descending (server order).
    pub items: Vec<CorporateAction>,
    /// `pageIndex`.
    pub page_index: Option<i64>,
    /// `pageSize`.
    pub page_size: Option<i64>,
    /// `pagesCount`.
    pub pages_count: Option<i64>,
    /// `itemsCount`.
    pub items_count: Option<i64>,
}

/// Page sizes the endpoint accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CorporateActionsPageSize {
    /// 10.
    Ten,
    /// 20.
    Twenty,
    /// 50.
    Fifty,
    /// 100 (default here).
    #[default]
    Hundred,
}

impl CorporateActionsPageSize {
    const fn get(self) -> u32 {
        match self {
            Self::Ten => 10,
            Self::Twenty => 20,
            Self::Fifty => 50,
            Self::Hundred => 100,
        }
    }
}

/// A corporate-actions query over an inclusive date range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorporateActionsQuery {
    symbol: Option<String>,
    from: NaiveDate,
    to: NaiveDate,
    language: Language,
    page_size: CorporateActionsPageSize,
}

impl CorporateActionsQuery {
    /// All symbols, `from..=to` (any length; the walker splits it).
    pub fn new(from: NaiveDate, to: NaiveDate) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "corporate actions: from is after to".to_owned(),
            ));
        }
        Ok(Self {
            symbol: None,
            from,
            to,
            language: Language::default(),
            page_size: CorporateActionsPageSize::default(),
        })
    }

    /// Restricts to one symbol.
    pub fn with_symbol(mut self, symbol: &str) -> Result<Self> {
        self.symbol = Some(check_symbol(symbol)?);
        Ok(self)
    }

    /// Title language (default Vietnamese).
    #[must_use]
    pub const fn with_language(mut self, language: Language) -> Self {
        self.language = language;
        self
    }

    /// Page size (default 100).
    #[must_use]
    pub const fn with_page_size(mut self, page_size: CorporateActionsPageSize) -> Self {
        self.page_size = page_size;
        self
    }

    /// Range start.
    pub const fn from(&self) -> NaiveDate {
        self.from
    }

    /// Range end (inclusive).
    pub const fn to(&self) -> NaiveDate {
        self.to
    }

    /// Consecutive non-overlapping windows of at most [`MAX_SPAN_DAYS`] covering the range.
    pub fn windows(&self) -> Vec<(NaiveDate, NaiveDate)> {
        let mut windows = Vec::new();
        let mut start = self.from;
        while start <= self.to {
            let end = (start + ChronoDuration::days(MAX_SPAN_DAYS)).min(self.to);
            windows.push((start, end));
            start = end + ChronoDuration::days(1);
        }
        windows
    }

    fn params(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        page_index: u32,
    ) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("from", format_vn_date(from)),
            ("to", format_vn_date(to)),
            ("language", self.language.as_str().to_owned()),
            ("pageIndex", page_index.to_string()),
            ("pageSize", self.page_size.get().to_string()),
        ];
        if let Some(symbol) = &self.symbol {
            params.push(("symbol", symbol.clone()));
        }
        params
    }
}

pub(crate) fn parse_page(body: Option<&[u8]>) -> Result<CorporateActionsPage> {
    let Some(body) = body else {
        return Ok(CorporateActionsPage::default());
    };
    let page: WirePage = decode(ENDPOINT, body, Sensitivity::Public)?;
    Ok(CorporateActionsPage {
        items: page.data.unwrap_or_default(),
        page_index: page.page_index,
        page_size: page.page_size,
        pages_count: page.pages_count,
        items_count: page.items_count,
    })
}

impl SsiClient {
    /// One page for one window. The window must span at most [`MAX_SPAN_DAYS`] days (the query
    /// range is used as is). HTTP 204 (no events) yields an empty page.
    pub async fn corporate_actions_page(
        &self,
        query: &CorporateActionsQuery,
        page_index: u32,
    ) -> Result<CorporateActionsPage> {
        if (query.to - query.from).num_days() > MAX_SPAN_DAYS {
            return Err(Error::InvalidRequest(format!(
                "one corporate-actions request spans at most {MAX_SPAN_DAYS} days; use corporate_actions()"
            )));
        }
        if page_index == 0 {
            return Err(Error::InvalidRequest("pageIndex starts at 1".to_owned()));
        }
        let params = query.params(query.from, query.to, page_index);
        parse_page(
            self.get_body(ENDPOINT, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// Every event in the query range: walks 5-day windows and all pages of each, then removes
    /// exact duplicates. Order: window by window as walked (oldest window first), server order
    /// inside a window.
    pub async fn corporate_actions(
        &self,
        query: &CorporateActionsQuery,
    ) -> Result<Vec<CorporateAction>> {
        let mut events: Vec<CorporateAction> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (from, to) in query.windows() {
            let mut page_index = 1;
            let mut previous: Option<Vec<CorporateAction>> = None;
            loop {
                let params = query.params(from, to, page_index);
                let page = parse_page(
                    self.get_body(ENDPOINT, &params, Sensitivity::Public)
                        .await?
                        .as_deref(),
                )?;
                let count = page.items.len();
                // The same page twice in one window: the server ignored `pageIndex`.
                if count > 0 && previous.as_ref() == Some(&page.items) {
                    tracing::warn!(%from, %to, page_index, "corporate actions page repeated; window ends");
                    break;
                }
                let mut added = 0_usize;
                for event in &page.items {
                    if seen.insert(event.clone()) {
                        events.push(event.clone());
                        added += 1;
                    }
                }
                tracing::debug!(%from, %to, page_index, count, added, pages = ?page.pages_count, "corporate actions page");
                // Without `pagesCount`, walk until an empty page; the repeated-page guard above
                // stops a server that ignores `pageIndex`.
                let last = page
                    .pages_count
                    .is_some_and(|pages| i64::from(page_index) >= pages);
                if count == 0 || last || page_index >= MAX_PAGES {
                    break;
                }
                previous = Some(page.items);
                page_index += 1;
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("date")
    }

    #[test]
    fn windows_cover_the_range_without_overlap() {
        let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-19")).expect("ok");
        let windows = query.windows();
        assert_eq!(
            windows,
            vec![
                (date("2026-11-01"), date("2026-11-06")),
                (date("2026-11-07"), date("2026-11-12")),
                (date("2026-11-13"), date("2026-11-18")),
                (date("2026-11-19"), date("2026-11-19")),
            ]
        );
        let single =
            CorporateActionsQuery::new(date("2026-11-19"), date("2026-11-19")).expect("ok");
        assert_eq!(single.windows().len(), 1);
    }

    #[test]
    fn parses_blank_fields() {
        let text = json!({
            "code": 200, "msg": "", "pageIndex": 1, "pageSize": 100, "pagesCount": 1,
            "itemsCount": 2,
            "data": [
                {"eventTitle": "PTH - Thông báo", "exchange": "", "exrightDate": "",
                 "issueDate": "2026/11/19", "ratio": "", "recordDate": "", "symbol": "PTH"},
                {"eventTitle": "S4A - Trả cổ tức", "exchange": "HOSE", "exrightDate": "2026/11/19",
                 "issueDate": "2026/12/08", "ratio": "0.9355", "recordDate": "2026/11/20",
                 "symbol": "S4A"},
                {"eventTitle": "FPT - Thưởng cổ phiếu", "exchange": "HOSE", "exrightDate": "2026/11/19",
                 "issueDate": "2026/12/15", "ratio": "10:1", "recordDate": "2026/11/20",
                 "symbol": "FPT"}
            ]
        })
        .to_string();
        let page = parse_page(Some(text.as_bytes())).expect("decodes");
        let first = page.items.first().expect("first");
        assert_eq!(first.ex_right_date, None);
        assert_eq!(first.exchange, None);
        assert_eq!(first.ratio, None);
        assert_eq!(first.ratio_text, None);
        let second = page.items.get(1).expect("second");
        assert_eq!(second.ratio, Some(dec!(0.9355)));
        assert_eq!(second.ratio_text.as_deref(), Some("0.9355"));
        assert_eq!(second.exchange, Some(Exchange::Hose));
        let third = page.items.get(2).expect("third");
        assert_eq!(third.ratio, None);
        assert_eq!(third.ratio_text.as_deref(), Some("10:1"));
        assert_eq!(page.pages_count, Some(1));
        assert_eq!(parse_page(None).expect("empty").items, Vec::new());
    }
}
