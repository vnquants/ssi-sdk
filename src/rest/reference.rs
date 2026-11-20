//! Reference data: securities, daily price limits and index lists.
//!
//! - `GET /api/v3/data/securitiesByBoard`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-securitiesByBoard>): lot size,
//!   board, listing dates, ICB code. V3 publishes **no tick-size table**; tick rules must come
//!   from the exchange rulebook.
//! - `GET /api/v3/data/masterdata`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-masterData>): ceiling, floor and
//!   reference price per symbol and trading date. Production names the exchange field `board`
//!   (the reference says `exchange`) and sends the prices as JSON numbers.
//! - `GET /api/v3/data/indexList`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-indexList>). Index codes can carry
//!   trailing spaces in production; they are trimmed here.

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{decode, decode_unwrapped};
use crate::{
    SsiClient,
    error::{Error, Result},
    http::Sensitivity,
    model::{Exchange, check_symbol},
    time::format_vn_date,
};

const SECURITIES: &str = "/api/v3/data/securitiesByBoard";
const MASTER_DATA: &str = "/api/v3/data/masterdata";
const INDEX_LIST: &str = "/api/v3/data/indexList";
const MASTER_DATA_PAGE_SIZE: u32 = 1000;
const MAX_PAGES: u32 = 1000;

/// Exactly one filter for [`SsiClient::securities`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecuritiesFilter {
    /// One symbol.
    Symbol(String),
    /// Every security of a board (`HOSE`, `HNX`, `UPCOM`).
    Board(Exchange),
    /// Constituents of an index (for example `VN30`).
    Index(String),
}

/// Static data of one security.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityInfo {
    /// Symbol.
    #[serde(deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Vietnamese name.
    #[serde(
        rename = "symbolNameVi",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub name_vi: Option<String>,
    /// English name.
    #[serde(
        rename = "symbolNameEn",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub name_en: Option<String>,
    /// Board.
    pub board: Exchange,
    /// Index code (populated when querying by index).
    #[serde(default, deserialize_with = "crate::de::opt_string")]
    pub index: Option<String>,
    /// `stockType` (undocumented; `Stock` for shares).
    #[serde(
        rename = "stockType",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub stock_type: Option<String>,
    /// Board lot in shares.
    #[serde(
        rename = "lotSize",
        default,
        deserialize_with = "crate::de::opt_quantity"
    )]
    pub lot_size: Option<u64>,
    /// Listed shares.
    #[serde(
        rename = "listedShare",
        default,
        deserialize_with = "crate::de::opt_quantity"
    )]
    pub listed_shares: Option<u64>,
    /// First trading date (often empty in production).
    #[serde(
        rename = "firstTradingDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    pub first_trading_date: Option<NaiveDate>,
    /// Last trading date (warrants, futures, bonds).
    #[serde(
        rename = "lastTradingDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    pub last_trading_date: Option<NaiveDate>,
    /// Maturity date (warrants, futures, bonds).
    #[serde(
        rename = "maturityDate",
        default,
        deserialize_with = "crate::de::opt_vn_date"
    )]
    pub maturity_date: Option<NaiveDate>,
    /// ICB industry code.
    #[serde(
        rename = "icbCode",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub icb_code: Option<String>,
    /// ICB industry name.
    #[serde(
        rename = "icbName",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub icb_name: Option<String>,
    /// Covered warrant: underlying symbol.
    #[serde(
        rename = "cwUnderlyingSymbol",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub cw_underlying_symbol: Option<String>,
    /// Covered warrant: exercise price.
    #[serde(
        rename = "cwExercisePrice",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub cw_exercise_price: Option<Decimal>,
    /// Covered warrant: execution ratio.
    #[serde(
        rename = "cwExecutionRatio",
        default,
        deserialize_with = "crate::de::opt_string"
    )]
    pub cw_execution_ratio: Option<String>,
    /// ETF indicative index value.
    #[serde(
        rename = "iIndex",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub i_index: Option<Decimal>,
    /// ETF indicative NAV.
    #[serde(rename = "iNav", default, deserialize_with = "crate::de::opt_decimal")]
    pub i_nav: Option<Decimal>,
    /// Futures open interest.
    #[serde(
        rename = "openInterest",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub open_interest: Option<Decimal>,
    /// Futures settlement price.
    #[serde(
        rename = "settlementPrice",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub settlement_price: Option<Decimal>,
}

/// Ceiling, floor and reference price of one symbol on one trading date.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLimit {
    /// Symbol.
    #[serde(deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Board (`board` in production, `exchange` in the reference).
    #[serde(alias = "exchange")]
    pub board: Exchange,
    /// Trading date.
    #[serde(rename = "tradingDate", deserialize_with = "crate::de::vn_date")]
    pub trading_date: NaiveDate,
    /// Ceiling price.
    #[serde(deserialize_with = "crate::de::decimal")]
    pub ceiling: Decimal,
    /// Floor price.
    #[serde(deserialize_with = "crate::de::decimal")]
    pub floor: Decimal,
    /// Reference price (`refPrice` or `referencePrice`).
    #[serde(
        rename = "refPrice",
        alias = "referencePrice",
        deserialize_with = "crate::de::decimal"
    )]
    pub reference: Decimal,
}

/// One page of [`PriceLimit`]s.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PriceLimitsPage {
    /// Rows.
    pub items: Vec<PriceLimit>,
    /// `pagesCount`.
    pub pages_count: Option<i64>,
    /// `itemsCount`.
    pub items_count: Option<i64>,
}

#[derive(Deserialize)]
struct WirePriceLimits {
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
    data: Option<Vec<PriceLimit>>,
}

/// An index of a board.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexInfo {
    /// Index code (trimmed).
    #[serde(deserialize_with = "trimmed")]
    pub index: String,
    /// Index name (trimmed).
    #[serde(rename = "indexName", default, deserialize_with = "trimmed")]
    pub name: String,
    /// Board.
    pub board: Exchange,
}

fn trimmed<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    crate::de::string(deserializer)
}

fn list<T: serde::de::DeserializeOwned>(endpoint: &str, body: Option<&[u8]>) -> Result<Vec<T>> {
    let Some(body) = body else {
        return Ok(Vec::new());
    };
    Ok(
        decode_unwrapped::<Option<Vec<T>>>(endpoint, body, Sensitivity::Public)?
            .unwrap_or_default(),
    )
}

pub(crate) fn parse_securities(body: Option<&[u8]>) -> Result<Vec<SecurityInfo>> {
    list(SECURITIES, body)
}

pub(crate) fn parse_indices(body: Option<&[u8]>) -> Result<Vec<IndexInfo>> {
    list(INDEX_LIST, body)
}

pub(crate) fn parse_price_limits(body: Option<&[u8]>) -> Result<PriceLimitsPage> {
    let Some(body) = body else {
        return Ok(PriceLimitsPage::default());
    };
    let page: WirePriceLimits = decode(MASTER_DATA, body, Sensitivity::Public)?;
    Ok(PriceLimitsPage {
        items: page.data.unwrap_or_default(),
        pages_count: page.pages_count,
        items_count: page.items_count,
    })
}

impl SsiClient {
    /// Static data of securities selected by one filter.
    pub async fn securities(&self, filter: &SecuritiesFilter) -> Result<Vec<SecurityInfo>> {
        let param = match filter {
            SecuritiesFilter::Symbol(symbol) => ("symbol", check_symbol(symbol)?),
            SecuritiesFilter::Board(board) => ("board", rest_board(board)?.to_owned()),
            SecuritiesFilter::Index(index) => ("index", check_symbol(index)?),
        };
        parse_securities(
            self.get_body(SECURITIES, &[param], Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// One page of daily price limits for every symbol. With no dates SSI returns the latest
    /// trading date. There is no symbol filter.
    pub async fn price_limits_page(
        &self,
        dates: Option<(NaiveDate, NaiveDate)>,
        page_index: u32,
        page_size: u32,
    ) -> Result<PriceLimitsPage> {
        if page_index == 0 || page_size == 0 {
            return Err(Error::InvalidRequest(
                "pageIndex and pageSize start at 1".to_owned(),
            ));
        }
        let mut params = vec![
            ("pageIndex", page_index.to_string()),
            ("pageSize", page_size.to_string()),
        ];
        if let Some((from, to)) = dates {
            if from > to {
                return Err(Error::InvalidRequest(
                    "price limits: from is after to".to_owned(),
                ));
            }
            params.push(("from", format_vn_date(from)));
            params.push(("to", format_vn_date(to)));
        }
        parse_price_limits(
            self.get_body(MASTER_DATA, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// Every price-limit row for the dates (latest trading date when `None`), all pages.
    pub async fn price_limits(
        &self,
        dates: Option<(NaiveDate, NaiveDate)>,
    ) -> Result<Vec<PriceLimit>> {
        let mut rows = Vec::new();
        let mut previous_first = None;
        for page_index in 1..=MAX_PAGES {
            let page = self
                .price_limits_page(dates, page_index, MASTER_DATA_PAGE_SIZE)
                .await?;
            let count = page.items.len();
            let first = page.items.first().cloned();
            if first.is_some() && first == previous_first {
                return Err(Error::Decode {
                    endpoint: MASTER_DATA.to_owned(),
                    detail: format!("page {page_index} repeated the previous page"),
                });
            }
            previous_first = first;
            rows.extend(page.items);
            // Without `pagesCount`, walk until an empty page.
            let last = page
                .pages_count
                .is_some_and(|pages| i64::from(page_index) >= pages);
            if count == 0 || last {
                break;
            }
        }
        Ok(rows)
    }

    /// Indices, optionally of one board.
    pub async fn indices(&self, board: Option<&Exchange>) -> Result<Vec<IndexInfo>> {
        let params = match board {
            Some(board) => vec![("board", rest_board(board)?.to_owned())],
            None => Vec::new(),
        };
        parse_indices(
            self.get_body(INDEX_LIST, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }
}

pub(super) fn rest_board(board: &Exchange) -> Result<&'static str> {
    match board {
        Exchange::Hose => Ok("HOSE"),
        Exchange::Hnx => Ok("HNX"),
        Exchange::Upcom => Ok("UPCOM"),
        other => Err(Error::InvalidRequest(format!(
            "board must be HOSE, HNX or UPCOM, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn securities_with_nulls() {
        let rows = parse_securities(Some(
            json!([{
                "board": "HOSE", "cwExecutionRatio": "0", "cwExercisePrice": "0",
                "cwUnderlyingSymbol": null, "firstTradingDate": "", "iIndex": null, "iNav": null,
                "icbCode": "3577", "icbName": "Thực phẩm", "lastTradingDate": "",
                "listedShare": "2089955445", "lotSize": "100", "maturityDate": "",
                "openInterest": null, "settlementPrice": null, "stockType": "Stock",
                "symbol": "VNM", "symbolNameEn": "Vietnam Dairy", "symbolNameVi": "Vinamilk"
            }])
            .to_string()
            .as_bytes(),
        ))
        .expect("decodes");
        let vnm = rows.first().expect("row");
        assert_eq!(vnm.lot_size, Some(100));
        assert_eq!(vnm.board, Exchange::Hose);
        assert_eq!(vnm.first_trading_date, None);
        assert_eq!(vnm.listed_shares, Some(2_089_955_445));
    }

    #[test]
    fn price_limits_numbers() {
        let page = parse_price_limits(Some(
            json!({
                "code": 200, "itemsCount": 2120, "pagesCount": 212, "pageIndex": 1, "pageSize": 10,
                "data": [{"board": "HNX", "ceiling": 999_999_999, "floor": 0, "refPrice": 100_000,
                          "symbol": "ASSC12601", "tradingDate": "2026/11/19"}]
            })
            .to_string()
            .as_bytes(),
        ))
        .expect("decodes");
        let row = page.items.first().expect("row");
        assert_eq!(row.reference, dec!(100000));
        assert_eq!(page.pages_count, Some(212));
    }

    #[test]
    fn index_codes_are_trimmed() {
        let rows = parse_indices(Some(
            json!([
                {"board": "HOSE", "index": "VNDIVIDEND ", "indexName": "VNDIVIDEND "}
            ])
            .to_string()
            .as_bytes(),
        ))
        .expect("decodes");
        assert_eq!(
            rows.first().map(|row| row.index.as_str()),
            Some("VNDIVIDEND")
        );
    }
}
