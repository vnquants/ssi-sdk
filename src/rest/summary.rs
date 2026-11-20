//! Daily market index and securities summaries.
//!
//! - `GET /api/v3/data/indexSummary`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-indexSummary>): index level, change,
//!   advance/decline/ceiling/floor breadth, matched/put-through/proprietary turnover and optional
//!   foreign flows. Requires **exactly one** of `board` (`HOSE`, `HNX`, `UPCOM`) or `index`.
//! - `GET /api/v3/data/securitiesSummary`
//!   (<https://developers.ssi.com.vn/docs/api-reference/data-securitiesSummary>): per-day OHLCV,
//!   buy/sell order counts, foreign flow, foreign room, put-through deals, open interest and
//!   settlement price. Requires **exactly one** of `symbol` or `index`, plus `from` and `to`.

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{decode, decode_unwrapped, reference::rest_board};
use crate::{
    SsiClient,
    error::{Error, Result},
    http::Sensitivity,
    model::{Exchange, check_symbol},
    time::format_vn_date,
};

const INDEX_SUMMARY: &str = "/api/v3/data/indexSummary";
const SECURITIES_SUMMARY: &str = "/api/v3/data/securitiesSummary";
/// Largest page size requested by [`SsiClient::securities_summary`].
pub const MAX_SUMMARY_PAGE_SIZE: u32 = 1000;
const MAX_PAGES: u32 = 1000;

/// Exactly one filter for [`SsiClient::index_summary`] (server error `504207` if both or neither
/// are sent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexSummaryFilter {
    /// Board summary (`HOSE`, `HNX`, or `UPCOM`).
    Board(Exchange),
    /// Index summary (for example `VNINDEX`, `VN30`, `HNX30`).
    Index(String),
}

/// Daily summary of a board or market index (`GET /api/v3/data/indexSummary`).
///
/// Foreign-trading fields are `None` when the server omits them or leaves them blank.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs, reason = "field names mirror the documented wire names")]
pub struct MarketIndexSummary {
    /// Trading date (`tradingDate`).
    #[serde(rename = "tradingDate", deserialize_with = "crate::de::vn_date")]
    pub trading_date: NaiveDate,
    #[serde(
        rename = "totalTrade",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_trade: u64,
    #[serde(
        rename = "totalTradeValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_trade_value: Decimal,
    #[serde(
        rename = "totalMatch",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_match: u64,
    #[serde(
        rename = "totalMatchValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_match_value: Decimal,
    #[serde(
        rename = "totalDeal",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_deal: u64,
    #[serde(
        rename = "totalDealValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_deal_value: Decimal,
    #[serde(
        rename = "indexChange",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub index_change: Decimal,
    #[serde(
        rename = "indexChangePercentage",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub index_change_percentage: Decimal,
    #[serde(
        rename = "indexValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub index_value: Decimal,
    #[serde(
        rename = "totalAdvanceStock",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_advance_stock: u64,
    #[serde(
        rename = "totalCeilingStock",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_ceiling_stock: u64,
    #[serde(
        rename = "totalDeclineStock",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_decline_stock: u64,
    #[serde(
        rename = "totalFloorStock",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_floor_stock: u64,
    #[serde(
        rename = "totalNoChangeStock",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_no_change_stock: u64,
    #[serde(
        rename = "totalPropBuy",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_prop_buy: u64,
    #[serde(
        rename = "totalPropBuyValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_prop_buy_value: Decimal,
    #[serde(
        rename = "totalPropSell",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_prop_sell: u64,
    #[serde(
        rename = "totalPropSellValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_prop_sell_value: Decimal,
    #[serde(
        rename = "totalBuyForeign",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub total_buy_foreign: Option<Decimal>,
    #[serde(
        rename = "totalBuyForeignValue",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub total_buy_foreign_value: Option<Decimal>,
    #[serde(
        rename = "totalSellForeign",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub total_sell_foreign: Option<Decimal>,
    #[serde(
        rename = "totalSellForeignValue",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub total_sell_foreign_value: Option<Decimal>,
    #[serde(
        rename = "netPurchasesForeignVolume",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub net_purchases_foreign_volume: Option<Decimal>,
    #[serde(
        rename = "netPurchasesForeignValue",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub net_purchases_foreign_value: Option<Decimal>,
}

/// Exactly one filter for [`SecuritiesSummaryQuery`] (server error `504209` if both or neither
/// are sent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecuritiesSummaryFilter {
    /// One security symbol.
    Symbol(String),
    /// Constituents of an index (for example `VN30`, `VN100`).
    Index(String),
}

/// Query for `GET /api/v3/data/securitiesSummary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecuritiesSummaryQuery {
    filter: SecuritiesSummaryFilter,
    from: NaiveDate,
    to: NaiveDate,
    page_size: u32,
}

impl SecuritiesSummaryQuery {
    /// Validates `filter` and builds a query over `from..=to`.
    pub fn new(filter: SecuritiesSummaryFilter, from: NaiveDate, to: NaiveDate) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "securities summary: from is after to".to_owned(),
            ));
        }
        let filter = match filter {
            SecuritiesSummaryFilter::Symbol(symbol) => {
                SecuritiesSummaryFilter::Symbol(check_symbol(&symbol)?)
            }
            SecuritiesSummaryFilter::Index(index) => {
                SecuritiesSummaryFilter::Index(check_symbol(&index)?)
            }
        };
        Ok(Self {
            filter,
            from,
            to,
            page_size: MAX_SUMMARY_PAGE_SIZE,
        })
    }

    /// Queries one symbol over `from..=to`.
    pub fn for_symbol(symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Self> {
        Self::new(SecuritiesSummaryFilter::Symbol(symbol.to_owned()), from, to)
    }

    /// Queries constituents of one index over `from..=to`.
    pub fn for_index(index: &str, from: NaiveDate, to: NaiveDate) -> Result<Self> {
        Self::new(SecuritiesSummaryFilter::Index(index.to_owned()), from, to)
    }

    /// Rows per page (`1..=1000`, default 1000).
    pub fn with_page_size(mut self, page_size: u32) -> Result<Self> {
        if !(1..=MAX_SUMMARY_PAGE_SIZE).contains(&page_size) {
            return Err(Error::InvalidRequest(format!(
                "page size must be 1..={MAX_SUMMARY_PAGE_SIZE}"
            )));
        }
        self.page_size = page_size;
        Ok(self)
    }

    /// Filter (symbol or index).
    pub const fn filter(&self) -> &SecuritiesSummaryFilter {
        &self.filter
    }

    /// Inclusive start date.
    pub const fn from(&self) -> NaiveDate {
        self.from
    }

    /// Inclusive end date.
    pub const fn to(&self) -> NaiveDate {
        self.to
    }

    fn params(&self, page_index: u32) -> Vec<(&'static str, String)> {
        let mut params = vec![
            match &self.filter {
                SecuritiesSummaryFilter::Symbol(symbol) => ("symbol", symbol.clone()),
                SecuritiesSummaryFilter::Index(index) => ("index", index.clone()),
            },
            ("from", format_vn_date(self.from)),
            ("to", format_vn_date(self.to)),
            ("pageIndex", page_index.to_string()),
            ("pageSize", self.page_size.to_string()),
        ];
        // Keep stable ordering for test matching.
        params.shrink_to_fit();
        params
    }
}

/// Daily trading summary for one security (`GET /api/v3/data/securitiesSummary`).
///
/// `ceiling`, `floor` and `ref_price` are `None` unless sent by the server (SSI's live endpoint
/// usually omits them; fetch them from [`SsiClient::price_limits`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs, reason = "field names mirror the documented wire names")]
pub struct SecuritySummary {
    #[serde(deserialize_with = "crate::de::string")]
    pub symbol: String,
    #[serde(rename = "tradingDate", deserialize_with = "crate::de::vn_date")]
    pub trading_date: NaiveDate,
    #[serde(
        rename = "priceChange",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub price_change: Decimal,
    #[serde(
        rename = "priceChangePercentage",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub price_change_percentage: Decimal,
    #[serde(default, deserialize_with = "crate::de::decimal_or_zero")]
    pub open: Decimal,
    #[serde(default, deserialize_with = "crate::de::decimal_or_zero")]
    pub high: Decimal,
    #[serde(default, deserialize_with = "crate::de::decimal_or_zero")]
    pub low: Decimal,
    #[serde(default, deserialize_with = "crate::de::decimal_or_zero")]
    pub close: Decimal,
    #[serde(default, deserialize_with = "crate::de::decimal_or_zero")]
    pub average: Decimal,
    #[serde(
        rename = "totalMatch",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_match: u64,
    #[serde(
        rename = "totalMatchValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_match_value: Decimal,
    #[serde(
        rename = "totalBuy",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_buy: u64,
    #[serde(
        rename = "totalTradeBuy",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_trade_buy: Decimal,
    #[serde(
        rename = "totalSell",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_sell: u64,
    #[serde(
        rename = "totalTradeSell",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_trade_sell: Decimal,
    #[serde(
        rename = "totalForeignBuy",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_foreign_buy: u64,
    #[serde(
        rename = "totalForeignBuyValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_foreign_buy_value: Decimal,
    #[serde(
        rename = "totalForeignSell",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_foreign_sell: u64,
    #[serde(
        rename = "totalForeignSellValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_foreign_sell_value: Decimal,
    #[serde(
        rename = "remainForeignRoom",
        default,
        deserialize_with = "crate::de::i64_or_zero"
    )]
    pub remain_foreign_room: i64,
    #[serde(
        rename = "totalForeignRoom",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_foreign_room: u64,
    #[serde(
        rename = "totalDeal",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_deal: u64,
    #[serde(
        rename = "totalDealValue",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_deal_value: Decimal,
    #[serde(
        rename = "openInterest",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub open_interest: Decimal,
    #[serde(
        rename = "settlementPrice",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub settlement_price: Decimal,
    #[serde(default, deserialize_with = "crate::de::opt_decimal")]
    pub ceiling: Option<Decimal>,
    #[serde(default, deserialize_with = "crate::de::opt_decimal")]
    pub floor: Option<Decimal>,
    #[serde(
        rename = "refPrice",
        alias = "referencePrice",
        default,
        deserialize_with = "crate::de::opt_decimal"
    )]
    pub ref_price: Option<Decimal>,
}

/// One page of [`SecuritySummary`] rows.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SecuritiesSummaryPage {
    /// Summary rows in server order.
    pub items: Vec<SecuritySummary>,
    /// `pageIndex`.
    pub page_index: Option<i64>,
    /// `pageSize`.
    pub page_size: Option<i64>,
    /// `pagesCount` (or `pageCount`).
    pub pages_count: Option<i64>,
    /// `itemsCount`.
    pub items_count: Option<i64>,
}

#[derive(Deserialize)]
struct WireSecuritiesSummaryPage {
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
    data: Option<Vec<SecuritySummary>>,
}

pub(crate) fn parse_index_summary(body: Option<&[u8]>) -> Result<Vec<MarketIndexSummary>> {
    let Some(body) = body else {
        return Ok(Vec::new());
    };
    Ok(decode_unwrapped::<Option<Vec<MarketIndexSummary>>>(
        INDEX_SUMMARY,
        body,
        Sensitivity::Public,
    )?
    .unwrap_or_default())
}

pub(crate) fn parse_securities_summary_page(body: Option<&[u8]>) -> Result<SecuritiesSummaryPage> {
    let Some(body) = body else {
        return Ok(SecuritiesSummaryPage::default());
    };
    let page: WireSecuritiesSummaryPage = decode(SECURITIES_SUMMARY, body, Sensitivity::Public)?;
    Ok(SecuritiesSummaryPage {
        items: page.data.unwrap_or_default(),
        page_index: page.page_index,
        page_size: page.page_size,
        pages_count: page.pages_count,
        items_count: page.items_count,
    })
}

impl SsiClient {
    /// Daily index or board summary (`GET /api/v3/data/indexSummary`).
    ///
    /// Pass `trading_date: None` for the latest trading date.
    pub async fn index_summary(
        &self,
        filter: &IndexSummaryFilter,
        trading_date: Option<NaiveDate>,
    ) -> Result<Vec<MarketIndexSummary>> {
        let mut params = vec![match filter {
            IndexSummaryFilter::Board(board) => ("board", rest_board(board)?.to_owned()),
            IndexSummaryFilter::Index(index) => ("index", check_symbol(index)?),
        }];
        if let Some(date) = trading_date {
            params.push(("tradingDate", format_vn_date(date)));
        }
        parse_index_summary(
            self.get_body(INDEX_SUMMARY, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// One page of daily securities summary (`GET /api/v3/data/securitiesSummary`).
    pub async fn securities_summary_page(
        &self,
        query: &SecuritiesSummaryQuery,
        page_index: u32,
    ) -> Result<SecuritiesSummaryPage> {
        if page_index == 0 {
            return Err(Error::InvalidRequest("pageIndex starts at 1".to_owned()));
        }
        let params = query.params(page_index);
        parse_securities_summary_page(
            self.get_body(SECURITIES_SUMMARY, &params, Sensitivity::Public)
                .await?
                .as_deref(),
        )
    }

    /// Every daily securities summary row in the query range, walking all pages.
    pub async fn securities_summary(
        &self,
        query: &SecuritiesSummaryQuery,
    ) -> Result<Vec<SecuritySummary>> {
        let mut rows = Vec::new();
        let mut previous_first: Option<SecuritySummary> = None;
        for page_index in 1..=MAX_PAGES {
            let page = self.securities_summary_page(query, page_index).await?;
            let count = page.items.len();
            let first = page.items.first().cloned();
            if first.is_some() && first == previous_first {
                return Err(Error::Decode {
                    endpoint: SECURITIES_SUMMARY.to_owned(),
                    detail: format!("page {page_index} repeated the previous page"),
                });
            }
            previous_first = first;
            rows.extend(page.items);
            let last = page
                .pages_count
                .is_some_and(|pages| i64::from(page_index) >= pages);
            if count == 0 || last {
                break;
            }
        }
        Ok(rows)
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
    fn parses_index_summary_with_and_without_foreign_fields() {
        let rows = parse_index_summary(Some(
            json!([
                {
                    "tradingDate": "2026/11/19",
                    "totalTrade": "1,024,500",
                    "totalTradeValue": "21500000000000.5",
                    "totalMatch": "950000",
                    "totalMatchValue": "19500000000000",
                    "totalDeal": "74500",
                    "totalDealValue": "2000000000000.5",
                    "indexChange": "-12.45",
                    "indexChangePercentage": "-0.98",
                    "indexValue": "1258.32",
                    "totalAdvanceStock": "140",
                    "totalCeilingStock": "5",
                    "totalDeclineStock": "210",
                    "totalFloorStock": "3",
                    "totalNoChangeStock": "65",
                    "totalPropBuy": "12000",
                    "totalPropBuyValue": "450000000",
                    "totalPropSell": "15000",
                    "totalPropSellValue": "510000000",
                    "totalBuyForeign": "30000",
                    "totalBuyForeignValue": "1200000000",
                    "totalSellForeign": "45000",
                    "totalSellForeignValue": "1800000000",
                    "netPurchasesForeignVolume": "-15000",
                    "netPurchasesForeignValue": "-600000000"
                },
                {
                    "tradingDate": "2026/11/18",
                    "indexValue": "1270.77"
                }
            ])
            .to_string()
            .as_bytes(),
        ))
        .expect("decodes");
        assert_eq!(rows.len(), 2);
        let first = rows.first().expect("first");
        assert_eq!(first.trading_date, date("2026-11-19"));
        assert_eq!(first.total_trade, 1_024_500);
        assert_eq!(first.index_change, dec!(-12.45));
        assert_eq!(first.net_purchases_foreign_volume, Some(dec!(-15000)));
        let second = rows.get(1).expect("second");
        assert_eq!(second.total_buy_foreign, None);
        assert_eq!(second.net_purchases_foreign_value, None);
        assert_eq!(parse_index_summary(None).expect("empty"), Vec::new());
    }

    #[test]
    fn parses_securities_summary_and_validates_query() {
        assert!(
            SecuritiesSummaryQuery::for_symbol("VNM", date("2026-11-20"), date("2026-11-19"))
                .is_err()
        );
        let query =
            SecuritiesSummaryQuery::for_index("vn30", date("2026-11-01"), date("2026-11-19"))
                .expect("valid")
                .with_page_size(50)
                .expect("size");
        let params = query.params(2);
        assert!(params.contains(&("index", "VN30".to_owned())));
        assert!(params.contains(&("from", "2026/11/01".to_owned())));
        assert!(params.contains(&("to", "2026/11/19".to_owned())));
        assert!(params.contains(&("pageIndex", "2".to_owned())));
        assert!(params.contains(&("pageSize", "50".to_owned())));

        let page = parse_securities_summary_page(Some(
            json!({
                "pageSize": 10,
                "pageIndex": 1,
                "pageCount": 3,
                "itemsCount": 25,
                "data": [{
                    "symbol": "VNM",
                    "tradingDate": "2026/11/19",
                    "priceChange": "-500",
                    "priceChangePercentage": "-0.97",
                    "open": "51500",
                    "high": "51800",
                    "low": "51000",
                    "close": "51100",
                    "average": "51350.25",
                    "totalMatch": "2,493,089",
                    "totalMatchValue": "128000000000",
                    "totalBuy": "1200",
                    "totalTradeBuy": "3100000",
                    "totalSell": "1400",
                    "totalTradeSell": "3400000",
                    "totalForeignBuy": "100000",
                    "totalForeignBuyValue": "5110000000",
                    "totalForeignSell": "200000",
                    "totalForeignSellValue": "10220000000",
                    "remainForeignRoom": "-100",
                    "totalForeignRoom": "2089955445",
                    "totalDeal": "50000",
                    "totalDealValue": "2555000000",
                    "openInterest": "0",
                    "settlementPrice": "0"
                }]
            })
            .to_string()
            .as_bytes(),
        ))
        .expect("decodes");
        assert_eq!(page.pages_count, Some(3));
        assert_eq!(page.items_count, Some(25));
        let row = page.items.first().expect("row");
        assert_eq!(row.symbol, "VNM");
        assert_eq!(row.total_match, 2_493_089);
        assert_eq!(row.remain_foreign_room, -100);
        assert_eq!(row.ceiling, None);
        assert_eq!(row.ref_price, None);
    }
}
