//! Flexible Conditional Orders (FCO): GTD, Stop, Stop-Limit, Trailing Stop, Trailing Stop-Limit,
//! OCO, and Bull-Bear placement, cancellation, listing, execution book, and status history.

use std::fmt;

use chrono::{DateTime, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, value::RawValue};

use crate::{
    client::SsiClient,
    config::Config,
    de,
    error::{Error, Result},
    http::Sensitivity,
    model::{
        AccountNo, FcoOperator, FcoPrice, FcoStatus, FcoType, OrderSide, OrderStatus, OrderType,
        check_symbol,
    },
    rest::{decimal_raw_number, decode_private},
    secret::Masked,
    time::{format_vn_datetime, parse_vn_datetime},
};

pub(crate) const EP_FCO_ORDER: &str = "/api/v3/trading/fco/order";
pub(crate) const EP_FCO_LIST: &str = "/api/v3/trading/fco/list";
pub(crate) const EP_FCO_ORDER_BOOK: &str = "/api/v3/trading/fco/orderbook";
pub(crate) const EP_FCO_STATUS_HISTORY: &str = "/api/v3/trading/fco/statusHistory";

/// Maximum FCO validity window allowed by SSI (31 days).
const FCO_MAX_WINDOW_SECS: i64 = 31 * 86_400;

/// Validated FCO validity window (`from <= to`, at most 31 days).
///
/// Serialized on the wire in exchange-local `Asia/Ho_Chi_Minh` `YYYY/MM/DD HH:MM:SS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FcoWindow {
    /// Start instant (UTC).
    pub from: DateTime<Utc>,
    /// End instant (UTC).
    pub to: DateTime<Utc>,
}

impl FcoWindow {
    /// Validates and constructs an FCO validity window.
    pub fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Self> {
        if from > to {
            return Err(Error::InvalidRequest(
                "FCO validity window 'from' cannot be after 'to'".to_owned(),
            ));
        }
        if (to - from).num_seconds() > FCO_MAX_WINDOW_SECS {
            return Err(Error::InvalidRequest(
                "FCO validity window cannot exceed 31 days".to_owned(),
            ));
        }
        Ok(Self { from, to })
    }

    pub(crate) fn format_pair(&self) -> Result<(String, String)> {
        let validated = Self::new(self.from, self.to)?;
        Ok((
            format_vn_datetime(validated.from),
            format_vn_datetime(validated.to),
        ))
    }
}

/// Good-till-date (`gtd`) strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GtdParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Limit price or order-type word.
    pub price: FcoPrice,
    /// Price slip (forced to `0` when `price` is a market word).
    pub price_slip: Decimal,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// Stop market (`stop`) strategy parameters. Sends price `"MTL"` with `priceSlip = 0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Trigger price.
    pub stop_price: Decimal,
    /// Trigger comparison operator.
    pub operator: FcoOperator,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// Stop-limit (`stop_limit`) strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopLimitParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Order price once triggered.
    pub price: FcoPrice,
    /// Price slip.
    pub price_slip: Decimal,
    /// Trigger price.
    pub stop_price: Decimal,
    /// Trigger comparison operator.
    pub operator: FcoOperator,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// Trailing stop market (`trailing_stop`) strategy parameters. Sends price `"MTL"`, `priceSlip = 0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingStopParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Activation price.
    pub active_price: Decimal,
    /// Trailing distance.
    pub trailing_amount: Decimal,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// Trailing stop-limit (`trailing_stop_limit`) strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingStopLimitParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Activation price.
    pub active_price: Decimal,
    /// Trailing distance.
    pub trailing_amount: Decimal,
    /// Price slip from trigger price.
    pub price_slip: Decimal,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// One-cancels-the-other (`oco`) strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OcoParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Take-profit activation price.
    pub tp_active_price: Decimal,
    /// Stop-loss activation price.
    pub sl_active_price: Decimal,
    /// Take-profit order price.
    pub tp_price: FcoPrice,
    /// Stop-loss order price.
    pub sl_price: FcoPrice,
    /// Take-profit price slip.
    pub tp_slip: Decimal,
    /// Stop-loss price slip.
    pub sl_slip: Decimal,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// Bull-bear bracket (`bullbear`) strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BullBearParams {
    /// Account number.
    pub account_no: AccountNo,
    /// Ticker symbol.
    pub symbol: String,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Entry order price.
    pub price: FcoPrice,
    /// Entry price slip (zeroed if `price` is a market word).
    pub price_slip: Decimal,
    /// Take-profit activation price.
    pub tp_active_price: Decimal,
    /// Stop-loss activation price.
    pub sl_active_price: Decimal,
    /// Take-profit order price.
    pub tp_price: FcoPrice,
    /// Stop-loss order price.
    pub sl_price: FcoPrice,
    /// Take-profit price slip.
    pub tp_slip: Decimal,
    /// Stop-loss price slip.
    pub sl_slip: Decimal,
    /// Validity window (`<= 31` days).
    pub window: FcoWindow,
}

/// All seven SSI `FastConnect` V3 Flexible Conditional Order strategies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcoStrategy {
    /// Good-till-date (`gtd`).
    Gtd(GtdParams),
    /// Stop market (`stop`).
    Stop(StopParams),
    /// Stop-limit (`stop_limit`).
    StopLimit(StopLimitParams),
    /// Trailing stop market (`trailing_stop`).
    TrailingStop(TrailingStopParams),
    /// Trailing stop-limit (`trailing_stop_limit`).
    TrailingStopLimit(TrailingStopLimitParams),
    /// One-cancels-the-other (`oco`).
    Oco(OcoParams),
    /// Bull-bear bracket (`bullbear`).
    BullBear(BullBearParams),
}

/// Request to place a Flexible Conditional Order (`POST /api/v3/trading/fco/order` or WS `fco.place`).
#[derive(Clone)]
pub struct FcoPlaceRequest {
    /// Conditional order strategy and parameters.
    pub strategy: FcoStrategy,
    /// Optional one-time code (`code`) when required by the account.
    otp_code: Option<SecretString>,
}

impl FcoPlaceRequest {
    /// Wraps a strategy without an explicit OTP code.
    pub const fn new(strategy: FcoStrategy) -> Self {
        Self {
            strategy,
            otp_code: None,
        }
    }

    /// Attaches an optional one-time OTP `code`.
    #[must_use]
    pub fn with_otp_code(mut self, code: impl Into<String>) -> Self {
        let trimmed = code.into().trim().to_owned();
        self.otp_code = (!trimmed.is_empty()).then(|| SecretString::from(trimmed));
        self
    }

    fn code_str(&self) -> Option<&str> {
        self.otp_code.as_ref().map(ExposeSecret::expose_secret)
    }
}

impl fmt::Debug for FcoPlaceRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FcoPlaceRequest")
            .field("strategy", &self.strategy)
            .field("otp_code", &self.code_str().map(Masked::new))
            .finish()
    }
}

/// Request to cancel an FCO (`DELETE /api/v3/trading/fco/order` or WS `fco.cancel`).
#[derive(Clone)]
pub struct FcoCancelRequest {
    /// Conditional order id (`fcoId`).
    pub fco_id: String,
    /// Optional one-time code (`code`) when required by the account.
    otp_code: Option<SecretString>,
}

impl FcoCancelRequest {
    /// Builds a cancel request for `fco_id`.
    pub fn new(fco_id: impl Into<String>) -> Self {
        Self {
            fco_id: fco_id.into(),
            otp_code: None,
        }
    }

    /// Attaches an optional one-time OTP `code`.
    #[must_use]
    pub fn with_otp_code(mut self, code: impl Into<String>) -> Self {
        let trimmed = code.into().trim().to_owned();
        self.otp_code = (!trimmed.is_empty()).then(|| SecretString::from(trimmed));
        self
    }

    fn code_str(&self) -> Option<&str> {
        self.otp_code.as_ref().map(ExposeSecret::expose_secret)
    }
}

impl fmt::Debug for FcoCancelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FcoCancelRequest")
            .field("fco_id", &self.fco_id)
            .field("otp_code", &self.code_str().map(Masked::new))
            .finish()
    }
}

/// Response from placing an FCO (`POST /api/v3/trading/fco/order` or WS `fco.place`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcoPlaceResponse {
    /// Created FCO id (`fcoId`).
    pub fco_id: String,
}

/// Response from cancelling an FCO (`DELETE /api/v3/trading/fco/order` or WS `fco.cancel`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcoCancelResponse {
    /// Cancelled FCO id (`fcoId`).
    pub fco_id: String,
}

/// Detailed trigger parameters returned inside [`FcoInfo`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FcoParams {
    /// Stop trigger price.
    #[serde(default, rename = "stopPrice", deserialize_with = "de::opt_decimal")]
    pub stop_price: Option<Decimal>,
    /// Side.
    #[serde(default)]
    pub side: Option<OrderSide>,
    /// Trailing activation price.
    #[serde(default, rename = "activePrice", deserialize_with = "de::opt_decimal")]
    pub active_price: Option<Decimal>,
    /// Trailing distance.
    #[serde(
        default,
        rename = "trailingAmount",
        deserialize_with = "de::opt_decimal"
    )]
    pub trailing_amount: Option<Decimal>,
    /// Take-profit activation price.
    #[serde(
        default,
        rename = "tpActivePrice",
        deserialize_with = "de::opt_decimal"
    )]
    pub tp_active_price: Option<Decimal>,
    /// Stop-loss activation price.
    #[serde(
        default,
        rename = "slActivePrice",
        deserialize_with = "de::opt_decimal"
    )]
    pub sl_active_price: Option<Decimal>,
    /// Take-profit order price text.
    #[serde(default, rename = "tpPrice", deserialize_with = "de::string")]
    pub tp_price: String,
    /// Stop-loss order price text.
    #[serde(default, rename = "slPrice", deserialize_with = "de::string")]
    pub sl_price: String,
    /// Take-profit slip.
    #[serde(default, rename = "tpSlip", deserialize_with = "de::opt_decimal")]
    pub tp_slip: Option<Decimal>,
    /// Stop-loss slip.
    #[serde(default, rename = "slSlip", deserialize_with = "de::opt_decimal")]
    pub sl_slip: Option<Decimal>,
    /// Trigger operator (`operator` or `price_operator`, numeric `0..=4` or name).
    #[serde(default, alias = "price_operator")]
    pub operator: Option<FcoOperator>,
}

/// One conditional order item returned by `GET /api/v3/trading/fco/list`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FcoInfo {
    /// FCO id (`fcoId` / `FcoId`).
    #[serde(
        default,
        rename = "fcoId",
        alias = "FcoId",
        deserialize_with = "de::string"
    )]
    pub fco_id: String,
    /// Owner username/client id (`username` / `Username`).
    #[serde(default, alias = "Username", deserialize_with = "de::string")]
    pub client_id: String,
    /// Account number (`accountNo` / `AccountNo`).
    #[serde(default, rename = "accountNo", alias = "AccountNo")]
    pub account_no: AccountNo,
    /// Quantity (`quantity` / `Quantity`).
    #[serde(default, alias = "Quantity", deserialize_with = "de::quantity_or_zero")]
    pub quantity: u64,
    /// Price text.
    #[serde(default, deserialize_with = "de::string")]
    pub price: String,
    /// Price slip.
    #[serde(default, rename = "priceSlip", deserialize_with = "de::opt_decimal")]
    pub price_slip: Option<Decimal>,
    /// Symbol (`symbol` / `Symbol`).
    #[serde(default, alias = "Symbol", deserialize_with = "de::string")]
    pub symbol: String,
    /// Strategy type.
    #[serde(default, rename = "type")]
    pub fco_type: FcoType,
    /// Validity start (`from`).
    #[serde(default, deserialize_with = "de::string")]
    pub from: String,
    /// Validity end (`to`).
    #[serde(default, deserialize_with = "de::string")]
    pub to: String,
    /// Matched quantity (`matchedQuantity` / `matchedQuality`).
    #[serde(
        default,
        rename = "matchedQuantity",
        alias = "matchedQuality",
        deserialize_with = "de::quantity_or_zero"
    )]
    pub matched_quantity: u64,
    /// Whether an exchange order has been placed (`isPlaceOrder` / `IsPlaceOrder`).
    #[serde(
        default,
        rename = "isPlaceOrder",
        alias = "IsPlaceOrder",
        deserialize_with = "de::bool_lenient"
    )]
    pub is_place_order: bool,
    /// Processing status.
    #[serde(default)]
    pub status: FcoStatus,
    /// Engine detail text.
    #[serde(default, deserialize_with = "de::string")]
    pub detail: String,
    /// Strategy trigger parameters (`params` / `fco_params` / `fcoParams`).
    #[serde(default, alias = "fco_params", alias = "fcoParams")]
    pub params: Option<FcoParams>,
    /// Underlying order type.
    #[serde(default, rename = "orderType")]
    pub order_type: Option<OrderType>,
}

/// Filter query for `GET /api/v3/trading/fco/list` and WS `fco.list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcoListQuery {
    /// Trading account number (required).
    pub account_no: AccountNo,
    /// Optional specific `fcoId`.
    pub fco_id: Option<String>,
    /// Optional strategy type filter.
    pub fco_type: Option<FcoType>,
    /// Optional status filters (repeated `processStatus` on REST; at most one on Trading WS).
    pub process_statuses: Vec<FcoStatus>,
    /// Optional symbol filter.
    pub symbol: Option<String>,
    /// Optional side filter.
    pub side: Option<OrderSide>,
    /// Optional lower timestamp bound (`YYYY/MM/DD HH:MM:SS` or `YYYY/MM/DD`).
    pub from: Option<String>,
    /// Optional upper timestamp bound (`YYYY/MM/DD HH:MM:SS` or `YYYY/MM/DD`).
    pub to: Option<String>,
    /// Optional 1-based page index.
    pub page_index: Option<u32>,
    /// Optional page size.
    pub page_size: Option<u32>,
}

impl FcoListQuery {
    /// Builds a query for all FCOs on `account_no`.
    pub const fn new(account_no: AccountNo) -> Self {
        Self {
            account_no,
            fco_id: None,
            fco_type: None,
            process_statuses: Vec::new(),
            symbol: None,
            side: None,
            from: None,
            to: None,
            page_index: None,
            page_size: None,
        }
    }

    /// Sets a specific `fcoId` filter.
    #[must_use]
    pub fn with_fco_id(mut self, fco_id: impl Into<String>) -> Self {
        self.fco_id = Some(fco_id.into());
        self
    }

    /// Filters by strategy type.
    #[must_use]
    pub fn with_type(mut self, fco_type: FcoType) -> Self {
        self.fco_type = Some(fco_type);
        self
    }

    /// Filters by one or more processing statuses.
    #[must_use]
    pub fn with_statuses(mut self, statuses: impl IntoIterator<Item = FcoStatus>) -> Self {
        self.process_statuses = statuses.into_iter().collect();
        self
    }

    /// Filters by symbol.
    #[must_use]
    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    /// Filters by side.
    #[must_use]
    pub fn with_side(mut self, side: OrderSide) -> Self {
        self.side = Some(side);
        self
    }

    /// Sets 1-based pagination.
    #[must_use]
    pub const fn with_page(mut self, page_index: u32, page_size: u32) -> Self {
        self.page_index = Some(page_index);
        self.page_size = Some(page_size);
        self
    }

    fn validate(&self) -> Result<()> {
        self.account_no.validate()?;
        if let Some(symbol) = &self.symbol {
            check_symbol(symbol)?;
        }
        let parsed_from = self
            .from
            .as_deref()
            .filter(|text| !text.trim().is_empty())
            .map(|text| {
                parse_vn_datetime(text)
                    .ok_or_else(|| Error::InvalidRequest("invalid FCO list 'from' date".to_owned()))
            })
            .transpose()?;
        let parsed_to = self
            .to
            .as_deref()
            .filter(|text| !text.trim().is_empty())
            .map(|text| {
                parse_vn_datetime(text)
                    .ok_or_else(|| Error::InvalidRequest("invalid FCO list 'to' date".to_owned()))
            })
            .transpose()?;
        if let (Some(from), Some(to)) = (parsed_from, parsed_to)
            && from > to
        {
            return Err(Error::InvalidRequest(
                "FCO list 'from' cannot be after 'to'".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn to_query_pairs(&self) -> Result<Vec<(&'static str, String)>> {
        self.validate()?;
        let mut pairs = vec![("accountNo", self.account_no.as_str().to_owned())];
        if let Some(fco_id) = self
            .fco_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            pairs.push(("fcoId", fco_id.to_owned()));
        }
        if let Some(fco_type) = &self.fco_type {
            pairs.push(("type", fco_type.as_str().to_owned()));
        }
        for status in &self.process_statuses {
            pairs.push(("processStatus", status.as_str().to_owned()));
        }
        if let Some(symbol) = &self.symbol {
            pairs.push(("symbol", check_symbol(symbol)?));
        }
        if let Some(side) = &self.side {
            pairs.push(("side", side.as_str().to_owned()));
        }
        if let Some(from) = self
            .from
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            pairs.push(("from", from.to_owned()));
        }
        if let Some(to) = self.to.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            pairs.push(("to", to.to_owned()));
        }
        if let Some(page) = self.page_index.filter(|&p| p > 0) {
            pairs.push(("pageIndex", page.to_string()));
        }
        if let Some(size) = self.page_size.filter(|&s| s > 0) {
            pairs.push(("pageSize", size.to_string()));
        }
        Ok(pairs)
    }

    /// Builds Trading WebSocket `fco.list` params (accepts at most one `processStatus`).
    pub(crate) fn to_ws_params(&self) -> Result<Value> {
        self.validate()?;
        if self.process_statuses.len() > 1 {
            return Err(Error::InvalidRequest(
                "Trading WS fco.list accepts at most one processStatus".to_owned(),
            ));
        }
        let mut map = Map::new();
        map.insert(
            "accountNo".to_owned(),
            Value::String(self.account_no.as_str().to_owned()),
        );
        if let Some(fco_id) = self
            .fco_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            map.insert("fcoId".to_owned(), Value::String(fco_id.to_owned()));
        }
        if let Some(fco_type) = &self.fco_type {
            map.insert(
                "type".to_owned(),
                Value::String(fco_type.as_str().to_owned()),
            );
        }
        if let Some(status) = self.process_statuses.first() {
            map.insert(
                "processStatus".to_owned(),
                Value::String(status.as_str().to_owned()),
            );
        }
        if let Some(symbol) = &self.symbol {
            map.insert("symbol".to_owned(), Value::String(check_symbol(symbol)?));
        }
        if let Some(side) = &self.side {
            map.insert("side".to_owned(), Value::String(side.as_str().to_owned()));
        }
        if let Some(from) = self
            .from
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            map.insert("from".to_owned(), Value::String(from.to_owned()));
        }
        if let Some(to) = self.to.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            map.insert("to".to_owned(), Value::String(to.to_owned()));
        }
        if let Some(page) = self.page_index.filter(|&p| p > 0) {
            map.insert("pageIndex".to_owned(), Value::from(page));
        }
        if let Some(size) = self.page_size.filter(|&s| s > 0) {
            map.insert("pageSize".to_owned(), Value::from(size));
        }
        Ok(Value::Object(map))
    }
}

/// One page of `GET /api/v3/trading/fco/list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcoListPage {
    /// 1-based page index.
    pub page_index: u32,
    /// Page size.
    pub page_size: u32,
    /// Total items reported by the server.
    pub items_count: u64,
    /// Pages count reported by the server (can miss the last partial page; page until empty).
    pub pages_count: u64,
    /// Items on this page.
    pub items: Vec<FcoInfo>,
}

impl Default for FcoListPage {
    fn default() -> Self {
        Self {
            page_index: 1,
            page_size: 10,
            items_count: 0,
            pages_count: 0,
            items: Vec::new(),
        }
    }
}

/// One child order entry in `GET /api/v3/trading/fco/orderbook`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FcoOrder {
    /// Parent FCO id (`fcoId` / `FcoId`).
    #[serde(
        default,
        rename = "fcoId",
        alias = "FcoId",
        deserialize_with = "de::string"
    )]
    pub fco_id: String,
    /// Account number (`accountNo` / `AccountNo`).
    #[serde(default, rename = "accountNo", alias = "AccountNo")]
    pub account_no: AccountNo,
    /// Quantity (`quantity` / `Quantity`).
    #[serde(default, alias = "Quantity", deserialize_with = "de::quantity_or_zero")]
    pub quantity: u64,
    /// Price text.
    #[serde(default, deserialize_with = "de::string")]
    pub price: String,
    /// Symbol (`symbol` / `Symbol`).
    #[serde(default, alias = "Symbol", deserialize_with = "de::string")]
    pub symbol: String,
    /// Side.
    #[serde(default)]
    pub side: OrderSide,
    /// Order type.
    #[serde(default, rename = "orderType")]
    pub order_type: OrderType,
    /// Whether this is the main entry order (`isMainOrder` / `IsMainOrder`).
    #[serde(
        default,
        rename = "isMainOrder",
        alias = "IsMainOrder",
        deserialize_with = "de::bool_lenient"
    )]
    pub is_main_order: bool,
    /// Whether this is an attached TP/SL order.
    #[serde(
        default,
        rename = "isAttachedOrder",
        deserialize_with = "de::bool_lenient"
    )]
    pub is_attached_order: bool,
    /// Creation time text.
    #[serde(default, rename = "createdTime", deserialize_with = "de::string")]
    pub created_time: String,
    /// Update time text.
    #[serde(default, rename = "updatedTime", deserialize_with = "de::string")]
    pub updated_time: String,
    /// Unique id (`uniqueId` / `uniquedId` / `UniqueId`).
    #[serde(
        default,
        rename = "uniqueId",
        alias = "uniquedId",
        alias = "UniqueId",
        deserialize_with = "de::string"
    )]
    pub unique_id: String,
    /// Exchange order id.
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    pub order_id: String,
    /// Matched quantity (`matchedQuantity` / `matchedQuality`).
    #[serde(
        default,
        rename = "matchedQuantity",
        alias = "matchedQuality",
        deserialize_with = "de::quantity_or_zero"
    )]
    pub matched_quantity: u64,
    /// Outstanding quantity.
    #[serde(
        default,
        rename = "osQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    pub os_quantity: u64,
    /// Average fill price.
    #[serde(default, rename = "avgPrice", deserialize_with = "de::decimal_or_zero")]
    pub avg_price: Decimal,
    /// Order status.
    #[serde(default)]
    pub status: OrderStatus,
    /// Engine detail text.
    #[serde(default, deserialize_with = "de::string")]
    pub detail: String,
    /// Parent strategy type.
    #[serde(default, rename = "type")]
    pub fco_type: FcoType,
}

/// One page of `GET /api/v3/trading/fco/orderbook`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcoOrderBookPage {
    /// 1-based page index.
    pub page_index: u32,
    /// Page size.
    pub page_size: u32,
    /// Total items reported by the server.
    pub items_count: u64,
    /// Pages count reported by the server.
    pub pages_count: u64,
    /// Child orders on this page.
    pub orders: Vec<FcoOrder>,
}

impl Default for FcoOrderBookPage {
    fn default() -> Self {
        Self {
            page_index: 1,
            page_size: 10,
            items_count: 0,
            pages_count: 0,
            orders: Vec::new(),
        }
    }
}

/// One transition in `GET /api/v3/trading/fco/statusHistory`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FcoStatusHistoryItem {
    /// FCO state name.
    #[serde(default, deserialize_with = "de::string")]
    pub state: String,
    /// Transition timestamp text.
    #[serde(default, deserialize_with = "de::string")]
    pub time: String,
    /// Optional status/error code.
    #[serde(default, deserialize_with = "de::opt_string")]
    pub code: Option<String>,
    /// Transition detail message.
    #[serde(default, deserialize_with = "de::string")]
    pub detail: String,
}

// ── shared validated wire builders ──────────────────────────────────────────

#[derive(Serialize)]
struct GtdWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "type")]
    fco_type: &'static str,
    symbol: String,
    side: String,
    price: String,
    #[serde(rename = "priceSlip")]
    price_slip: Box<RawValue>,
    quantity: u64,
    from: String,
    to: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct StopWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "type")]
    fco_type: &'static str,
    symbol: String,
    side: String,
    price: String,
    #[serde(rename = "priceSlip")]
    price_slip: Box<RawValue>,
    quantity: u64,
    from: String,
    to: String,
    #[serde(rename = "stopPrice")]
    stop_price: Box<RawValue>,
    operator: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct TrailingStopWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "type")]
    fco_type: &'static str,
    symbol: String,
    side: String,
    quantity: u64,
    from: String,
    to: String,
    #[serde(rename = "activePrice")]
    active_price: Box<RawValue>,
    #[serde(rename = "trailingAmount")]
    trailing_amount: Box<RawValue>,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(rename = "priceSlip")]
    price_slip: Box<RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct OcoWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "type")]
    fco_type: &'static str,
    symbol: String,
    side: String,
    quantity: u64,
    from: String,
    to: String,
    #[serde(rename = "tpActivePrice")]
    tp_active_price: Box<RawValue>,
    #[serde(rename = "slActivePrice")]
    sl_active_price: Box<RawValue>,
    #[serde(rename = "tpPrice")]
    tp_price: String,
    #[serde(rename = "slPrice")]
    sl_price: String,
    #[serde(rename = "tpSlip")]
    tp_slip: Box<RawValue>,
    #[serde(rename = "slSlip")]
    sl_slip: Box<RawValue>,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    price: &'static str,
    #[serde(rename = "priceSlip")]
    price_slip: i32,
    #[serde(rename = "stopPrice")]
    stop_price: i32,
    #[serde(rename = "activePrice")]
    active_price: i32,
    #[serde(rename = "trailingAmount")]
    trailing_amount: i32,
    operator: &'static str,
    code: String,
}

#[derive(Serialize)]
struct BullBearWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "type")]
    fco_type: &'static str,
    symbol: String,
    side: String,
    quantity: u64,
    price: String,
    #[serde(rename = "priceSlip")]
    price_slip: Box<RawValue>,
    from: String,
    to: String,
    #[serde(rename = "tpActivePrice")]
    tp_active_price: Box<RawValue>,
    #[serde(rename = "slActivePrice")]
    sl_active_price: Box<RawValue>,
    #[serde(rename = "tpPrice")]
    tp_price: String,
    #[serde(rename = "slPrice")]
    sl_price: String,
    #[serde(rename = "tpSlip")]
    tp_slip: Box<RawValue>,
    #[serde(rename = "slSlip")]
    sl_slip: Box<RawValue>,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct FcoCancelWire {
    #[serde(rename = "fcoId")]
    fco_id: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent", skip_serializing_if = "Option::is_none")]
    user_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

fn require_qty(qty: u64) -> Result<()> {
    if qty == 0 {
        return Err(Error::InvalidRequest(
            "FCO quantity must be strictly positive".to_owned(),
        ));
    }
    Ok(())
}

fn serialize_fco<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|_| Error::InvalidRequest("FCO payload could not be serialized".to_owned()))
}

/// Validates and serializes an FCO placement request (shared by REST and Trading WS).
#[allow(
    clippy::too_many_lines,
    reason = "dispatches all seven FCO strategy wire shapes in one place"
)]
pub(crate) fn build_fco_place_json(config: &Config, req: &FcoPlaceRequest) -> Result<String> {
    let device_id = config.resolved_device_id().to_owned();
    let user_agent = config.user_agent.clone();
    let code = req.code_str().map(ToOwned::to_owned);

    match &req.strategy {
        FcoStrategy::Gtd(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            let (price, slip) = p.price.wire_and_slip(p.price_slip)?;
            serialize_fco(&GtdWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::Gtd.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                price,
                price_slip: decimal_raw_number(slip)?,
                quantity: p.quantity,
                from,
                to,
                device_id,
                user_agent,
                code,
            })
        }
        FcoStrategy::Stop(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            serialize_fco(&StopWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::Stop.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                price: OrderType::Mtl.as_str().to_owned(),
                price_slip: decimal_raw_number(Decimal::ZERO)?,
                quantity: p.quantity,
                from,
                to,
                stop_price: decimal_raw_number(p.stop_price)?,
                operator: p.operator.as_str().to_owned(),
                device_id,
                user_agent,
                code,
            })
        }
        FcoStrategy::StopLimit(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            let (price, _) = p.price.wire_and_slip(p.price_slip)?;
            serialize_fco(&StopWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::StopLimit.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                price,
                price_slip: decimal_raw_number(p.price_slip)?,
                quantity: p.quantity,
                from,
                to,
                stop_price: decimal_raw_number(p.stop_price)?,
                operator: p.operator.as_str().to_owned(),
                device_id,
                user_agent,
                code,
            })
        }
        FcoStrategy::TrailingStop(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            serialize_fco(&TrailingStopWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::TrailingStop.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                quantity: p.quantity,
                from,
                to,
                active_price: decimal_raw_number(p.active_price)?,
                trailing_amount: decimal_raw_number(p.trailing_amount)?,
                user_agent,
                device_id,
                price: Some(OrderType::Mtl.as_str().to_owned()),
                price_slip: decimal_raw_number(Decimal::ZERO)?,
                code,
            })
        }
        FcoStrategy::TrailingStopLimit(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            serialize_fco(&TrailingStopWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::TrailingStopLimit.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                quantity: p.quantity,
                from,
                to,
                active_price: decimal_raw_number(p.active_price)?,
                trailing_amount: decimal_raw_number(p.trailing_amount)?,
                user_agent,
                device_id,
                price: None,
                price_slip: decimal_raw_number(p.price_slip)?,
                code,
            })
        }
        FcoStrategy::Oco(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            let (tp_price, _) = p.tp_price.wire_and_slip(p.tp_slip)?;
            let (sl_price, _) = p.sl_price.wire_and_slip(p.sl_slip)?;
            serialize_fco(&OcoWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::Oco.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                quantity: p.quantity,
                from,
                to,
                tp_active_price: decimal_raw_number(p.tp_active_price)?,
                sl_active_price: decimal_raw_number(p.sl_active_price)?,
                tp_price,
                sl_price,
                tp_slip: decimal_raw_number(p.tp_slip)?,
                sl_slip: decimal_raw_number(p.sl_slip)?,
                user_agent,
                device_id,
                price: "MP",
                price_slip: 0,
                stop_price: 0,
                active_price: 0,
                trailing_amount: 0,
                operator: "",
                code: code.unwrap_or_default(),
            })
        }
        FcoStrategy::BullBear(p) => {
            p.account_no.validate()?;
            let symbol = check_symbol(&p.symbol)?;
            require_qty(p.quantity)?;
            let (from, to) = p.window.format_pair()?;
            let (price, slip) = p.price.wire_and_slip(p.price_slip)?;
            let (tp_price, tp_slip) = p.tp_price.wire_and_slip(p.tp_slip)?;
            let (sl_price, sl_slip) = p.sl_price.wire_and_slip(p.sl_slip)?;
            serialize_fco(&BullBearWire {
                account_no: p.account_no.as_str().to_owned(),
                fco_type: FcoType::BullBear.as_str(),
                symbol,
                side: p.side.as_str().to_owned(),
                quantity: p.quantity,
                price,
                price_slip: decimal_raw_number(slip)?,
                from,
                to,
                tp_active_price: decimal_raw_number(p.tp_active_price)?,
                sl_active_price: decimal_raw_number(p.sl_active_price)?,
                tp_price,
                sl_price,
                tp_slip: decimal_raw_number(tp_slip)?,
                sl_slip: decimal_raw_number(sl_slip)?,
                user_agent,
                device_id,
                code,
            })
        }
    }
}

/// Validates and serializes an FCO cancel request (shared by REST and Trading WS).
pub(crate) fn build_fco_cancel_json(config: &Config, req: &FcoCancelRequest) -> Result<String> {
    let fco_id = req.fco_id.trim();
    if fco_id.is_empty() {
        return Err(Error::InvalidRequest("fcoId cannot be empty".to_owned()));
    }
    serialize_fco(&FcoCancelWire {
        fco_id: fco_id.to_owned(),
        device_id: config.resolved_device_id().to_owned(),
        user_agent: config.explicit_user_agent().map(ToOwned::to_owned),
        code: req.code_str().map(ToOwned::to_owned),
    })
}

// ── wire response parsing ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct WireFcoId {
    #[serde(default, rename = "fcoId", deserialize_with = "de::opt_string")]
    fco_id: Option<String>,
    #[serde(default)]
    data: Option<Box<Self>>,
}

pub(crate) fn parse_fco_id(endpoint: &str, bytes: &[u8]) -> Result<String> {
    let parsed: WireFcoId = decode_private(endpoint, bytes)?;
    Ok(parsed
        .data
        .and_then(|inner| inner.fco_id)
        .or(parsed.fco_id)
        .unwrap_or_default())
}

#[derive(Deserialize)]
struct WireFcoListPage {
    #[serde(default, rename = "pageIndex", deserialize_with = "de::opt_quantity")]
    page_index: Option<u64>,
    #[serde(default, rename = "pageSize", deserialize_with = "de::opt_quantity")]
    page_size: Option<u64>,
    #[serde(
        default,
        rename = "itemsCount",
        deserialize_with = "de::quantity_or_zero"
    )]
    items_count: u64,
    #[serde(
        default,
        rename = "pagesCount",
        alias = "pageCount",
        deserialize_with = "de::quantity_or_zero"
    )]
    pages_count: u64,
    #[serde(
        default,
        rename = "data",
        alias = "fcoList",
        alias = "fco_list",
        deserialize_with = "de::null_as_empty"
    )]
    items: Vec<FcoInfo>,
}

pub(crate) fn parse_fco_list_page(bytes: &[u8]) -> Result<FcoListPage> {
    let wire: WireFcoListPage = decode_private(EP_FCO_LIST, bytes)?;
    Ok(FcoListPage {
        page_index: wire
            .page_index
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(1),
        page_size: wire
            .page_size
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(10),
        items_count: wire.items_count,
        pages_count: wire.pages_count,
        items: wire.items,
    })
}

#[derive(Deserialize)]
struct WireFcoOrderBookPage {
    #[serde(default, rename = "pageIndex", deserialize_with = "de::opt_quantity")]
    page_index: Option<u64>,
    #[serde(default, rename = "pageSize", deserialize_with = "de::opt_quantity")]
    page_size: Option<u64>,
    #[serde(
        default,
        rename = "itemsCount",
        deserialize_with = "de::quantity_or_zero"
    )]
    items_count: u64,
    #[serde(
        default,
        rename = "pagesCount",
        alias = "pageCount",
        deserialize_with = "de::quantity_or_zero"
    )]
    pages_count: u64,
    #[serde(
        default,
        rename = "data",
        alias = "orderBook",
        alias = "order_book",
        deserialize_with = "de::null_as_empty"
    )]
    orders: Vec<FcoOrder>,
}

pub(crate) fn parse_fco_order_book_page(bytes: &[u8]) -> Result<FcoOrderBookPage> {
    let wire: WireFcoOrderBookPage = decode_private(EP_FCO_ORDER_BOOK, bytes)?;
    Ok(FcoOrderBookPage {
        page_index: wire
            .page_index
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(1),
        page_size: wire
            .page_size
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(10),
        items_count: wire.items_count,
        pages_count: wire.pages_count,
        orders: wire.orders,
    })
}

#[derive(Deserialize)]
struct WrappedStatusHistory {
    #[serde(default, deserialize_with = "de::null_as_empty")]
    data: Vec<FcoStatusHistoryItem>,
}

pub(crate) fn parse_fco_status_history(bytes: &[u8]) -> Result<Vec<FcoStatusHistoryItem>> {
    if bytes.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'[') {
        decode_private(EP_FCO_STATUS_HISTORY, bytes)
    } else {
        let wrapped: WrappedStatusHistory = decode_private(EP_FCO_STATUS_HISTORY, bytes)?;
        Ok(wrapped.data)
    }
}

impl SsiClient {
    /// Places a Flexible Conditional Order (`POST /api/v3/trading/fco/order`). Signed and **never
    /// retried**.
    pub async fn place_fco(&self, request: &FcoPlaceRequest) -> Result<FcoPlaceResponse> {
        let body = build_fco_place_json(self.config(), request)?;
        let reply = self
            .send_signed_body(Method::POST, EP_FCO_ORDER, body)
            .await?;
        Ok(FcoPlaceResponse {
            fco_id: parse_fco_id(EP_FCO_ORDER, &reply)?,
        })
    }

    /// Cancels a Flexible Conditional Order (`DELETE /api/v3/trading/fco/order`). Signed and
    /// **never retried**.
    pub async fn cancel_fco(&self, request: &FcoCancelRequest) -> Result<FcoCancelResponse> {
        let body = build_fco_cancel_json(self.config(), request)?;
        let reply = self
            .send_signed_body(Method::DELETE, EP_FCO_ORDER, body)
            .await?;
        Ok(FcoCancelResponse {
            fco_id: parse_fco_id(EP_FCO_ORDER, &reply)?,
        })
    }

    /// Lists Flexible Conditional Orders (`GET /api/v3/trading/fco/list`).
    pub async fn fco_list(&self, query: &FcoListQuery) -> Result<FcoListPage> {
        let params = query.to_query_pairs()?;
        let Some(body) = self
            .get_body(EP_FCO_LIST, &params, Sensitivity::Private)
            .await?
        else {
            return Ok(FcoListPage::default());
        };
        parse_fco_list_page(&body)
    }

    /// Lists child orders produced by one FCO (`GET /api/v3/trading/fco/orderbook`).
    pub async fn fco_order_book(
        &self,
        fco_id: &str,
        page_index: u32,
        page_size: u32,
    ) -> Result<FcoOrderBookPage> {
        let fco_id = fco_id.trim();
        if fco_id.is_empty() {
            return Err(Error::InvalidRequest("fcoId cannot be empty".to_owned()));
        }
        let page = page_index.max(1);
        let size = page_size.max(1);
        let query = vec![
            ("fcoId", fco_id.to_owned()),
            ("pageIndex", page.to_string()),
            ("pageSize", size.to_string()),
        ];
        let Some(body) = self
            .get_body(EP_FCO_ORDER_BOOK, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(FcoOrderBookPage::default());
        };
        parse_fco_order_book_page(&body)
    }

    /// Lists state transitions of one FCO (`GET /api/v3/trading/fco/statusHistory?fcoid=...`).
    pub async fn fco_status_history(&self, fco_id: &str) -> Result<Vec<FcoStatusHistoryItem>> {
        let fco_id = fco_id.trim();
        if fco_id.is_empty() {
            return Err(Error::InvalidRequest("fcoId cannot be empty".to_owned()));
        }
        // SSI REST statusHistory expects lowercase `fcoid`!
        let query = vec![("fcoid", fco_id.to_owned())];
        let Some(body) = self
            .get_body(EP_FCO_STATUS_HISTORY, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(Vec::new());
        };
        parse_fco_status_history(&body)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;
    use crate::Credentials;

    fn cfg() -> Config {
        Config::new(Credentials::new("k", "s").expect("creds")).with_device_id("dev-fco")
    }

    #[test]
    fn window_validates_ordering_and_31_day_limit() {
        let t0 = parse_vn_datetime("2026/07/01 09:00:00").expect("t0");
        let t1 = parse_vn_datetime("2026/07/20 15:00:00").expect("t1");
        let t_late = parse_vn_datetime("2026/08/15 15:00:00").expect("late");
        assert!(FcoWindow::new(t0, t1).is_ok());
        assert!(FcoWindow::new(t1, t0).is_err());
        assert!(FcoWindow::new(t0, t_late).is_err());
    }

    #[test]
    fn fco_wire_payloads_match_ssi_contract() {
        let t0 = parse_vn_datetime("2026/07/01 09:00:00").expect("t0");
        let t1 = parse_vn_datetime("2026/07/10 15:00:00").expect("t1");
        let window = FcoWindow::new(t0, t1).expect("window");
        let acct = AccountNo::new("003C123451");

        let gtd = FcoPlaceRequest::new(FcoStrategy::Gtd(GtdParams {
            account_no: acct.clone(),
            symbol: "vnm".to_owned(),
            side: OrderSide::Buy,
            price: FcoPrice::OrderType(OrderType::Mtl),
            price_slip: dec!(500),
            quantity: 100,
            window,
        }));
        let val: Value =
            serde_json::from_str(&build_fco_place_json(&cfg(), &gtd).expect("ok")).expect("json");
        assert_eq!(val.get("type").and_then(Value::as_str), Some("gtd"));
        assert_eq!(val.get("price").and_then(Value::as_str), Some("MTL"));
        assert_eq!(val.get("priceSlip").and_then(Value::as_i64), Some(0));
        assert_eq!(
            val.get("from").and_then(Value::as_str),
            Some("2026/07/01 09:00:00")
        );
        assert_eq!(
            val.get("to").and_then(Value::as_str),
            Some("2026/07/10 15:00:00")
        );

        let oco = FcoPlaceRequest::new(FcoStrategy::Oco(OcoParams {
            account_no: acct,
            symbol: "VNM".to_owned(),
            quantity: 100,
            side: OrderSide::Sell,
            tp_active_price: dec!(70000),
            sl_active_price: dec!(60000),
            tp_price: FcoPrice::Limit(dec!(70500)),
            sl_price: FcoPrice::OrderType(OrderType::Mp),
            tp_slip: dec!(100),
            sl_slip: dec!(0),
            window,
        }));
        let val: Value =
            serde_json::from_str(&build_fco_place_json(&cfg(), &oco).expect("ok")).expect("json");
        assert_eq!(val.get("type").and_then(Value::as_str), Some("oco"));
        assert_eq!(val.get("price").and_then(Value::as_str), Some("MP"));
        assert_eq!(
            val.get("tpActivePrice").and_then(Value::as_i64),
            Some(70000)
        );
        assert_eq!(val.get("tpPrice").and_then(Value::as_str), Some("70500"));

        // Cancel omits userAgent unless explicitly set on Config.
        let cancel = FcoCancelRequest::new("fco-99").with_otp_code("999888");
        assert!(!format!("{cancel:?}").contains("999888"));
        let val: Value = serde_json::from_str(&build_fco_cancel_json(&cfg(), &cancel).expect("ok"))
            .expect("json");
        assert!(val.get("userAgent").is_none());
        assert_eq!(val.get("fcoId").and_then(Value::as_str), Some("fco-99"));
        assert_eq!(val.get("code").and_then(Value::as_str), Some("999888"));
    }
}
