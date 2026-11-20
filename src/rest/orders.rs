//! Signed single and batch order placement, modification, and cancellation.
//!
//! All mutating order endpoints are signed with RSA PKCS#1 v1.5 SHA-256 (`X-Signature` hex on
//! REST; `signature` base64/hex on Trading WebSocket) over the exact JSON payload bytes and are
//! **never retried automatically**.

use reqwest::Method;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    client::SsiClient,
    config::Config,
    de::{self, format_plain_decimal},
    error::{Error, Result},
    model::{AccountNo, ClientRequestId, OrderSide, OrderStatus, OrderType, check_symbol},
    rest::decode_private,
};

pub(crate) const EP_TRADING_ORDER: &str = "/api/v3/trading/order";
pub(crate) const EP_TRADING_ORDER_BATCH: &str = "/api/v3/trading/order/batch";

/// Request to place a single order (`POST /api/v3/trading/order` or WS `order.place`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceOrderRequest {
    /// Trading account number.
    pub account_no: AccountNo,
    /// Ticker symbol (upper-cased automatically).
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Quantity (`> 0`).
    pub quantity: u64,
    /// Order price: strictly positive for `LO` / `PLO`, `0` for market-priced types
    /// (`ATO`, `ATC`, `MP`, `MTL`, `MOK`, `MAK`).
    pub price: Decimal,
    /// Order type.
    pub order_type: OrderType,
    /// Caller-supplied idempotency key (1–20 chars). Generated automatically when `None`.
    pub client_request_id: Option<ClientRequestId>,
}

impl PlaceOrderRequest {
    /// Builds a limit (`LO`) order request.
    pub fn limit(
        account_no: AccountNo,
        symbol: impl Into<String>,
        side: OrderSide,
        quantity: u64,
        price: Decimal,
    ) -> Self {
        Self {
            account_no,
            symbol: symbol.into(),
            side,
            quantity,
            price,
            order_type: OrderType::Lo,
            client_request_id: None,
        }
    }

    /// Builds a market-priced order request (`price = 0`, e.g. `MTL`, `ATO`, `ATC`, `MP`).
    pub fn market(
        account_no: AccountNo,
        symbol: impl Into<String>,
        side: OrderSide,
        quantity: u64,
        order_type: OrderType,
    ) -> Self {
        Self {
            account_no,
            symbol: symbol.into(),
            side,
            quantity,
            price: Decimal::ZERO,
            order_type,
            client_request_id: None,
        }
    }

    /// Attaches an explicit caller idempotency key (`1..=20` chars).
    #[must_use]
    pub fn with_client_request_id(mut self, id: ClientRequestId) -> Self {
        self.client_request_id = Some(id);
        self
    }
}

/// Identifies an existing order by either SSI's `orderId` or the caller's `clientRequestId`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderTarget {
    /// Server-assigned order id.
    OrderId(String),
    /// Original caller idempotency key (`clientRequestId`).
    ClientRequestId(ClientRequestId),
}

impl OrderTarget {
    fn validate_and_split(&self) -> Result<(Option<String>, Option<String>)> {
        match self {
            Self::OrderId(id) => {
                let trimmed = id.trim();
                if trimmed.is_empty() {
                    return Err(Error::InvalidRequest("orderId cannot be empty".to_owned()));
                }
                Ok((Some(trimmed.to_owned()), None))
            }
            Self::ClientRequestId(id) => Ok((None, Some(id.as_str().to_owned()))),
        }
    }
}

/// One change to apply in [`ModifyOrderRequest`].
///
/// SSI forbids changing both price and quantity in the same request (`400107`); this enum
/// enforces that invariant at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModifyChange {
    /// New limit price (must be strictly positive).
    Price(Decimal),
    /// New order quantity (must be strictly positive).
    Quantity(u64),
}

/// Request to amend an existing order (`PUT /api/v3/trading/order` or WS `order.amend`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyOrderRequest {
    /// Trading account number.
    pub account_no: AccountNo,
    /// Target order (`orderId` or `clientRequestId`).
    pub target: OrderTarget,
    /// Price OR quantity change.
    pub change: ModifyChange,
    /// Optional idempotency key for this modify command (`clientModifyId`). Generated when `None`.
    pub client_modify_id: Option<ClientRequestId>,
}

impl ModifyOrderRequest {
    /// Builds a modify-price request.
    pub const fn price(account_no: AccountNo, target: OrderTarget, price: Decimal) -> Self {
        Self {
            account_no,
            target,
            change: ModifyChange::Price(price),
            client_modify_id: None,
        }
    }

    /// Builds a modify-quantity request.
    pub const fn quantity(account_no: AccountNo, target: OrderTarget, quantity: u64) -> Self {
        Self {
            account_no,
            target,
            change: ModifyChange::Quantity(quantity),
            client_modify_id: None,
        }
    }

    /// Attaches an explicit `clientModifyId`.
    #[must_use]
    pub fn with_client_modify_id(mut self, id: ClientRequestId) -> Self {
        self.client_modify_id = Some(id);
        self
    }
}

/// Request to cancel an existing order (`DELETE /api/v3/trading/order` or WS `order.cancel`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrderRequest {
    /// Trading account number.
    pub account_no: AccountNo,
    /// Target order (`orderId` or `clientRequestId`).
    pub target: OrderTarget,
    /// Optional idempotency key for this cancel command (`clientCancelId`). Generated when `None`.
    pub client_cancel_id: Option<ClientRequestId>,
}

impl CancelOrderRequest {
    /// Builds a cancel request.
    pub const fn new(account_no: AccountNo, target: OrderTarget) -> Self {
        Self {
            account_no,
            target,
            client_cancel_id: None,
        }
    }

    /// Attaches an explicit `clientCancelId`.
    #[must_use]
    pub fn with_client_cancel_id(mut self, id: ClientRequestId) -> Self {
        self.client_cancel_id = Some(id);
        self
    }
}

/// One order inside [`SsiClient::place_batch_orders`].
pub type BatchPlaceOrderItem = PlaceOrderRequest;

/// One cancel entry inside [`SsiClient::cancel_batch_orders`].
pub type BatchCancelOrderItem = CancelOrderRequest;

/// Response from `POST /api/v3/trading/order` or WS `order.place`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PlaceOrderResponse {
    /// SSI order id (may be empty on async `PD` acknowledgement).
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    pub order_id: String,
    /// Idempotency key echoed by SSI.
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    pub client_request_id: String,
    /// Immediate order status (typically `PD` / `QU`).
    #[serde(default, rename = "orderStatus")]
    pub status: OrderStatus,
}

/// Response from `PUT /api/v3/trading/order` or WS `order.amend`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ModifyOrderResponse {
    /// Idempotency key of the modify command.
    #[serde(default, rename = "clientModifyId", deserialize_with = "de::string")]
    pub client_modify_id: String,
    /// SSI order id.
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    pub order_id: String,
    /// Original order's `clientRequestId`.
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    pub client_request_id: String,
    /// Order status.
    #[serde(default, rename = "orderStatus")]
    pub status: OrderStatus,
}

/// Response from `DELETE /api/v3/trading/order` or WS `order.cancel`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CancelOrderResponse {
    /// Idempotency key of the cancel command.
    #[serde(default, rename = "clientCancelId", deserialize_with = "de::string")]
    pub client_cancel_id: String,
    /// SSI order id.
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    pub order_id: String,
    /// Original order's `clientRequestId`.
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    pub client_request_id: String,
    /// Order status.
    #[serde(default, rename = "orderStatus")]
    pub status: OrderStatus,
}

/// Outcome of one item in a batch place or cancel response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BatchOrderResult {
    /// Order's `clientRequestId`.
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    pub client_request_id: String,
    /// SSI order id.
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    pub order_id: String,
    /// Order status.
    #[serde(default, rename = "orderStatus")]
    pub status: OrderStatus,
    /// Whether this individual item succeeded.
    #[serde(default, deserialize_with = "de::bool_lenient")]
    pub success: bool,
    /// Per-item error code, if failed.
    #[serde(default, rename = "errorCode", deserialize_with = "de::string")]
    pub error_code: String,
    /// Per-item error message, if failed.
    #[serde(default, rename = "errorMessage", deserialize_with = "de::string")]
    pub error_message: String,
    /// Cancel idempotency key (populated for batch cancel).
    #[serde(default, rename = "clientCancelId", deserialize_with = "de::string")]
    pub client_cancel_id: String,
}

/// Response from `POST` / `DELETE /api/v3/trading/order/batch` or WS batch commands.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct BatchOrderResponse {
    /// Batch idempotency key echoed by SSI.
    #[serde(default, rename = "batchRequestId", deserialize_with = "de::string")]
    pub batch_request_id: String,
    /// Per-order outcomes, in request order.
    #[serde(default, deserialize_with = "de::null_as_empty")]
    pub results: Vec<BatchOrderResult>,
}

// ── shared validated wire builders (REST + Trading WS) ──────────────────────

#[derive(Serialize)]
struct PlaceOrderWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    symbol: String,
    side: String,
    quantity: u64,
    price: String,
    #[serde(rename = "orderType")]
    order_type: String,
    #[serde(rename = "clientRequestId")]
    client_request_id: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
}

#[derive(Serialize)]
struct ModifyOrderWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "clientModifyId")]
    client_modify_id: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "orderId", skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
    #[serde(rename = "clientRequestId", skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quantity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
}

#[derive(Serialize)]
struct CancelOrderWire {
    #[serde(rename = "accountNo")]
    account_no: String,
    #[serde(rename = "clientCancelId")]
    client_cancel_id: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "orderId", skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
    #[serde(rename = "clientRequestId", skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
}

#[derive(Serialize)]
struct BatchWire<T> {
    #[serde(rename = "batchRequestId")]
    batch_request_id: String,
    #[serde(rename = "batchRequestTime")]
    batch_request_time: i64,
    orders: Vec<T>,
}

fn build_place_wire(config: &Config, req: &PlaceOrderRequest) -> Result<PlaceOrderWire> {
    req.account_no.validate()?;
    let symbol = check_symbol(&req.symbol)?;
    if req.quantity == 0 {
        return Err(Error::InvalidRequest(
            "order quantity must be strictly positive".to_owned(),
        ));
    }
    if req.price < Decimal::ZERO {
        return Err(Error::InvalidRequest(
            "order price cannot be negative".to_owned(),
        ));
    }
    if req.order_type.is_priced() && req.price <= Decimal::ZERO {
        return Err(Error::InvalidRequest(format!(
            "price must be strictly positive for {} orders",
            req.order_type
        )));
    }
    let price_str = if req.order_type.is_market_word() {
        "0".to_owned()
    } else {
        format_plain_decimal(req.price)
    };
    let client_request_id = req
        .client_request_id
        .clone()
        .unwrap_or_else(ClientRequestId::generate);
    Ok(PlaceOrderWire {
        account_no: req.account_no.as_str().to_owned(),
        symbol,
        side: req.side.as_str().to_owned(),
        quantity: req.quantity,
        price: price_str,
        order_type: req.order_type.as_str().to_owned(),
        client_request_id: client_request_id.as_str().to_owned(),
        device_id: config.resolved_device_id().to_owned(),
        user_agent: config.user_agent.clone(),
    })
}

fn build_cancel_wire(config: &Config, req: &CancelOrderRequest) -> Result<CancelOrderWire> {
    req.account_no.validate()?;
    let (order_id, client_request_id) = req.target.validate_and_split()?;
    let client_cancel_id = req
        .client_cancel_id
        .clone()
        .unwrap_or_else(ClientRequestId::generate);
    Ok(CancelOrderWire {
        account_no: req.account_no.as_str().to_owned(),
        client_cancel_id: client_cancel_id.as_str().to_owned(),
        device_id: config.resolved_device_id().to_owned(),
        user_agent: config.user_agent.clone(),
        order_id,
        client_request_id,
    })
}

fn serialize_wire<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|_| Error::InvalidRequest("order payload could not be serialized".to_owned()))
}

/// Validates and serializes a single place-order body (signed verbatim by REST and Trading WS).
pub(crate) fn build_place_order_json(config: &Config, req: &PlaceOrderRequest) -> Result<String> {
    serialize_wire(&build_place_wire(config, req)?)
}

/// Validates and serializes a single modify-order body.
pub(crate) fn build_modify_order_json(config: &Config, req: &ModifyOrderRequest) -> Result<String> {
    req.account_no.validate()?;
    let (order_id, client_request_id) = req.target.validate_and_split()?;
    let (price, quantity) = match req.change {
        ModifyChange::Price(price) => {
            if price <= Decimal::ZERO {
                return Err(Error::InvalidRequest(
                    "modified price must be strictly positive".to_owned(),
                ));
            }
            (Some(format_plain_decimal(price)), None)
        }
        ModifyChange::Quantity(qty) => {
            if qty == 0 {
                return Err(Error::InvalidRequest(
                    "modified quantity must be strictly positive".to_owned(),
                ));
            }
            (None, Some(qty))
        }
    };
    let client_modify_id = req
        .client_modify_id
        .clone()
        .unwrap_or_else(ClientRequestId::generate);
    serialize_wire(&ModifyOrderWire {
        account_no: req.account_no.as_str().to_owned(),
        client_modify_id: client_modify_id.as_str().to_owned(),
        device_id: config.resolved_device_id().to_owned(),
        user_agent: config.user_agent.clone(),
        order_id,
        client_request_id,
        quantity,
        price,
    })
}

/// Validates and serializes a single cancel-order body.
pub(crate) fn build_cancel_order_json(config: &Config, req: &CancelOrderRequest) -> Result<String> {
    serialize_wire(&build_cancel_wire(config, req)?)
}

fn check_batch_len(len: usize, limit: usize) -> Result<()> {
    if len == 0 {
        return Err(Error::InvalidRequest(
            "a batch needs at least one order".to_owned(),
        ));
    }
    if len > limit {
        return Err(Error::InvalidRequest(format!(
            "a batch holds at most {limit} orders, got {len}"
        )));
    }
    Ok(())
}

/// Validates every item first (all-or-nothing) and serializes a batch-place body.
pub(crate) fn build_batch_place_json(
    config: &Config,
    items: &[BatchPlaceOrderItem],
    batch_id: Option<ClientRequestId>,
) -> Result<String> {
    check_batch_len(items.len(), config.max_batch_orders())?;
    let mut orders = Vec::with_capacity(items.len());
    for item in items {
        orders.push(build_place_wire(config, item)?);
    }
    let batch_request_id = batch_id
        .unwrap_or_else(ClientRequestId::generate)
        .as_str()
        .to_owned();
    serialize_wire(&BatchWire {
        batch_request_id,
        batch_request_time: config.clock.now().timestamp_millis(),
        orders,
    })
}

/// Validates every item first (all-or-nothing) and serializes a batch-cancel body.
pub(crate) fn build_batch_cancel_json(
    config: &Config,
    items: &[BatchCancelOrderItem],
    batch_id: Option<ClientRequestId>,
) -> Result<String> {
    check_batch_len(items.len(), config.max_batch_orders())?;
    let mut orders = Vec::with_capacity(items.len());
    for item in items {
        orders.push(build_cancel_wire(config, item)?);
    }
    let batch_request_id = batch_id
        .unwrap_or_else(ClientRequestId::generate)
        .as_str()
        .to_owned();
    serialize_wire(&BatchWire {
        batch_request_id,
        batch_request_time: config.clock.now().timestamp_millis(),
        orders,
    })
}

impl SsiClient {
    /// Places one order (`POST /api/v3/trading/order`). Signed and **never retried**.
    pub async fn place_order(&self, request: &PlaceOrderRequest) -> Result<PlaceOrderResponse> {
        let body = build_place_order_json(self.config(), request)?;
        let reply = self
            .send_signed_body(Method::POST, EP_TRADING_ORDER, body)
            .await?;
        decode_private(EP_TRADING_ORDER, &reply)
    }

    /// Modifies the price OR quantity of one order (`PUT /api/v3/trading/order`). Signed and
    /// **never retried**.
    pub async fn modify_order(&self, request: &ModifyOrderRequest) -> Result<ModifyOrderResponse> {
        let body = build_modify_order_json(self.config(), request)?;
        let reply = self
            .send_signed_body(Method::PUT, EP_TRADING_ORDER, body)
            .await?;
        decode_private(EP_TRADING_ORDER, &reply)
    }

    /// Cancels one order (`DELETE /api/v3/trading/order`). Signed and **never retried**.
    pub async fn cancel_order(&self, request: &CancelOrderRequest) -> Result<CancelOrderResponse> {
        let body = build_cancel_order_json(self.config(), request)?;
        let reply = self
            .send_signed_body(Method::DELETE, EP_TRADING_ORDER, body)
            .await?;
        decode_private(EP_TRADING_ORDER, &reply)
    }

    /// Places `1..=max_batch_orders` orders in one request (`POST /api/v3/trading/order/batch`).
    ///
    /// All items are validated locally before anything is signed or sent; one invalid order
    /// rejects the entire batch. Signed and **never retried**.
    pub async fn place_batch_orders(
        &self,
        items: &[BatchPlaceOrderItem],
        batch_id: Option<ClientRequestId>,
    ) -> Result<BatchOrderResponse> {
        let body = build_batch_place_json(self.config(), items, batch_id)?;
        let reply = self
            .send_signed_body(Method::POST, EP_TRADING_ORDER_BATCH, body)
            .await?;
        decode_private(EP_TRADING_ORDER_BATCH, &reply)
    }

    /// Cancels `1..=max_batch_orders` orders in one request
    /// (`DELETE /api/v3/trading/order/batch`). Signed and **never retried**.
    pub async fn cancel_batch_orders(
        &self,
        items: &[BatchCancelOrderItem],
        batch_id: Option<ClientRequestId>,
    ) -> Result<BatchOrderResponse> {
        let body = build_batch_cancel_json(self.config(), items, batch_id)?;
        let reply = self
            .send_signed_body(Method::DELETE, EP_TRADING_ORDER_BATCH, body)
            .await?;
        decode_private(EP_TRADING_ORDER_BATCH, &reply)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::Value;

    use super::*;
    use crate::Credentials;

    fn cfg() -> Config {
        Config::new(Credentials::new("k", "s").expect("creds"))
            .with_device_id("dev-1")
            .with_user_agent("test-agent/1.0")
    }

    #[test]
    fn place_order_validation_and_wire_format() {
        let acct = AccountNo::new("003C123451");
        let bad_lo = PlaceOrderRequest::limit(acct.clone(), "VNM", OrderSide::Buy, 100, dec!(0));
        assert!(build_place_order_json(&cfg(), &bad_lo).is_err());

        let ok_lo =
            PlaceOrderRequest::limit(acct.clone(), "vnm", OrderSide::Buy, 100, dec!(65500.00))
                .with_client_request_id(ClientRequestId::new("req-1").expect("id"));
        let json: Value =
            serde_json::from_str(&build_place_order_json(&cfg(), &ok_lo).expect("ok"))
                .expect("json");
        assert_eq!(json.get("symbol").and_then(Value::as_str), Some("VNM"));
        assert_eq!(json.get("price").and_then(Value::as_str), Some("65500"));
        assert_eq!(json.get("orderType").and_then(Value::as_str), Some("LO"));
        assert_eq!(
            json.get("clientRequestId").and_then(Value::as_str),
            Some("req-1")
        );
        assert_eq!(json.get("deviceId").and_then(Value::as_str), Some("dev-1"));
        assert_eq!(
            json.get("userAgent").and_then(Value::as_str),
            Some("test-agent/1.0")
        );

        let ok_ato = PlaceOrderRequest::market(acct, "VNM", OrderSide::Sell, 100, OrderType::Ato);
        let json: Value =
            serde_json::from_str(&build_place_order_json(&cfg(), &ok_ato).expect("ok"))
                .expect("json");
        assert_eq!(json.get("price").and_then(Value::as_str), Some("0"));
    }

    #[test]
    fn batch_enforces_limits_and_all_or_nothing() {
        let acct = AccountNo::new("003C123451");
        assert!(build_batch_place_json(&cfg(), &[], None).is_err());
        let too_many: Vec<_> = (0..21)
            .map(|_| {
                PlaceOrderRequest::limit(acct.clone(), "VNM", OrderSide::Buy, 100, dec!(60000))
            })
            .collect();
        assert!(build_batch_place_json(&cfg(), &too_many, None).is_err());

        // One bad item fails the whole batch.
        let mixed = vec![
            PlaceOrderRequest::limit(acct.clone(), "VNM", OrderSide::Buy, 100, dec!(60000)),
            PlaceOrderRequest::limit(acct, "VNM", OrderSide::Buy, 0, dec!(60000)),
        ];
        assert!(build_batch_place_json(&cfg(), &mixed, None).is_err());
    }
}
