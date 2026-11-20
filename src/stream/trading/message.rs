//! Typed real-time trading event payloads (`orderEvent`, `orderMatchEvent`,
//! `clientPortfolioEvent`, `fcoEvent`).

use std::fmt;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    de,
    model::{AccountNo, FcoStatus, FcoType, OrderPrice, OrderSide, OrderStatus, OrderType},
    time::parse_vn_datetime,
};

/// Real-time order status update (`eventType = "orderEvent"` on `order.<account>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderEvent {
    /// Account number (masked in `Debug` / `Display`).
    pub account_no: AccountNo,
    /// Caller idempotency key (`clientRequestId`).
    pub client_request_id: String,
    /// SSI order id.
    pub order_id: String,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Order type (`LO`, `ATO`, `ATC`, `MTL`, ...).
    pub order_type: OrderType,
    /// Limit price or market-priced word.
    pub price: Option<OrderPrice>,
    /// Original order quantity.
    pub quantity: u64,
    /// Outstanding open quantity (`osQty`).
    pub os_quantity: u64,
    /// Cumulative filled quantity (`filledQty`).
    pub filled_quantity: u64,
    /// Cancelled quantity (`cancelQty`).
    pub cancel_quantity: u64,
    /// Current order status (`PD`, `QU`, `PF`, `FF`, `CL`, `RJ`, ...).
    pub status: OrderStatus,
    /// Input timestamp (UTC).
    pub input_time: Option<DateTime<Utc>>,
    /// Raw `inputTime` string.
    pub input_time_raw: String,
    /// Modified timestamp (UTC).
    pub modified_time: Option<DateTime<Utc>>,
    /// Raw `modifiedTime` / `modifyTime` string.
    pub modified_time_raw: String,
    /// Average fill price (limit decimal or market word if sent).
    pub avg_price: Option<OrderPrice>,
    /// Rejection reason text.
    pub reject_reason: String,
    /// SSI system request id.
    pub system_request_id: String,
    /// Error code when rejected (`RJ`).
    pub error_code: String,
    /// Error message when rejected.
    pub error_message: String,
    /// Notification id.
    pub notify_id: String,
    /// Connection id.
    pub connection_id: String,
}

/// Real-time trade fill event (`eventType = "orderMatchEvent"` on `order.<account>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderMatchEvent {
    /// Notification id.
    pub notify_id: String,
    /// Connection id.
    pub connection_id: String,
    /// Caller idempotency key.
    pub client_request_id: String,
    /// SSI order id.
    pub order_id: String,
    /// Ticker symbol.
    pub symbol: String,
    /// Buy or Sell.
    pub side: OrderSide,
    /// Account number.
    pub account_no: AccountNo,
    /// Match execution price.
    pub match_price: Option<OrderPrice>,
    /// Matched quantity for this fill.
    pub match_qty: u64,
    /// Match timestamp (UTC).
    pub match_time: Option<DateTime<Utc>>,
    /// Raw `matchTime` string.
    pub match_time_raw: String,
}

/// Real-time position update (`eventType = "clientPortfolioEvent"` on `portfolio.<account>`).
///
/// SSI currently emits this event for derivative accounts only; equity cash/stock fields remain
/// `None`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PortfolioEvent {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Total asset (normally `None`).
    #[serde(default, rename = "totalAsset", deserialize_with = "de::opt_decimal")]
    pub total_asset: Option<Decimal>,
    /// Cash balance (normally `None`).
    #[serde(default, rename = "cashBalance", deserialize_with = "de::opt_decimal")]
    pub cash_balance: Option<Decimal>,
    /// Stock value (normally `None`).
    #[serde(default, rename = "stockValue", deserialize_with = "de::opt_decimal")]
    pub stock_value: Option<Decimal>,
    /// Contract symbol.
    #[serde(default, deserialize_with = "de::string")]
    pub symbol: String,
    /// Long quantity (`longQty`).
    #[serde(default, rename = "longQty", deserialize_with = "de::i64_or_zero")]
    pub long_qty: i64,
    /// Short quantity (`shortQty`).
    #[serde(default, rename = "shortQty", deserialize_with = "de::i64_or_zero")]
    pub short_qty: i64,
    /// Net position.
    #[serde(default, deserialize_with = "de::i64_or_zero")]
    pub net: i64,
    /// Average long price.
    #[serde(default, rename = "bidAvgPrice", deserialize_with = "de::opt_decimal")]
    pub bid_avg_price: Option<Decimal>,
    /// Average short price.
    #[serde(default, rename = "askAvgPrice", deserialize_with = "de::opt_decimal")]
    pub ask_avg_price: Option<Decimal>,
    /// Latest trade price.
    #[serde(default, rename = "tradePrice", deserialize_with = "de::opt_decimal")]
    pub trade_price: Option<Decimal>,
    /// Market price.
    #[serde(default, rename = "marketPrice", deserialize_with = "de::opt_decimal")]
    pub market_price: Option<Decimal>,
    /// Floating P&L.
    #[serde(default, rename = "floatingPL", deserialize_with = "de::opt_decimal")]
    pub floating_pl: Option<Decimal>,
    /// Realized trading P&L.
    #[serde(default, rename = "tradingPL", deserialize_with = "de::opt_decimal")]
    pub trading_pl: Option<Decimal>,
    /// Position type label.
    #[serde(default, rename = "positionType", deserialize_with = "de::string")]
    pub position_type: String,
}

/// Real-time Flexible Conditional Order update (`eventType = "fcoEvent"`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FcoEvent {
    /// FCO id (`fcoId` / `FcoId`).
    #[serde(
        default,
        rename = "fcoId",
        alias = "FcoId",
        deserialize_with = "de::string"
    )]
    pub fco_id: String,
    /// Processing status (`INIT`, `WAIT`, `TRI`, `TER`, `FIS`, ...).
    #[serde(default, rename = "processStatus")]
    pub process_status: FcoStatus,
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
    /// Ticker symbol (`symbol` / `Symbol`).
    #[serde(default, alias = "Symbol", deserialize_with = "de::string")]
    pub symbol: String,
    /// Order quantity (`quantity` / `Quantity`).
    #[serde(default, alias = "Quantity", deserialize_with = "de::quantity_or_zero")]
    pub quantity: u64,
    /// Price text.
    #[serde(default, deserialize_with = "de::string")]
    pub price: String,
    /// Account number (`accountNo` / `AccountNo`).
    #[serde(default, rename = "accountNo", alias = "AccountNo")]
    pub account_no: AccountNo,
    /// Update timestamp string.
    #[serde(default, rename = "updatedTime", deserialize_with = "de::string")]
    pub updated_time: String,
    /// Raw status text.
    #[serde(default, deserialize_with = "de::string")]
    pub status: String,
    /// Server message.
    #[serde(default, deserialize_with = "de::string")]
    pub message: String,
    /// Owner username.
    #[serde(default, deserialize_with = "de::string")]
    pub username: String,
    /// Event type (`fcoEvent`).
    #[serde(default, rename = "eventType", deserialize_with = "de::string")]
    pub event_type: String,
    /// Strategy type.
    #[serde(default, rename = "type")]
    pub fco_type: FcoType,
}

/// Typed payload of a `TRADING`-channel event.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TradingEventData {
    /// Order lifecycle update (`orderEvent`).
    Order(Box<OrderEvent>),
    /// Trade fill notification (`orderMatchEvent`).
    OrderMatch(Box<OrderMatchEvent>),
    /// Derivative portfolio update (`clientPortfolioEvent`).
    Portfolio(Box<PortfolioEvent>),
    /// Flexible Conditional Order update (`fcoEvent`).
    Fco(Box<FcoEvent>),
    /// Payload whose `eventType` or JSON shape was unrecognised.
    ///
    /// `Debug` redacts `raw` because trading payloads carry account numbers.
    Unknown {
        /// `eventType` field if present.
        event_type: Option<String>,
        /// Why decoding fell back to `Unknown`.
        reason: String,
        /// Unmodified JSON payload text.
        raw: String,
    },
}

impl fmt::Debug for TradingEventData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Order(event) => f.debug_tuple("Order").field(event).finish(),
            Self::OrderMatch(event) => f.debug_tuple("OrderMatch").field(event).finish(),
            Self::Portfolio(event) => f.debug_tuple("Portfolio").field(event).finish(),
            Self::Fco(event) => f.debug_tuple("Fco").field(event).finish(),
            Self::Unknown {
                event_type,
                reason,
                raw,
            } => f
                .debug_struct("Unknown")
                .field("event_type", event_type)
                .field("reason", reason)
                .field("raw_bytes", &raw.len())
                .finish(),
        }
    }
}

#[derive(Deserialize)]
struct EventTypePeek {
    #[serde(default, rename = "eventType", deserialize_with = "de::opt_string")]
    event_type: Option<String>,
}

#[derive(Deserialize)]
struct WireOrderEvent {
    #[serde(default, rename = "accountNo")]
    account_no: AccountNo,
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    client_request_id: String,
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    order_id: String,
    #[serde(default, deserialize_with = "de::string")]
    symbol: String,
    #[serde(default)]
    side: OrderSide,
    #[serde(default, rename = "orderType")]
    order_type: OrderType,
    #[serde(default, deserialize_with = "de::opt_string")]
    price: Option<String>,
    #[serde(default, deserialize_with = "de::quantity_or_zero")]
    quantity: u64,
    #[serde(
        default,
        rename = "osQty",
        alias = "osQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    os_qty: u64,
    #[serde(
        default,
        rename = "filledQty",
        alias = "filledQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    filled_qty: u64,
    #[serde(
        default,
        rename = "cancelQty",
        alias = "cancelQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    cancel_qty: u64,
    #[serde(default, rename = "orderStatus")]
    order_status: OrderStatus,
    #[serde(default, rename = "inputTime", deserialize_with = "de::string")]
    input_time: String,
    #[serde(
        default,
        rename = "modifiedTime",
        alias = "modifyTime",
        deserialize_with = "de::string"
    )]
    modified_time: String,
    #[serde(default, rename = "avgPrice", deserialize_with = "de::opt_string")]
    avg_price: Option<String>,
    #[serde(default, rename = "rejectReason", deserialize_with = "de::string")]
    reject_reason: String,
    #[serde(default, rename = "systemRequestId", deserialize_with = "de::string")]
    system_request_id: String,
    #[serde(default, rename = "errorCode", deserialize_with = "de::string")]
    error_code: String,
    #[serde(default, rename = "errorMessage", deserialize_with = "de::string")]
    error_message: String,
    #[serde(default, rename = "notifyId", deserialize_with = "de::string")]
    notify_id: String,
    #[serde(default, rename = "connectionId", deserialize_with = "de::string")]
    connection_id: String,
}

#[derive(Deserialize)]
struct WireOrderMatchEvent {
    #[serde(default, rename = "notifyId", deserialize_with = "de::string")]
    notify_id: String,
    #[serde(default, rename = "connectionId", deserialize_with = "de::string")]
    connection_id: String,
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    client_request_id: String,
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    order_id: String,
    #[serde(default, deserialize_with = "de::string")]
    symbol: String,
    #[serde(default)]
    side: OrderSide,
    #[serde(default, rename = "accountNo")]
    account_no: AccountNo,
    #[serde(default, rename = "matchPrice", deserialize_with = "de::opt_string")]
    match_price: Option<String>,
    #[serde(
        default,
        rename = "matchQty",
        deserialize_with = "de::quantity_or_zero"
    )]
    match_qty: u64,
    #[serde(default, rename = "matchTime", deserialize_with = "de::string")]
    match_time: String,
}

/// Decodes one `data` payload from a `TRADING` frame.
///
/// Routed by `data.eventType` (`orderEvent`, `orderMatchEvent`, `clientPortfolioEvent`,
/// `fcoEvent`), falling back to the topic prefix (`order.` / `portfolio.`) when `eventType` is
/// absent.
pub fn decode_text(topic: &str, payload: &str) -> TradingEventData {
    let event_type = serde_json::from_str::<EventTypePeek>(payload)
        .ok()
        .and_then(|peek| peek.event_type);

    let route = event_type.as_deref().or_else(|| {
        let lower = topic.to_ascii_lowercase();
        if lower.starts_with("order.") {
            Some("orderEvent")
        } else if lower.starts_with("portfolio.") {
            Some("clientPortfolioEvent")
        } else {
            None
        }
    });

    match route {
        Some("orderEvent") => match serde_json::from_str::<WireOrderEvent>(payload) {
            Ok(w) => TradingEventData::Order(Box::new(OrderEvent {
                account_no: w.account_no,
                client_request_id: w.client_request_id,
                order_id: w.order_id,
                symbol: w.symbol,
                side: w.side,
                order_type: w.order_type,
                price: w.price.as_deref().and_then(OrderPrice::parse_opt),
                quantity: w.quantity,
                os_quantity: w.os_qty,
                filled_quantity: w.filled_qty,
                cancel_quantity: w.cancel_qty,
                status: w.order_status,
                input_time: parse_vn_datetime(&w.input_time),
                input_time_raw: w.input_time,
                modified_time: parse_vn_datetime(&w.modified_time),
                modified_time_raw: w.modified_time,
                avg_price: w.avg_price.as_deref().and_then(OrderPrice::parse_opt),
                reject_reason: w.reject_reason,
                system_request_id: w.system_request_id,
                error_code: w.error_code,
                error_message: w.error_message,
                notify_id: w.notify_id,
                connection_id: w.connection_id,
            })),
            Err(_) => TradingEventData::Unknown {
                event_type,
                reason: "orderEvent payload did not match schema".to_owned(),
                raw: payload.to_owned(),
            },
        },
        Some("orderMatchEvent") => match serde_json::from_str::<WireOrderMatchEvent>(payload) {
            Ok(w) => TradingEventData::OrderMatch(Box::new(OrderMatchEvent {
                notify_id: w.notify_id,
                connection_id: w.connection_id,
                client_request_id: w.client_request_id,
                order_id: w.order_id,
                symbol: w.symbol,
                side: w.side,
                account_no: w.account_no,
                match_price: w.match_price.as_deref().and_then(OrderPrice::parse_opt),
                match_qty: w.match_qty,
                match_time: parse_vn_datetime(&w.match_time),
                match_time_raw: w.match_time,
            })),
            Err(_) => TradingEventData::Unknown {
                event_type,
                reason: "orderMatchEvent payload did not match schema".to_owned(),
                raw: payload.to_owned(),
            },
        },
        Some("clientPortfolioEvent") => serde_json::from_str::<PortfolioEvent>(payload)
            .map_or_else(
                |_| TradingEventData::Unknown {
                    event_type,
                    reason: "clientPortfolioEvent payload did not match schema".to_owned(),
                    raw: payload.to_owned(),
                },
                |event| TradingEventData::Portfolio(Box::new(event)),
            ),
        Some("fcoEvent") => serde_json::from_str::<FcoEvent>(payload).map_or_else(
            |_| TradingEventData::Unknown {
                event_type,
                reason: "fcoEvent payload did not match schema".to_owned(),
                raw: payload.to_owned(),
            },
            |event| TradingEventData::Fco(Box::new(event)),
        ),
        _ => TradingEventData::Unknown {
            event_type,
            reason: "unrecognised trading eventType".to_owned(),
            raw: payload.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn decodes_all_four_trading_event_types() {
        let order = decode_text(
            "order.003C123451",
            r#"{"eventType":"orderEvent","accountNo":"003C123451","orderId":"O1","clientRequestId":"req1","symbol":"VNM","side":"B","orderType":"LO","price":"65500","quantity":100,"osQty":50,"filledQty":50,"orderStatus":"PF","modifyTime":"2026/07/29 10:00:00"}"#,
        );
        let TradingEventData::Order(order) = order else {
            panic!("order expected");
        };
        assert_eq!(order.price, Some(OrderPrice::Limit(dec!(65500))));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.os_quantity, 50);
        assert!(!format!("{order:?}").contains("123451"));

        let fill = decode_text(
            "order.003C123451",
            r#"{"eventType":"orderMatchEvent","accountNo":"003C123451","orderId":"O1","symbol":"VNM","side":"B","matchPrice":"65500","matchQty":50,"matchTime":"2026/07/29 10:00:01"}"#,
        );
        assert!(matches!(fill, TradingEventData::OrderMatch(m) if m.match_qty == 50));

        let pf = decode_text(
            "portfolio.003C123458",
            r#"{"eventType":"clientPortfolioEvent","accountNo":"003C123458","symbol":"VN30F2612","longQty":2,"shortQty":0,"net":2,"floatingPL":"1500000"}"#,
        );
        assert!(
            matches!(pf, TradingEventData::Portfolio(p) if p.floating_pl == Some(dec!(1500000)))
        );

        let fco = decode_text(
            "order.003C123451",
            r#"{"eventType":"fcoEvent","fcoId":"FCO-1","processStatus":"TRI","matchedQuality":100,"isPlaceOrder":true,"symbol":"VNM","quantity":100,"accountNo":"003C123451","type":"stop_limit"}"#,
        );
        assert!(
            matches!(fco, TradingEventData::Fco(f) if f.process_status == FcoStatus::Tri && f.matched_quantity == 100)
        );

        let unknown = decode_text(
            "order.003C123451",
            r#"{"eventType":"futureEvent","accountNo":"003C123451"}"#,
        );
        assert!(!format!("{unknown:?}").contains("123451"));
    }
}
