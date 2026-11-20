//! Typed REST endpoints for SSI `FastConnect` V3.
//!
//! - **Core (always enabled):** [`account`] (`accounts`, `balance`, `positions`, `ppmmr`,
//!   `order_book`).
//! - **`feature = "data"`:** [`ohlc`], [`corporate`], [`mod@reference`], [`summary`], [`mod@file`].
//! - **`feature = "trading"`:** [`account::MaxBuySell`] (`max_buy_sell`), [`orders`], [`fco`].

pub mod account;
#[cfg(feature = "data")]
pub mod corporate;
#[cfg(feature = "trading")]
pub mod fco;
#[cfg(feature = "data")]
pub mod file;
#[cfg(feature = "data")]
pub mod ohlc;
#[cfg(feature = "trading")]
pub mod orders;
#[cfg(feature = "data")]
pub mod reference;
#[cfg(feature = "data")]
pub mod summary;

use std::collections::HashMap;

#[cfg(feature = "trading")]
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

pub use self::account::{
    Account, AccountBalance, DerivativeAccountBalance, DerivativePosition, DerivativePositions,
    DerivativePpmmr, EquityAccountBalance, EquityPosition, EquityPpmmr, Order, OrderBook,
    OrderBookQuery, Positions, Ppmmr,
};
#[cfg(feature = "trading")]
pub use self::{
    account::MaxBuySell,
    fco::{
        BullBearParams, FcoCancelRequest, FcoCancelResponse, FcoInfo, FcoListPage, FcoListQuery,
        FcoOrder, FcoOrderBookPage, FcoParams, FcoPlaceRequest, FcoPlaceResponse,
        FcoStatusHistoryItem, FcoStrategy, FcoWindow, GtdParams, OcoParams, StopLimitParams,
        StopParams, TrailingStopLimitParams, TrailingStopParams,
    },
    orders::{
        BatchCancelOrderItem, BatchOrderResponse, BatchOrderResult, BatchPlaceOrderItem,
        CancelOrderRequest, CancelOrderResponse, ModifyChange, ModifyOrderRequest,
        ModifyOrderResponse, OrderTarget, PlaceOrderRequest, PlaceOrderResponse,
    },
};
#[cfg(feature = "data")]
pub use self::{
    corporate::{
        CorporateAction, CorporateActionsPage, CorporateActionsPageSize, CorporateActionsQuery,
        Language,
    },
    file::{MarketFile, MarketFileQuery, MarketFileTimeframe},
    ohlc::{Bar, OhlcPage, OhlcQuery, Timeframe},
    reference::{IndexInfo, PriceLimit, PriceLimitsPage, SecuritiesFilter, SecurityInfo},
    summary::{
        IndexSummaryFilter, MarketIndexSummary, SecuritiesSummaryFilter, SecuritiesSummaryPage,
        SecuritiesSummaryQuery, SecuritySummary,
    },
};
#[cfg(feature = "trading")]
use crate::de::format_plain_decimal;
use crate::{
    error::{Error, Result},
    http::Sensitivity,
};

/// Decodes a JSON body into a typed payload directly from JSON text so numbers reach `Decimal`
/// without passing through `f64`.
pub(crate) fn decode<T: DeserializeOwned>(
    endpoint: &str,
    body: &[u8],
    sensitivity: Sensitivity,
) -> Result<T> {
    serde_json::from_slice(body).map_err(|error| Error::Decode {
        endpoint: endpoint.to_owned(),
        detail: match sensitivity {
            Sensitivity::Public => error.to_string(),
            Sensitivity::Private => "payload does not match the documented shape".to_owned(),
        },
    })
}

/// Decodes `T` from the payload, or from its `data` member when the payload is a plain wrapper
/// (`{"data": ...}` with at most `code` / `msg` / `message` beside it).
pub(crate) fn decode_unwrapped<T: DeserializeOwned>(
    endpoint: &str,
    body: &[u8],
    sensitivity: Sensitivity,
) -> Result<T> {
    if let Ok(object) = serde_json::from_slice::<HashMap<String, Box<RawValue>>>(body)
        && let Some(inner) = object.get("data")
        && matches!(
            inner.get().trim_start().as_bytes().first(),
            Some(b'{' | b'[')
        )
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "data" | "code" | "msg" | "message"))
    {
        return decode(endpoint, inner.get().as_bytes(), sensitivity);
    }
    decode(endpoint, body, sensitivity)
}

/// Decodes a private JSON response without leaking response text in error messages.
#[cfg(feature = "trading")]
pub(crate) fn decode_private<T: DeserializeOwned>(endpoint: &str, bytes: &[u8]) -> Result<T> {
    decode(endpoint, bytes, Sensitivity::Private)
}

/// Converts a `Decimal` into an exact unquoted JSON number token (`26500`, `25.5`).
#[cfg(feature = "trading")]
pub(crate) fn decimal_raw_number(value: Decimal) -> Result<Box<RawValue>> {
    RawValue::from_string(format_plain_decimal(value))
        .map_err(|_| Error::InvalidRequest("invalid decimal number".to_owned()))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn unwraps_only_plain_wrappers() {
        let decode_value = |text: &str| -> Value {
            decode_unwrapped("/x", text.as_bytes(), Sensitivity::Public).expect("decodes")
        };
        assert_eq!(decode_value(r#"{"data": {"a": 1}}"#), json!({"a": 1}));
        assert_eq!(decode_value(r#"{"code": 200, "data": [1]}"#), json!([1]));
        assert_eq!(
            decode_value(r#"{"pageIndex": 1, "data": [1]}"#),
            json!({"pageIndex": 1, "data": [1]})
        );
        assert_eq!(decode_value(r#"{"equity": []}"#), json!({"equity": []}));
        assert_eq!(decode_value("[1, 2]"), json!([1, 2]));
    }
}
