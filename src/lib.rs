//! Typed async Rust SDK for SSI `FastConnect` V3.
//!
//! V3 unifies market data and trading behind one REST API (`https://api.ssi.com.vn/api/v3/...`),
//! one streaming WebSocket (`wss://stream.ssi.com.vn/ws/v3`), one optional Trading WebSocket RPC
//! endpoint (`wss://api.ssi.com.vn/ws/v3/trading`), and one API key pair.
//!
//! Build one [`SsiClient`] and clone it across tasks. Enable only the capabilities you need:
//!
//! - **Core (always enabled):** [`auth::TokenManager`] (automatic data/query login + single-use
//!   `/refresh` + [`auth::StoredToken`] persistence), [`config::Config`], [`error`], [`http`],
//!   [`model`], [`time`], and read-only account queries (`accounts`, `balance`, `positions`,
//!   `ppmmr`, `order_book`).
//! - **`data` (default feature):** market-data REST queries (`ohlc`, `corporate_actions`,
//!   `securities`, `price_limits`, `indices`, `index_summary`, `securities_summary`,
//!   `market_files`, `download_market_file`) and the supervised `DATA` WebSocket stream
//!   ([`stream::MarketStream`]). Zero RSA/XML dependencies are compiled when `trading` is off.
//! - **`trading` (opt-in feature):** OTP / Smart OTP push-approval auth, RSA PKCS#1 v1.5
//!   SHA-256 request signing, `max_buy_sell`, signed order / batch / Flexible Conditional Order
//!   (FCO) REST commands, the supervised `TRADING` event stream ([`stream::TradingStream`]), and
//!   the Trading WebSocket RPC client ([`ws::TradingWsClient`]).
//!
//! # Precision, time, and security invariants
//!
//! - Prices and monetary amounts are [`rust_decimal::Decimal`] decoded straight from JSON text
//!   via `RawValue` (never through `f64`).
//! - Exchange timestamps are `Asia/Ho_Chi_Minh` local wall-clock strings converted to UTC.
//! - Secrets (`SSI_API_KEY`, `SSI_API_SECRET`, `SSI_PRIVATE_KEY`, tokens, OTPs, download
//!   `token=` parameters) and account numbers are masked in `Debug`, `Display`, and errors.
//! - Signed mutating trading commands (`POST`, `PUT`, `DELETE`) are **never** retried
//!   automatically; an OTP/Smart OTP trading session is **never** silently downgraded to an
//!   OTP-less data token on refresh failure.
//! - Keep `tungstenite=debug` (or stricter) in your `tracing` filter so `tungstenite` does not
//!   log the `Authorization: Bearer` header at `TRACE`.

pub mod auth;
mod client;
pub mod config;
#[cfg(feature = "trading")]
mod crypto;
pub(crate) mod de;
#[cfg(feature = "trading")]
pub mod device;
pub mod error;
pub mod http;
pub mod model;
pub mod rest;
pub mod secret;
pub mod stream;
pub mod time;
#[cfg(feature = "trading")]
pub mod ws;

#[cfg(feature = "trading")]
pub use crate::{
    auth::OtpResponse,
    config::{
        DEFAULT_MAX_BATCH_ORDERS, DEFAULT_TRADING_WS_URL, HARD_MAX_BATCH_ORDERS, PRIVATE_KEY_ENV,
        TRADING_WS_URL_ENV,
    },
    model::{ClientRequestId, FcoOperator, FcoPrice, FcoStatus, FcoType, MAX_CLIENT_REQUEST_ID},
    rest::{
        BatchCancelOrderItem, BatchOrderResponse, BatchOrderResult, BatchPlaceOrderItem,
        BullBearParams, CancelOrderRequest, CancelOrderResponse, FcoCancelRequest,
        FcoCancelResponse, FcoInfo, FcoListPage, FcoListQuery, FcoOrder, FcoOrderBookPage,
        FcoParams, FcoPlaceRequest, FcoPlaceResponse, FcoStatusHistoryItem, FcoStrategy, FcoWindow,
        GtdParams, MaxBuySell, ModifyChange, ModifyOrderRequest, ModifyOrderResponse, OcoParams,
        OrderTarget, PlaceOrderRequest, PlaceOrderResponse, StopLimitParams, StopParams,
        TrailingStopLimitParams, TrailingStopParams,
    },
    stream::{
        FcoEvent, OrderEvent, OrderMatchEvent, PortfolioEvent, TradingEvent, TradingEventData,
        TradingStream, TradingStreamConfig, TradingStreamEvent, TradingTopic, TradingTopicKind,
    },
    ws::{
        SignatureEncoding, TradingWsClient, TradingWsConfig, TradingWsEvent, WsCall, WsRateLimit,
        WsResponse, is_mutating_method,
    },
};
pub use crate::{
    auth::{EpochUnit, StoredToken, TokenInfo, TokenManager, TokenSource},
    client::{RawResponse, SsiClient},
    config::{
        API_KEY_ENV, API_SECRET_ENV, API_URL_ENV, CLIENT_ID_ENV, Config, Credentials,
        DEFAULT_API_URL, DEFAULT_STREAM_URL, STREAM_URL_ENV, TRADING_URL_ENV,
    },
    error::{ApiError, ApiErrorCode, Error, RateLimited, Result, TradingWsError},
    http::RateLimitSnapshot,
    model::{AccountNo, AccountType, Exchange, OrderPrice, OrderSide, OrderStatus, OrderType},
    rest::{
        Account, AccountBalance, DerivativeAccountBalance, DerivativePosition, DerivativePositions,
        DerivativePpmmr, EquityAccountBalance, EquityPosition, EquityPpmmr, Order, OrderBook,
        OrderBookQuery, Positions, Ppmmr,
    },
    stream::{Backoff, DisconnectReason, HeartbeatKind, StreamStats, SubscriptionAck},
    time::{
        Clock, SystemClock, VN_TZ, format_vn_date, format_vn_datetime, parse_vn_date,
        parse_vn_datetime, vn_date_of, vn_date_start_utc, vn_local_to_utc,
    },
};
#[cfg(feature = "data")]
pub use crate::{
    rest::{
        Bar, CorporateAction, CorporateActionsPage, CorporateActionsPageSize,
        CorporateActionsQuery, IndexInfo, IndexSummaryFilter, Language, MarketFile,
        MarketFileQuery, MarketFileTimeframe, MarketIndexSummary, OhlcPage, OhlcQuery, PriceLimit,
        PriceLimitsPage, SecuritiesFilter, SecuritiesSummaryFilter, SecuritiesSummaryPage,
        SecuritiesSummaryQuery, SecurityInfo, SecuritySummary, Timeframe,
    },
    stream::{
        ActivityWindow, BookPrice, Candle, DataEvent, ForeignRoom, IndexSummary, Interval, Level,
        MarketData, MarketStream, MarketStreamConfig, MarketStreamEvent, OddLot, PriceLimitsUpdate,
        PutThrough, Quote, SessionFlag, StalePolicy, Topic, TopicKind, Trade,
        vn_equity_trading_hours,
    },
};
