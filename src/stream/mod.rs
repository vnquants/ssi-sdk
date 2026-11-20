//! Supervised real-time WebSocket streams on `wss://stream.ssi.com.vn/ws/v3`.
//!
//! - **Shared (always enabled):** [`Backoff`], [`DisconnectReason`], [`HeartbeatKind`],
//!   [`StreamStats`], [`SubscriptionAck`].
//! - **`feature = "data"`:** [`market`] (`MarketStream`, `MarketStreamConfig`,
//!   `MarketStreamEvent`, `Topic`, `MarketData`, `StalePolicy`, ...).
//! - **`feature = "trading"`:** [`trading`] (`TradingStream`, `TradingStreamConfig`,
//!   `TradingStreamEvent`, `TradingTopic`, `TradingEventData`, ...).

pub(crate) mod common;
#[cfg(feature = "data")]
pub mod market;
#[cfg(feature = "trading")]
pub mod trading;

pub use self::common::{Backoff, DisconnectReason, HeartbeatKind, StreamStats, SubscriptionAck};
#[cfg(feature = "data")]
pub use self::market::{
    ActivityWindow, BookPrice, Candle, DataEvent, ForeignRoom, IndexSummary, Interval, Level,
    MarketData, MarketStream, MarketStreamConfig, MarketStreamEvent, OddLot, PriceLimitsUpdate,
    PutThrough, Quote, SessionFlag, StalePolicy, Topic, TopicKind, Trade, vn_equity_trading_hours,
};
#[cfg(feature = "trading")]
pub use self::trading::{
    FcoEvent, OrderEvent, OrderMatchEvent, PortfolioEvent, TradingEvent, TradingEventData,
    TradingStream, TradingStreamConfig, TradingStreamEvent, TradingTopic, TradingTopicKind,
};
