//! Typed market-data messages.
//!
//! Field codes follow the subscribeUnsubscribeMarketData page. Prices are kept exactly as sent
//! (no unit conversion). The reference samples show trade prices in dong (`"45500"`) but quote,
//! put-through, odd-lot and market prices with two decimals (`"45.45"`); on the live feed
//! (2026-11-20) trade **and** quote prices are in dong (`"45400"`). Put-through, odd-lot and
//! market prices have not been seen live yet: check their unit before mixing them.
//!
//! Times are `Asia/Ho_Chi_Minh` wall-clock strings, converted to UTC: trade times carry
//! milliseconds on the live feed (`2026/11/20 09:53:33.771`), quote times are whole seconds;
//! both forms parse.
//!
//! Book levels: the server always sends 10 levels per side and pads the missing ones with
//! `["0","0"]` (live 2026-11-20: 3 real levels on HOSE, 10 on HNX and UPCOM). A level with
//! quantity 0 and a zero or blank price (`["0","0"]`, `["",""]`, `[]`) is padding and is dropped,
//! so `bids.len()` and `asks.len()` are the real depth. Every other level is kept: a zero price
//! with a quantity, a price with quantity 0, and a word price (`ATO`, `ATC`). A blank price
//! with a quantity is kept as [`BookPrice::Empty`], a blank or `null` quantity reads as 0, and
//! extra items after the quantity are ignored. A level that is not an array, or a quantity that
//! is not a non-negative whole number, fails the payload. A payload that does not fit becomes
//! [`MarketData::Unknown`] with the reason (counted in the stream statistics and logged once per
//! topic).
//! Payloads are decoded from their JSON text, so numeric prices keep every digit.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::IgnoredAny};
use serde_json::{Value, value::RawValue};

use super::topic::TopicKind;
use crate::{de::raw_scalar, model::OrderSide, time::parse_vn_datetime};

/// One market-data message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum MarketData {
    /// A matched trade (`trade.<symbol>`).
    Trade(Trade),
    /// A tick of an index value (`trade.index.<index>`); `price` is the index level.
    IndexTick(Trade),
    /// A candle (`trade.<symbol>@1m|@5m`, `trade.index.<index>@1m|@5m`).
    Candle(Candle),
    /// Bid/ask book (`quote.<symbol>`).
    Quote(Quote),
    /// Foreign room (`room.<symbol>`).
    Room(ForeignRoom),
    /// Put-through deal (`put.<symbol>`).
    PutThrough(PutThrough),
    /// Odd-lot trade and book (`oddlot.<symbol>`).
    OddLot(OddLot),
    /// Daily ceiling, floor and reference price (`market.<symbol>`).
    PriceLimits(PriceLimitsUpdate),
    /// Session flag of a board (`market.flag`).
    SessionFlag(SessionFlag),
    /// Index breadth and turnover (`indexsummary.<index>`).
    IndexSummary(IndexSummary),
    /// A payload this crate could not type; kept as sent.
    Unknown {
        /// Why it was not typed.
        reason: String,
        /// The `data` object as sent.
        data: Value,
    },
}

/// Price of a book level: a number, a session word (`ATO`, `ATC`) during auctions, or empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum BookPrice {
    /// Numeric price, as sent.
    Price(Decimal),
    /// Non-numeric price, as sent.
    Word(String),
    /// Blank or `null` price on a level that has a quantity (a blank level without one is
    /// padding and is dropped).
    Empty,
}

impl BookPrice {
    /// The numeric price, if any.
    pub const fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Price(price) => Some(*price),
            Self::Word(_) | Self::Empty => None,
        }
    }
}

/// One book level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Level {
    /// Price.
    pub price: BookPrice,
    /// Quantity in shares.
    pub quantity: u64,
}

impl Level {
    /// Whether this is a padding level: no quantity and a zero or blank price.
    ///
    /// The live feed pads every side to 10 levels with `["0","0"]`; those never sit between
    /// real levels. A zero price that comes with a quantity is not padding (no such level was
    /// seen in continuous trading on 2026-11-20; an auction feed could send one).
    const fn is_padding(&self) -> bool {
        self.quantity == 0
            && match &self.price {
                BookPrice::Price(price) => price.is_zero(),
                BookPrice::Empty => true,
                BookPrice::Word(_) => false,
            }
    }
}

/// Book levels as sent, best first, without the padding levels.
fn levels<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Level>, D::Error> {
    use serde::de::Error as _;
    let raw = Option::<Vec<Box<RawValue>>>::deserialize(deserializer)?.unwrap_or_default();
    let mut levels = Vec::with_capacity(raw.len());
    for level in &raw {
        let level = level_of(level.get()).map_err(D::Error::custom)?;
        if !level.is_padding() {
            levels.push(level);
        }
    }
    Ok(levels)
}

/// One `[price, quantity]` level.
///
/// Only blank or `null` items are tolerated (empty price, quantity 0). A level that is not an
/// array, or a quantity that is not a non-negative whole number, fails the whole payload so it
/// surfaces as [`MarketData::Unknown`].
fn level_of(text: &str) -> Result<Level, &'static str> {
    let items: Vec<Box<RawValue>> =
        serde_json::from_str(text).map_err(|_| "a book level is not a [price, quantity] array")?;
    let price = match items.first() {
        None => None,
        Some(item) => raw_scalar(item.get()).map_err(|_| "a book level price is not a scalar")?,
    }
    .map_or(BookPrice::Empty, |price| {
        crate::de::parse_decimal(&price).map_or(BookPrice::Word(price), BookPrice::Price)
    });
    let quantity = match items.get(1) {
        None => None,
        Some(item) => {
            raw_scalar(item.get()).map_err(|_| "a book level quantity is not a scalar")?
        }
    }
    .map(|quantity| {
        crate::de::parse_quantity(&quantity)
            .ok_or("a book level quantity is not a non-negative whole number")
    })
    .transpose()?
    .unwrap_or_default();
    Ok(Level { price, quantity })
}

/// A matched trade (`trade.<symbol>`), or an index tick (`trade.index.<index>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    /// Symbol or index code (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Exchange time (`t`). The live feed sends milliseconds (`2026/11/20 09:53:33.771`); the
    /// reference documents whole seconds, which parse too. Many trades can share one
    /// millisecond (one aggressive order matching several resting orders); order them by
    /// `total_volume`.
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Matched price (`p`).
    #[serde(rename = "p", deserialize_with = "crate::de::decimal")]
    pub price: Decimal,
    /// Matched quantity (`q`).
    #[serde(
        rename = "q",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub quantity: u64,
    /// Day's volume-weighted average matched price (`a`), rounded to two decimals (trailing
    /// zeros dropped: `"13102.4"`); `0` before the first match.
    #[serde(rename = "a", default, deserialize_with = "crate::de::opt_decimal")]
    pub average_price: Option<Decimal>,
    /// Aggressor side (`si`): `B` buy-up (the buyer took the ask), `S` sell-down (the seller hit
    /// the bid); `None` when blank (documented for index ticks; never blank in continuous
    /// trading on 2026-11-20).
    #[serde(rename = "si", default, deserialize_with = "opt_side")]
    pub side: Option<OrderSide>,
    /// Day open (`o`).
    #[serde(rename = "o", default, deserialize_with = "crate::de::opt_decimal")]
    pub open: Option<Decimal>,
    /// Day high (`h`).
    #[serde(rename = "h", default, deserialize_with = "crate::de::opt_decimal")]
    pub high: Option<Decimal>,
    /// Day low (`l`).
    #[serde(rename = "l", default, deserialize_with = "crate::de::opt_decimal")]
    pub low: Option<Decimal>,
    /// Day's cumulative matched volume (`v`), including this trade: each trade adds exactly its
    /// `quantity`. Use it to order trades that share one millisecond and to detect gaps: there
    /// is no sequence number or trade id.
    #[serde(
        rename = "v",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_volume: u64,
}

fn opt_side<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<OrderSide>, D::Error> {
    Ok(crate::de::opt_string(deserializer)?.map(|text| OrderSide::parse(&text)))
}

/// A candle (`@1m`, `@5m`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candle {
    /// Symbol or index (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Candle start (`st`).
    #[serde(
        rename = "st",
        default,
        deserialize_with = "crate::de::opt_vn_datetime"
    )]
    pub start: Option<DateTime<Utc>>,
    /// Time of the latest tick in the candle (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Open (`o`).
    #[serde(rename = "o", deserialize_with = "crate::de::decimal")]
    pub open: Decimal,
    /// High (`h`).
    #[serde(rename = "h", deserialize_with = "crate::de::decimal")]
    pub high: Decimal,
    /// Low (`l`).
    #[serde(rename = "l", deserialize_with = "crate::de::decimal")]
    pub low: Decimal,
    /// Close (`c`).
    #[serde(rename = "c", deserialize_with = "crate::de::decimal")]
    pub close: Decimal,
    /// Volume (`v`).
    #[serde(
        rename = "v",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub volume: u64,
}

/// Bid/ask book.
///
/// Each message is a full snapshot of the published book: 10 levels per side on the wire,
/// padded with `["0","0"]`, which the decoder drops (see the module docs). Real depth on
/// 2026-11-20 was 3 levels per side for HOSE and 10 for HNX and UPCOM. The reference says a
/// later version will send only changed levels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    /// Symbol (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Exchange time (`t`), whole seconds on the live feed.
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Bid levels, best (highest) first, padding dropped.
    #[serde(default, deserialize_with = "levels")]
    pub bids: Vec<Level>,
    /// Ask levels, best (lowest) first, padding dropped.
    #[serde(default, deserialize_with = "levels")]
    pub asks: Vec<Level>,
}

/// Foreign room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignRoom {
    /// Symbol (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Exchange time (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Total foreign room (`tr`).
    #[serde(rename = "tr", default, deserialize_with = "crate::de::opt_quantity")]
    pub total_room: Option<u64>,
    /// Remaining foreign room (`cr`).
    #[serde(rename = "cr", default, deserialize_with = "crate::de::opt_quantity")]
    pub current_room: Option<u64>,
    /// Foreign buy quantity (`bq`).
    #[serde(
        rename = "bq",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub buy_quantity: u64,
    /// Foreign buy value (`bv`).
    #[serde(
        rename = "bv",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub buy_value: Decimal,
    /// Foreign sell quantity (`sq`).
    #[serde(
        rename = "sq",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub sell_quantity: u64,
    /// Foreign sell value (`sv`).
    #[serde(
        rename = "sv",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub sell_value: Decimal,
}

/// Put-through deal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutThrough {
    /// Symbol (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Exchange time (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Deal price (`p`).
    #[serde(rename = "p", deserialize_with = "crate::de::decimal")]
    pub price: Decimal,
    /// Deal quantity (`q`).
    #[serde(
        rename = "q",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub quantity: u64,
    /// Day's put-through quantity (`tq`).
    #[serde(
        rename = "tq",
        default,
        deserialize_with = "crate::de::quantity_or_zero"
    )]
    pub total_quantity: u64,
    /// Day's put-through value (`tv`).
    #[serde(
        rename = "tv",
        default,
        deserialize_with = "crate::de::decimal_or_zero"
    )]
    pub total_value: Decimal,
}

/// Odd-lot trade and book.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OddLot {
    /// Symbol (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Exchange time (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Last odd-lot price (`p`).
    #[serde(rename = "p", default, deserialize_with = "crate::de::opt_decimal")]
    pub price: Option<Decimal>,
    /// Last odd-lot quantity (`q`).
    #[serde(rename = "q", default, deserialize_with = "crate::de::opt_quantity")]
    pub quantity: Option<u64>,
    /// Odd-lot bids, padding dropped as for [`Quote`].
    #[serde(default, deserialize_with = "levels")]
    pub bids: Vec<Level>,
    /// Odd-lot asks, padding dropped as for [`Quote`].
    #[serde(default, deserialize_with = "levels")]
    pub asks: Vec<Level>,
}

/// Daily ceiling, floor and reference price (`market.<symbol>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLimitsUpdate {
    /// Symbol (`s`).
    #[serde(rename = "s", deserialize_with = "crate::de::string")]
    pub symbol: String,
    /// Board (`b`).
    #[serde(rename = "b", default, deserialize_with = "crate::de::opt_string")]
    pub board: Option<String>,
    /// Trading date (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_date")]
    pub trading_date: NaiveDate,
    /// Ceiling (`ce`).
    #[serde(rename = "ce", default, deserialize_with = "crate::de::opt_decimal")]
    pub ceiling: Option<Decimal>,
    /// Floor (`fl`).
    #[serde(rename = "fl", default, deserialize_with = "crate::de::opt_decimal")]
    pub floor: Option<Decimal>,
    /// Reference (`ref`).
    #[serde(rename = "ref", default, deserialize_with = "crate::de::opt_decimal")]
    pub reference: Option<Decimal>,
}

/// Session flag of a board (`market.flag`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFlag {
    /// Board (`b`): `HOSE`, `HNX`, `UPCOM`, `DER`/`DERIVATIVES`.
    #[serde(rename = "b", deserialize_with = "crate::de::string")]
    pub board: String,
    /// Exchange time (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    /// Flag (`f`): `ATO`, `LO`, `ATC`, ... as sent.
    #[serde(rename = "f", deserialize_with = "crate::de::string")]
    pub flag: String,
}

/// Index breadth and turnover (`indexsummary.<index>`). Every numeric field is optional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    missing_docs,
    reason = "fields mirror the documented codes listed on each attribute"
)]
pub struct IndexSummary {
    /// Index code (`s`, backfilled from the topic if omitted by the server).
    #[serde(rename = "s", default, deserialize_with = "crate::de::string")]
    pub index: String,
    /// Exchange (`b`).
    #[serde(rename = "b", default, deserialize_with = "crate::de::opt_string")]
    pub exchange: Option<String>,
    /// Trading session (`ss`).
    #[serde(rename = "ss", default, deserialize_with = "crate::de::opt_string")]
    pub session: Option<String>,
    /// Exchange time (`t`).
    #[serde(rename = "t", deserialize_with = "crate::de::vn_datetime")]
    pub time: DateTime<Utc>,
    #[serde(rename = "v", default, deserialize_with = "crate::de::opt_decimal")]
    pub value: Option<Decimal>,
    #[serde(rename = "c", default, deserialize_with = "crate::de::opt_decimal")]
    pub change: Option<Decimal>,
    #[serde(rename = "cr", default, deserialize_with = "crate::de::opt_decimal")]
    pub change_percent: Option<Decimal>,
    #[serde(rename = "adv", default, deserialize_with = "crate::de::opt_quantity")]
    pub advances: Option<u64>,
    #[serde(rename = "dec", default, deserialize_with = "crate::de::opt_quantity")]
    pub declines: Option<u64>,
    #[serde(rename = "nc", default, deserialize_with = "crate::de::opt_quantity")]
    pub unchanged: Option<u64>,
    #[serde(rename = "ce", default, deserialize_with = "crate::de::opt_quantity")]
    pub ceilings: Option<u64>,
    #[serde(rename = "fl", default, deserialize_with = "crate::de::opt_quantity")]
    pub floors: Option<u64>,
    #[serde(rename = "tmv", default, deserialize_with = "crate::de::opt_decimal")]
    pub matched_value: Option<Decimal>,
    #[serde(rename = "tmq", default, deserialize_with = "crate::de::opt_quantity")]
    pub matched_quantity: Option<u64>,
    #[serde(rename = "tvp", default, deserialize_with = "crate::de::opt_decimal")]
    pub put_through_value: Option<Decimal>,
    #[serde(rename = "tqp", default, deserialize_with = "crate::de::opt_quantity")]
    pub put_through_quantity: Option<u64>,
    #[serde(rename = "tq", default, deserialize_with = "crate::de::opt_quantity")]
    pub total_quantity: Option<u64>,
    #[serde(rename = "tv", default, deserialize_with = "crate::de::opt_decimal")]
    pub total_value: Option<Decimal>,
}

impl MarketData {
    /// Exchange time of the message, if it has one.
    pub const fn exchange_time(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Trade(trade) | Self::IndexTick(trade) => Some(trade.time),
            Self::Candle(candle) => Some(candle.time),
            Self::Quote(quote) => Some(quote.time),
            Self::Room(room) => Some(room.time),
            Self::PutThrough(put) => Some(put.time),
            Self::OddLot(odd) => Some(odd.time),
            Self::SessionFlag(flag) => Some(flag.time),
            Self::IndexSummary(summary) => Some(summary.time),
            Self::PriceLimits(_) | Self::Unknown { .. } => None,
        }
    }

    /// Symbol or index code, if any.
    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::Trade(trade) | Self::IndexTick(trade) => Some(&trade.symbol),
            Self::Candle(candle) => Some(&candle.symbol),
            Self::Quote(quote) => Some(&quote.symbol),
            Self::Room(room) => Some(&room.symbol),
            Self::PutThrough(put) => Some(&put.symbol),
            Self::OddLot(odd) => Some(&odd.symbol),
            Self::PriceLimits(limits) => Some(&limits.symbol),
            Self::IndexSummary(summary) => Some(&summary.index),
            Self::SessionFlag(_) | Self::Unknown { .. } => None,
        }
    }

    /// Short family name for counters and logs.
    pub const fn family(&self) -> &'static str {
        match self {
            Self::Trade(_) => "trade",
            Self::IndexTick(_) => "trade.index",
            Self::Candle(_) => "candle",
            Self::Quote(_) => "quote",
            Self::Room(_) => "room",
            Self::PutThrough(_) => "put",
            Self::OddLot(_) => "oddlot",
            Self::PriceLimits(_) => "market",
            Self::SessionFlag(_) => "market.flag",
            Self::IndexSummary(_) => "indexsummary",
            Self::Unknown { .. } => "unknown",
        }
    }
}

/// Types one `data` payload of a DATA-channel frame by its topic.
///
/// The payload has already been parsed, so a JSON number in it may have lost digits; the
/// stream itself decodes from the frame text with [`decode_text`].
pub fn decode(topic: &str, data: &Value) -> MarketData {
    decode_text(topic, &data.to_string())
}

/// Types one `data` payload, given as JSON text, by its topic.
pub fn decode_text(topic: &str, data: &str) -> MarketData {
    #[derive(Deserialize)]
    struct Markers {
        #[serde(default)]
        st: Option<IgnoredAny>,
        #[serde(default)]
        f: Option<IgnoredAny>,
        #[serde(default)]
        p: Option<IgnoredAny>,
    }
    let lower = topic.to_ascii_lowercase();
    let markers = serde_json::from_str::<Markers>(data).ok();
    let candle = lower.ends_with("@1m")
        || lower.ends_with("@5m")
        || markers.as_ref().is_some_and(|markers| markers.st.is_some());
    let flag =
        lower == "market.flag" || markers.as_ref().is_some_and(|markers| markers.f.is_some());
    let has_price = markers.as_ref().is_some_and(|markers| markers.p.is_some());
    let result = match TopicKind::of(&lower) {
        Some(TopicKind::IndexTrade | TopicKind::Trade) if candle => typed(data, MarketData::Candle),
        Some(TopicKind::IndexTrade) if has_price => typed(data, MarketData::IndexTick),
        Some(TopicKind::IndexTrade | TopicKind::IndexSummary) => decode_index_summary(topic, data),
        Some(TopicKind::Trade) => typed(data, MarketData::Trade),
        Some(TopicKind::Quote) => typed(data, MarketData::Quote),
        Some(TopicKind::Room) => typed(data, MarketData::Room),
        Some(TopicKind::Put) => typed(data, MarketData::PutThrough),
        Some(TopicKind::OddLot) => typed(data, MarketData::OddLot),
        Some(TopicKind::Market) if flag => typed(data, MarketData::SessionFlag),
        Some(TopicKind::Market) => typed(data, MarketData::PriceLimits),
        None => Err("unknown topic".to_owned()),
    };
    result.unwrap_or_else(|reason| {
        tracing::debug!(topic, %reason, "market data not typed");
        MarketData::Unknown {
            reason,
            data: serde_json::from_str(data).unwrap_or_else(|_| Value::String(data.to_owned())),
        }
    })
}

fn decode_index_summary(topic: &str, data: &str) -> Result<MarketData, String> {
    let mut summary = serde_json::from_str::<IndexSummary>(data).map_err(|e| e.to_string())?;
    if summary.index.is_empty()
        && let Some(code) = topic_code(topic)
    {
        code.clone_into(&mut summary.index);
    }
    Ok(MarketData::IndexSummary(summary))
}

fn topic_code(topic: &str) -> Option<&str> {
    let tail = topic.rsplit('.').next()?;
    let code = tail.split('@').next()?.trim();
    (!code.is_empty() && code != "*").then_some(code)
}

fn typed<T: serde::de::DeserializeOwned>(
    data: &str,
    wrap: fn(T) -> MarketData,
) -> Result<MarketData, String> {
    serde_json::from_str::<T>(data)
        .map(wrap)
        .map_err(|error| error.to_string())
}

/// Parses an exchange time string the way the stream sends it (exposed for consumers that
/// keep raw payloads).
pub fn parse_exchange_time(text: &str) -> Option<DateTime<Utc>> {
    parse_vn_datetime(text)
}

#[cfg(test)]
#[allow(clippy::indexing_slicing, reason = "test assertions on known shapes")]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn documented_samples_decode() {
        let trade = decode(
            "trade.SSI",
            &json!({"s": "SSI", "t": "2026/07/29 09:15:32", "p": "45500", "q": "1000",
                   "a": "45350", "si": "B", "o": "45000", "h": "45800", "l": "44900",
                   "v": "1250000"}),
        );
        let MarketData::Trade(trade) = trade else {
            panic!("trade expected: {trade:?}")
        };
        assert_eq!(trade.price, dec!(45500));
        assert_eq!(trade.side, Some(OrderSide::Buy));
        assert_eq!(trade.time.to_rfc3339(), "2026-07-29T02:15:32+00:00");

        let quote = decode(
            "quote.SSI",
            &json!({"s": "SSI", "t": "2026/07/29 09:15:32",
                   "bids": [["45.45", "3200"], ["ATO", "100"]], "asks": [["45.50", "2800"]]}),
        );
        let MarketData::Quote(quote) = quote else {
            panic!("quote expected")
        };
        assert_eq!(quote.bids.len(), 2);
        assert_eq!(
            quote.bids.get(1).map(|l| &l.price),
            Some(&BookPrice::Word("ATO".to_owned()))
        );

        let flag = decode(
            "market.flag",
            &json!({"b": "HOSE", "t": "2026/07/29 09:00:00", "f": "ATO"}),
        );
        assert!(matches!(flag, MarketData::SessionFlag(ref f) if f.flag == "ATO"));

        let limits = decode(
            "market.SSI",
            &json!({"s": "SSI", "b": "HOSE", "t": "2026/07/29", "ce": "48.50", "fl": "42.10",
                   "ref": "45.30"}),
        );
        assert!(matches!(limits, MarketData::PriceLimits(ref l) if l.ceiling == Some(dec!(48.50))));

        let index = decode(
            "trade.index.VN30",
            &json!({"s": "VN30", "t": "2026/09/14 13:39:10", "p": "1928.81", "q": "75412",
                   "a": "0", "si": "", "o": "0", "h": "0", "l": "0", "v": "129161843"}),
        );
        assert!(matches!(index, MarketData::IndexTick(ref t) if t.side.is_none()));

        let candle = decode(
            "trade.VN30@1m",
            &json!({"s": "VN30", "st": "2026/09/14 13:39:00", "t": "2026/09/14 13:39:10",
                   "o": "1", "h": "2", "l": "1", "c": "2", "v": "10"}),
        );
        assert!(matches!(candle, MarketData::Candle(_)));
    }

    #[test]
    fn live_trade_times_carry_milliseconds() {
        let message = decode_text(
            "trade.FPT",
            r#"{"a":"45691.44","h":"46000","l":"45350","o":"45600","p":"45400","q":"100","s":"FPT","si":"B","t":"2026/11/20 09:53:33.771","v":"2700400"}"#,
        );
        let MarketData::Trade(trade) = message else {
            panic!("trade expected: {message:?}")
        };
        assert_eq!(trade.time.to_rfc3339(), "2026-11-20T02:53:33.771+00:00");
        assert_eq!(trade.price, dec!(45400));
        assert_eq!(trade.total_volume, 2_700_400);
    }

    #[test]
    fn tolerant_book_levels() {
        let decode_quote = |levels: &str| {
            let text = format!(r#"{{"s":"SSI","t":"2026/07/29 09:01:00",{levels}}}"#);
            match decode_text("quote.SSI", &text) {
                MarketData::Quote(quote) => quote,
                other => panic!("quote expected for {levels}: {other:?}"),
            }
        };
        let quote = decode_quote(r#""bids":[["45.45","3200"],["",""]],"asks":[["","100"]]"#);
        assert_eq!(
            quote.bids.len(),
            1,
            "a blank level without quantity is padding"
        );
        assert_eq!(quote.asks[0].price, BookPrice::Empty);
        assert_eq!(quote.asks[0].quantity, 100);
        let quote = decode_quote(r#""bids":[["45.45",null]],"asks":[["ATC",500]]"#);
        assert_eq!(quote.bids[0].quantity, 0);
        assert_eq!(quote.asks[0].price, BookPrice::Word("ATC".to_owned()));
        assert_eq!(quote.asks[0].quantity, 500);
        let quote = decode_quote(r#""bids":[["45.45","3200","7"]],"asks":[[45.45,3200]]"#);
        assert_eq!(quote.bids[0].quantity, 3200);
        assert_eq!(quote.asks[0].price, BookPrice::Price(dec!(45.45)));
        let quote = decode_quote(r#""bids":null,"asks":[[],["46","1"]]"#);
        assert_eq!(quote.bids, Vec::new());
        assert_eq!(quote.asks.len(), 1);
        assert_eq!(quote.asks[0].price, BookPrice::Price(dec!(46)));
    }

    #[test]
    fn padding_levels_are_dropped() {
        // quote.FPT as received on 2026-11-20 09:53:33 (HOSE: 3 real levels, 7 padding).
        let message = decode_text(
            "quote.FPT",
            r#"{"asks":[["45400","4600"],["45450","12600"],["45500","25100"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"]],"bids":[["45350","62300"],["45300","78000"],["45250","54800"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"],["0","0"]],"s":"FPT","t":"2026/11/20 09:53:33"}"#,
        );
        let MarketData::Quote(quote) = message else {
            panic!("quote expected: {message:?}")
        };
        let side = |levels: &[Level]| -> Vec<(Option<Decimal>, u64)> {
            levels
                .iter()
                .map(|level| (level.price.as_decimal(), level.quantity))
                .collect()
        };
        assert_eq!(
            side(&quote.bids),
            vec![
                (Some(dec!(45350)), 62_300),
                (Some(dec!(45300)), 78_000),
                (Some(dec!(45250)), 54_800)
            ]
        );
        assert_eq!(
            side(&quote.asks),
            vec![
                (Some(dec!(45400)), 4_600),
                (Some(dec!(45450)), 12_600),
                (Some(dec!(45500)), 25_100)
            ]
        );

        let decode_bids = |levels: &str| {
            let text = format!(r#"{{"s":"SSI","t":"2026/11/20 09:53:33","bids":{levels}}}"#);
            match decode_text("quote.SSI", &text) {
                MarketData::Quote(quote) => side(&quote.bids),
                other => panic!("quote expected for {levels}: {other:?}"),
            }
        };
        for padding in [
            r#"[["0","0"]]"#,
            "[[0,0]]",
            r#"[["0.00","0"]]"#,
            r#"[["0",""]]"#,
            "[[null,null]]",
            r#"[["",""]]"#,
            "[[]]",
        ] {
            assert_eq!(decode_bids(padding), Vec::new(), "{padding} is padding");
        }
        assert_eq!(
            decode_bids(r#"[["0","500"],["45400","0"],["ATO","0"]]"#),
            vec![(Some(dec!(0)), 500), (Some(dec!(45400)), 0), (None, 0)],
            "a level with a quantity or a non-zero price is kept"
        );
    }

    #[test]
    fn malformed_book_levels_fail_the_payload() {
        let cases = [
            ("garbage quantity", r#""bids":[["45.45","abc"]],"asks":[]"#),
            (
                "fractional quantity",
                r#""bids":[],"asks":[["45.50","12.5"]]"#,
            ),
            ("negative quantity", r#""bids":[["45.45","-5"]],"asks":[]"#),
            (
                "object levels",
                r#""bids":[{"p":"45.45","q":"3200"}],"asks":[]"#,
            ),
            (
                "flat levels",
                r#""bids":["45.45","3200","45.40","100"],"asks":[]"#,
            ),
            ("levels as a string", r#""bids":"45.45|3200","asks":[]"#),
            ("price as an array", r#""bids":[[["45.45"],"1"]],"asks":[]"#),
        ];
        for (label, levels) in cases {
            let text = format!(r#"{{"s":"SSI","t":"2026/07/29 09:01:00",{levels}}}"#);
            let message = decode_text("quote.SSI", &text);
            assert!(
                matches!(message, MarketData::Unknown { .. }),
                "{label} must not decode: {message:?}"
            );
        }
        let odd = decode_text(
            "oddlot.SSI",
            r#"{"s":"SSI","t":"2026/07/29 09:01:00","p":"45.5","q":"80","bids":[["45.45","x"]]}"#,
        );
        assert!(matches!(odd, MarketData::Unknown { .. }));
    }

    #[test]
    fn numeric_prices_keep_every_digit() {
        let message = decode_text(
            "trade.SSI",
            r#"{"s":"SSI","t":"2026/07/29 09:15:32","p":85907.33212345678912,"q":1000,"a":1e-7,"v":12345678901234567890}"#,
        );
        let MarketData::Trade(trade) = message else {
            panic!("trade expected: {message:?}")
        };
        assert_eq!(trade.price.to_string(), "85907.33212345678912");
        assert_eq!(trade.average_price, Some(dec!(0.0000001)));
        assert_eq!(trade.total_volume, 12_345_678_901_234_567_890);
    }

    #[test]
    fn hostile_scalars_never_panic() {
        let hostile = [
            "1e999999999",
            "1e-999999999",
            "9e30",
            "-",
            "e",
            "1e+5",
            "+1",
            ".5",
            "5.",
            "0x10",
            "NaN",
            "inf",
            "-0",
            "79228162514264337593543950336",
            "١٢٣",
            " 12 ",
            "1_000",
            "99999999999999999999999999999999999999",
        ];
        for text in hostile {
            let _ = decode(
                "trade.X",
                &json!({"s": "X", "t": "2026/07/29 09:15:32", "p": text, "q": text, "v": text}),
            );
            let _ = decode(
                "quote.X",
                &json!({"s": "X", "t": text, "bids": [[text, text]], "asks": [[text, "1"]]}),
            );
        }
        for text in [
            "0000/00/00",
            "9999/99/99 99:99:99",
            "2026/02/30",
            "+262143/12/31 23:59:59",
            "2026/07/29 24:00:00",
            "1795110482626",
        ] {
            let message = decode("trade.X", &json!({"s": "X", "t": text, "p": "1"}));
            assert!(
                matches!(message, MarketData::Unknown { .. }),
                "{text}: {message:?}"
            );
        }
    }

    #[test]
    fn bad_payloads_are_kept() {
        let data = json!({"s": "SSI", "t": "not a time", "p": "1"});
        let message = decode("trade.SSI", &data);
        assert!(matches!(message, MarketData::Unknown { data: ref d, .. } if *d == data));
        assert!(matches!(
            decode("weird.X", &json!({})),
            MarketData::Unknown { .. }
        ));
    }

    #[test]
    fn index_trade_summary_shape_and_topic_code_backfill() {
        // Go SDK v3.3.2 IndexTickMessage shape on trade.index.<index> (no `p`, uses `v`/`b`/`ss`).
        let sdk_tick = decode(
            "trade.index.VN30",
            &json!({
                "b": "HOSE", "ss": "LO", "t": "2026/11/20 10:15:00",
                "v": "1928.81", "c": "12.34", "cr": "0.64",
                "adv": "20", "dec": "8", "nc": "2"
            }),
        );
        let MarketData::IndexSummary(summary) = sdk_tick else {
            panic!("index summary expected: {sdk_tick:?}")
        };
        assert_eq!(summary.index, "VN30");
        assert_eq!(summary.value, Some(dec!(1928.81)));
        assert_eq!(summary.advances, Some(20));

        // indexsummary.<index> with omitted `s` also backfills index code from topic.
        let summary_msg = decode(
            "indexsummary.VNINDEX",
            &json!({"t": "2026/11/20 10:15:00", "v": "1258.32", "tq": "500000"}),
        );
        let MarketData::IndexSummary(summary) = summary_msg else {
            panic!("index summary expected: {summary_msg:?}")
        };
        assert_eq!(summary.index, "VNINDEX");
        assert_eq!(summary.total_quantity, Some(500_000));
    }
}
