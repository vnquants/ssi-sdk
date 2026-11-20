//! Market-data topics.
//!
//! Topic grammar (subscribeUnsubscribeMarketData page, checked live):
//! `trade.<filter>[@tick|@1m|@5m]`, `trade.index.<index>[@tick|@1m|@5m]`, `quote.<filter>`,
//! `room.<filter>`, `put.<filter>`, `oddlot.<filter>`, `market.<filter>`, `market.flag` (session
//! flags of every board), `indexsummary.<index|*>`. A filter is a symbol, several symbols joined
//! with `-`, an exchange (`HOSE`), an index basket (`VN30`) or `*`. The server fans an exchange or
//! basket out to its symbols only with the `@*` suffix (`quote.HOSE@*`), as the official SDK
//! does. Unknown topics are acknowledged with `status: ok` and listed after `Invalid:` in the
//! acknowledgement message.

use std::fmt;

use crate::{
    error::{Error, Result},
    model::Exchange,
};

/// Interval suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Interval {
    /// `@tick`: every trade (the default when no suffix is given).
    Tick,
    /// `@1m`: one-minute candles (trade topics only).
    Minute1,
    /// `@5m`: five-minute candles (trade topics only).
    Minute5,
    /// `@*`: fan an exchange or index basket out to its symbols.
    All,
}

impl Interval {
    const fn suffix(self) -> &'static str {
        match self {
            Self::Tick => "@tick",
            Self::Minute1 => "@1m",
            Self::Minute5 => "@5m",
            Self::All => "@*",
        }
    }
}

/// Topic family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicKind {
    /// `trade.`: matched trades (and candles with `@1m`/`@5m`).
    Trade,
    /// `trade.index.`: the value of an index itself.
    IndexTrade,
    /// `quote.`: bid/ask book.
    Quote,
    /// `room.`: foreign room.
    Room,
    /// `put.`: put-through deals.
    Put,
    /// `oddlot.`: odd-lot trades and book.
    OddLot,
    /// `market.`: daily ceiling/floor/reference, and `market.flag` session flags.
    Market,
    /// `indexsummary.`: index breadth and turnover.
    IndexSummary,
}

impl TopicKind {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Trade => "trade.",
            Self::IndexTrade => "trade.index.",
            Self::Quote => "quote.",
            Self::Room => "room.",
            Self::Put => "put.",
            Self::OddLot => "oddlot.",
            Self::Market => "market.",
            Self::IndexSummary => "indexsummary.",
        }
    }

    /// Classifies a topic string (case-insensitive).
    pub fn of(topic: &str) -> Option<Self> {
        let lower = topic.to_ascii_lowercase();
        [
            Self::IndexTrade,
            Self::Trade,
            Self::Quote,
            Self::Room,
            Self::Put,
            Self::OddLot,
            Self::Market,
            Self::IndexSummary,
        ]
        .into_iter()
        .find(|kind| lower.starts_with(kind.prefix()))
    }
}

/// A validated topic string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Topic(String);

impl Topic {
    /// `trade.<symbol>`: tick trades.
    pub fn trade(filter: &str) -> Result<Self> {
        Self::build(TopicKind::Trade, filter, None)
    }

    /// `trade.<symbol>@<interval>`: trades or candles.
    pub fn trade_with(filter: &str, interval: Interval) -> Result<Self> {
        Self::build(TopicKind::Trade, filter, Some(interval))
    }

    /// `trade.index.<index>[@interval]`: the index value itself (not its constituents).
    pub fn index_trade(index: &str, interval: Option<Interval>) -> Result<Self> {
        if interval == Some(Interval::All) {
            return Err(Error::InvalidRequest(
                "trade.index takes no @* suffix".to_owned(),
            ));
        }
        Self::build(TopicKind::IndexTrade, index, interval)
    }

    /// `quote.<filter>`: bid/ask book.
    pub fn quote(filter: &str) -> Result<Self> {
        Self::build(TopicKind::Quote, filter, None)
    }

    /// `room.<filter>`: foreign room.
    pub fn room(filter: &str) -> Result<Self> {
        Self::build(TopicKind::Room, filter, None)
    }

    /// `put.<filter>`: put-through deals.
    pub fn put(filter: &str) -> Result<Self> {
        Self::build(TopicKind::Put, filter, None)
    }

    /// `oddlot.<filter>`: odd lots.
    pub fn oddlot(filter: &str) -> Result<Self> {
        Self::build(TopicKind::OddLot, filter, None)
    }

    /// `market.<filter>`: daily ceiling, floor and reference price.
    pub fn market(filter: &str) -> Result<Self> {
        Self::build(TopicKind::Market, filter, None)
    }

    /// `market.flag`: session flags (ATO, LO, ATC, ...) of every board.
    pub fn market_flag() -> Self {
        Self("market.flag".to_owned())
    }

    /// `indexsummary.<index|*>`.
    pub fn index_summary(index: &str) -> Result<Self> {
        Self::build(TopicKind::IndexSummary, index, None)
    }

    /// Any topic family with an optional interval suffix.
    ///
    /// Fan-out (`@*`) is only valid for `trade`, `quote`, `room`, `put` and `oddlot` with a
    /// single concrete board or index code (for example `HOSE` or `VN30`, never `*` or `A-B`).
    pub fn new(kind: TopicKind, filter: &str, interval: Option<Interval>) -> Result<Self> {
        Self::build(kind, filter, interval)
    }

    /// Fans a board (`HOSE`, `HNX`, `UPCOM`, `DER`) out to all of its symbols (`<kind>.<BOARD>@*`).
    ///
    /// Prefer this over `Topic::trade("HOSE")`: without `@*`, SSI treats `HOSE` as a literal
    /// symbol and delivers no data.
    pub fn board_fanout(kind: TopicKind, board: &Exchange) -> Result<Self> {
        Self::build(kind, stream_board(board), Some(Interval::All))
    }

    /// Fans an index basket (for example `VN30`) out to all of its constituents
    /// (`<kind>.<INDEX>@*`).
    ///
    /// Prefer this over `Topic::quote("VN30")`: without `@*`, SSI treats `VN30` as a literal
    /// symbol and delivers no data.
    pub fn basket_fanout(kind: TopicKind, index: &str) -> Result<Self> {
        Self::build(kind, index, Some(Interval::All))
    }

    /// Fan-out topics for several families on one board (`<kind>.<BOARD>@*`).
    pub fn for_board(kinds: &[TopicKind], board: &Exchange) -> Result<Vec<Self>> {
        let mut topics = Vec::with_capacity(kinds.len());
        for kind in kinds {
            topics.push(Self::board_fanout(*kind, board)?);
        }
        Ok(topics)
    }

    /// Fan-out topics for several families on one index basket (`<kind>.<INDEX>@*`).
    pub fn for_basket(kinds: &[TopicKind], index: &str) -> Result<Vec<Self>> {
        let mut topics = Vec::with_capacity(kinds.len());
        for kind in kinds {
            topics.push(Self::basket_fanout(*kind, index)?);
        }
        Ok(topics)
    }

    /// Accepts a topic string as is after a character check (escape hatch).
    pub fn raw(topic: &str) -> Result<Self> {
        let topic = topic.trim();
        let valid = !topic.is_empty()
            && topic.len() <= 200
            && topic
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '@' | '*' | '_'));
        if !valid {
            return Err(Error::InvalidRequest(format!("invalid topic {topic:?}")));
        }
        Ok(Self(topic.to_owned()))
    }

    fn build(kind: TopicKind, filter: &str, interval: Option<Interval>) -> Result<Self> {
        let filter = filter.trim().to_ascii_uppercase();
        let is_single_code = !filter.is_empty()
            && filter != "*"
            && !filter.contains('-')
            && filter.chars().all(|ch| ch.is_ascii_alphanumeric());
        let valid = !filter.is_empty()
            && (filter == "*"
                || filter.split('-').all(|part| {
                    !part.is_empty() && part.chars().all(|ch| ch.is_ascii_alphanumeric())
                }));
        if !valid {
            return Err(Error::InvalidRequest(format!(
                "invalid topic filter {filter:?}"
            )));
        }
        if interval.is_some() && matches!(kind, TopicKind::Market | TopicKind::IndexSummary) {
            return Err(Error::InvalidRequest(
                "market and indexsummary topics take no interval".to_owned(),
            ));
        }
        if interval == Some(Interval::All) {
            if !matches!(
                kind,
                TopicKind::Trade
                    | TopicKind::Quote
                    | TopicKind::Room
                    | TopicKind::Put
                    | TopicKind::OddLot
            ) {
                return Err(Error::InvalidRequest(
                    "@* fan-out is only valid for trade, quote, room, put and oddlot".to_owned(),
                ));
            }
            if !is_single_code {
                return Err(Error::InvalidRequest(
                    "@* fan-out requires a single concrete board or index code".to_owned(),
                ));
            }
        }
        if kind == TopicKind::IndexTrade && interval.is_some() && !is_single_code {
            return Err(Error::InvalidRequest(
                "trade.index with an interval requires a single concrete index code".to_owned(),
            ));
        }
        if matches!(interval, Some(Interval::Minute1 | Interval::Minute5))
            && !matches!(kind, TopicKind::Trade | TopicKind::IndexTrade)
        {
            return Err(Error::InvalidRequest(
                "candle intervals exist only for trade and trade.index".to_owned(),
            ));
        }
        let suffix = interval.map_or("", Interval::suffix);
        Ok(Self(format!("{}{filter}{suffix}", kind.prefix())))
    }

    /// One topic per symbol for several families at once (for example trade + quote).
    pub fn for_symbols<'a>(
        kinds: &[TopicKind],
        symbols: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<Self>> {
        let symbols: Vec<&str> = symbols.into_iter().collect();
        let mut topics = Vec::with_capacity(kinds.len() * symbols.len());
        for kind in kinds {
            for symbol in &symbols {
                topics.push(Self::build(*kind, symbol, None)?);
            }
        }
        Ok(topics)
    }

    /// The topic string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The topic family.
    pub fn kind(&self) -> Option<TopicKind> {
        TopicKind::of(&self.0)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

const fn stream_board(board: &Exchange) -> &str {
    match board {
        Exchange::Hose => "HOSE",
        Exchange::Hnx => "HNX",
        Exchange::Upcom => "UPCOM",
        Exchange::Derivatives => "DER",
        Exchange::Other(other) => other.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_documented_topics() {
        assert_eq!(Topic::trade("vnm").expect("ok").as_str(), "trade.VNM");
        assert_eq!(
            Topic::trade_with("SSI", Interval::Minute1)
                .expect("ok")
                .as_str(),
            "trade.SSI@1m"
        );
        assert_eq!(
            Topic::index_trade("VN30", None).expect("ok").as_str(),
            "trade.index.VN30"
        );
        assert_eq!(
            Topic::quote("ACB-SSI").expect("ok").as_str(),
            "quote.ACB-SSI"
        );
        assert_eq!(
            Topic::new(TopicKind::Quote, "hose", Some(Interval::All))
                .expect("ok")
                .as_str(),
            "quote.HOSE@*"
        );
        assert_eq!(Topic::market_flag().as_str(), "market.flag");
        assert_eq!(
            Topic::index_summary("*").expect("ok").as_str(),
            "indexsummary.*"
        );
        assert!(Topic::market("VNM").is_ok());
    }

    #[test]
    fn rejects_bad_topics() {
        assert!(Topic::trade("VN M").is_err());
        assert!(Topic::trade("A--B").is_err());
        assert!(Topic::new(TopicKind::Quote, "VNM", Some(Interval::Minute1)).is_err());
        assert!(Topic::new(TopicKind::Market, "VNM", Some(Interval::Tick)).is_err());
        assert!(Topic::new(TopicKind::Trade, "*", Some(Interval::All)).is_err());
        assert!(Topic::new(TopicKind::Quote, "A-B", Some(Interval::All)).is_err());
        assert!(Topic::new(TopicKind::IndexTrade, "VN30", Some(Interval::All)).is_err());
        assert!(Topic::index_trade("*", Some(Interval::Minute1)).is_err());
        assert!(Topic::index_trade("VN30-HNX30", Some(Interval::Minute5)).is_err());
        assert!(Topic::raw("trade.VNM;drop").is_err());
    }

    #[test]
    fn board_and_basket_fanout_helpers() {
        assert_eq!(
            Topic::board_fanout(TopicKind::Quote, &Exchange::Hose)
                .expect("ok")
                .as_str(),
            "quote.HOSE@*"
        );
        assert_eq!(
            Topic::board_fanout(TopicKind::Trade, &Exchange::Derivatives)
                .expect("ok")
                .as_str(),
            "trade.DER@*"
        );
        assert_eq!(
            Topic::basket_fanout(TopicKind::Room, "vn30")
                .expect("ok")
                .as_str(),
            "room.VN30@*"
        );
        let board_topics =
            Topic::for_board(&[TopicKind::Trade, TopicKind::Quote], &Exchange::Hnx).expect("ok");
        assert_eq!(
            board_topics.iter().map(Topic::as_str).collect::<Vec<_>>(),
            vec!["trade.HNX@*", "quote.HNX@*"]
        );
        let basket_topics =
            Topic::for_basket(&[TopicKind::Trade, TopicKind::Quote], "VN30").expect("ok");
        assert_eq!(
            basket_topics.iter().map(Topic::as_str).collect::<Vec<_>>(),
            vec!["trade.VN30@*", "quote.VN30@*"]
        );
    }

    #[test]
    fn kinds() {
        assert_eq!(
            TopicKind::of("trade.index.VN30@1m"),
            Some(TopicKind::IndexTrade)
        );
        assert_eq!(TopicKind::of("TRADE.VNM"), Some(TopicKind::Trade));
        assert_eq!(TopicKind::of("market.flag"), Some(TopicKind::Market));
        assert_eq!(TopicKind::of("order.123"), None);
        let topics =
            Topic::for_symbols(&[TopicKind::Trade, TopicKind::Quote], ["VNM", "FPT"]).expect("ok");
        assert_eq!(topics.len(), 4);
    }
}
