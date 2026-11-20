#![doc = "Contract tests built from the 2026-11-20 market-hours capture of the V3 market stream."]
#![allow(clippy::expect_used, clippy::indexing_slicing)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Timelike as _, Utc};
use rust_decimal::{Decimal, dec};
use serde_json::Value;
use ssi_sdk::{
    model::OrderSide,
    stream::{BookPrice, Level, MarketData, Quote, Trade, market::message::decode_text},
};

/// The first 31 s (407 lines) of `examples/stream --dump`, started 09:53:33 ICT on 2026-11-20
/// for VNM, FPT, HPG (HOSE), SHS, PVS (HNX) and VEA (UPCOM). Public market data only: no
/// token and no account field. The dump re-serialised each frame (keys sorted); every value is
/// as sent.
const CAPTURE: &str = "stream_market_hours_20261120";

const BOARDS: [(&str, &str); 6] = [
    ("FPT", "HOSE"),
    ("HPG", "HOSE"),
    ("VNM", "HOSE"),
    ("PVS", "HNX"),
    ("SHS", "HNX"),
    ("VEA", "UPCOM"),
];

fn board(symbol: &str) -> &'static str {
    BOARDS
        .iter()
        .find(|(known, _)| *known == symbol)
        .map(|(_, board)| *board)
        .expect("a captured symbol")
}

/// One `DATA` frame of the capture.
struct Frame {
    topic: String,
    data: Value,
    received_at: DateTime<Utc>,
}

impl Frame {
    fn decode(&self) -> MarketData {
        decode_text(&self.topic, &self.data.to_string())
    }
}

fn data_frames() -> Vec<Frame> {
    common::fixture_lines(CAPTURE)
        .into_iter()
        .filter_map(|line| {
            let frame = line.get("frame")?;
            Some(Frame {
                topic: frame.get("topic")?.as_str()?.to_owned(),
                data: frame.get("data")?.clone(),
                received_at: line.get("received_at")?.as_str()?.parse().ok()?,
            })
        })
        .collect()
}

/// Captured trades in receive order, with their receive times.
fn trades() -> Vec<(Trade, DateTime<Utc>)> {
    data_frames()
        .iter()
        .filter(|frame| frame.topic.starts_with("trade."))
        .map(|frame| match frame.decode() {
            MarketData::Trade(trade) => Ok((trade, frame.received_at)),
            other => Err(format!("{}: {other:?}", frame.topic)),
        })
        .collect::<Result<_, _>>()
        .expect("every captured trade decodes as a Trade")
}

/// Captured quotes in receive order, with the number of levels sent per side.
fn quotes() -> Vec<(Quote, (usize, usize), DateTime<Utc>)> {
    data_frames()
        .iter()
        .filter(|frame| frame.topic.starts_with("quote."))
        .map(|frame| {
            let sent = |side: &str| frame.data[side].as_array().map_or(0, Vec::len);
            match frame.decode() {
                MarketData::Quote(quote) => {
                    Ok((quote, (sent("bids"), sent("asks")), frame.received_at))
                }
                other => Err(format!("{}: {other:?}", frame.topic)),
            }
        })
        .collect::<Result<_, _>>()
        .expect("every captured quote decodes as a Quote")
}

fn utc(text: &str) -> DateTime<Utc> {
    text.parse().expect("RFC 3339 time")
}

fn level(price: i64, quantity: u64) -> Level {
    Level {
        price: BookPrice::Price(Decimal::from(price)),
        quantity,
    }
}

fn book(levels: &[(i64, u64)]) -> Vec<Level> {
    levels
        .iter()
        .map(|&(price, quantity)| level(price, quantity))
        .collect()
}

#[test]
fn every_captured_line_is_accounted_for_and_no_payload_is_unknown() {
    let lines = common::fixture_lines(CAPTURE);
    let mut kinds = BTreeMap::<String, usize>::new();
    for line in &lines {
        let kind = line["kind"].as_str().expect("kind");
        let key = match (kind, line["frame"]["topic"].as_str()) {
            ("frame", Some(topic)) => {
                let data = decode_text(topic, &line["frame"]["data"].to_string());
                assert!(
                    !matches!(data, MarketData::Unknown { .. }),
                    "{topic}: {data:?}"
                );
                assert_eq!(topic.split('.').next(), Some(data.family()), "{topic}");
                data.family().to_owned()
            }
            ("frame", None) => "control frame".to_owned(),
            (other, _) => other.to_owned(),
        };
        *kinds.entry(key).or_default() += 1;
    }
    let expected = [
        ("control frame", 3),
        ("event", 1),
        ("heartbeat", 2),
        ("quote", 282),
        ("trade", 119),
    ];
    assert_eq!(
        kinds,
        expected
            .into_iter()
            .map(|(kind, count)| (kind.to_owned(), count))
            .collect(),
    );
    assert_eq!(lines.len(), 407);
}

#[test]
fn the_first_trade_per_exchange_decodes_every_field() {
    let trades = trades();
    let first = |symbol: &str| {
        trades
            .iter()
            .find(|(trade, _)| trade.symbol == symbol)
            .map(|(trade, _)| trade.clone())
            .expect("a trade")
    };
    let expected = [
        // {"a":"45691.44","h":"46000","l":"45350","o":"45600","p":"45400","q":"100","s":"FPT",
        //  "si":"B","t":"2026/11/20 09:53:33.771","v":"2700400"}
        Trade {
            symbol: "FPT".to_owned(),
            time: utc("2026-11-20T02:53:33.771Z"),
            price: dec!(45400),
            quantity: 100,
            average_price: Some(dec!(45691.44)),
            side: Some(OrderSide::Buy),
            open: Some(dec!(45600)),
            high: Some(dec!(46000)),
            low: Some(dec!(45350)),
            total_volume: 2_700_400,
        },
        // {"a":"13102.4","h":"13300","l":"12900","o":"13200","p":"13100","q":"500","s":"SHS",
        //  "si":"S","t":"2026/11/20 09:53:37.462","v":"2389400"}
        Trade {
            symbol: "SHS".to_owned(),
            time: utc("2026-11-20T02:53:37.462Z"),
            price: dec!(13100),
            quantity: 500,
            average_price: Some(dec!(13102.4)),
            side: Some(OrderSide::Sell),
            open: Some(dec!(13200)),
            high: Some(dec!(13300)),
            low: Some(dec!(12900)),
            total_volume: 2_389_400,
        },
        // {"a":"29994.52","h":"30200","l":"29900","o":"30100","p":"30100","q":"200","s":"VEA",
        //  "si":"B","t":"2026/11/20 09:54:04.952","v":"67500"}
        Trade {
            symbol: "VEA".to_owned(),
            time: utc("2026-11-20T02:54:04.952Z"),
            price: dec!(30100),
            quantity: 200,
            average_price: Some(dec!(29994.52)),
            side: Some(OrderSide::Buy),
            open: Some(dec!(30100)),
            high: Some(dec!(30200)),
            low: Some(dec!(29900)),
            total_volume: 67_500,
        },
    ];
    for trade in expected {
        assert_eq!(first(&trade.symbol), trade);
    }
}

/// The field meanings, checked on every captured trade: `v` is the day's cumulative volume
/// (it grows by `q`), `a` the day's volume-weighted average price rounded to two decimals
/// (`a * v` grows by `p * q`), `o`/`h`/`l` the day's open, high and low, `si` the aggressor
/// side, and `t` an exchange time with milliseconds, shortly before the receive time.
#[test]
fn every_trade_follows_the_documented_field_meanings() {
    let mut last = BTreeMap::<String, Trade>::new();
    let mut sub_second = 0;
    for (trade, received_at) in trades() {
        assert_eq!(trade.time.nanosecond() % 1_000_000, 0, "whole milliseconds");
        if trade.time.nanosecond() != 0 {
            sub_second += 1;
        }
        let lag = received_at - trade.time;
        assert!(
            lag > Duration::zero() && lag < Duration::seconds(2),
            "{lag}"
        );
        assert!(matches!(trade.side, Some(OrderSide::Buy | OrderSide::Sell)));
        let (Some(low), Some(high)) = (trade.low, trade.high) else {
            panic!("low and high expected: {trade:?}")
        };
        assert!(low <= trade.price && trade.price <= high, "{trade:?}");
        if let Some(previous) = last.get(&trade.symbol) {
            assert_eq!(
                trade.total_volume,
                previous.total_volume + trade.quantity,
                "{trade:?} after {previous:?}"
            );
            assert_eq!(trade.open, previous.open);
            assert_eq!(trade.high, previous.high.map(|high| high.max(trade.price)));
            assert_eq!(trade.low, previous.low.map(|low| low.min(trade.price)));
            let value =
                |trade: &Trade| trade.average_price.expect("a") * Decimal::from(trade.total_volume);
            let rounding = dec!(0.005) * Decimal::from(trade.total_volume + previous.total_volume);
            let matched = trade.price * Decimal::from(trade.quantity);
            assert!(
                (value(&trade) - value(previous) - matched).abs() <= rounding,
                "{trade:?} after {previous:?}"
            );
            assert!(trade.time >= previous.time);
        }
        last.insert(trade.symbol.clone(), trade);
    }
    assert_eq!(last.len(), BOARDS.len(), "every symbol traded");
    assert!(sub_second > 100, "{sub_second}");
}

/// Trades of one aggressive order share one millisecond and arrive one frame each; only the
/// receive order and the cumulative volume tell them apart.
#[test]
fn same_millisecond_batches_are_ordered_by_total_volume() {
    let trades = trades();
    for (symbol, time, count, side, price, volume) in [
        // A buy order lifting VNM asks at 52 000: 21 900 shares in 19 fills.
        (
            "VNM",
            "2026-11-20T02:54:00.541Z",
            19,
            OrderSide::Buy,
            dec!(52000),
            (651_400, 673_300),
        ),
        // VEA (UPCOM): 1 500 shares in 7 fills at 30 100.
        (
            "VEA",
            "2026-11-20T02:54:04.952Z",
            7,
            OrderSide::Buy,
            dec!(30100),
            (67_300, 68_800),
        ),
    ] {
        let batch: Vec<&Trade> = trades
            .iter()
            .map(|(trade, _)| trade)
            .filter(|trade| trade.symbol == symbol && trade.time == utc(time))
            .collect();
        assert_eq!(batch.len(), count, "{symbol}");
        assert!(
            batch
                .iter()
                .all(|trade| trade.side.as_ref() == Some(&side) && trade.price == price),
            "{symbol}"
        );
        let mut volume_before = volume.0;
        for trade in &batch {
            assert_eq!(trade.total_volume, volume_before + trade.quantity);
            volume_before = trade.total_volume;
        }
        assert_eq!(volume_before, volume.1, "{symbol}");
    }
}

#[test]
fn quotes_carry_the_real_depth_per_exchange() {
    let mut depth = BTreeMap::<&str, BTreeSet<(usize, usize)>>::new();
    for (quote, sent, received_at) in quotes() {
        assert_eq!(sent, (10, 10), "10 levels per side on the wire");
        assert_eq!(quote.time.nanosecond(), 0, "quote times are whole seconds");
        let lag = received_at - quote.time;
        assert!(
            lag > Duration::zero() && lag < Duration::seconds(2),
            "{lag}"
        );
        let prices = |levels: &[Level]| -> Vec<Decimal> {
            levels
                .iter()
                .map(|level| level.price.as_decimal().expect("numeric price"))
                .collect()
        };
        let (bids, asks) = (prices(&quote.bids), prices(&quote.asks));
        assert!(bids.windows(2).all(|pair| pair[0] > pair[1]), "{quote:?}");
        assert!(asks.windows(2).all(|pair| pair[0] < pair[1]), "{quote:?}");
        assert!(bids[0] < asks[0], "not crossed: {quote:?}");
        assert!(
            quote
                .bids
                .iter()
                .chain(&quote.asks)
                .all(|level| level.quantity > 0),
            "{quote:?}"
        );
        assert!(
            bids.iter()
                .chain(&asks)
                .all(|price| *price > dec!(1000) && price.fract().is_zero()),
            "prices in dong: {quote:?}"
        );
        depth
            .entry(board(&quote.symbol))
            .or_default()
            .insert((quote.bids.len(), quote.asks.len()));
    }
    let expected = [("HNX", (10, 10)), ("HOSE", (3, 3)), ("UPCOM", (10, 10))];
    assert_eq!(
        depth,
        expected
            .into_iter()
            .map(|(board, levels)| (board, BTreeSet::from([levels])))
            .collect(),
    );
}

#[test]
fn the_first_quote_per_exchange_decodes_every_level() {
    let quotes = quotes();
    let first = |symbol: &str| {
        quotes
            .iter()
            .find(|(quote, _, _)| quote.symbol == symbol)
            .map(|(quote, _, _)| quote.clone())
            .expect("a quote")
    };
    // HOSE: 3 real levels, then 7 × ["0","0"] per side.
    assert_eq!(
        first("FPT"),
        Quote {
            symbol: "FPT".to_owned(),
            time: utc("2026-11-20T02:53:33Z"),
            bids: book(&[(45_350, 62_300), (45_300, 78_000), (45_250, 54_800)]),
            asks: book(&[(45_400, 4_600), (45_450, 12_600), (45_500, 25_100)]),
        }
    );
    let pvs = first("PVS");
    assert_eq!(pvs.time, utc("2026-11-20T02:53:33Z"));
    assert_eq!(
        pvs.bids,
        book(&[
            (27_100, 12_100),
            (27_000, 10_300),
            (26_900, 300),
            (26_800, 3_100),
            (26_700, 7_800),
            (26_600, 2_700),
            (26_500, 49_800),
            (26_400, 5_700),
            (26_300, 13_400),
            (26_200, 47_200),
        ])
    );
    assert_eq!(
        pvs.asks,
        book(&[
            (27_200, 7_500),
            (27_300, 22_300),
            (27_400, 61_200),
            (27_500, 39_400),
            (27_600, 22_800),
            (27_700, 30_400),
            (27_800, 12_100),
            (27_900, 24_400),
            (28_000, 25_100),
            (28_100, 13_900),
        ])
    );
    let vea = first("VEA");
    assert_eq!(vea.time, utc("2026-11-20T02:53:56Z"));
    assert_eq!(vea.bids.len(), 10);
    assert_eq!(vea.asks.len(), 10);
    assert_eq!(vea.bids.first(), Some(&level(30_000, 25_600)));
    assert_eq!(vea.bids.last(), Some(&level(29_100, 16_600)));
    assert_eq!(vea.asks.first(), Some(&level(30_100, 9_400)));
    assert_eq!(vea.asks.last(), Some(&level(31_000, 1_500)));
}
