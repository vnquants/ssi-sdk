//! Subscribes `trade` + `quote` for some symbols plus the `market` channel for N seconds and
//! logs message counts per channel, quote depth per exchange, timestamp resolution and
//! heartbeat behaviour. Depth counts real levels: the decoder drops the `["0","0"]` padding.
//! Timestamp resolution and the exchange-to-receive lag are reported per message family
//! (trade times carry milliseconds, quote times whole seconds, so a quote lag includes up to
//! 1 s of truncation).
//!
//! ```text
//! SSI_DOTENV=/path/to/.env cargo run --example stream -- \
//!     1800 VNM,FPT,HPG,SHS,PVS,VEA --dump /path/to/frames.jsonl
//! ```
//!
//! `--dump <path.jsonl>` appends one JSON line per received text frame
//! (`{"received_at", "kind": "frame", "frame"}`), plus heartbeat and connection events, flushed
//! line by line so a killed run keeps what it read. Account-like fields and trading topics are
//! redacted. Ctrl-C ends the run early and still prints the summary.

#[path = "common/mod.rs"]
mod common;
#[path = "common/dump.rs"]
mod dump;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use chrono::Timelike as _;
use ssi_sdk::{
    SsiClient,
    rest::SecuritiesFilter,
    stream::{
        HeartbeatKind, MarketData, MarketStreamConfig, MarketStreamEvent, StalePolicy, Topic,
        TopicKind,
    },
};
use tokio::time::Instant;

#[derive(Default)]
struct Depth {
    messages: u64,
    max_bids: usize,
    max_asks: usize,
    sizes: BTreeSet<(usize, usize)>,
}

/// Exchange-time resolution and lag of one message family.
#[derive(Default)]
struct Timing {
    messages: u64,
    sub_second: u64,
    max_lag_ms: i64,
}

#[derive(Default)]
struct Stats {
    boards: BTreeMap<String, String>,
    counts: BTreeMap<String, u64>,
    heartbeats: BTreeMap<&'static str, Vec<String>>,
    depth: BTreeMap<String, Depth>,
    timing: BTreeMap<&'static str, Timing>,
}

impl Stats {
    fn record(&mut self, event: MarketStreamEvent, started: Instant) {
        match event {
            MarketStreamEvent::Data(data) => {
                *self
                    .counts
                    .entry(data.data.family().to_owned())
                    .or_default() += 1;
                if let Some(time) = data.data.exchange_time() {
                    let timing = self.timing.entry(data.data.family()).or_default();
                    timing.messages += 1;
                    if time.nanosecond() != 0 {
                        timing.sub_second += 1;
                    }
                    timing.max_lag_ms = timing
                        .max_lag_ms
                        .max((data.received_at - time).num_milliseconds());
                }
                if let MarketData::Quote(quote) = &data.data {
                    let board = self
                        .boards
                        .get(&quote.symbol)
                        .cloned()
                        .unwrap_or_else(|| "?".to_owned());
                    let entry = self.depth.entry(board).or_default();
                    entry.messages += 1;
                    entry.max_bids = entry.max_bids.max(quote.bids.len());
                    entry.max_asks = entry.max_asks.max(quote.asks.len());
                    entry.sizes.insert((quote.bids.len(), quote.asks.len()));
                }
                if self.counts.values().sum::<u64>() <= 5 {
                    tracing::info!(topic = %data.topic, data = ?data.data, "sample message");
                }
            }
            MarketStreamEvent::Heartbeat { kind, .. } => {
                let label = match kind {
                    HeartbeatKind::TransportPong => "transport_pong",
                    HeartbeatKind::TransportPing => "transport_ping",
                    HeartbeatKind::ServerPing => "server_json_ping",
                    HeartbeatKind::PingAck => "ping_ack",
                };
                self.heartbeats
                    .entry(label)
                    .or_default()
                    .push(format!("{:.1}s", started.elapsed().as_secs_f64()));
            }
            other => tracing::info!(event = ?other, "stream event"),
        }
    }

    fn report(&self, seconds: u64) {
        tracing::info!(seconds, counts = ?self.counts, "message counts per channel");
        for (board, stats) in &self.depth {
            tracing::info!(
                %board,
                messages = stats.messages,
                max_bid_levels = stats.max_bids,
                max_ask_levels = stats.max_asks,
                level_counts_seen = ?stats.sizes,
                "quote depth"
            );
        }
        if self.depth.is_empty() {
            tracing::info!("no quote messages: nothing to measure (market closed?)");
        }
        for (family, timing) in &self.timing {
            tracing::info!(
                %family,
                timed_messages = timing.messages,
                sub_second_timestamps = timing.sub_second,
                max_exchange_to_receive_lag_ms = timing.max_lag_ms,
                "timestamp resolution"
            );
        }
        for (kind, times) in &self.heartbeats {
            tracing::info!(%kind, count = times.len(), at = ?times, "heartbeats");
        }
    }
}

async fn boards(
    client: &SsiClient,
    symbols: &[String],
) -> Result<BTreeMap<String, String>, common::BoxError> {
    let mut boards = BTreeMap::new();
    for symbol in symbols {
        let info = client
            .securities(&SecuritiesFilter::Symbol(symbol.clone()))
            .await?;
        let board = info
            .first()
            .map_or_else(|| "?".to_owned(), |row| row.board.to_string());
        tracing::info!(%symbol, %board, "symbol board");
        boards.insert(symbol.clone(), board);
    }
    Ok(boards)
}

struct Args {
    seconds: u64,
    symbols: Vec<String>,
    dump: Option<PathBuf>,
}

fn parse_args() -> Result<Args, common::BoxError> {
    let mut positional = Vec::new();
    let mut dump = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--dump" {
            dump = Some(PathBuf::from(
                args.next().ok_or("--dump needs a file path")?,
            ));
        } else if let Some(path) = arg.strip_prefix("--dump=") {
            dump = Some(PathBuf::from(path));
        } else {
            positional.push(arg);
        }
    }
    let seconds = positional.first().map_or(Ok(60), |text| text.parse())?;
    let symbols = positional
        .get(1)
        .map_or("VNM,FPT,HPG", String::as_str)
        .split(',')
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect();
    Ok(Args {
        seconds,
        symbols,
        dump,
    })
}

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let Args {
        seconds,
        symbols,
        dump,
    } = parse_args()?;
    let mut dump = dump.as_deref().map(dump::Dump::open).transpose()?;
    let client = common::client()?;
    let mut stats = Stats {
        boards: boards(&client, &symbols).await?,
        ..Stats::default()
    };

    let mut topics = Topic::for_symbols(
        &[TopicKind::Trade, TopicKind::Quote, TopicKind::Market],
        symbols.iter().map(String::as_str),
    )?;
    topics.push(Topic::market_flag());
    let config = MarketStreamConfig::new(topics)
        .with_heartbeat_events(true)
        .with_raw_frames(dump.is_some())
        .with_stale_policy(StalePolicy::vn_equity_hours(Duration::from_mins(1)));
    let mut stream = client.market_stream(config);

    let started = Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    let mut asked_list = false;
    loop {
        let event = tokio::select! {
            () = &mut deadline => break,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupted");
                break;
            }
            event = stream.recv() => match event {
                Some(event) => event,
                None => break,
            },
        };
        if !asked_list && started.elapsed() > Duration::from_secs(3) {
            stream.request_subscriptions()?;
            asked_list = true;
        }
        if let Some(dump) = dump.as_mut() {
            dump.write(&event)?;
        }
        if !matches!(event, MarketStreamEvent::RawFrame { .. }) {
            stats.record(event, started);
        }
    }
    let totals = stream.stats();
    stream.shutdown().await;
    tracing::info!(
        connections = totals.connections,
        frames = totals.frames,
        messages = totals.messages,
        unknown_messages = totals.unknown_messages,
        unrecognised_frames = totals.unrecognised_frames,
        consumer_stalls = totals.consumer_stalls,
        longest_consumer_stall_ms =
            u64::try_from(totals.longest_consumer_stall.as_millis()).unwrap_or(u64::MAX),
        "stream statistics"
    );
    if let Some(dump) = dump {
        tracing::info!(lines = dump.lines(), "raw frames dumped");
    }
    stats.report(seconds);
    Ok(())
}
