//! Downloads OHLC bars for a date range and optionally writes them as CSV.
//!
//! ```text
//! SSI_DOTENV=/path/to/.env cargo run --example ohlc -- VNM 1d 2025-01-01 2025-12-31 [out.csv]
//! ```

#[path = "common/mod.rs"]
mod common;

use std::io::Write as _;

use chrono::NaiveDate;
use ssi_sdk::rest::{OhlcQuery, Timeframe};

fn arg(args: &[String], index: usize, default: &str) -> String {
    args.get(index)
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let args: Vec<String> = std::env::args().collect();
    let symbol = arg(&args, 1, "VNM");
    let timeframe: Timeframe = arg(&args, 2, "1d").parse()?;
    let from = NaiveDate::parse_from_str(&arg(&args, 3, "2025-01-01"), "%Y-%m-%d")?;
    let to = NaiveDate::parse_from_str(&arg(&args, 4, "2025-12-31"), "%Y-%m-%d")?;
    let client = common::client()?;
    let query = OhlcQuery::days(&symbol, timeframe, from, to)?;
    let started = std::time::Instant::now();
    let bars = client.ohlc(&query).await?;
    tracing::info!(
        %symbol,
        %timeframe,
        bars = bars.len(),
        first = ?bars.first().map(|bar| bar.time),
        last = ?bars.last().map(|bar| bar.time),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "downloaded"
    );
    for bar in bars.iter().take(2).chain(bars.iter().rev().take(2)) {
        tracing::info!(
            time = %bar.time,
            open = %bar.open,
            high = %bar.high,
            low = %bar.low,
            close = %bar.close,
            volume = bar.volume,
            value = %bar.value,
            "bar"
        );
    }
    if let Some(path) = args.get(5) {
        let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
        writeln!(
            file,
            "symbol,time_utc,trading_date,open,high,low,close,volume,value"
        )?;
        for bar in &bars {
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{}",
                bar.symbol,
                bar.time.to_rfc3339(),
                bar.trading_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.value
            )?;
        }
        file.flush()?;
        tracing::info!(%path, "CSV written");
    }
    if let Some(rate) = client.rate_limit() {
        tracing::info!(limit = ?rate.limit, remaining = ?rate.remaining, "rate-limit headers");
    }
    Ok(())
}
