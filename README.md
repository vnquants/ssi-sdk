# SSI FastConnect V3 SDK (`ssi-sdk`)

[![MSRV](https://img.shields.io/badge/rustc-1.100+-orange.svg)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Unified, typed async Rust SDK for **SSI FastConnect V3** (`https://api.ssi.com.vn/api/v3/...`,
`wss://stream.ssi.com.vn/ws/v3`, `wss://api.ssi.com.vn/ws/v3/trading`). Library name: `ssi_sdk`.

One [`SsiClient`] serves both market data and trading behind clean Cargo feature gates:

| Feature | Default | What it enables |
|---|---|---|
| *(core, always on)* | yes | `SsiClient`, `TokenManager` (automatic read-only login, single-use `/refresh`, `StoredToken` export/import), `Config`, `Credentials`, `Error`, `ApiErrorCode`, `time`, and read-only account queries (`accounts`, `balance`, `positions`, `ppmmr`, `order_book`). |
| `data` | **yes** | Market-data REST (`ohlc`, `ohlc_today`, `corporate_actions`, `securities`, `price_limits`, `indices`, `index_summary`, `securities_summary`, `market_files`, `download_market_file`) and supervised `DATA` WebSocket (`MarketStream`). Zero RSA/XML dependencies are compiled when `trading` is disabled. |
| `trading` | no | OTP & Smart OTP push-approval auth, RSA PKCS#1 v1.5 SHA-256 request signing, `max_buy_sell`, signed single & batch orders, Flexible Conditional Orders (all 7 FCO strategies), supervised `TRADING` event stream (`TradingStream`), and explicit opt-in Trading WebSocket RPC (`TradingWsClient`). |

## Installation

Requires Rust **1.100** or newer (`edition = "2024"`).

```toml
# Market data only (default; no RSA / XML dependencies):
ssi-sdk = "0.1"

# Trading only (no market-data endpoints):
ssi-sdk = { version = "0.1", default-features = false, features = ["trading"] }

# Full SDK (both market data and trading on one SsiClient):
ssi-sdk = { version = "0.1", features = ["data", "trading"] }
```

## Configuration

`SsiClient::from_env()` reads credentials and optional endpoint overrides from the environment
(see [`.env.example`](.env.example)):

| Variable | Required | Feature | Purpose |
|---|---|---|---|
| `SSI_API_KEY` | yes | core | API key (iBoard → API key management) |
| `SSI_API_SECRET` | yes | core | API secret |
| `SSI_CLIENT_ID` | no | core | Optional client id sent on balance & position queries |
| `SSI_PRIVATE_KEY` | for orders/FCO | `trading` | RSA signing key (base64 XML `RSAKeyValue` or PEM PKCS#1 / PKCS#8) |
| `SSI_V3_API_URL` | no | core | REST host (default `https://api.ssi.com.vn`) |
| `SSI_V3_TRADING_URL` | no | core | Optional override host for `/api/v3/trading/*` and `/api/v3/account/*` |
| `SSI_V3_STREAM_URL` | no | core | Streaming WS host (default `wss://stream.ssi.com.vn/ws/v3`) |
| `SSI_V3_TRADING_WS_URL` | no | `trading` | Trading WS RPC host (default `wss://api.ssi.com.vn/ws/v3/trading`) |

## Quick Start

### Market Data (`data` feature — default)

```rust,no_run
use std::time::Duration;

use chrono::NaiveDate;
use ssi_sdk::{
    SsiClient,
    rest::{OhlcQuery, Timeframe},
    stream::{MarketStreamConfig, MarketStreamEvent, StalePolicy, Topic, TopicKind},
};

async fn run(from: NaiveDate, to: NaiveDate) -> Result<(), ssi_sdk::Error> {
    let client = SsiClient::from_env()?; // SSI_API_KEY, SSI_API_SECRET
    let bars = client.ohlc(&OhlcQuery::days("VNM", Timeframe::Day1, from, to)?).await?;
    tracing::info!(bars = bars.len(), "daily bars");

    let topics = Topic::for_symbols(&[TopicKind::Trade, TopicKind::Quote], ["VNM", "FPT"])?;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics)
            .with_stale_policy(StalePolicy::vn_equity_hours(Duration::from_secs(60))),
    );
    while let Some(event) = stream.recv().await {
        match event {
            MarketStreamEvent::Data(ev) => tracing::debug!(topic = %ev.topic, "market data"),
            MarketStreamEvent::Resubscribed { .. } => { /* refill gap from REST */ }
            other => tracing::info!(?other, "stream"),
        }
    }
    Ok(())
}
```

### Trading (`trading` feature)

```rust,no_run
# #[cfg(feature = "trading")]
async fn trade(otp: &str) -> Result<(), ssi_sdk::Error> {
    use rust_decimal::dec;
    use ssi_sdk::{AccountNo, OrderSide, PlaceOrderRequest, SsiClient};

    let client = SsiClient::from_env()?; // SSI_API_KEY, SSI_API_SECRET, SSI_PRIVATE_KEY
    client.auth().authenticate_with_otp(otp).await?;

    let account = AccountNo::new("003C123451");
    let order = PlaceOrderRequest::limit(account, "VNM", OrderSide::Buy, 100, dec!(65000));
    let placed = client.place_order(&order).await?;
    tracing::info!(order_id = %placed.order_id, status = ?placed.status, "placed");
    Ok(())
}
```

## Precision, Time, and Security Guarantees

- **Zero `f64` price decoding:** all prices, values, and balances decode straight from JSON text
  into `rust_decimal::Decimal` via `serde_json::value::RawValue`.
- **VN timezone handling:** exchange timestamps (`Asia/Ho_Chi_Minh`) convert to UTC cleanly.
- **Sticky trading sessions:** once OTP / Smart OTP auth succeeds (or a trading `StoredToken` is
  imported), `TokenManager` never silently downgrades to an OTP-less data token on refresh
  failure — it clears the session and returns `Error::ReauthenticationRequired`.
- **Single-use refresh tokens:** refresh tokens are taken out before each `/refresh` call so a
  cancelled or concurrent refresh can never replay a spent refresh token.
- **No automatic retries on mutating commands:** `POST` / `PUT` / `DELETE` order and FCO commands
  and Trading WS mutations are never retried on timeout or server error.
- **Secret redaction:** `SSI_API_KEY`, `SSI_API_SECRET`, `SSI_PRIVATE_KEY`, Bearer/refresh tokens,
  OTPs, Parquet download `token=` parameters, and `AccountNo` values are masked in `Debug`,
  `Display`, and errors. Keep `tungstenite=debug` (or stricter) in your `tracing` filter.

## Examples

```bash
# Market data (default feature `data`):
SSI_DOTENV=/path/to/.env cargo run --example token_info
SSI_DOTENV=/path/to/.env cargo run --example ohlc -- VNM 1d 2025-01-01 2025-12-31
SSI_DOTENV=/path/to/.env cargo run --example stream -- 60 VNM,FPT,HPG
SSI_DOTENV=/path/to/.env cargo run --example live_check -- 2026-11-19

# Trading (opt-in feature `trading`):
SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --features trading --example auth_otp
SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --features trading --example account_overview
SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --features trading --example trading_stream
```

See [`docs/v3-reference.md`](docs/v3-reference.md) for the consolidated SSI FastConnect V3 wire
protocol reference.
