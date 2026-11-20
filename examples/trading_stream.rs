//! Connects to the supervised `TRADING` event stream (`wss://stream.ssi.com.vn/ws/v3`) and
//! logs real-time order, fill, portfolio, and FCO updates until Ctrl-C.
//!
//! Usage:
//! ```bash
//! SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --example trading_stream
//! ```

mod common;

use ssi_sdk::{TradingStreamConfig, TradingStreamEvent, TradingTopic};

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let client = common::client()?;
    common::authenticate(&client).await?;

    let topics = [TradingTopic::all_orders(), TradingTopic::all_portfolios()];
    let mut stream = client.trading_stream(TradingStreamConfig::new(topics));
    let cancel = stream.cancel_token();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received; shutting down trading stream");
            cancel.cancel();
        }
    });

    while let Some(event) = stream.recv().await {
        match event {
            TradingStreamEvent::Connected { connection, at } => {
                tracing::info!(connection, %at, "trading stream connected");
            }
            TradingStreamEvent::Resubscribed {
                connection,
                topics,
                at,
            } => {
                tracing::warn!(
                    connection,
                    topics,
                    %at,
                    "trading stream resubscribed; reconcile open orders via REST"
                );
            }
            TradingStreamEvent::Disconnected {
                reason, retry_in, ..
            } => {
                tracing::warn!(?reason, ?retry_in, "trading stream disconnected");
            }
            TradingStreamEvent::Event(ev) => {
                tracing::info!(topic = %ev.topic, ?ev.data, "trading event");
            }
            TradingStreamEvent::Ack(ack) => {
                tracing::info!(?ack, "subscription ack");
            }
            other => {
                tracing::debug!(?other, "stream event");
            }
        }
    }

    let stats = stream.stats();
    tracing::info!(?stats, "trading stream stopped");
    Ok(())
}
