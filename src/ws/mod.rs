//! Explicit opt-in Trading WebSocket RPC client (`wss://api.ssi.com.vn/ws/v3/trading`).
//!
//! Opens a dedicated WebSocket connection via [`SsiClient::connect_trading_ws`] for
//! low-latency signed order entry, batch orders, FCO, account queries, and single-account push
//! events over a single socket.
//!
//! Safety guarantees:
//!
//! - **Verbatim signing:** `params` is serialized once, signed over those exact bytes, and
//!   embedded verbatim so the gateway's signature check never drifts (`401013`).
//! - **Fail closed:** mutating commands (`order.place`, `order.amend`, `order.cancel`,
//!   `order.batchNew`, `order.batchCancel`, `fco.place`, `fco.cancel`) require a configured RSA
//!   private key.
//! - **Never retried on timeout:** timed-out commands return [`crate::Error::Timeout`] without
//!   re-sending. Only pre-execution signature encoding refusals (`401006` / `401008` / `401013`)
//!   in [`SignatureEncoding::Auto`] mode retry once with the alternate encoding.
//! - **Post-upgrade refusal detection:** waits [`TradingWsConfig::handshake_grace`] after HTTP
//!   101 to catch SSI's `{"code":401|403|429|500,...}` refusal frame immediately.

mod client;
mod config;
mod protocol;

use tokio::sync::mpsc;

pub use self::{
    client::{TradingWsClient, TradingWsEvent, WsCall},
    config::{SignatureEncoding, TradingWsConfig},
    protocol::{WsRateLimit, WsResponse, is_mutating_method},
};
use crate::{client::SsiClient, error::Result};

impl SsiClient {
    /// Connects to the Trading WebSocket RPC endpoint (`wss://api.ssi.com.vn/ws/v3/trading`).
    ///
    /// Returns the cloneable command client and a bounded receiver for unsolicited push events
    /// and subscription acknowledgements.
    pub async fn connect_trading_ws(
        &self,
        config: TradingWsConfig,
    ) -> Result<(TradingWsClient, mpsc::Receiver<TradingWsEvent>)> {
        TradingWsClient::connect(self.clone(), config).await
    }
}
