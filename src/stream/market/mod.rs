//! Supervised real-time market-data stream (`wss://stream.ssi.com.vn/ws/v3`, `DATA` channel).
//!
//! [`SsiClient::market_stream`] spawns a supervisor task and returns an owned, `'static`
//! [`MarketStream`] handle. The supervisor:
//!
//! - connects with a fresh access token, sends every subscription, and emits
//!   [`MarketStreamEvent::Connected`] (and [`MarketStreamEvent::Resubscribed`] after a reconnect);
//! - reconnects forever with exponential backoff and jitter, emitting
//!   [`MarketStreamEvent::Disconnected`] with the reason and delay before the next attempt;
//! - pings (`PING_PONG`) every 25 s, answers server `PING`, and treats 75 s without any frame as
//!   a dead connection;
//! - reports silence on data topics as [`MarketStreamEvent::Stale`] per [`StalePolicy`], and
//!   [`MarketStreamEvent::Resumed`] when data returns;
//! - renews the token after a `401` / `403` / close `1008` rejection.
//!
//! **Logging:** `tungstenite` logs the WebSocket upgrade request (`Authorization: Bearer`
//! included) at `TRACE`. Keep `tungstenite=debug` (or stricter) in your log filter.

mod config;
pub mod message;
mod protocol;
mod supervisor;
pub mod topic;

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use self::{
    config::{ActivityWindow, MarketStreamConfig, StalePolicy, vn_equity_trading_hours},
    message::{
        BookPrice, Candle, ForeignRoom, IndexSummary, Level, MarketData, OddLot, PriceLimitsUpdate,
        PutThrough, Quote, SessionFlag, Trade,
    },
    topic::{Interval, Topic, TopicKind},
};
use super::common::Counters;
pub use super::common::{Backoff, DisconnectReason, HeartbeatKind, StreamStats, SubscriptionAck};
use crate::{
    SsiClient,
    error::{Error, Result},
};

/// One market-data message with its topic and local receive time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataEvent {
    /// Topic as sent by the server.
    pub topic: String,
    /// Typed payload.
    pub data: MarketData,
    /// Local time the frame was read from the socket.
    pub received_at: DateTime<Utc>,
}

/// Events produced by [`MarketStream`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MarketStreamEvent {
    /// A socket is open and the subscriptions were sent.
    Connected {
        /// 1 for the first connection, then counting reconnects.
        connection: u64,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Subscriptions were re-sent after a reconnect. Data published while disconnected is lost.
    Resubscribed {
        /// Connection number.
        connection: u64,
        /// Number of topics re-sent.
        topics: usize,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// The socket ended or could not be opened; the next attempt follows after `retry_in`.
    Disconnected {
        /// Why.
        reason: DisconnectReason,
        /// Delay before the next attempt.
        retry_in: Duration,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// No market data for `silent_for` while the stale policy expects data.
    Stale {
        /// Silence so far.
        silent_for: Duration,
        /// Last data receive time, if any.
        last_data_at: Option<DateTime<Utc>>,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Data arrived again after a `Stale` event.
    Resumed {
        /// Length of the silence.
        silent_for: Duration,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// A market-data message (boxed: market messages are large).
    Data(Box<DataEvent>),
    /// The server's reply to a subscribe or unsubscribe request.
    Ack(SubscriptionAck),
    /// The server's reply to [`MarketStream::request_subscriptions`].
    Subscriptions {
        /// DATA-channel topics.
        data: Vec<String>,
        /// TRADING-channel topics (masked).
        trading: Vec<String>,
    },
    /// Heartbeat traffic (only with [`MarketStreamConfig::with_heartbeat_events`]).
    Heartbeat {
        /// Kind.
        kind: HeartbeatKind,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Every text frame as received, before classification (only with
    /// [`MarketStreamConfig::with_raw_frames`]).
    RawFrame {
        /// The frame text.
        text: String,
        /// Local receive time.
        received_at: DateTime<Utc>,
    },
}

#[derive(Debug)]
pub(crate) enum Command {
    Subscribe(Vec<Topic>),
    Unsubscribe(Vec<Topic>),
    UnsubscribeAll,
    ListSubscriptions,
}

/// Owned handle of a supervised market-data stream.
///
/// Dropping the handle stops the supervisor. Use [`MarketStream::recv`] or the
/// [`futures_util::Stream`] implementation to read events.
#[derive(Debug)]
pub struct MarketStream {
    events: mpsc::Receiver<MarketStreamEvent>,
    commands: mpsc::UnboundedSender<Command>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    counters: Arc<Counters>,
}

impl MarketStream {
    /// Adds topics; they are sent now if connected and after every reconnect.
    pub fn subscribe(&self, topics: impl IntoIterator<Item = Topic>) -> Result<()> {
        self.command(Command::Subscribe(topics.into_iter().collect()))
    }

    /// Removes topics.
    pub fn unsubscribe(&self, topics: impl IntoIterator<Item = Topic>) -> Result<()> {
        self.command(Command::Unsubscribe(topics.into_iter().collect()))
    }

    /// Removes every currently tracked topic (sends `UNSUBSCRIBE` immediately when connected and
    /// clears the list re-sent after reconnects).
    pub fn unsubscribe_all(&self) -> Result<()> {
        self.command(Command::UnsubscribeAll)
    }

    /// Asks the server for the current subscriptions; the reply arrives as
    /// [`MarketStreamEvent::Subscriptions`].
    pub fn request_subscriptions(&self) -> Result<()> {
        self.command(Command::ListSubscriptions)
    }

    fn command(&self, command: Command) -> Result<()> {
        self.commands.send(command).map_err(|_| Error::StreamClosed)
    }

    /// Next event; `None` once the supervisor has stopped.
    pub async fn recv(&mut self) -> Option<MarketStreamEvent> {
        self.events.recv().await
    }

    /// Counters: connections, frames, messages, untyped messages, and consumer stalls.
    pub fn stats(&self) -> StreamStats {
        self.counters.snapshot()
    }

    /// Whether the supervisor task has ended.
    pub fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    /// A token that stops the supervisor when cancelled.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Stops the supervisor (closing the socket) and waits for it to finish.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "market stream supervisor ended abnormally");
        }
    }
}

impl Drop for MarketStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl futures_util::Stream for MarketStream {
    type Item = MarketStreamEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_recv(cx)
    }
}

impl SsiClient {
    /// Starts a supervised market-data stream (`DATA` channel on `wss://stream.ssi.com.vn/ws/v3`).
    /// Must be called inside a Tokio runtime.
    pub fn market_stream(&self, config: MarketStreamConfig) -> MarketStream {
        let (event_tx, events) = mpsc::channel(config.channel_capacity);
        let (commands, command_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(
            supervisor::Supervisor::new(
                self.clone(),
                config,
                event_tx,
                command_rx,
                cancel.clone(),
                Arc::clone(&counters),
            )
            .run(),
        );
        MarketStream {
            events,
            commands,
            cancel,
            task: Some(task),
            counters,
        }
    }
}
