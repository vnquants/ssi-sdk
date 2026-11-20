//! Supervised real-time trading event stream (`wss://stream.ssi.com.vn/ws/v3`, `TRADING`
//! channel).
//!
//! [`SsiClient::trading_stream`] spawns a supervisor task and returns an owned, `'static`
//! [`TradingStream`] handle. The supervisor:
//!
//! - connects with an OTP-authenticated trading access token (`Authorization: Bearer`),
//!   subscribes to all configured `TRADING` topics, and emits [`TradingStreamEvent::Connected`]
//!   (and [`TradingStreamEvent::Resubscribed`] after a reconnect);
//! - reconnects with exponential backoff and jitter, emitting
//!   [`TradingStreamEvent::Disconnected`];
//! - pings (`PING_PONG` on `HEARTBEAT`) every 25 s, answers server `PING`, and treats 75 s
//!   without any frame as a dead connection;
//! - invalidates the token after `401` / `403` / WebSocket close `1008` so the next reconnect
//!   uses a refreshed access token (or reports [`DisconnectReason::ReauthenticationRequired`] if
//!   the refresh chain has expired and a new OTP is needed).
//!
//! **No replay:** SSI does not replay missed order or fill events across disconnects. On every
//! [`TradingStreamEvent::Resubscribed`], reconcile open orders and positions via REST
//! ([`SsiClient::order_book`], [`SsiClient::positions`]).

mod config;
pub mod message;
pub(crate) mod protocol;
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
    config::TradingStreamConfig,
    message::{FcoEvent, OrderEvent, OrderMatchEvent, PortfolioEvent, TradingEventData},
    topic::{TradingTopic, TradingTopicKind},
};
use super::common::Counters;
pub use super::common::{Backoff, DisconnectReason, HeartbeatKind, StreamStats, SubscriptionAck};
use crate::{
    SsiClient,
    error::{Error, Result},
};

/// One trading event with its masked topic and local receive time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingEvent {
    /// Topic with the account number masked (`order.*******451`).
    pub topic: String,
    /// Typed event payload.
    pub data: TradingEventData,
    /// Local time the frame was read.
    pub received_at: DateTime<Utc>,
}

/// Events produced by [`TradingStream`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TradingStreamEvent {
    /// Socket is open and subscriptions were sent.
    Connected {
        /// 1-based connection counter.
        connection: u64,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Subscriptions were re-sent after a reconnect. Reconcile via REST (`order_book` /
    /// `positions`) because SSI does not replay missed events.
    Resubscribed {
        /// Connection counter.
        connection: u64,
        /// Number of topics re-sent.
        topics: usize,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Socket ended or could not be opened; reconnect follows after `retry_in`.
    Disconnected {
        /// Why.
        reason: DisconnectReason,
        /// Delay before the next attempt.
        retry_in: Duration,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// One real-time trading event (`orderEvent`, `orderMatchEvent`, `clientPortfolioEvent`,
    /// `fcoEvent`).
    Event(Box<TradingEvent>),
    /// Server reply to `SUBSCRIBE` / `UNSUBSCRIBE`.
    Ack(SubscriptionAck),
    /// Server reply to [`TradingStream::request_subscriptions`].
    Subscriptions {
        /// `DATA`-channel topics.
        data: Vec<String>,
        /// `TRADING`-channel topics (masked).
        trading: Vec<String>,
    },
    /// Heartbeat traffic (only when [`TradingStreamConfig::with_heartbeat_events`] is enabled).
    Heartbeat {
        /// Kind.
        kind: HeartbeatKind,
        /// Local time.
        at: DateTime<Utc>,
    },
    /// Every raw text frame (only when [`TradingStreamConfig::with_raw_frames`] is enabled).
    /// Contains unmasked account data — redact before storing.
    RawFrame {
        /// Raw frame text.
        text: String,
        /// Local receive time.
        received_at: DateTime<Utc>,
    },
}

#[derive(Debug)]
pub(crate) enum Command {
    Subscribe(Vec<TradingTopic>),
    Unsubscribe(Vec<TradingTopic>),
    UnsubscribeAll,
    ListSubscriptions,
}

/// Owned handle of a supervised `TRADING` event stream.
///
/// Dropping the handle cancels and stops the supervisor. Use [`TradingStream::recv`] or the
/// [`futures_util::Stream`] implementation to consume events.
#[derive(Debug)]
pub struct TradingStream {
    events: mpsc::Receiver<TradingStreamEvent>,
    commands: mpsc::UnboundedSender<Command>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    counters: Arc<Counters>,
}

impl TradingStream {
    /// Adds topics; sent immediately if connected and re-sent after every reconnect.
    pub fn subscribe(&self, topics: impl IntoIterator<Item = TradingTopic>) -> Result<()> {
        self.command(Command::Subscribe(topics.into_iter().collect()))
    }

    /// Removes topics.
    pub fn unsubscribe(&self, topics: impl IntoIterator<Item = TradingTopic>) -> Result<()> {
        self.command(Command::Unsubscribe(topics.into_iter().collect()))
    }

    /// Removes every currently tracked topic (sends `UNSUBSCRIBE` immediately when connected and
    /// clears the list re-sent after reconnects).
    pub fn unsubscribe_all(&self) -> Result<()> {
        self.command(Command::UnsubscribeAll)
    }

    /// Asks SSI for the active subscription list; reply arrives as
    /// [`TradingStreamEvent::Subscriptions`].
    pub fn request_subscriptions(&self) -> Result<()> {
        self.command(Command::ListSubscriptions)
    }

    fn command(&self, command: Command) -> Result<()> {
        self.commands.send(command).map_err(|_| Error::StreamClosed)
    }

    /// Next event; `None` once the supervisor has stopped.
    pub async fn recv(&mut self) -> Option<TradingStreamEvent> {
        self.events.recv().await
    }

    /// Counters: connections, frames, messages, unknown messages, and consumer stalls.
    pub fn stats(&self) -> StreamStats {
        self.counters.snapshot()
    }

    /// Whether the supervisor task has ended.
    pub fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    /// Cancellation token that stops the supervisor when cancelled.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Stops the supervisor (closing the socket) and awaits task exit.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "trading stream supervisor ended abnormally");
        }
    }
}

impl Drop for TradingStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl futures_util::Stream for TradingStream {
    type Item = TradingStreamEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_recv(cx)
    }
}

impl SsiClient {
    /// Starts a supervised `TRADING` event stream (`wss://stream.ssi.com.vn/ws/v3`). Requires an
    /// OTP / Smart OTP trading session and must be called inside a Tokio runtime.
    pub fn trading_stream(&self, config: TradingStreamConfig) -> TradingStream {
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
        TradingStream {
            events,
            commands,
            cancel,
            task: Some(task),
            counters,
        }
    }
}
