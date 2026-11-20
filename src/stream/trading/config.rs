//! Trading event-stream settings.

use std::time::Duration;

use super::topic::TradingTopic;
use crate::stream::common::{Backoff, DEFAULT_MAX_FRAME_BYTES};

/// Settings of a [`super::TradingStream`].
#[derive(Debug, Clone)]
pub struct TradingStreamConfig {
    pub(crate) topics: Vec<TradingTopic>,
    pub(crate) channel_capacity: usize,
    pub(crate) ping_interval: Option<Duration>,
    pub(crate) idle_timeout: Duration,
    pub(crate) backoff: Backoff,
    pub(crate) connect_timeout: Duration,
    pub(crate) max_frame_bytes: usize,
    pub(crate) emit_heartbeats: bool,
    pub(crate) raw_frames: bool,
}

impl Default for TradingStreamConfig {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            channel_capacity: 4096,
            ping_interval: Some(Duration::from_secs(25)),
            idle_timeout: Duration::from_secs(75),
            backoff: Backoff::default(),
            connect_timeout: Duration::from_secs(15),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            emit_heartbeats: false,
            raw_frames: false,
        }
    }
}

impl TradingStreamConfig {
    /// Defaults with initial `TRADING` topics.
    pub fn new(topics: impl IntoIterator<Item = TradingTopic>) -> Self {
        Self {
            topics: topics.into_iter().collect(),
            ..Self::default()
        }
    }

    /// Event channel capacity (default 4096). A full channel back-pressures the socket reader.
    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    /// Client `PING_PONG` interval (default 25 s; `None` or zero disables; minimum 100 ms).
    #[must_use]
    pub fn with_ping_interval(mut self, interval: Option<Duration>) -> Self {
        self.ping_interval = interval
            .filter(|interval| !interval.is_zero())
            .map(|interval| interval.max(Duration::from_millis(100)));
        self
    }

    /// Reconnect when no frame at all (events, acks, or heartbeats) arrives for this long
    /// (default 75 s; minimum 100 ms).
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout.max(Duration::from_millis(100));
        self
    }

    /// Reconnect backoff parameters.
    #[must_use]
    pub const fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// WebSocket connect timeout (default 15 s).
    #[must_use]
    pub const fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Emit [`super::TradingStreamEvent::Heartbeat`] events (default off).
    #[must_use]
    pub const fn with_heartbeat_events(mut self, emit: bool) -> Self {
        self.emit_heartbeats = emit;
        self
    }

    /// Emit [`super::TradingStreamEvent::RawFrame`] for every received text frame (default off).
    ///
    /// Warning: trading frames contain unmasked account numbers; redact before storing or logging.
    #[must_use]
    pub const fn with_raw_frames(mut self, emit: bool) -> Self {
        self.raw_frames = emit;
        self
    }
}
