//! Market-data stream settings and staleness policy.

use std::{fmt, sync::Arc, time::Duration};

use chrono::{DateTime, Datelike as _, NaiveTime, Timelike as _, Utc, Weekday};

use super::topic::Topic;
use crate::{
    stream::common::{Backoff, DEFAULT_MAX_FRAME_BYTES},
    time::VN_TZ,
};

/// Predicate deciding whether market data is expected at an instant.
pub type ActivityWindow = Arc<dyn Fn(DateTime<Utc>) -> bool + Send + Sync>;

/// When silence on the data topics counts as stale.
#[derive(Clone, Default)]
pub enum StalePolicy {
    /// Never report staleness (default).
    #[default]
    Disabled,
    /// Report after this much silence, at any time of day.
    Always(Duration),
    /// Report after `after` of silence, only while `window` says data is expected.
    During {
        /// Whether data is expected at an instant.
        window: ActivityWindow,
        /// Silence threshold.
        after: Duration,
    },
}

impl fmt::Debug for StalePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Always(after) => f.debug_tuple("Always").field(after).finish(),
            Self::During { after, .. } => f
                .debug_struct("During")
                .field("after", after)
                .finish_non_exhaustive(),
        }
    }
}

impl StalePolicy {
    /// Stale after `after` of silence during VN equity continuous trading: Monday to Friday,
    /// 09:00–11:30 and 13:00–14:45 `Asia/Ho_Chi_Minh`. Exchange holidays are not known here, so
    /// a holiday produces a `Stale` event; pass your own window if that matters.
    pub fn vn_equity_hours(after: Duration) -> Self {
        Self::During {
            window: Arc::new(vn_equity_trading_hours),
            after,
        }
    }

    /// The silence threshold regardless of the activity window.
    pub(crate) const fn after(&self) -> Option<Duration> {
        match self {
            Self::Disabled => None,
            Self::Always(after) | Self::During { after, .. } => Some(*after),
        }
    }

    pub(crate) fn threshold(&self, now: DateTime<Utc>) -> Option<Duration> {
        match self {
            Self::Disabled => None,
            Self::Always(after) => Some(*after),
            Self::During { window, after } => window(now).then_some(*after),
        }
    }
}

/// Whether `now` falls in VN equity continuous trading hours (weekdays, no holiday calendar).
pub fn vn_equity_trading_hours(now: DateTime<Utc>) -> bool {
    let local = now.with_timezone(&VN_TZ);
    if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) {
        return false;
    }
    let time = NaiveTime::from_hms_opt(local.hour(), local.minute(), 0).unwrap_or(NaiveTime::MIN);
    let at = |hour, minute| NaiveTime::from_hms_opt(hour, minute, 0).unwrap_or(NaiveTime::MIN);
    (at(9, 0) <= time && time < at(11, 30)) || (at(13, 0) <= time && time < at(14, 45))
}

/// Settings of a [`super::MarketStream`].
#[derive(Debug, Clone)]
pub struct MarketStreamConfig {
    pub(crate) topics: Vec<Topic>,
    pub(crate) channel_capacity: usize,
    pub(crate) ping_interval: Option<Duration>,
    pub(crate) idle_timeout: Duration,
    pub(crate) stale: StalePolicy,
    pub(crate) backoff: Backoff,
    pub(crate) connect_timeout: Duration,
    pub(crate) max_frame_bytes: usize,
    pub(crate) emit_heartbeats: bool,
    pub(crate) raw_frames: bool,
}

impl Default for MarketStreamConfig {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            channel_capacity: 8192,
            ping_interval: Some(Duration::from_secs(25)),
            idle_timeout: Duration::from_secs(75),
            stale: StalePolicy::Disabled,
            backoff: Backoff::default(),
            connect_timeout: Duration::from_secs(15),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            emit_heartbeats: false,
            raw_frames: false,
        }
    }
}

impl MarketStreamConfig {
    /// Defaults with initial topics.
    pub fn new(topics: impl IntoIterator<Item = Topic>) -> Self {
        Self {
            topics: topics.into_iter().collect(),
            ..Self::default()
        }
    }

    /// Event channel capacity (default 8192). A full channel back-pressures the socket reader.
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

    /// Reconnect when no frame at all arrives for this long (default 75 s; minimum 100 ms).
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout.max(Duration::from_millis(100));
        self
    }

    /// When silence on the data topics is reported as [`super::MarketStreamEvent::Stale`].
    #[must_use]
    pub fn with_stale_policy(mut self, policy: StalePolicy) -> Self {
        self.stale = policy;
        self
    }

    /// Reconnect delays.
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

    /// Emit [`super::MarketStreamEvent::Heartbeat`] events (default off).
    #[must_use]
    pub const fn with_heartbeat_events(mut self, emit: bool) -> Self {
        self.emit_heartbeats = emit;
        self
    }

    /// Emit [`super::MarketStreamEvent::RawFrame`] with every text frame before it is classified
    /// (default off), for capturing the live feed.
    #[must_use]
    pub const fn with_raw_frames(mut self, emit: bool) -> Self {
        self.raw_frames = emit;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("time")
            .with_timezone(&Utc)
    }

    #[test]
    fn trading_hours() {
        // Friday 2026-11-20, 09:30 local = 02:30 UTC.
        assert!(vn_equity_trading_hours(utc("2026-11-20T02:30:00Z")));
        // 12:00 local lunch break.
        assert!(!vn_equity_trading_hours(utc("2026-11-20T05:00:00Z")));
        // Saturday.
        assert!(!vn_equity_trading_hours(utc("2026-11-21T02:30:00Z")));
        let policy = StalePolicy::vn_equity_hours(Duration::from_mins(1));
        assert_eq!(
            policy.threshold(utc("2026-11-20T02:30:00Z")),
            Some(Duration::from_mins(1))
        );
        assert_eq!(policy.threshold(utc("2026-11-20T10:00:00Z")), None);
    }
}
