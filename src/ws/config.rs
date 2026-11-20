//! Configuration for the explicit opt-in Trading WebSocket (`/ws/v3/trading`).

use std::time::Duration;

/// Signature encoding used for mutating Trading WebSocket commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SignatureEncoding {
    /// Start with standard base64 (as documented); if SSI rejects the signature before execution
    /// (`401006`, `401008`, `401013`), retry once with lowercase hex and remember whichever
    /// encoding the gateway accepts.
    #[default]
    Auto,
    /// Always encode the RSA-SHA256 signature as standard base64.
    Base64,
    /// Always encode the RSA-SHA256 signature as lowercase hex.
    Hex,
}

/// Settings for [`super::TradingWsClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingWsConfig {
    /// Signature encoding strategy (default [`SignatureEncoding::Auto`]).
    pub signature_encoding: SignatureEncoding,
    /// Per-command response timeout (default 10 s). Timed-out commands are **never** retried.
    pub request_timeout: Duration,
    /// Post-upgrade grace window to catch `{"code":401|403|429|500,...}` refusal frames after
    /// HTTP 101 (default 500 ms; `Duration::ZERO` skips waiting).
    pub handshake_grace: Duration,
    /// Wait up to `max_rate_limit_wait` when the last response reported `remaining == 0`
    /// (default `true`).
    pub respect_rate_limit: bool,
    /// Cap on rate-limit pacing waits (default 2 s).
    pub max_rate_limit_wait: Duration,
    /// Periodic `PING_PONG` interval (default `Some(30 s)`; `None` disables).
    pub ping_interval: Option<Duration>,
    /// Bounded channel capacity for unsolicited events and acks (default 1024).
    pub event_channel_capacity: usize,
}

impl Default for TradingWsConfig {
    fn default() -> Self {
        Self {
            signature_encoding: SignatureEncoding::Auto,
            request_timeout: Duration::from_secs(10),
            handshake_grace: Duration::from_millis(500),
            respect_rate_limit: true,
            max_rate_limit_wait: Duration::from_secs(2),
            ping_interval: Some(Duration::from_secs(30)),
            event_channel_capacity: 1024,
        }
    }
}

impl TradingWsConfig {
    /// Sets the signature encoding mode.
    #[must_use]
    pub const fn with_signature_encoding(mut self, encoding: SignatureEncoding) -> Self {
        self.signature_encoding = encoding;
        self
    }

    /// Sets the per-command response timeout (clamped to >= 50 ms).
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout.max(Duration::from_millis(50));
        self
    }

    /// Sets the post-upgrade refusal grace period.
    #[must_use]
    pub const fn with_handshake_grace(mut self, grace: Duration) -> Self {
        self.handshake_grace = grace;
        self
    }

    /// Enables or disables honoring server-reported `rateLimit.resetMs` when `remaining == 0`.
    #[must_use]
    pub const fn with_respect_rate_limit(mut self, respect: bool, max_wait: Duration) -> Self {
        self.respect_rate_limit = respect;
        self.max_rate_limit_wait = max_wait;
        self
    }

    /// Sets the periodic `PING_PONG` interval (`None` disables).
    #[must_use]
    pub fn with_ping_interval(mut self, interval: Option<Duration>) -> Self {
        self.ping_interval = interval
            .filter(|d| !d.is_zero())
            .map(|d| d.max(Duration::from_millis(100)));
        self
    }

    /// Sets the bounded capacity of the unsolicited event channel.
    #[must_use]
    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity.max(1);
        self
    }
}
