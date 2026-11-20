//! Shared stream types, counters, reconnect backoff, and wire-frame helpers.

#[cfg(any(feature = "data", feature = "trading"))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

#[cfg(any(feature = "data", feature = "trading"))]
use serde_json::{Map, Value, json};

#[cfg(any(feature = "data", feature = "trading"))]
use crate::secret::mask_topic;

/// SSI keeps frames under 4096 bytes; subscribe/unsubscribe requests are split below this cap.
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 4000;

/// Target under which `tungstenite` logs the WebSocket upgrade request (`Authorization` included).
#[cfg(any(feature = "data", feature = "trading"))]
const HANDSHAKE_TARGET: &str = "tungstenite::handshake::client";

/// A consumer stall at or above this threshold is recorded in [`StreamStats`].
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) const STALL_THRESHOLD: Duration = Duration::from_millis(100);

/// Reconnect delays: exponential with jitter, no attempt limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// First retry delay (default 500 ms).
    pub initial: Duration,
    /// Cap of the exponential delay (default 30 s).
    pub max: Duration,
    /// Minimum wait after SSI refuses the connection with `429` (default 30 s; SSI allows at most
    /// 10 sockets per API key).
    pub rate_limited: Duration,
    /// Minimum wait when re-authentication with a new OTP is required (default 15 s).
    pub reauth_wait: Duration,
    /// A connection that lasted this long resets the backoff (default 30 s).
    pub stable_after: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            rate_limited: Duration::from_secs(30),
            reauth_wait: Duration::from_secs(15),
            stable_after: Duration::from_secs(30),
        }
    }
}

impl Backoff {
    /// Delay before retry number `attempt` (0-based): `initial * 2^attempt`, capped at `max`,
    /// then scaled by a random factor in `[0.5, 1.0]`.
    pub fn delay(&self, attempt: u32) -> Duration {
        let exponential = self
            .initial
            .saturating_mul(2_u32.saturating_pow(attempt.min(20)))
            .min(self.max);
        exponential.mul_f64(fastrand::f64().mul_add(0.5, 0.5))
    }
}

/// Why a stream connection ended or could not be opened.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// No token could be obtained (not authenticated or transient REST auth error).
    TokenUnavailable(String),
    /// A sticky OTP trading session's refresh token expired or was rejected; re-authenticate with
    /// a new OTP.
    ReauthenticationRequired(String),
    /// The WebSocket handshake failed (`status` is the HTTP status if there was one).
    ConnectFailed {
        /// HTTP status of a refused upgrade.
        status: Option<u16>,
        /// Error text.
        message: String,
    },
    /// SSI accepted the socket (HTTP 101) and then sent `{"code":401|403|429|500,...}`.
    Rejected {
        /// Numeric error code (3-digit HTTP or 6-digit V3 `ErrCode`).
        code: i64,
        /// Server text (with trading topics masked).
        message: String,
    },
    /// SSI closed the socket.
    Closed {
        /// Close code.
        code: Option<u16>,
        /// Close reason.
        reason: String,
    },
    /// Socket read or write failed.
    Transport(String),
    /// No frame arrived within the configured idle timeout.
    IdleTimeout(Duration),
}

/// Heartbeat traffic kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatKind {
    /// Unsolicited WebSocket pong control frame from SSI (sent roughly every 30 s).
    TransportPong,
    /// WebSocket ping control frame from SSI (answered automatically).
    TransportPing,
    /// JSON `PING` from SSI (answered with `PONG`).
    ServerPing,
    /// Reply to the client's `PING_PONG`.
    PingAck,
}

/// Reply to `SUBSCRIBE` / `UNSUBSCRIBE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionAck {
    /// Method as echoed (`SUBSCRIBE`, `subscribe`, `UNSUBSCRIBE`, ...).
    pub method: String,
    /// Channel (`DATA` or `TRADING`).
    pub channel: String,
    /// `ok` or `error`.
    pub status: String,
    /// Server text (with any account numbers in trading topic names masked).
    pub message: String,
    /// Topics the server called invalid (masked).
    pub invalid_topics: Vec<String>,
}

impl SubscriptionAck {
    /// Whether the server reported success and no invalid topic.
    pub fn is_clean(&self) -> bool {
        self.status.eq_ignore_ascii_case("ok") && self.invalid_topics.is_empty()
    }
}

/// Snapshot of live counters returned by a stream's `stats()` method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamStats {
    /// Sockets opened so far.
    pub connections: u64,
    /// Text frames read.
    pub frames: u64,
    /// Domain messages emitted (typed or `Unknown`).
    pub messages: u64,
    /// Domain messages that did not match a documented schema and were kept as `Unknown`.
    pub unknown_messages: u64,
    /// Text frames matching no known envelope shape and ignored.
    pub unrecognised_frames: u64,
    /// Times the consumer stalled event delivery for at least 100 ms.
    pub consumer_stalls: u64,
    /// Cumulative consumer stall duration.
    pub consumer_stalled: Duration,
    /// Longest single stall.
    pub longest_consumer_stall: Duration,
}

/// Live atomic counters shared between a stream supervisor and its handle.
#[cfg(any(feature = "data", feature = "trading"))]
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) connections: AtomicU64,
    pub(crate) frames: AtomicU64,
    pub(crate) messages: AtomicU64,
    pub(crate) unknown_messages: AtomicU64,
    pub(crate) unrecognised_frames: AtomicU64,
    stalls: AtomicU64,
    stalled_ms: AtomicU64,
    max_stall_ms: AtomicU64,
}

#[cfg(any(feature = "data", feature = "trading"))]
impl Counters {
    pub(crate) fn record_stall(&self, stalled: Duration) {
        let millis = millis(stalled);
        self.stalls.fetch_add(1, Ordering::Relaxed);
        self.stalled_ms.fetch_add(millis, Ordering::Relaxed);
        self.max_stall_ms.fetch_max(millis, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> StreamStats {
        StreamStats {
            connections: self.connections.load(Ordering::Relaxed),
            frames: self.frames.load(Ordering::Relaxed),
            messages: self.messages.load(Ordering::Relaxed),
            unknown_messages: self.unknown_messages.load(Ordering::Relaxed),
            unrecognised_frames: self.unrecognised_frames.load(Ordering::Relaxed),
            consumer_stalls: self.stalls.load(Ordering::Relaxed),
            consumer_stalled: Duration::from_millis(self.stalled_ms.load(Ordering::Relaxed)),
            longest_consumer_stall: Duration::from_millis(
                self.max_stall_ms.load(Ordering::Relaxed),
            ),
        }
    }
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Warns once per process when `tungstenite` TRACE logging would print `Authorization: Bearer`.
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn warn_if_handshake_is_traced() {
    static WARNED: AtomicBool = AtomicBool::new(false);
    let traced = tracing::enabled!(target: HANDSHAKE_TARGET, tracing::Level::TRACE)
        || log::log_enabled!(target: HANDSHAKE_TARGET, log::Level::Trace);
    if traced && !WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            "TRACE logging is enabled for `tungstenite`, which logs the WebSocket upgrade request \
             including the `Authorization: Bearer` token; add `tungstenite=debug` to the log filter"
        );
    }
}

/// A value-free description of a JSON frame for logs and diagnostics.
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn shape_of(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
            keys.sort_unstable();
            let keys: Vec<String> = keys
                .into_iter()
                .map(|key| key.chars().take(32).collect())
                .collect();
            format!("object{{{}}}", keys.join(","))
        }
        Value::Array(_) => "array".to_owned(),
        Value::String(_) => "string".to_owned(),
        Value::Number(_) => "number".to_owned(),
        Value::Bool(_) => "bool".to_owned(),
        Value::Null => "null".to_owned(),
    }
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn text<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn code(object: &Map<String, Value>) -> Option<i64> {
    match object.get("code")? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn retry_after(object: &Map<String, Value>) -> Option<Duration> {
    let raw = object
        .get("retryAfter")
        .or_else(|| object.get("retry_after"))?;
    let seconds = match raw {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    (seconds.is_finite() && seconds > 0.0).then(|| Duration::from_secs_f64(seconds.min(86_400.0)))
}

/// Normalizes a 6-digit V3 `ErrCode` (e.g. `401004`, `429000`) to its 3-digit HTTP status
/// (`401`, `429`). Three-digit codes are returned unchanged.
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) const fn rejection_http_status(code: i64) -> i64 {
    if code >= 100_000 { code / 1000 } else { code }
}

/// Whether a `{code, msg}` rejection frame terminates the stream connection.
#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) const fn is_fatal_rejection(code: i64) -> bool {
    matches!(rejection_http_status(code), 401 | 403 | 429 | 500..=599)
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn split_topics(text: &str) -> Vec<String> {
    text.split(';')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .map(mask_topic)
        .collect()
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn invalid_topics(message: &str) -> Vec<String> {
    let lower = message.to_ascii_lowercase();
    lower.find("invalid:").map_or_else(Vec::new, |index| {
        message
            .get(index + "invalid:".len()..)
            .unwrap_or_default()
            .split([',', ';', ' '])
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .map(mask_topic)
            .collect()
    })
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn ping_frame() -> String {
    json!({ "method": "PING_PONG", "channel": "HEARTBEAT" }).to_string()
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn pong_frame(time: Option<&str>) -> String {
    let mut frame = json!({ "method": "PONG", "channel": "HEARTBEAT" });
    if let (Some(time), Some(object)) = (time, frame.as_object_mut()) {
        object.insert("time".to_owned(), Value::String(time.to_owned()));
    }
    frame.to_string()
}

#[cfg(any(feature = "data", feature = "trading"))]
pub(crate) fn list_frame() -> String {
    json!({ "method": "LIST_SUBSCRIPTION" }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let backoff = Backoff::default();
        for attempt in 0..30 {
            let delay = backoff.delay(attempt);
            assert!(delay <= backoff.max);
            assert!(delay >= backoff.initial / 2);
        }
        assert!(backoff.delay(10) >= Duration::from_secs(15));
    }
}
