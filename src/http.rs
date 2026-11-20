//! HTTP transport: host routing, per-endpoint pacing with `X-RATELIMIT-LIMIT` capping,
//! `Accept-Language`, `X-Signature` raw-body delivery, binary downloads, and V3 response
//! classification.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use chrono::{DateTime, Utc};
use reqwest::{
    Method, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Map, Value};
use tokio::time::Instant;
use url::Url;

use crate::{
    config::Config,
    error::{ApiError, ApiErrorCode, Error, RateLimited, Result},
    time::Clock,
};

/// Header carrying the hex-encoded RSA PKCS#1 v1.5 SHA-256 signature of the raw JSON body.
pub(crate) const HEADER_SIGNATURE: HeaderName = HeaderName::from_static("x-signature");

/// Rate-limit headers observed on one response.
///
/// SSI documents `X-RATELIMIT-LIMIT`, `X-RATELIMIT-REMAINING` and `X-RATELIMIT-RESET` but not
/// their units; `reset` keeps the raw header and `reset_at` is the best interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitSnapshot {
    /// Endpoint path of the response.
    pub endpoint: String,
    /// `X-RATELIMIT-LIMIT`.
    pub limit: Option<u64>,
    /// `X-RATELIMIT-REMAINING`.
    pub remaining: Option<u64>,
    /// Raw `X-RATELIMIT-RESET`.
    pub reset: Option<String>,
    /// `X-RATELIMIT-RESET` read as an instant (epoch seconds/milliseconds, seconds from now,
    /// or an RFC 3339 / HTTP date).
    pub reset_at: Option<DateTime<Utc>>,
    /// `Retry-After`, when present.
    pub retry_after: Option<Duration>,
    /// When the response was received.
    pub observed_at: DateTime<Utc>,
}

impl RateLimitSnapshot {
    fn from_headers(endpoint: &str, headers: &HeaderMap, now: DateTime<Utc>) -> Option<Self> {
        let text = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        };
        let limit = text("x-ratelimit-limit");
        let remaining = text("x-ratelimit-remaining");
        let reset = text("x-ratelimit-reset");
        let retry_after = text("retry-after").and_then(|value| parse_retry_after(&value, now));
        if limit.is_none() && remaining.is_none() && reset.is_none() && retry_after.is_none() {
            return None;
        }
        Some(Self {
            endpoint: endpoint.to_owned(),
            limit: limit.as_deref().and_then(leading_u64),
            remaining: remaining.as_deref().and_then(leading_u64),
            reset_at: reset.as_deref().and_then(|value| parse_reset(value, now)),
            reset,
            retry_after,
            observed_at: now,
        })
    }

    /// How long to wait before the next request: `Retry-After`, else time until the reset.
    pub fn advised_wait(&self, now: DateTime<Utc>) -> Option<Duration> {
        self.retry_after.or_else(|| {
            self.reset_at
                .and_then(|reset| (reset - now).to_std().ok())
                .filter(|wait| !wait.is_zero())
        })
    }
}

fn leading_u64(text: &str) -> Option<u64> {
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn parse_retry_after(text: &str, now: DateTime<Utc>) -> Option<Duration> {
    if let Ok(seconds) = text.parse::<f64>() {
        return (seconds.is_finite() && seconds >= 0.0)
            .then(|| Duration::from_secs_f64(seconds.min(86_400.0)));
    }
    DateTime::parse_from_rfc2822(text)
        .ok()
        .and_then(|at| (at.with_timezone(&Utc) - now).to_std().ok())
}

fn parse_reset(text: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if let Ok(number) = text.parse::<i64>() {
        return if number >= 100_000_000_000 {
            DateTime::from_timestamp_millis(number)
        } else if number >= 1_000_000_000 {
            DateTime::from_timestamp(number, 0)
        } else {
            Some(now + chrono::Duration::seconds(number))
        };
    }
    if let Ok(seconds) = text.parse::<f64>() {
        let millis = (seconds * 1000.0).round();
        return (millis.is_finite() && millis.abs() < 1e12)
            .then(|| now + chrono::Duration::milliseconds(float_to_i64(millis)));
    }
    DateTime::parse_from_rfc3339(text)
        .or_else(|_| DateTime::parse_from_rfc2822(text))
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "the caller bounds the value to well inside the i64 range"
)]
const fn float_to_i64(value: f64) -> i64 {
    value as i64
}

/// Whether an endpoint returns private data (account numbers, balances, orders, tokens).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sensitivity {
    #[allow(dead_code, reason = "used by market-data endpoints and unit tests")]
    Public,
    Private,
}

/// Which configured REST base URL serves a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Host {
    /// Auth & market-data host (`Config::api_url`).
    Api,
    /// Trading/account host (`Config::trading_url`, defaulting to `Config::api_url`).
    Trading,
}

impl Host {
    /// Routes `/api/v3/trading/*` and `/api/v3/account/*` to [`Host::Trading`], everything else
    /// to [`Host::Api`].
    pub(crate) fn for_path(path: &str) -> Self {
        if path.starts_with("/api/v3/trading/") || path.starts_with("/api/v3/account/") {
            Self::Trading
        } else {
            Self::Api
        }
    }
}

/// A response before interpretation. No `Debug`: the body can hold tokens or account data.
pub(crate) struct HttpReply {
    pub(crate) status: StatusCode,
    pub(crate) body: Vec<u8>,
    pub(crate) rate: Option<RateLimitSnapshot>,
}

/// Whether a request waits in the endpoint's pacing queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pacing {
    /// Query requests: paced per endpoint.
    Paced,
    /// Auth and mutating order commands: never queue behind read queries.
    Immediate,
}

/// Wire media type expected from an endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseFormat {
    Json,
    #[cfg(feature = "data")]
    Binary,
}

/// Paces requests per endpoint, caps tighter endpoints from `X-RATELIMIT-LIMIT`, and pauses an
/// endpoint after its rate-limit window is used up.
#[derive(Debug)]
struct Limiter {
    interval: Option<Duration>,
    endpoint_intervals: Mutex<HashMap<String, Duration>>,
    next_slot: Mutex<HashMap<String, Instant>>,
}

impl Limiter {
    fn new(config: &Config) -> Self {
        Self {
            interval: config
                .requests_per_second
                .map(|rate| Duration::from_secs(1) / rate.get()),
            endpoint_intervals: Mutex::new(HashMap::new()),
            next_slot: Mutex::new(HashMap::new()),
        }
    }

    fn interval_for(&self, endpoint: &str) -> Duration {
        let Some(base) = self.interval else {
            return Duration::ZERO;
        };
        self.endpoint_intervals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(endpoint)
            .copied()
            .map_or(base, |capped| base.max(capped))
    }

    fn observe_limit(&self, endpoint: &str, limit: u64) {
        let Some(base) = self.interval else {
            return;
        };
        let Ok(limit) = u32::try_from(limit) else {
            return;
        };
        if limit == 0 {
            return;
        }
        let server_interval = Duration::from_secs(1) / limit;
        if server_interval > base {
            self.endpoint_intervals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(endpoint.to_owned(), server_interval);
        }
    }

    async fn acquire(&self, endpoint: &str) {
        let now = Instant::now();
        let step = self.interval_for(endpoint);
        let wake = {
            let mut slots = self
                .next_slot
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let next = slots.entry(endpoint.to_owned()).or_insert(now);
            let slot = (*next).max(now);
            *next = slot + step;
            drop(slots);
            slot
        };
        tokio::time::sleep_until(wake).await;
    }

    fn pause_for(&self, endpoint: &str, wait: Duration) {
        let until = Instant::now() + wait;
        let mut slots = self
            .next_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let next = slots.entry(endpoint.to_owned()).or_insert(until);
        if until > *next {
            *next = until;
        }
        drop(slots);
    }
}

/// Shared HTTP transport.
pub(crate) struct Transport {
    http: reqwest::Client,
    api_base: Url,
    trading_base: Url,
    accept_language: Option<HeaderValue>,
    limiter: Limiter,
    last_rate: Mutex<Option<RateLimitSnapshot>>,
    clock: Arc<dyn Clock>,
    max_pause: Duration,
}

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transport")
            .field("api_base", &self.api_base.as_str())
            .field("trading_base", &self.trading_base.as_str())
            .finish_non_exhaustive()
    }
}

impl Transport {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let accept_language = config
            .accept_language
            .as_deref()
            .map(|value| {
                HeaderValue::from_str(value)
                    .map_err(|_| Error::Config("invalid Accept-Language header value".to_owned()))
            })
            .transpose()?;
        // No redirects: a redirected request would re-post credentials, tokens, signed orders,
        // or Bearer download headers to another origin.
        let http = reqwest::Client::builder()
            .user_agent(config.user_agent.clone())
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .https_only(!config.allow_insecure)
            .build()?;
        Ok(Self {
            http,
            api_base: config.api_url.clone(),
            trading_base: config.trading_url().clone(),
            accept_language,
            limiter: Limiter::new(config),
            last_rate: Mutex::new(None),
            clock: Arc::clone(&config.clock),
            max_pause: config.max_rate_limit_wait,
        })
    }

    pub(crate) fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    /// The absolute URL for an endpoint path on the selected host.
    pub(crate) fn url_for(&self, host: Host, path: &str) -> Result<Url> {
        let base = match host {
            Host::Api => &self.api_base,
            Host::Trading => &self.trading_base,
        };
        base.join(path)
            .map_err(|error| Error::InvalidRequest(format!("bad endpoint path: {error}")))
    }

    pub(crate) fn last_rate_limit(&self) -> Option<RateLimitSnapshot> {
        self.last_rate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Sends one JSON request with an optional raw JSON body and optional `X-Signature` header.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal transport call site; fields are distinct request parts"
    )]
    pub(crate) async fn send(
        &self,
        host: Host,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        raw_body: Option<Vec<u8>>,
        signature_hex: Option<&str>,
        bearer: Option<&SecretString>,
        pacing: Pacing,
    ) -> Result<HttpReply> {
        self.send_inner(
            host,
            method,
            path,
            query,
            raw_body,
            signature_hex,
            bearer,
            pacing,
            ResponseFormat::Json,
        )
        .await
    }

    /// Sends one authenticated binary `GET` (`Accept: application/octet-stream`).
    #[cfg(feature = "data")]
    pub(crate) async fn send_binary(
        &self,
        host: Host,
        path: &str,
        query: &[(&str, String)],
        bearer: &SecretString,
        pacing: Pacing,
    ) -> Result<HttpReply> {
        self.send_inner(
            host,
            Method::GET,
            path,
            query,
            None,
            None,
            Some(bearer),
            pacing,
            ResponseFormat::Binary,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "internal transport helper shared by send() and send_binary()"
    )]
    async fn send_inner(
        &self,
        host: Host,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        raw_body: Option<Vec<u8>>,
        signature_hex: Option<&str>,
        bearer: Option<&SecretString>,
        pacing: Pacing,
        format: ResponseFormat,
    ) -> Result<HttpReply> {
        let url = self.url_for(host, path)?;
        if pacing == Pacing::Paced {
            self.limiter.acquire(path).await;
        }
        let mut request = self.http.request(method.clone(), url);
        request = match format {
            ResponseFormat::Json => request
                .header(reqwest::header::ACCEPT, "application/json")
                .header(reqwest::header::CONTENT_TYPE, "application/json"),
            #[cfg(feature = "data")]
            ResponseFormat::Binary => {
                request.header(reqwest::header::ACCEPT, "application/octet-stream")
            }
        };
        if let Some(language) = &self.accept_language {
            request = request.header(reqwest::header::ACCEPT_LANGUAGE, language.clone());
        }
        if !query.is_empty() {
            request = request.query(query);
        }
        if let Some(body) = raw_body {
            request = request.body(body);
        }
        if let Some(signature) = signature_hex {
            request = request.header(HEADER_SIGNATURE, signature);
        }
        if let Some(token) = bearer {
            request = request.bearer_auth(token.expose_secret());
        }
        let started = Instant::now();
        let response = request.send().await?;
        let status = response.status();
        let now = self.clock.now();
        let rate = RateLimitSnapshot::from_headers(path, response.headers(), now);
        let body = response.bytes().await?.to_vec();
        tracing::debug!(
            method = %method,
            endpoint = path,
            status = status.as_u16(),
            bytes = body.len(),
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            rate_limit = rate.as_ref().and_then(|r| r.limit),
            rate_remaining = rate.as_ref().and_then(|r| r.remaining),
            rate_reset = rate.as_ref().and_then(|r| r.reset.as_deref()),
            "SSI HTTP response"
        );
        if let Some(snapshot) = &rate {
            self.observe_rate(snapshot, now);
        }
        Ok(HttpReply { status, body, rate })
    }

    fn observe_rate(&self, snapshot: &RateLimitSnapshot, now: DateTime<Utc>) {
        *self
            .last_rate
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(snapshot.clone());
        if let Some(limit) = snapshot.limit {
            self.limiter.observe_limit(&snapshot.endpoint, limit);
        }
        if snapshot.remaining == Some(0) {
            let wait = snapshot
                .advised_wait(now)
                .unwrap_or(Duration::from_secs(1))
                .min(self.max_pause);
            tracing::debug!(
                endpoint = %snapshot.endpoint,
                wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                "SSI rate-limit window exhausted; pausing this endpoint"
            );
            self.limiter.pause_for(&snapshot.endpoint, wait);
        }
    }

    /// Pauses one endpoint (after a 429 on a read query).
    pub(crate) fn pause_for(&self, endpoint: &str, wait: Duration) {
        self.limiter.pause_for(endpoint, wait);
    }
}

/// Interprets a binary download reply (`/api/v3/data/download`): returns the raw bytes on 2xx
/// and classifies any non-2xx reply (with optional JSON `{code, msg}`) as an [`Error`].
#[cfg(feature = "data")]
pub(crate) fn interpret_binary(
    endpoint: &str,
    reply: HttpReply,
    now: DateTime<Utc>,
) -> Result<Vec<u8>> {
    if reply.status.is_success() {
        return Ok(reply.body);
    }
    let parsed = serde_json::from_slice::<Value>(&reply.body).ok();
    Err(error_for(
        endpoint,
        reply.status,
        parsed.as_ref().and_then(Value::as_object),
        reply.rate.as_ref(),
        Sensitivity::Public,
        now,
    ))
}

/// Interprets a reply: `Ok(None)` for "no data" (HTTP 204, empty 2xx body, `{"code": 204}`),
/// `Ok(Some(body))` for data (exact JSON bytes so decimals keep every digit), or a typed `Err`
/// (`SmartOtpPending`, `SmartOtpRejected`, `DuplicateRequest`, `RateLimited`, `Api`, `Decode`).
pub(crate) fn interpret(
    endpoint: &str,
    reply: HttpReply,
    sensitivity: Sensitivity,
    now: DateTime<Utc>,
) -> Result<Option<Vec<u8>>> {
    let status = reply.status;
    let rate = reply.rate.as_ref();
    let blank = reply.body.iter().all(u8::is_ascii_whitespace);
    if blank {
        if status.is_success() && status != StatusCode::ACCEPTED {
            return Ok(None);
        }
        return Err(error_for(endpoint, status, None, rate, sensitivity, now));
    }
    let value: Value = match serde_json::from_slice(&reply.body) {
        Ok(value) => value,
        Err(error) if status.is_success() => {
            return Err(Error::Decode {
                endpoint: endpoint.to_owned(),
                detail: match sensitivity {
                    Sensitivity::Public => error.to_string(),
                    Sensitivity::Private => "body is not valid JSON".to_owned(),
                },
            });
        }
        Err(_) => return Err(error_for(endpoint, status, None, rate, sensitivity, now)),
    };
    if let Value::Object(object) = &value
        && let Some(code) = body_code(object)
    {
        if code == 204 && status.is_success() {
            return Ok(None);
        }
        let is_error_envelope = code != 200
            && (object.contains_key("msg")
                || object.contains_key("message")
                || status == StatusCode::ACCEPTED
                || !status.is_success()
                || ApiErrorCode::from_code(code).is_smart_otp_pending()
                || ApiErrorCode::from_code(code).is_smart_otp_terminal());
        if is_error_envelope {
            return Err(error_for(
                endpoint,
                status,
                Some(object),
                rate,
                sensitivity,
                now,
            ));
        }
    }
    if status == StatusCode::ACCEPTED || !status.is_success() {
        return Err(error_for(
            endpoint,
            status,
            value.as_object(),
            rate,
            sensitivity,
            now,
        ));
    }
    Ok(Some(reply.body))
}

fn body_code(object: &Map<String, Value>) -> Option<i64> {
    match object.get("code")? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn body_message(object: &Map<String, Value>) -> String {
    ["msg", "message"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .to_owned()
}

fn error_for(
    endpoint: &str,
    status: StatusCode,
    object: Option<&Map<String, Value>>,
    rate: Option<&RateLimitSnapshot>,
    sensitivity: Sensitivity,
    now: DateTime<Utc>,
) -> Error {
    let code = object
        .and_then(body_code)
        .unwrap_or_else(|| i64::from(status.as_u16()));
    let typed = ApiErrorCode::from_code(code);
    let message = object.map_or_default(body_message);

    let api_error = ApiError {
        http_status: status.as_u16(),
        code: typed,
        message: message.clone(),
        endpoint: endpoint.to_owned(),
        private: sensitivity == Sensitivity::Private,
    };

    if typed.is_smart_otp_pending() || status == StatusCode::ACCEPTED {
        return Error::SmartOtpPending(ApiError {
            code: if typed.is_smart_otp_pending() {
                typed
            } else {
                ApiErrorCode::PushApprovalPending
            },
            ..api_error
        });
    }
    if typed.is_smart_otp_terminal() {
        return Error::SmartOtpRejected(api_error);
    }
    if typed.is_duplicate()
        || (status == StatusCode::CONFLICT
            && !matches!(
                typed,
                ApiErrorCode::InsufficientBuyingPower
                    | ApiErrorCode::InsufficientAccountBalance
                    | ApiErrorCode::ResourceAlreadyExists
            ))
    {
        return Error::DuplicateRequest(api_error);
    }

    let throttled = status == StatusCode::TOO_MANY_REQUESTS || code == 429_000 || code == 429;
    let business_limit = (429_100..=429_199).contains(&code);
    if throttled && !business_limit {
        return Error::RateLimited(Box::new(RateLimited {
            endpoint: endpoint.to_owned(),
            code: object.and_then(body_code).map(ApiErrorCode::from_code),
            retry_after: rate.and_then(|snapshot| snapshot.advised_wait(now)),
            snapshot: rate.cloned(),
            message,
        }));
    }

    Error::Api(api_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_780_000_000, 0).expect("valid")
    }

    fn reply(status: u16, body: &str) -> HttpReply {
        HttpReply {
            status: StatusCode::from_u16(status).expect("valid"),
            body: body.as_bytes().to_vec(),
            rate: None,
        }
    }

    #[test]
    fn host_routing_splits_data_auth_and_trading() {
        assert_eq!(Host::for_path("/api/v3/auth/token"), Host::Api);
        assert_eq!(Host::for_path("/api/v3/data/ohlc"), Host::Api);
        assert_eq!(Host::for_path("/api/v3/trading/order"), Host::Trading);
        assert_eq!(Host::for_path("/api/v3/account/info"), Host::Trading);
    }

    #[test]
    fn classifies_smart_otp_duplicates_and_throttling() {
        let priv_s = Sensitivity::Private;
        let pending = interpret(
            "/api/v3/auth/token",
            reply(202, r#"{"code":401114,"msg":"Push approval is pending"}"#),
            priv_s,
            now(),
        )
        .expect_err("pending");
        assert!(matches!(pending, Error::SmartOtpPending(_)));

        let dup = interpret(
            "/api/v3/trading/order",
            reply(409, r#"{"code":409100,"msg":"Duplicate clientRequestId"}"#),
            priv_s,
            now(),
        )
        .expect_err("dup");
        assert!(matches!(dup, Error::DuplicateRequest(_)));
        assert_eq!(dup.api_code(), Some(ApiErrorCode::DuplicateRequest));

        let throttled = interpret(
            "/api/v3/data/ohlc",
            reply(429, r#"{"code":429000,"msg":"slow"}"#),
            Sensitivity::Public,
            now(),
        )
        .expect_err("throttled");
        assert!(matches!(throttled, Error::RateLimited(_)));

        let business = interpret(
            "/api/v3/trading/order",
            reply(429, r#"{"code":429100,"msg":"daily"}"#),
            priv_s,
            now(),
        )
        .expect_err("business");
        assert!(matches!(business, Error::Api(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn server_limit_caps_only_the_tighter_endpoint() {
        let config = Config::new(crate::Credentials::new("k", "s").expect("valid"))
            .with_requests_per_second(std::num::NonZeroU32::new(10));
        let limiter = Limiter::new(&config);
        limiter.observe_limit("/api/v3/data/corporateActions", 1);
        limiter.observe_limit("/api/v3/data/ohlc", 20);
        let start = Instant::now();
        limiter.acquire("/api/v3/data/corporateActions").await;
        limiter.acquire("/api/v3/data/ohlc").await;
        limiter.acquire("/api/v3/data/ohlc").await;
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        limiter.acquire("/api/v3/data/corporateActions").await;
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }
}
