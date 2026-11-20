//! Client configuration and credentials.

use std::{env, fmt, num::NonZeroU32, sync::Arc, time::Duration};

use secrecy::{ExposeSecret as _, SecretString};
use url::Url;

use crate::{
    error::{Error, Result},
    secret::Masked,
    time::{Clock, SystemClock},
};

/// Production REST base URL for auth (`/api/v3/auth/*`), market data (`/api/v3/data/*`) and
/// default trading REST host.
pub const DEFAULT_API_URL: &str = "https://api.ssi.com.vn";
/// Production streaming WebSocket URL (`DATA` and `TRADING` channels).
pub const DEFAULT_STREAM_URL: &str = "wss://stream.ssi.com.vn/ws/v3";
/// Production Trading WebSocket RPC URL (`/ws/v3/trading`).
#[cfg(feature = "trading")]
pub const DEFAULT_TRADING_WS_URL: &str = "wss://api.ssi.com.vn/ws/v3/trading";
/// Default upper bound on orders in one batch request (matches the official SDK default of 20;
/// SSI's gateway allows up to 50).
#[cfg(feature = "trading")]
pub const DEFAULT_MAX_BATCH_ORDERS: usize = 20;
/// Hard ceiling accepted by SSI for a single batch request.
#[cfg(feature = "trading")]
pub const HARD_MAX_BATCH_ORDERS: usize = 50;

/// Environment variable holding the API key.
pub const API_KEY_ENV: &str = "SSI_API_KEY";
/// Environment variable holding the API secret.
pub const API_SECRET_ENV: &str = "SSI_API_SECRET";
/// Environment variable holding the RSA private key (base64 XML or PEM). Optional for reads.
#[cfg(feature = "trading")]
pub const PRIVATE_KEY_ENV: &str = "SSI_PRIVATE_KEY";
/// Optional environment variable holding the SSI client id.
pub const CLIENT_ID_ENV: &str = "SSI_CLIENT_ID";
/// Optional environment variable overriding [`DEFAULT_API_URL`].
pub const API_URL_ENV: &str = "SSI_V3_API_URL";
/// Optional environment variable overriding the trading/account REST host
/// (`/api/v3/trading/*`, `/api/v3/account/*`). Defaults to the API URL.
pub const TRADING_URL_ENV: &str = "SSI_V3_TRADING_URL";
/// Optional environment variable overriding [`DEFAULT_STREAM_URL`].
pub const STREAM_URL_ENV: &str = "SSI_V3_STREAM_URL";
/// Optional environment variable overriding [`DEFAULT_TRADING_WS_URL`].
#[cfg(feature = "trading")]
pub const TRADING_WS_URL_ENV: &str = "SSI_V3_TRADING_WS_URL";

/// API key pair (and optional RSA signing key when `feature = "trading"` is enabled) issued on
/// the SSI developer portal.
///
/// Secrets are held in [`SecretString`] and masked in `Debug`.
#[derive(Clone)]
pub struct Credentials {
    api_key: SecretString,
    api_secret: SecretString,
    #[cfg(feature = "trading")]
    private_key: Option<SecretString>,
    client_id: Option<String>,
}

impl Credentials {
    /// Builds credentials without a signing key (suitable for market data and read-only queries).
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Result<Self> {
        let api_key = api_key.into().trim().to_owned();
        let api_secret = api_secret.into().trim().to_owned();
        if api_key.is_empty() {
            return Err(Error::Config(format!("{API_KEY_ENV} is empty")));
        }
        if api_secret.is_empty() {
            return Err(Error::Config(format!("{API_SECRET_ENV} is empty")));
        }
        Ok(Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            #[cfg(feature = "trading")]
            private_key: None,
            client_id: None,
        })
    }

    /// Attaches the RSA private key (base64-encoded XML `<RSAKeyValue>` or PEM PKCS#1/PKCS#8)
    /// used to sign mutating order and FCO requests.
    #[cfg(feature = "trading")]
    pub fn with_private_key(mut self, private_key: impl Into<String>) -> Result<Self> {
        let trimmed = private_key.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(Error::InvalidPrivateKey("private key is empty".to_owned()));
        }
        self.private_key = Some(trimmed.into());
        Ok(self)
    }

    /// Attaches an optional SSI client id (sent on balance and position queries when set).
    #[must_use]
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        let trimmed = client_id.into().trim().to_owned();
        self.client_id = (!trimmed.is_empty()).then_some(trimmed);
        self
    }

    /// Reads `SSI_API_KEY`, `SSI_API_SECRET`, optional `SSI_CLIENT_ID`, and (with
    /// `feature = "trading"`) optional `SSI_PRIVATE_KEY` from the process environment.
    pub fn from_env() -> Result<Self> {
        let mut creds = Self::new(required_env(API_KEY_ENV)?, required_env(API_SECRET_ENV)?)?;
        #[cfg(feature = "trading")]
        if let Some(key) = optional_env(PRIVATE_KEY_ENV)? {
            creds = creds.with_private_key(key)?;
        }
        if let Some(client_id) = optional_env(CLIENT_ID_ENV)? {
            creds = creds.with_client_id(client_id);
        }
        Ok(creds)
    }

    pub(crate) const fn api_key(&self) -> &SecretString {
        &self.api_key
    }

    pub(crate) const fn api_secret(&self) -> &SecretString {
        &self.api_secret
    }

    #[cfg(feature = "trading")]
    pub(crate) const fn private_key(&self) -> Option<&SecretString> {
        self.private_key.as_ref()
    }

    /// Optional client id, if configured.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Credentials");
        debug
            .field("api_key", &Masked::new(self.api_key.expose_secret()))
            .field("api_secret", &Masked::new(self.api_secret.expose_secret()));
        #[cfg(feature = "trading")]
        debug.field(
            "private_key",
            &self
                .private_key
                .as_ref()
                .map(|key| Masked::new(key.expose_secret())),
        );
        debug
            .field("client_id", &self.client_id.as_deref().map(Masked::new))
            .finish()
    }
}

/// Settings for [`crate::SsiClient`].
#[derive(Clone)]
pub struct Config {
    pub(crate) credentials: Credentials,
    pub(crate) api_url: Url,
    pub(crate) trading_url: Option<Url>,
    pub(crate) stream_url: Url,
    #[cfg(feature = "trading")]
    pub(crate) trading_ws_url: Url,
    pub(crate) request_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) user_agent: String,
    pub(crate) accept_language: Option<String>,
    #[cfg(feature = "trading")]
    pub(crate) explicit_user_agent: bool,
    #[cfg(feature = "trading")]
    pub(crate) device_id: Option<String>,
    pub(crate) refresh_lead: Duration,
    #[cfg(feature = "trading")]
    pub(crate) otp_poll_interval: Duration,
    #[cfg(feature = "trading")]
    pub(crate) otp_poll_max_wait: Duration,
    #[cfg(feature = "trading")]
    pub(crate) max_batch_orders: usize,
    pub(crate) requests_per_second: Option<NonZeroU32>,
    pub(crate) max_rate_limit_retries: u32,
    pub(crate) max_rate_limit_wait: Duration,
    pub(crate) max_transient_retries: u32,
    pub(crate) allow_insecure: bool,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Config {
    /// Production defaults for the given credentials.
    pub fn new(credentials: Credentials) -> Self {
        Self {
            credentials,
            api_url: default_url(DEFAULT_API_URL),
            trading_url: None,
            stream_url: default_url(DEFAULT_STREAM_URL),
            #[cfg(feature = "trading")]
            trading_ws_url: default_url(DEFAULT_TRADING_WS_URL),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            user_agent: concat!("ssi-sdk/", env!("CARGO_PKG_VERSION")).to_owned(),
            accept_language: None,
            #[cfg(feature = "trading")]
            explicit_user_agent: false,
            #[cfg(feature = "trading")]
            device_id: None,
            refresh_lead: Duration::from_mins(5),
            #[cfg(feature = "trading")]
            otp_poll_interval: Duration::from_secs(5),
            #[cfg(feature = "trading")]
            otp_poll_max_wait: Duration::from_mins(1),
            #[cfg(feature = "trading")]
            max_batch_orders: DEFAULT_MAX_BATCH_ORDERS,
            requests_per_second: NonZeroU32::new(10),
            max_rate_limit_retries: 3,
            max_rate_limit_wait: Duration::from_mins(1),
            max_transient_retries: 2,
            allow_insecure: false,
            clock: Arc::new(SystemClock),
        }
    }

    /// Reads credentials and optional endpoint overrides from the process environment.
    pub fn from_env() -> Result<Self> {
        let mut config = Self::new(Credentials::from_env()?);
        if let Some(url) = optional_env(API_URL_ENV)? {
            config = config.with_api_url(&url)?;
        }
        if let Some(url) = optional_env(TRADING_URL_ENV)? {
            config = config.with_trading_url(&url)?;
        }
        if let Some(url) = optional_env(STREAM_URL_ENV)? {
            config = config.with_stream_url(&url)?;
        }
        #[cfg(feature = "trading")]
        if let Some(url) = optional_env(TRADING_WS_URL_ENV)? {
            config = config.with_trading_ws_url(&url)?;
        }
        Ok(config)
    }

    /// Overrides the REST base URL (`https` only unless [`Self::allow_insecure`]).
    pub fn with_api_url(mut self, url: &str) -> Result<Self> {
        self.api_url = parse_url(url, &["https", "http"])?;
        self.check_schemes()?;
        Ok(self)
    }

    /// Overrides the dedicated trading/account REST host (`/api/v3/trading/*`,
    /// `/api/v3/account/*`). When unset, those requests use [`Self::api_url`].
    pub fn with_trading_url(mut self, url: &str) -> Result<Self> {
        self.trading_url = Some(parse_url(url, &["https", "http"])?);
        self.check_schemes()?;
        Ok(self)
    }

    /// Overrides the streaming WebSocket URL (`wss` only unless [`Self::allow_insecure`]).
    pub fn with_stream_url(mut self, url: &str) -> Result<Self> {
        self.stream_url = parse_url(url, &["wss", "ws"])?;
        self.check_schemes()?;
        Ok(self)
    }

    /// Overrides the Trading WebSocket RPC URL (`wss` only unless [`Self::allow_insecure`]).
    #[cfg(feature = "trading")]
    pub fn with_trading_ws_url(mut self, url: &str) -> Result<Self> {
        self.trading_ws_url = parse_url(url, &["wss", "ws"])?;
        self.check_schemes()?;
        Ok(self)
    }

    /// Permits plain `http`/`ws` URLs. Only for local mock servers in tests.
    #[must_use]
    pub const fn allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }

    /// Per-request timeout for REST calls (default 30 s).
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Connect timeout for REST and WebSocket handshakes (default 10 s).
    #[must_use]
    pub const fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Renew the access token when it expires within this window (default 5 min; capped at
    /// half the observed token lifetime).
    #[must_use]
    pub const fn with_refresh_lead(mut self, lead: Duration) -> Self {
        self.refresh_lead = lead;
        self
    }

    /// Default Smart OTP push-approval polling interval and total wait (defaults: 5 s, 60 s).
    #[cfg(feature = "trading")]
    #[must_use]
    pub fn with_otp_poll(mut self, interval: Duration, max_wait: Duration) -> Self {
        self.otp_poll_interval = interval.max(Duration::from_millis(100));
        self.otp_poll_max_wait = max_wait.max(self.otp_poll_interval);
        self
    }

    /// Maximum orders accepted in a single batch place/cancel request (default 20, clamped to
    /// `1..=50`).
    #[cfg(feature = "trading")]
    #[must_use]
    pub fn with_max_batch_orders(mut self, max: usize) -> Self {
        self.max_batch_orders = max.clamp(1, HARD_MAX_BATCH_ORDERS);
        self
    }

    /// Client-side request pacing per endpoint (default 10 requests/s; `None` disables it).
    #[must_use]
    pub const fn with_requests_per_second(mut self, rate: Option<NonZeroU32>) -> Self {
        self.requests_per_second = rate;
        self
    }

    /// How often a throttled (HTTP 429) **read-only `GET`** is retried, and the longest wait
    /// accepted (defaults: 3 retries, 60 s). Mutating requests are never retried.
    #[must_use]
    pub const fn with_rate_limit_retries(mut self, retries: u32, max_wait: Duration) -> Self {
        self.max_rate_limit_retries = retries;
        self.max_rate_limit_wait = max_wait;
        self
    }

    /// Retries of a **read-only `GET`** after a transport error or HTTP 502/503/504 (default 2).
    /// Mutating requests are never retried.
    #[must_use]
    pub const fn with_transient_retries(mut self, retries: u32) -> Self {
        self.max_transient_retries = retries;
        self
    }

    /// User-Agent sent on HTTP requests and order payloads (default `ssi-sdk/<version>`).
    #[must_use]
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        let value = user_agent.into().trim().to_owned();
        if !value.is_empty() {
            self.user_agent = value;
            #[cfg(feature = "trading")]
            {
                self.explicit_user_agent = true;
            }
        }
        self
    }

    /// Optional `Accept-Language` header sent on REST requests (for example `"en"` or `"vi"`).
    /// An empty or whitespace-only string clears the header so SSI uses its default (`en`).
    #[must_use]
    pub fn with_accept_language(mut self, language: impl Into<String>) -> Self {
        let language = language.into().trim().to_owned();
        self.accept_language = (!language.is_empty()).then_some(language);
        self
    }

    /// Configured `Accept-Language` header, if any.
    pub fn accept_language(&self) -> Option<&str> {
        self.accept_language.as_deref()
    }

    /// Overrides the machine `deviceId` sent in order and FCO payloads. When unset, the OS
    /// machine identifier is resolved once per process (see [`crate::device::machine_id`]).
    #[cfg(feature = "trading")]
    #[must_use]
    pub fn with_device_id(mut self, device_id: impl Into<String>) -> Self {
        let trimmed = device_id.into().trim().to_owned();
        self.device_id = (!trimmed.is_empty()).then_some(trimmed);
        self
    }

    /// Clock used for token expiry, intraday clamping, batch replay timestamps and receive times.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// REST auth / market-data base URL.
    pub const fn api_url(&self) -> &Url {
        &self.api_url
    }

    /// Effective trading/account REST base URL (`trading_url` override if set, else `api_url`).
    pub fn trading_url(&self) -> &Url {
        self.trading_url.as_ref().unwrap_or(&self.api_url)
    }

    /// Streaming WebSocket URL (`wss://stream.ssi.com.vn/ws/v3`).
    pub const fn stream_url(&self) -> &Url {
        &self.stream_url
    }

    /// Trading WebSocket RPC URL (`wss://api.ssi.com.vn/ws/v3/trading`).
    #[cfg(feature = "trading")]
    pub const fn trading_ws_url(&self) -> &Url {
        &self.trading_ws_url
    }

    /// Configured max orders per batch request.
    #[cfg(feature = "trading")]
    pub const fn max_batch_orders(&self) -> usize {
        self.max_batch_orders
    }

    /// Resolved device id used for order/FCO payloads.
    #[cfg(feature = "trading")]
    pub fn resolved_device_id(&self) -> &str {
        self.device_id
            .as_deref()
            .unwrap_or_else(|| crate::device::machine_id())
    }

    /// Optional explicit user agent (omitted on FCO cancel unless explicitly configured, matching
    /// the official V3 SDKs).
    #[cfg(feature = "trading")]
    pub(crate) fn explicit_user_agent(&self) -> Option<&str> {
        self.explicit_user_agent.then_some(self.user_agent.as_str())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.check_schemes()
    }

    fn check_schemes(&self) -> Result<()> {
        if self.allow_insecure {
            return Ok(());
        }
        if self.api_url.scheme() != "https" {
            return Err(Error::Config("the REST API URL must use https".to_owned()));
        }
        if let Some(url) = &self.trading_url
            && url.scheme() != "https"
        {
            return Err(Error::Config(
                "the trading REST URL must use https".to_owned(),
            ));
        }
        if self.stream_url.scheme() != "wss" {
            return Err(Error::Config("the stream URL must use wss".to_owned()));
        }
        #[cfg(feature = "trading")]
        if self.trading_ws_url.scheme() != "wss" {
            return Err(Error::Config(
                "the trading WebSocket URL must use wss".to_owned(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Config");
        debug
            .field("credentials", &self.credentials)
            .field("api_url", &self.api_url.as_str())
            .field("trading_url", &self.trading_url.as_ref().map(Url::as_str))
            .field("stream_url", &self.stream_url.as_str());
        #[cfg(feature = "trading")]
        debug.field("trading_ws_url", &self.trading_ws_url.as_str());
        debug
            .field("request_timeout", &self.request_timeout)
            .field("accept_language", &self.accept_language)
            .field("refresh_lead", &self.refresh_lead);
        #[cfg(feature = "trading")]
        debug.field("max_batch_orders", &self.max_batch_orders);
        debug
            .field("requests_per_second", &self.requests_per_second)
            .field("allow_insecure", &self.allow_insecure)
            .finish_non_exhaustive()
    }
}

#[allow(
    clippy::expect_used,
    reason = "the default URL constants are valid; a unit test parses them"
)]
fn default_url(text: &str) -> Url {
    Url::parse(text).expect("default URL constants are valid")
}

fn parse_url(text: &str, schemes: &[&str]) -> Result<Url> {
    let url =
        Url::parse(text.trim()).map_err(|error| Error::Config(format!("invalid URL: {error}")))?;
    if !schemes.contains(&url.scheme()) {
        return Err(Error::Config(format!(
            "unsupported URL scheme {}",
            url.scheme()
        )));
    }
    Ok(url)
}

fn required_env(name: &str) -> Result<String> {
    optional_env(name)?.ok_or_else(|| Error::Config(format!("{name} is not set")))
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(Error::Config(format!("{name} is not UTF-8"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> Credentials {
        Credentials::new("key-1234567890", "secret-abcdefgh")
            .expect("valid")
            .with_client_id("client-99999")
    }

    #[test]
    fn debug_masks_all_secrets() {
        let shown = format!("{:?}", Config::new(credentials()));
        assert!(!shown.contains("key-1234567890"));
        assert!(!shown.contains("secret-abcdefgh"));
        assert!(!shown.contains("client-99999"));
        assert!(shown.contains("api.ssi.com.vn"));
    }

    #[test]
    fn insecure_urls_need_opt_in() {
        assert!(
            Config::new(credentials())
                .with_api_url("http://127.0.0.1:1")
                .is_err()
        );
        let config = Config::new(credentials())
            .allow_insecure(true)
            .with_api_url("http://127.0.0.1:1")
            .expect("allowed")
            .with_accept_language("vi");
        assert_eq!(config.api_url().scheme(), "http");
        assert_eq!(config.accept_language(), Some("vi"));
    }
}
