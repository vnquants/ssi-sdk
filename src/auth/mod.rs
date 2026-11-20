//! Unified token manager for `/api/v3/auth/token`, `/api/v3/auth/refresh`, and (with
//! `feature = "trading"`) `/api/v3/auth/requestOtp`.
//!
//! One [`TokenManager`] serves both data/read-only and OTP-authenticated trading sessions:
//!
//! - **Data / read-only mode (default):** calling [`TokenManager::access_token`] automatically
//!   logs in with `apiKey` + `apiSecret` (no OTP), caches the token, refreshes it inside the lead
//!   window, and logs in again if `/refresh` fails or the 8-hour refresh chain ends. Failed
//!   logins back off (1 s doubling to 60 s; 10 min on revoked key `401104` / `401105`) while
//!   continuing to serve any still-valid access token.
//! - **Trading (OTP / Smart OTP) mode (`feature = "trading"`):** once authenticated via
//!   `authenticate_with_otp`, `authenticate_with_transaction_id`, `poll_smart_otp`, or an
//!   imported trading [`StoredToken`], the manager enters sticky trading mode. It refreshes via
//!   single-use `/refresh` and **never** silently downgrades to an OTP-less data token: when the
//!   refresh token expires or is rejected (`401101`–`401104`, `401106`–`401108`), the session is
//!   cleared and [`Error::ReauthenticationRequired`] is returned.
//! - **Single-use refresh tokens:** SSI treats a reused refresh token as a credential leak and
//!   revokes the key's whole session. Every refresh takes the refresh token out before the
//!   network call so it can never be sent twice, even under cancellation or concurrency.

mod backend;
#[cfg(test)]
mod tests;

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret, SecretString};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "trading")]
pub use self::backend::OtpResponse;
pub(crate) use self::backend::{AuthBackend, AuthMode, HttpAuthBackend, RawToken};
use crate::{
    config::Credentials,
    error::{ApiError, ApiErrorCode, Error, Result},
    secret::{Masked, fingerprint},
    time::Clock,
};

/// Epoch values at or above this are read as milliseconds (10^11 s is the year 5138).
const MILLIS_THRESHOLD: i64 = 100_000_000_000;
/// A freshly issued/refreshed token younger than this is reused even under slight clock skew.
const MIN_TOKEN_REUSE: Duration = Duration::from_secs(10);
/// Maximum lifetime observations kept in [`TokenManager::history`].
const HISTORY: usize = 32;
/// A token expiring within this margin is not served as a fallback during failed renewal.
const VALID_MARGIN: chrono::Duration = chrono::Duration::seconds(5);
/// Refresh tokens expiring within this margin are considered unusable.
const REFRESH_MARGIN: chrono::Duration = chrono::Duration::seconds(30);
/// Longest backoff after ordinary data-mode renewal failures.
const MAX_FAILURE_BACKOFF: Duration = Duration::from_mins(1);
/// Backoff after SSI rejects the API key itself (`401104`, `401105`).
const REVOKED_BACKOFF: Duration = Duration::from_mins(10);
/// Expected production access-token lifetime (15 min), used only for clock-skew warnings.
const EXPECTED_LIFETIME: Duration = Duration::from_mins(15);

/// How a token set was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TokenSource {
    /// `POST /api/v3/auth/token` with a normal OTP code.
    #[cfg(feature = "trading")]
    Otp,
    /// `POST /api/v3/auth/token` after Smart OTP push-approval (`transactionId`).
    #[cfg(feature = "trading")]
    SmartOtp,
    /// `POST /api/v3/auth/token` without OTP (data / read-only query token).
    ReadOnly,
    /// `POST /api/v3/auth/refresh` with the previous single-use refresh token.
    Refreshed,
    /// Installed via [`TokenManager::set_token`] from persisted state.
    Imported,
}

/// Unit SSI used for `expiresAt` / `refreshExpiresAt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochUnit {
    /// Epoch seconds.
    Seconds,
    /// Epoch milliseconds.
    Milliseconds,
}

/// Observed metadata for one token set. Contains no secret, only fingerprints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenInfo {
    /// How it was obtained.
    pub source: TokenSource,
    /// Local time the response was received.
    pub obtained_at: DateTime<Utc>,
    /// Access-token expiry (`expiresAt`).
    pub expires_at: DateTime<Utc>,
    /// Refresh-token expiry (`refreshExpiresAt`), if sent.
    pub refresh_expires_at: Option<DateTime<Utc>>,
    /// `expires_at - obtained_at`.
    pub access_lifetime: Duration,
    /// `refresh_expires_at - obtained_at`.
    pub refresh_lifetime: Option<Duration>,
    /// Unit of the expiry fields as sent.
    pub epoch_unit: EpochUnit,
    /// `tokenType` (normally `Bearer`).
    pub token_type: String,
    /// Fingerprint of the access token.
    pub access_fingerprint: u32,
    /// Fingerprint of the refresh token.
    pub refresh_fingerprint: Option<u32>,
    /// For a refresh: whether SSI rotated the refresh token.
    pub refresh_token_rotated: Option<bool>,
}

/// Snapshot of a session's secrets and expiry timestamps for persistence across restarts.
///
/// `Debug` masks both tokens. Treat serialized copies as credentials (encrypt at rest, restrict
/// file permissions to `0600`).
#[derive(Clone)]
pub struct StoredToken {
    access_token: SecretString,
    refresh_token: Option<SecretString>,
    /// Access-token expiry in UTC.
    pub expires_at: DateTime<Utc>,
    /// Refresh-token expiry in UTC, if known.
    pub refresh_expires_at: Option<DateTime<Utc>>,
    /// Token type (default `"Bearer"`).
    pub token_type: String,
    /// Whether this token came from an OTP / Smart OTP trading session (when `true`, refresh
    /// failures return [`Error::ReauthenticationRequired`] instead of falling back to read-only).
    pub is_trading_session: bool,
}

impl StoredToken {
    /// Constructs a token snapshot for [`TokenManager::set_token`].
    pub fn new(
        access_token: impl Into<String>,
        refresh_token: Option<String>,
        expires_at: DateTime<Utc>,
        refresh_expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            access_token: SecretString::from(access_token.into()),
            refresh_token: refresh_token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty())
                .map(SecretString::from),
            expires_at,
            refresh_expires_at,
            token_type: "Bearer".to_owned(),
            is_trading_session: false,
        }
    }

    /// Marks whether this persisted token belongs to an OTP / Smart OTP trading session.
    #[must_use]
    pub const fn with_trading_session(mut self, trading: bool) -> Self {
        self.is_trading_session = trading;
        self
    }

    /// Raw access token (for encrypted persistence only; never log).
    pub fn access_token(&self) -> &str {
        self.access_token.expose_secret()
    }

    /// Raw refresh token, if present (for encrypted persistence only; never log).
    pub fn refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_ref().map(ExposeSecret::expose_secret)
    }
}

impl fmt::Debug for StoredToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredToken")
            .field("access_token", &Masked::new(self.access_token()))
            .field("refresh_token", &self.refresh_token().map(Masked::new))
            .field("expires_at", &self.expires_at)
            .field("refresh_expires_at", &self.refresh_expires_at)
            .field("token_type", &self.token_type)
            .field("is_trading_session", &self.is_trading_session)
            .finish()
    }
}

/// Whether the manager may automatically re-login without OTP when no refresh token is usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SessionMode {
    /// Market-data / read-only mode: auto-login and re-login with `apiKey` + `apiSecret`.
    #[default]
    AutoData,
    /// Sticky OTP / Smart OTP trading mode: only `/refresh`; dead refresh returns
    /// `ReauthenticationRequired` and never downgrades silently.
    TradingOtp,
}

struct TokenSet {
    access: SecretString,
    refresh: Option<SecretString>,
    info: TokenInfo,
    rejected: bool,
}

impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSet")
            .field("access", &Masked::new(self.access.expose_secret()))
            .field("info", &self.info)
            .field("rejected", &self.rejected)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct State {
    mode: SessionMode,
    current: Option<TokenSet>,
    failures: u32,
    retry_at: Option<DateTime<Utc>>,
    last_failure: Option<String>,
    lifetime_warned: bool,
}

struct Inner {
    backend: Arc<dyn AuthBackend>,
    credentials: Credentials,
    clock: Arc<dyn Clock>,
    refresh_lead: Duration,
    #[cfg(feature = "trading")]
    otp_poll_interval: Duration,
    #[cfg(feature = "trading")]
    otp_poll_max_wait: Duration,
    /// Token and failure state. Never held across an `await`.
    state: Mutex<State>,
    /// Single-flight renewal lock: held across `/token` or `/refresh`.
    renewal: tokio::sync::Mutex<()>,
    latest: Mutex<Option<TokenInfo>>,
    history: Mutex<Vec<TokenInfo>>,
}

enum Decision {
    Use(SecretString),
    Renew {
        fallback: Option<SecretString>,
        mode: SessionMode,
    },
    Wait {
        retry_in: Duration,
        reason: String,
    },
}

/// Shared, cloneable V3 token manager.
#[derive(Clone)]
pub struct TokenManager {
    inner: Arc<Inner>,
}

impl fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenManager")
            .field("credentials", &self.inner.credentials)
            .field("latest", &self.info())
            .finish_non_exhaustive()
    }
}

impl TokenManager {
    pub(crate) fn new(
        backend: Arc<dyn AuthBackend>,
        credentials: Credentials,
        clock: Arc<dyn Clock>,
        refresh_lead: Duration,
        #[cfg(feature = "trading")] otp_poll_interval: Duration,
        #[cfg(feature = "trading")] otp_poll_max_wait: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                backend,
                credentials,
                clock,
                refresh_lead,
                #[cfg(feature = "trading")]
                otp_poll_interval,
                #[cfg(feature = "trading")]
                otp_poll_max_wait,
                state: Mutex::new(State::default()),
                renewal: tokio::sync::Mutex::new(()),
                latest: Mutex::new(None),
                history: Mutex::new(Vec::new()),
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Requests an OTP (SMS/email/authenticator) or Smart OTP push-approval via
    /// `POST /api/v3/auth/requestOtp`.
    #[cfg(feature = "trading")]
    pub async fn request_otp(&self) -> Result<OtpResponse> {
        self.inner
            .backend
            .request_otp(&self.inner.credentials)
            .await
    }

    /// Authenticates with a normal OTP code (`POST /api/v3/auth/token` with `otp`) and enters
    /// sticky trading mode.
    #[cfg(feature = "trading")]
    pub async fn authenticate_with_otp(&self, otp: &str) -> Result<TokenInfo> {
        let trimmed = otp.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidRequest("otp cannot be empty".to_owned()));
        }
        let mode = AuthMode::Otp(SecretString::from(trimmed.to_owned()));
        let raw = self
            .inner
            .backend
            .issue(&self.inner.credentials, &mode)
            .await?;
        let (_, info) = self.install(raw, TokenSource::Otp, SessionMode::TradingOtp, None);
        Ok(info)
    }

    /// Single Smart OTP exchange attempt (`POST /api/v3/auth/token` with `transactionId`).
    ///
    /// Returns [`Error::SmartOtpPending`] (HTTP 202 / `401114`) while waiting for approval, and
    /// [`Error::SmartOtpRejected`] (`401113`, `401115`, `401116`) on terminal failure.
    #[cfg(feature = "trading")]
    pub async fn authenticate_with_transaction_id(
        &self,
        transaction_id: &str,
    ) -> Result<TokenInfo> {
        let trimmed = transaction_id.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidRequest(
                "transaction_id cannot be empty".to_owned(),
            ));
        }
        let mode = AuthMode::TransactionId(SecretString::from(trimmed.to_owned()));
        let raw = self
            .inner
            .backend
            .issue(&self.inner.credentials, &mode)
            .await?;
        let (_, info) = self.install(raw, TokenSource::SmartOtp, SessionMode::TradingOtp, None);
        Ok(info)
    }

    /// Polls `POST /api/v3/auth/token` with `transactionId` until the user approves Smart OTP,
    /// a terminal error occurs, `cancel` fires, or the configured polling budget runs out.
    #[cfg(feature = "trading")]
    pub async fn poll_smart_otp(
        &self,
        transaction_id: &str,
        cancel: CancellationToken,
    ) -> Result<TokenInfo> {
        let interval = self.inner.otp_poll_interval;
        let attempts = compute_poll_attempts(interval, self.inner.otp_poll_max_wait);
        self.poll_smart_otp_with(transaction_id, interval, attempts, cancel)
            .await
    }

    /// Polls Smart OTP with an explicit interval and maximum attempt count.
    #[cfg(feature = "trading")]
    pub async fn poll_smart_otp_with(
        &self,
        transaction_id: &str,
        interval: Duration,
        max_attempts: u32,
        cancel: CancellationToken,
    ) -> Result<TokenInfo> {
        let attempts = max_attempts.max(1);
        let interval = interval.max(Duration::from_millis(50));
        for attempt in 1..=attempts {
            match self.authenticate_with_transaction_id(transaction_id).await {
                Ok(info) => return Ok(info),
                Err(Error::SmartOtpPending(api_err)) => {
                    if attempt >= attempts {
                        return Err(Error::SmartOtpPending(api_err));
                    }
                    tracing::info!(
                        attempt,
                        max_attempts = attempts,
                        wait_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
                        "Smart OTP approval still pending; waiting to poll again"
                    );
                    tokio::select! {
                        () = cancel.cancelled() => {
                            return Err(Error::Authentication(
                                "Smart OTP polling cancelled".to_owned(),
                            ));
                        }
                        () = tokio::time::sleep(interval) => {}
                    }
                }
                Err(other) => return Err(other),
            }
        }
        Err(Error::Authentication(
            "Smart OTP polling exhausted".to_owned(),
        ))
    }

    /// Explicitly obtains an OTP-less read-only token (`POST /api/v3/auth/token` without `otp`
    /// or `transactionId`) and switches the manager to auto-data mode.
    pub async fn authenticate_read_only(&self) -> Result<TokenInfo> {
        let raw = self
            .inner
            .backend
            .issue(&self.inner.credentials, &AuthMode::ReadOnly)
            .await?;
        let (_, info) = self.install(raw, TokenSource::ReadOnly, SessionMode::AutoData, None);
        Ok(info)
    }

    /// Installs a previously persisted session (from [`Self::export_token`]).
    pub fn set_token(&self, stored: StoredToken) -> Result<TokenInfo> {
        let access = stored.access_token().trim().to_owned();
        if access.is_empty() {
            return Err(Error::InvalidRequest(
                "stored access_token cannot be empty".to_owned(),
            ));
        }
        let session_mode = if stored.is_trading_session {
            SessionMode::TradingOtp
        } else {
            SessionMode::AutoData
        };
        let raw = RawToken {
            access_token: SecretString::from(access),
            refresh_token: stored.refresh_token,
            token_type: (!stored.token_type.trim().is_empty()).then_some(stored.token_type),
            expires_at: utc_to_epoch(stored.expires_at),
            refresh_expires_at: stored.refresh_expires_at.map(utc_to_epoch),
        };
        let (_, info) = self.install(raw, TokenSource::Imported, session_mode, None);
        Ok(info)
    }

    /// Exports the currently held non-rejected session (if any) for encrypted persistence across
    /// restarts.
    pub fn export_token(&self) -> Option<StoredToken> {
        let state = self.state();
        let current = state.current.as_ref().filter(|current| !current.rejected)?;
        let exported = StoredToken {
            access_token: current.access.clone(),
            refresh_token: current.refresh.clone(),
            expires_at: current.info.expires_at,
            refresh_expires_at: current.info.refresh_expires_at,
            token_type: current.info.token_type.clone(),
            is_trading_session: state.mode == SessionMode::TradingOtp,
        };
        drop(state);
        Some(exported)
    }

    /// Returns a valid access token for market data, streams, and read-only queries.
    ///
    /// - In data/read-only mode: logs in automatically with `apiKey` + `apiSecret`, refreshes
    ///   inside the lead window, re-logs in if refresh fails or expires, and backs off on login
    ///   errors (serving any still-valid token meanwhile, or returning [`Error::AuthBackoff`]).
    /// - In sticky trading (OTP / Smart OTP) mode: refreshes via `/refresh` and never silently
    ///   downgrades to an OTP-less token; returns [`Error::ReauthenticationRequired`] if the
    ///   refresh chain is dead.
    pub async fn access_token(&self) -> Result<SecretString> {
        self.access_token_inner(false).await
    }

    /// Returns a valid access token for trading operations (signed orders, Trading WS,
    /// `TRADING` event stream).
    ///
    /// Never performs an implicit OTP-less login: returns [`Error::Authentication`] when no
    /// session has been established yet, and [`Error::ReauthenticationRequired`] when a trading
    /// session's refresh token has expired or been rejected.
    #[cfg(feature = "trading")]
    pub(crate) async fn trading_access_token(&self) -> Result<SecretString> {
        self.access_token_inner(true).await
    }

    async fn access_token_inner(&self, require_existing_session: bool) -> Result<SecretString> {
        if require_existing_session && let Some(err) = self.check_session_present() {
            return Err(err);
        }
        let (fallback, mode) = match self.decide(self.inner.clock.now()) {
            Decision::Use(token) => return Ok(token),
            Decision::Wait { retry_in, reason } => {
                return Err(Error::AuthBackoff { retry_in, reason });
            }
            Decision::Renew { fallback, mode } => (fallback, mode),
        };
        let _guard = match fallback {
            Some(token) => match self.inner.renewal.try_lock() {
                Ok(guard) => guard,
                Err(_) => return Ok(token),
            },
            None => self.inner.renewal.lock().await,
        };
        if require_existing_session && let Some(err) = self.check_session_present() {
            return Err(err);
        }
        match self.decide(self.inner.clock.now()) {
            Decision::Use(token) => return Ok(token),
            Decision::Wait { retry_in, reason } => {
                return Err(Error::AuthBackoff { retry_in, reason });
            }
            Decision::Renew { .. } => {}
        }
        let result = if mode == SessionMode::TradingOtp || require_existing_session {
            self.refresh_locked().await.map(|(token, _)| token)
        } else {
            self.renew_data_locked().await
        };
        match result {
            Ok(token) => Ok(token),
            Err(error) => {
                if matches!(error, Error::ReauthenticationRequired(_)) {
                    return Err(error);
                }
                let retry_in = if mode == SessionMode::AutoData && !require_existing_session {
                    Some(self.record_failure(&error))
                } else {
                    None
                };
                // Re-check whether the previous token is still usable (not rejected mid-flight).
                if let Decision::Use(token) = self.decide(self.inner.clock.now()) {
                    tracing::warn!(
                        %error,
                        retry_in_ms = retry_in.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                        "SSI token renewal failed; the current token is still valid"
                    );
                    return Ok(token);
                }
                Err(error)
            }
        }
    }

    fn check_session_present(&self) -> Option<Error> {
        let (has_current, mode) = {
            let state = self.state();
            (state.current.is_some(), state.mode)
        };
        if has_current {
            return None;
        }
        if mode == SessionMode::TradingOtp {
            return Some(Error::ReauthenticationRequired(local_reauth_error(
                "trading session expired; authenticate again with a new OTP",
            )));
        }
        Some(Error::Authentication(
            "not authenticated; call authenticate_with_otp or poll_smart_otp first".to_owned(),
        ))
    }

    fn decide(&self, now: DateTime<Utc>) -> Decision {
        let (mode, current, retry_at, reason) = {
            let state = self.state();
            let current = state
                .current
                .as_ref()
                .filter(|current| !current.rejected)
                .map(|current| (current.access.clone(), current.info.clone()));
            (
                state.mode,
                current,
                state.retry_at,
                state.last_failure.clone(),
            )
        };
        if let Some((access, info)) = &current
            && !self.needs_renewal(info, now)
        {
            return Decision::Use(access.clone());
        }
        let fallback = current
            .filter(|(_, info)| info.expires_at > now + VALID_MARGIN)
            .map(|(access, _)| access);
        match retry_at.filter(|retry_at| *retry_at > now) {
            Some(retry_at) => fallback.map_or_else(
                || Decision::Wait {
                    retry_in: (retry_at - now).to_std().unwrap_or(Duration::ZERO),
                    reason: reason.unwrap_or_default(),
                },
                Decision::Use,
            ),
            None => Decision::Renew { fallback, mode },
        }
    }

    fn record_failure(&self, error: &Error) -> Duration {
        let revoked = matches!(
            error.api_code(),
            Some(ApiErrorCode::InvalidApiKeyOrSecret | ApiErrorCode::InvalidApiKeyReference)
        );
        let now = self.inner.clock.now();
        let mut state = self.state();
        state.failures = state.failures.saturating_add(1);
        let backoff = if revoked {
            REVOKED_BACKOFF
        } else {
            Duration::from_secs(1)
                .saturating_mul(2_u32.saturating_pow(state.failures.saturating_sub(1).min(6)))
                .min(MAX_FAILURE_BACKOFF)
        };
        state.retry_at = Some(now + chrono::Duration::from_std(backoff).unwrap_or_default());
        state.last_failure = Some(error.to_string());
        drop(state);
        if revoked {
            tracing::error!(
                %error,
                backoff_s = backoff.as_secs(),
                "SSI rejected the API key itself; check that the key is active on the developer portal"
            );
        } else {
            tracing::warn!(
                %error,
                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                "SSI token renewal failed"
            );
        }
        backoff
    }

    /// Forces a single-flight `/api/v3/auth/refresh` using the held refresh token (never falls
    /// back to `/token`).
    pub async fn refresh(&self) -> Result<TokenInfo> {
        let _guard = self.inner.renewal.lock().await;
        let (_, info) = self.refresh_locked().await?;
        Ok(info)
    }

    /// Refreshes now (in data mode, falls back to re-logging in if there is no usable refresh
    /// token; in trading mode, only refreshes and never downgrades).
    pub async fn force_refresh(&self) -> Result<TokenInfo> {
        let _guard = self.inner.renewal.lock().await;
        let mode = self.state().mode;
        if mode == SessionMode::TradingOtp {
            let (_, info) = self.refresh_locked().await?;
            return Ok(info);
        }
        match self.renew_data_locked().await {
            Ok(_) => self
                .info()
                .ok_or_else(|| Error::Config("token renewal produced no token".to_owned())),
            Err(error) => {
                self.record_failure(&error);
                Err(error)
            }
        }
    }

    /// Marks `token` as rejected (for example after HTTP 401 or WS 1008) so the next call renews
    /// it. Keeps the refresh token intact.
    pub fn invalidate(&self, token: &SecretString) {
        let mut state = self.state();
        if let Some(current) = state
            .current
            .as_mut()
            .filter(|current| current.access.expose_secret() == token.expose_secret())
        {
            tracing::info!(
                access = %Masked::new(token.expose_secret()),
                "SSI access token rejected; it will be renewed"
            );
            current.rejected = true;
        }
    }

    /// Clears the held tokens, resets failure backoff, and returns the manager to default
    /// auto-data mode.
    pub fn clear(&self) {
        let mut state = self.state();
        state.mode = SessionMode::AutoData;
        state.current = None;
        state.failures = 0;
        state.retry_at = None;
        state.last_failure = None;
        drop(state);
        self.clear_latest();
    }

    /// Whether a non-rejected token with more than a few seconds of validity is currently held.
    pub fn is_authenticated(&self) -> bool {
        let now = self.inner.clock.now();
        self.state().current.as_ref().is_some_and(|current| {
            !current.rejected && current.info.expires_at > now + VALID_MARGIN
        })
    }

    /// Alias for [`Self::is_authenticated`].
    pub fn has_valid_token(&self) -> bool {
        self.is_authenticated()
    }

    /// Whether a usable (unexpired) refresh token is held.
    pub fn can_refresh(&self) -> bool {
        let now = self.inner.clock.now();
        self.state().current.as_ref().is_some_and(|current| {
            current.refresh.is_some()
                && current
                    .info
                    .refresh_expires_at
                    .is_none_or(|expiry| expiry > now + REFRESH_MARGIN)
        })
    }

    /// Whether a 401 retry on a read-only GET can obtain a fresh token (always true in auto-data
    /// mode; requires a usable refresh token in sticky trading mode).
    pub(crate) fn can_renew_after_401(&self) -> bool {
        self.state().mode == SessionMode::AutoData || self.can_refresh()
    }

    /// Metadata of the currently held token, if any.
    pub fn info(&self) -> Option<TokenInfo> {
        self.inner
            .latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// History of token sets obtained so far (most recent last, capped at 32).
    pub fn history(&self) -> Vec<TokenInfo> {
        self.inner
            .history
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Time until the held token enters its refresh window (`None` if no token is held).
    pub fn time_until_renewal(&self) -> Option<Duration> {
        let info = self.info()?;
        let now = self.inner.clock.now();
        let due = info.expires_at - chrono::Duration::from_std(self.lead_for(&info)).ok()?;
        Some((due - now).to_std().unwrap_or(Duration::ZERO))
    }

    /// Starts a background task that renews the token `refresh_lead` before it expires.
    ///
    /// Stops when `cancel` fires, or in sticky trading mode when the refresh chain expires
    /// ([`Error::ReauthenticationRequired`]).
    pub fn spawn_auto_refresh(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move { manager.auto_refresh(cancel).await })
    }

    async fn auto_refresh(self, cancel: CancellationToken) {
        let mut trading_failures: u32 = 0;
        loop {
            let mode = self.state().mode;
            let wait = if mode == SessionMode::TradingOtp && trading_failures > 0 {
                Duration::from_secs(1)
                    .saturating_mul(2_u32.saturating_pow(trading_failures.min(6)))
                    .min(MAX_FAILURE_BACKOFF)
            } else {
                self.next_attempt_in()
                    .unwrap_or_else(|| {
                        if mode == SessionMode::TradingOtp {
                            Duration::from_secs(5)
                        } else {
                            Duration::ZERO
                        }
                    })
                    .max(Duration::from_secs(1))
            };
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
            if mode == SessionMode::TradingOtp && !self.can_refresh() && self.info().is_none() {
                continue;
            }
            match self.access_token().await {
                Ok(_) => {
                    trading_failures = 0;
                }
                Err(Error::ReauthenticationRequired(error)) => {
                    tracing::error!(
                        %error,
                        "SSI trading session expired and cannot be refreshed; stopping auto-refresh — re-authenticate with a new OTP"
                    );
                    return;
                }
                Err(error) => {
                    trading_failures = trading_failures.saturating_add(1);
                    tracing::warn!(%error, "SSI token auto-refresh failed; will retry");
                }
            }
        }
    }

    fn next_attempt_in(&self) -> Option<Duration> {
        let now = self.inner.clock.now();
        let retry_at = self.state().retry_at.filter(|retry_at| *retry_at > now);
        retry_at.map_or_else(
            || self.time_until_renewal(),
            |retry_at| (retry_at - now).to_std().ok(),
        )
    }

    fn lead_for(&self, info: &TokenInfo) -> Duration {
        self.inner.refresh_lead.min(info.access_lifetime / 2)
    }

    fn needs_renewal(&self, info: &TokenInfo, now: DateTime<Utc>) -> bool {
        if info.source != TokenSource::Imported {
            let age = (now - info.obtained_at).to_std().unwrap_or(Duration::ZERO);
            if age < MIN_TOKEN_REUSE {
                return false;
            }
        }
        let lead = chrono::Duration::from_std(self.lead_for(info)).unwrap_or_default();
        now >= info.expires_at - lead
    }

    /// Data-mode renewal: tries single-use `/refresh`, falls back to `/token` with API key pair.
    async fn renew_data_locked(&self) -> Result<SecretString> {
        let now = self.inner.clock.now();
        let refresh = {
            let mut state = self.state();
            state.current.as_mut().and_then(|current| {
                let usable = current
                    .info
                    .refresh_expires_at
                    .is_none_or(|expiry| expiry > now + REFRESH_MARGIN);
                // Take the refresh token out: used at most once even if dropped mid-await.
                usable.then(|| current.refresh.take()).flatten()
            })
        };
        if let Some(refresh_token) = refresh {
            let previous = fingerprint(refresh_token.expose_secret());
            match self.inner.backend.refresh(&refresh_token).await {
                Ok(raw) => {
                    let (access, _) = self.install(
                        raw,
                        TokenSource::Refreshed,
                        SessionMode::AutoData,
                        Some(previous),
                    );
                    return Ok(access);
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "SSI token refresh failed; logging in again with the API key"
                    );
                }
            }
        }
        let raw = self
            .inner
            .backend
            .issue(&self.inner.credentials, &AuthMode::ReadOnly)
            .await?;
        let (access, _) = self.install(raw, TokenSource::ReadOnly, SessionMode::AutoData, None);
        Ok(access)
    }

    /// Strict `/refresh` (never falls back to `/token`).
    async fn refresh_locked(&self) -> Result<(SecretString, TokenInfo)> {
        let now = self.inner.clock.now();
        let (refresh_token, mode) = {
            let mut state = self.state();
            let mode = state.mode;
            let Some(current) = state.current.as_mut() else {
                return Err(if mode == SessionMode::TradingOtp {
                    Error::ReauthenticationRequired(local_reauth_error(
                        "trading session expired; authenticate again with a new OTP",
                    ))
                } else {
                    Error::Authentication(
                        "no refresh token available; authenticate first".to_owned(),
                    )
                });
            };
            let usable = current
                .info
                .refresh_expires_at
                .is_none_or(|expiry| expiry > now + REFRESH_MARGIN);
            if !usable {
                state.current = None;
                drop(state);
                self.clear_latest();
                return Err(Error::ReauthenticationRequired(local_reauth_error(
                    "refresh token expired; authenticate again with a new OTP",
                )));
            }
            // Take the refresh token out so it can NEVER be sent twice even if dropped mid-await.
            let Some(token) = current.refresh.take() else {
                state.current = None;
                drop(state);
                self.clear_latest();
                return Err(Error::ReauthenticationRequired(local_reauth_error(
                    "no single-use refresh token left; authenticate again with a new OTP",
                )));
            };
            (token, mode)
        };

        let previous = fingerprint(refresh_token.expose_secret());
        match self.inner.backend.refresh(&refresh_token).await {
            Ok(raw) => Ok(self.install(raw, TokenSource::Refreshed, mode, Some(previous))),
            Err(Error::Api(api)) if api.code.is_refresh_token_error() => {
                self.clear_token_preserving_mode();
                Err(Error::ReauthenticationRequired(api))
            }
            Err(error) => {
                if matches!(&error, Error::Api(api) if api.http_status == 401) {
                    let api = match error {
                        Error::Api(api) => api,
                        other => return Err(other),
                    };
                    self.clear_token_preserving_mode();
                    return Err(Error::ReauthenticationRequired(api));
                }
                Err(error)
            }
        }
    }

    fn clear_token_preserving_mode(&self) {
        let mut state = self.state();
        state.current = None;
        drop(state);
        self.clear_latest();
    }

    fn clear_latest(&self) {
        *self
            .inner
            .latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn install(
        &self,
        raw: RawToken,
        source: TokenSource,
        mode: SessionMode,
        previous_refresh: Option<u32>,
    ) -> (SecretString, TokenInfo) {
        let obtained_at = self.inner.clock.now();
        let (expires_at, epoch_unit) = epoch_to_utc(raw.expires_at);
        let refresh_expires_at = raw.refresh_expires_at.map(|value| epoch_to_utc(value).0);
        let refresh_fingerprint = raw
            .refresh_token
            .as_ref()
            .map(|token| fingerprint(token.expose_secret()));
        let info = TokenInfo {
            source,
            obtained_at,
            expires_at,
            refresh_expires_at,
            access_lifetime: (expires_at - obtained_at)
                .to_std()
                .unwrap_or(Duration::ZERO),
            refresh_lifetime: refresh_expires_at
                .map(|expiry| (expiry - obtained_at).to_std().unwrap_or(Duration::ZERO)),
            epoch_unit,
            token_type: raw.token_type.unwrap_or_else(|| "Bearer".to_owned()),
            access_fingerprint: fingerprint(raw.access_token.expose_secret()),
            refresh_fingerprint,
            refresh_token_rotated: previous_refresh
                .zip(refresh_fingerprint)
                .map(|(before, after)| before != after),
        };
        tracing::info!(
            source = ?info.source,
            access = %Masked::new(raw.access_token.expose_secret()),
            expires_at = %info.expires_at,
            refresh_expires_at = ?info.refresh_expires_at,
            access_lifetime_s = info.access_lifetime.as_secs(),
            refresh_lifetime_s = info.refresh_lifetime.map(|lifetime| lifetime.as_secs()),
            refresh_rotated = ?info.refresh_token_rotated,
            "SSI token obtained"
        );
        *self
            .inner
            .latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(info.clone());
        {
            let mut history = self
                .inner
                .history
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if history.len() >= HISTORY {
                history.remove(0);
            }
            history.push(info.clone());
        }
        let skewed =
            source != TokenSource::Imported && lifetime_is_suspect(expires_at - obtained_at);
        let mut state = self.state();
        if skewed && !std::mem::replace(&mut state.lifetime_warned, true) {
            tracing::warn!(
                lifetime_s = (expires_at - obtained_at).num_seconds(),
                expected_s = EXPECTED_LIFETIME.as_secs(),
                "SSI token lifetime is far from the 900 s SSI issues; the local clock may be skewed"
            );
        }
        state.mode = mode;
        state.failures = 0;
        state.retry_at = None;
        state.last_failure = None;
        let access = raw.access_token.clone();
        state.current = Some(TokenSet {
            access: raw.access_token,
            refresh: raw.refresh_token,
            info: info.clone(),
            rejected: false,
        });
        drop(state);
        (access, info)
    }
}

fn local_reauth_error(message: &str) -> ApiError {
    ApiError {
        http_status: 401,
        code: ApiErrorCode::RefreshTokenRevokedOrExpired,
        message: message.to_owned(),
        endpoint: backend::EP_REFRESH_TOKEN.to_owned(),
        private: true,
    }
}

#[cfg(feature = "trading")]
fn compute_poll_attempts(interval: Duration, max_wait: Duration) -> u32 {
    let wait_ms = interval.as_millis().max(1);
    let max_ms = max_wait.as_millis().max(wait_ms);
    let attempts = max_ms.div_ceil(wait_ms);
    u32::try_from(attempts).unwrap_or(u32::MAX).max(1)
}

fn lifetime_is_suspect(lifetime: chrono::Duration) -> bool {
    let expected = i64::try_from(EXPECTED_LIFETIME.as_secs()).unwrap_or(i64::MAX);
    (lifetime.num_seconds() - expected).abs() > 120
}

const fn utc_to_epoch(time: DateTime<Utc>) -> i64 {
    if time.timestamp_subsec_millis() == 0 {
        time.timestamp()
    } else {
        time.timestamp_millis()
    }
}

fn epoch_to_utc(value: i64) -> (DateTime<Utc>, EpochUnit) {
    if value >= MILLIS_THRESHOLD {
        (
            DateTime::from_timestamp_millis(value).unwrap_or(DateTime::UNIX_EPOCH),
            EpochUnit::Milliseconds,
        )
    } else {
        (
            DateTime::from_timestamp(value, 0).unwrap_or(DateTime::UNIX_EPOCH),
            EpochUnit::Seconds,
        )
    }
}
