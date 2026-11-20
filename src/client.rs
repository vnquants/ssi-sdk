//! The unified [`SsiClient`] facade.

use std::{fmt, sync::Arc, time::Duration};

use reqwest::Method;
use serde_json::Value;

#[cfg(feature = "trading")]
use crate::crypto::RequestSigner;
#[cfg(feature = "data")]
use crate::http::interpret_binary;
use crate::{
    auth::{HttpAuthBackend, TokenManager},
    config::Config,
    error::{Error, Result},
    http::{Host, Pacing, RateLimitSnapshot, Sensitivity, Transport, interpret},
};

struct Inner {
    config: Config,
    transport: Arc<Transport>,
    auth: TokenManager,
    #[cfg(feature = "trading")]
    signer: Option<RequestSigner>,
}

/// Unified SSI `FastConnect` V3 client for market data (`feature = "data"`), read-only account
/// queries, and trading (`feature = "trading"`).
///
/// Cloning is cheap (one `Arc`); clones share the token manager, per-endpoint rate pacing, last
/// observed rate-limit snapshot, and (when `feature = "trading"` is enabled) RSA request signer.
///
/// **Non-retry invariant:** read-only `GET` queries retry bounded transient / 429 failures and
/// one 401 token renewal. Signed mutating requests (`POST`, `PUT`, `DELETE`) are **never**
/// retried automatically.
#[derive(Clone)]
pub struct SsiClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for SsiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("SsiClient");
        debug.field("config", &self.inner.config);
        #[cfg(feature = "trading")]
        debug.field("has_signer", &self.inner.signer.is_some());
        debug.finish_non_exhaustive()
    }
}

/// An uninterpreted response from [`SsiClient::get_raw`].
///
/// Account and trading responses contain private data; do not log `json`.
#[derive(Debug, Clone)]
pub struct RawResponse {
    /// HTTP status code.
    pub status: u16,
    /// Body parsed as JSON, if valid JSON.
    pub json: Option<Value>,
    /// Body length in bytes.
    pub bytes: usize,
    /// Rate-limit headers observed on this response.
    pub rate_limit: Option<RateLimitSnapshot>,
}

impl SsiClient {
    /// Builds a client. When `feature = "trading"` is enabled, parses and validates any
    /// configured RSA private key eagerly. Makes no network call until the first request.
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        #[cfg(feature = "trading")]
        let signer = config
            .credentials
            .private_key()
            .map(|secret| {
                use secrecy::ExposeSecret as _;
                RequestSigner::parse(secret.expose_secret())
            })
            .transpose()?;
        let transport = Arc::new(Transport::new(&config)?);
        let auth = TokenManager::new(
            Arc::new(HttpAuthBackend::new(Arc::clone(&transport))),
            config.credentials.clone(),
            Arc::clone(&config.clock),
            config.refresh_lead,
            #[cfg(feature = "trading")]
            config.otp_poll_interval,
            #[cfg(feature = "trading")]
            config.otp_poll_max_wait,
        );
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                transport,
                auth,
                #[cfg(feature = "trading")]
                signer,
            }),
        })
    }

    /// Builds a client from `SSI_API_KEY`, `SSI_API_SECRET`, and optional overrides (see
    /// [`Config::from_env`]).
    pub fn from_env() -> Result<Self> {
        Self::new(Config::from_env()?)
    }

    /// The token manager shared by all clones.
    pub fn auth(&self) -> &TokenManager {
        &self.inner.auth
    }

    /// The client configuration.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Whether an RSA signing key is configured (required for placing/modifying/cancelling
    /// orders and FCOs).
    #[cfg(feature = "trading")]
    pub fn has_signer(&self) -> bool {
        self.inner.signer.is_some()
    }

    /// Rate-limit headers of the most recent REST response that carried any.
    pub fn rate_limit(&self) -> Option<RateLimitSnapshot> {
        self.inner.transport.last_rate_limit()
    }

    pub(crate) fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.config.clock.now()
    }

    #[cfg(feature = "trading")]
    pub(crate) fn signer(&self) -> Option<&RequestSigner> {
        self.inner.signer.as_ref()
    }

    /// Sends one authenticated `GET` and returns the raw response without interpreting it.
    ///
    /// Restricted to plain `/api/v3/...` paths (no `.` / `..`, query, or fragment). The body may
    /// carry private account information — do not log it.
    pub async fn get_raw(&self, path: &str, query: &[(&str, String)]) -> Result<RawResponse> {
        check_raw_path(path)?;
        let host = Host::for_path(path);
        let resolved = self.inner.transport.url_for(host, path)?;
        if resolved.path() != path {
            return Err(Error::InvalidRequest(
                "raw requests are limited to plain /api/v3/ paths".to_owned(),
            ));
        }
        let token = self.auth().access_token().await?;
        let reply = self
            .inner
            .transport
            .send(
                host,
                Method::GET,
                path,
                query,
                None,
                None,
                Some(&token),
                Pacing::Paced,
            )
            .await?;
        Ok(RawResponse {
            status: reply.status.as_u16(),
            json: serde_json::from_slice(&reply.body).ok(),
            bytes: reply.body.len(),
            rate_limit: reply.rate,
        })
    }

    /// Authenticated **read-only `GET`** with one 401 token renewal retry and bounded 429 /
    /// transient retries. Returns the JSON body text; `Ok(None)` means SSI answered "no data".
    pub(crate) async fn get_body(
        &self,
        path: &str,
        query: &[(&str, String)],
        sensitivity: Sensitivity,
    ) -> Result<Option<Vec<u8>>> {
        let config = &self.inner.config;
        let host = Host::for_path(path);
        let mut renewed = false;
        let mut throttled: u32 = 0;
        let mut transient: u32 = 0;
        loop {
            let token = self.auth().access_token().await?;
            let outcome = match self
                .inner
                .transport
                .send(
                    host,
                    Method::GET,
                    path,
                    query,
                    None,
                    None,
                    Some(&token),
                    Pacing::Paced,
                )
                .await
            {
                Ok(reply) => interpret(path, reply, sensitivity, self.now()),
                Err(error) => Err(error),
            };
            let error = match outcome {
                Ok(body) => return Ok(body),
                Err(error) => error,
            };
            match &error {
                Error::Api(api)
                    if (api.code.is_access_token_error() || api.http_status == 401)
                        && !renewed
                        && self.auth().can_renew_after_401() =>
                {
                    renewed = true;
                    self.auth().invalidate(&token);
                }
                Error::RateLimited(limited) if throttled < config.max_rate_limit_retries => {
                    let wait = limited.retry_after.unwrap_or_else(|| {
                        Duration::from_secs(1) * 2_u32.saturating_pow(throttled.min(5))
                    });
                    if wait > config.max_rate_limit_wait {
                        return Err(error);
                    }
                    throttled += 1;
                    tracing::warn!(
                        endpoint = path,
                        wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                        attempt = throttled,
                        "SSI throttled GET (HTTP 429); waiting"
                    );
                    self.inner.transport.pause_for(path, wait);
                }
                _ if error.is_transient()
                    && !matches!(error, Error::RateLimited(_) | Error::AuthBackoff { .. })
                    && transient < config.max_transient_retries =>
                {
                    transient += 1;
                    let wait = Duration::from_millis(500) * 2_u32.saturating_pow(transient.min(6));
                    tracing::warn!(endpoint = path, %error, attempt = transient, "retrying GET");
                    tokio::time::sleep(wait).await;
                }
                _ => return Err(error),
            }
        }
    }

    /// Authenticated binary `GET` (`/api/v3/data/download`) with 401 Bearer renewal and bounded
    /// retries on 429 / 503 / transport failures.
    ///
    /// Per SSI's `data-marketFileDownload` rules: 403 (invalid/expired/used download token) and
    /// 404 fail immediately without invalidating the Bearer access token; 429, 503 and
    /// interrupted transport calls do not invalidate the one-time download link and are retried.
    #[cfg(feature = "data")]
    pub(crate) async fn get_bytes(&self, path: &str, query: &[(&str, String)]) -> Result<Vec<u8>> {
        let config = &self.inner.config;
        let host = Host::for_path(path);
        let mut renewed = false;
        let mut throttled: u32 = 0;
        let mut transient: u32 = 0;
        loop {
            let token = self.auth().access_token().await?;
            let outcome = match self
                .inner
                .transport
                .send_binary(host, path, query, &token, Pacing::Paced)
                .await
            {
                Ok(reply) => interpret_binary(path, reply, self.now()),
                Err(error) => Err(error),
            };
            let error = match outcome {
                Ok(bytes) => return Ok(bytes),
                Err(error) => error,
            };
            match &error {
                Error::Api(api)
                    if (api.http_status == 401
                        || (api.http_status != 403 && api.code.is_access_token_error()))
                        && !renewed
                        && self.auth().can_renew_after_401() =>
                {
                    renewed = true;
                    self.auth().invalidate(&token);
                }
                Error::RateLimited(limited) if throttled < config.max_rate_limit_retries => {
                    let wait = limited.retry_after.unwrap_or_else(|| {
                        Duration::from_secs(1) * 2_u32.saturating_pow(throttled.min(5))
                    });
                    if wait > config.max_rate_limit_wait {
                        return Err(error);
                    }
                    throttled += 1;
                    tracing::warn!(
                        endpoint = path,
                        wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                        attempt = throttled,
                        "SSI throttled binary download (HTTP 429); waiting"
                    );
                    self.inner.transport.pause_for(path, wait);
                }
                _ if error.is_transient()
                    && !matches!(error, Error::RateLimited(_) | Error::AuthBackoff { .. })
                    && transient < config.max_transient_retries =>
                {
                    transient += 1;
                    let wait = Duration::from_millis(500) * 2_u32.saturating_pow(transient.min(6));
                    tracing::warn!(
                        endpoint = path,
                        %error,
                        attempt = transient,
                        "retrying binary GET"
                    );
                    tokio::time::sleep(wait).await;
                }
                _ => return Err(error),
            }
        }
    }

    /// Sends one RSA-signed mutating request (`POST`, `PUT`, `DELETE`).
    ///
    /// Requires an authenticated session (never auto-logs in without OTP), signs the exact UTF-8
    /// `body_json` bytes as lowercase hex in `X-Signature`, and sends once with **zero retries**:
    /// a timeout or transport error leaves the order state unknown, so the caller must reconcile
    /// through the order book or order events.
    #[cfg(feature = "trading")]
    pub(crate) async fn send_signed_body(
        &self,
        method: Method,
        path: &str,
        body_json: String,
    ) -> Result<Vec<u8>> {
        let Some(signer) = self.inner.signer.as_ref() else {
            return Err(Error::InvalidPrivateKey(
                "private key is required for signed trading commands (set SSI_PRIVATE_KEY or Credentials::with_private_key)"
                    .to_owned(),
            ));
        };
        let token = self.auth().trading_access_token().await?;
        let signature_hex = signer.sign_hex(body_json.as_bytes())?;
        let reply = self
            .inner
            .transport
            .send(
                Host::for_path(path),
                method,
                path,
                &[],
                Some(body_json.into_bytes()),
                Some(&signature_hex),
                Some(&token),
                Pacing::Immediate,
            )
            .await?;
        let parsed = interpret(path, reply, Sensitivity::Private, self.now());
        if let Err(Error::Api(ref api)) = parsed
            && (api.code.is_access_token_error() || api.http_status == 401)
        {
            self.auth().invalidate(&token);
        }
        parsed?.ok_or_else(|| Error::Decode {
            endpoint: path.to_owned(),
            detail: "empty trading response body".to_owned(),
        })
    }
}

fn check_raw_path(path: &str) -> Result<()> {
    let allowed = path
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.'));
    let segments_ok = path.strip_prefix("/api/v3/").is_some_and(|rest| {
        !rest.is_empty()
            && rest
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
    });
    if allowed && segments_ok {
        Ok(())
    } else {
        Err(Error::InvalidRequest(
            "raw requests are limited to plain /api/v3/ paths".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::check_raw_path;

    #[test]
    fn raw_paths_stay_under_api_v3() {
        assert!(check_raw_path("/api/v3/data/indexList").is_ok());
        assert!(check_raw_path("/api/v3/account/info").is_ok());
        for bad in [
            "/api/v3/../../internal/admin",
            "/api/v3/data/./x",
            "/api/v3/%2e%2e/x",
            "/api/v3//x",
            "/api/v3/x?y=1",
            "/internal",
            "/api/v3/",
            "/api/v3/.\t./internal",
            "/api/v3/x/..;/admin",
        ] {
            assert!(check_raw_path(bad).is_err(), "{bad} must be rejected");
        }
    }
}
