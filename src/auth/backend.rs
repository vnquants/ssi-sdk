//! Auth HTTP backend for `/api/v3/auth/token`, `/api/v3/auth/refresh`, and (with
//! `feature = "trading"`) `/api/v3/auth/requestOtp`.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use reqwest::Method;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};

#[cfg(feature = "trading")]
use crate::secret::Masked;
use crate::{
    config::Credentials,
    de,
    error::{Error, Result},
    http::{Host, Pacing, Sensitivity, Transport, interpret},
};

#[cfg(feature = "trading")]
pub(crate) const EP_REQUEST_OTP: &str = "/api/v3/auth/requestOtp";
pub(crate) const EP_ACCESS_TOKEN: &str = "/api/v3/auth/token";
pub(crate) const EP_REFRESH_TOKEN: &str = "/api/v3/auth/refresh";

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Response from `POST /api/v3/auth/requestOtp`.
///
/// Both normal OTP (SMS / email / authenticator) and Smart OTP (push-approval) share the same
/// endpoint: SSI sets `transaction_id` only for Smart OTP accounts.
#[cfg(feature = "trading")]
#[derive(Clone)]
pub struct OtpResponse {
    /// Human-readable server message (for example `"OTP sent"`).
    pub message: String,
    transaction_id: Option<SecretString>,
}

#[cfg(feature = "trading")]
impl OtpResponse {
    /// Builds an OTP response (mainly for tests).
    pub fn new(message: impl Into<String>, transaction_id: Option<String>) -> Self {
        let tx = transaction_id
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(SecretString::from);
        Self {
            message: message.into(),
            transaction_id: tx,
        }
    }

    /// Whether the account uses Smart OTP push-approval (`transaction_id` is present).
    pub const fn is_smart_otp(&self) -> bool {
        self.transaction_id.is_some()
    }

    /// The Smart OTP `transactionId` to pass to [`super::TokenManager::poll_smart_otp`] or
    /// [`super::TokenManager::authenticate_with_transaction_id`], if present.
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id
            .as_ref()
            .map(ExposeSecret::expose_secret)
    }
}

#[cfg(feature = "trading")]
impl fmt::Debug for OtpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtpResponse")
            .field("message", &self.message)
            .field("is_smart_otp", &self.is_smart_otp())
            .field(
                "transaction_id",
                &self
                    .transaction_id
                    .as_ref()
                    .map(|tx| Masked::new(tx.expose_secret())),
            )
            .finish()
    }
}

/// How to authenticate at `POST /api/v3/auth/token`.
#[derive(Clone)]
pub(crate) enum AuthMode {
    /// Normal OTP code entered by the user.
    #[cfg(feature = "trading")]
    Otp(SecretString),
    /// Smart OTP push-approval transaction id.
    #[cfg(feature = "trading")]
    TransactionId(SecretString),
    /// Read-only / market-data token without OTP (cannot place orders).
    ReadOnly,
}

impl fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "trading")]
            Self::Otp(code) => f
                .debug_tuple("Otp")
                .field(&Masked::new(code.expose_secret()))
                .finish(),
            #[cfg(feature = "trading")]
            Self::TransactionId(tx) => f
                .debug_tuple("TransactionId")
                .field(&Masked::new(tx.expose_secret()))
                .finish(),
            Self::ReadOnly => f.write_str("ReadOnly"),
        }
    }
}

/// Raw token payload parsed from `/api/v3/auth/token` or `/api/v3/auth/refresh`.
pub(crate) struct RawToken {
    pub(crate) access_token: SecretString,
    pub(crate) refresh_token: Option<SecretString>,
    pub(crate) token_type: Option<String>,
    pub(crate) expires_at: i64,
    pub(crate) refresh_expires_at: Option<i64>,
}

/// Abstracts auth endpoints so [`super::TokenManager`] can be unit-tested offline.
pub(crate) trait AuthBackend: Send + Sync {
    #[cfg(feature = "trading")]
    fn request_otp<'a>(
        &'a self,
        credentials: &'a Credentials,
    ) -> BoxFuture<'a, Result<OtpResponse>>;

    fn issue<'a>(
        &'a self,
        credentials: &'a Credentials,
        mode: &'a AuthMode,
    ) -> BoxFuture<'a, Result<RawToken>>;

    fn refresh<'a>(&'a self, refresh_token: &'a SecretString) -> BoxFuture<'a, Result<RawToken>>;
}

/// Live HTTP implementation of [`AuthBackend`].
pub(crate) struct HttpAuthBackend {
    transport: Arc<Transport>,
}

impl HttpAuthBackend {
    pub(crate) const fn new(transport: Arc<Transport>) -> Self {
        Self { transport }
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(body)
            .map_err(|_| Error::InvalidRequest("auth request body is not JSON".to_owned()))?;
        let reply = self
            .transport
            .send(
                Host::Api,
                Method::POST,
                path,
                &[],
                Some(bytes),
                None,
                None,
                Pacing::Immediate,
            )
            .await?;
        interpret(path, reply, Sensitivity::Private, self.transport.now())?.ok_or_else(|| {
            Error::Decode {
                endpoint: path.to_owned(),
                detail: "empty auth response".to_owned(),
            }
        })
    }
}

impl AuthBackend for HttpAuthBackend {
    #[cfg(feature = "trading")]
    fn request_otp<'a>(
        &'a self,
        credentials: &'a Credentials,
    ) -> BoxFuture<'a, Result<OtpResponse>> {
        Box::pin(async move {
            let body = json!({
                "apiKey": credentials.api_key().expose_secret(),
                "apiSecret": credentials.api_secret().expose_secret(),
            });
            let raw = self.post_json(EP_REQUEST_OTP, &body).await?;
            parse_otp_response(&raw)
        })
    }

    fn issue<'a>(
        &'a self,
        credentials: &'a Credentials,
        mode: &'a AuthMode,
    ) -> BoxFuture<'a, Result<RawToken>> {
        Box::pin(async move {
            let mut payload = serde_json::Map::new();
            payload.insert(
                "apiKey".to_owned(),
                Value::String(credentials.api_key().expose_secret().to_owned()),
            );
            payload.insert(
                "apiSecret".to_owned(),
                Value::String(credentials.api_secret().expose_secret().to_owned()),
            );
            match mode {
                #[cfg(feature = "trading")]
                AuthMode::Otp(otp) => {
                    payload.insert(
                        "otp".to_owned(),
                        Value::String(otp.expose_secret().to_owned()),
                    );
                }
                #[cfg(feature = "trading")]
                AuthMode::TransactionId(tx) => {
                    payload.insert(
                        "transactionId".to_owned(),
                        Value::String(tx.expose_secret().to_owned()),
                    );
                }
                AuthMode::ReadOnly => {}
            }
            let raw = self
                .post_json(EP_ACCESS_TOKEN, &Value::Object(payload))
                .await?;
            parse_token_response(EP_ACCESS_TOKEN, &raw)
        })
    }

    fn refresh<'a>(&'a self, refresh_token: &'a SecretString) -> BoxFuture<'a, Result<RawToken>> {
        Box::pin(async move {
            let body = json!({
                "refreshToken": refresh_token.expose_secret(),
            });
            let raw = self.post_json(EP_REFRESH_TOKEN, &body).await?;
            parse_token_response(EP_REFRESH_TOKEN, &raw)
        })
    }
}

#[cfg(feature = "trading")]
#[derive(Deserialize)]
struct WireOtp {
    #[serde(default, deserialize_with = "de::string")]
    message: String,
    #[serde(default, alias = "msg", deserialize_with = "de::string")]
    msg: String,
    #[serde(
        default,
        rename = "transactionId",
        alias = "transaction_id",
        deserialize_with = "de::opt_string"
    )]
    transaction_id: Option<String>,
    #[serde(default)]
    data: Option<Box<Self>>,
}

#[cfg(feature = "trading")]
fn parse_otp_response(bytes: &[u8]) -> Result<OtpResponse> {
    let mut parsed: WireOtp = serde_json::from_slice(bytes).map_err(|_| Error::Decode {
        endpoint: EP_REQUEST_OTP.to_owned(),
        detail: "unexpected requestOtp payload".to_owned(),
    })?;
    let inner = parsed.data.take().map_or(parsed, |nested| *nested);
    let message = if inner.message.is_empty() {
        inner.msg
    } else {
        inner.message
    };
    Ok(OtpResponse::new(message, inner.transaction_id))
}

#[derive(Deserialize)]
struct WireTokenPayload {
    #[serde(
        default,
        rename = "accessToken",
        alias = "access_token",
        deserialize_with = "de::opt_string"
    )]
    access_token: Option<String>,
    #[serde(
        default,
        rename = "refreshToken",
        alias = "refresh_token",
        deserialize_with = "de::opt_string"
    )]
    refresh_token: Option<String>,
    #[serde(
        default,
        rename = "tokenType",
        alias = "token_type",
        deserialize_with = "de::opt_string"
    )]
    token_type: Option<String>,
    #[serde(
        default,
        rename = "expiresAt",
        alias = "expires_at",
        deserialize_with = "de::opt_i64"
    )]
    expires_at: Option<i64>,
    #[serde(
        default,
        rename = "refreshExpiresAt",
        alias = "refresh_expires_at",
        deserialize_with = "de::opt_i64"
    )]
    refresh_expires_at: Option<i64>,
    #[serde(default)]
    data: Option<Box<Self>>,
}

pub(crate) fn parse_token_response(endpoint: &str, bytes: &[u8]) -> Result<RawToken> {
    let mut parsed: WireTokenPayload =
        serde_json::from_slice(bytes).map_err(|_| Error::Decode {
            endpoint: endpoint.to_owned(),
            detail: "unexpected token payload".to_owned(),
        })?;
    let payload = parsed.data.take().map_or(parsed, |nested| *nested);
    let access_token = payload.access_token.ok_or_else(|| Error::Decode {
        endpoint: endpoint.to_owned(),
        detail: "token response missing accessToken".to_owned(),
    })?;
    let expires_at = payload.expires_at.ok_or_else(|| Error::Decode {
        endpoint: endpoint.to_owned(),
        detail: "token response missing expiresAt".to_owned(),
    })?;
    Ok(RawToken {
        access_token: SecretString::from(access_token),
        refresh_token: payload.refresh_token.map(SecretString::from),
        token_type: payload.token_type,
        expires_at,
        refresh_expires_at: payload.refresh_expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_and_wrapped_tokens() {
        let wrapped = parse_token_response(
            EP_ACCESS_TOKEN,
            br#"{"data":{"accessToken":"tok1","refreshToken":"ref1","tokenType":"Bearer","expiresAt":1780000900,"refreshExpiresAt":1780028800}}"#,
        )
        .expect("wrapped");
        assert_eq!(wrapped.access_token.expose_secret(), "tok1");
        assert_eq!(
            wrapped
                .refresh_token
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("ref1")
        );
        assert_eq!(wrapped.expires_at, 1_780_000_900);
    }

    #[cfg(feature = "trading")]
    #[test]
    fn parses_otp_and_masks_transaction_id() {
        let otp = parse_otp_response(br#"{"message":"Push sent","transactionId":"tx-secret-1"}"#)
            .expect("otp");
        assert!(otp.is_smart_otp());
        assert_eq!(otp.transaction_id(), Some("tx-secret-1"));
        assert!(!format!("{otp:?}").contains("tx-secret-1"));

        let normal = parse_otp_response(br#"{"message":"SMS sent"}"#).expect("normal");
        assert!(!normal.is_smart_otp());
        assert_eq!(normal.transaction_id(), None);
    }
}
