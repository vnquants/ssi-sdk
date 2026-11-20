//! Shared example setup: tracing (with `tungstenite=debug` redaction) and safe dotenv loading.
#![allow(dead_code, reason = "each example binary uses a subset of helpers")]

use std::path::Path;

use ssi_sdk::{Config, Credentials, SsiClient};

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Filter directives that keep `Authorization: Bearer` tokens out of TRACE logs.
pub(crate) const REDACTING_DIRECTIVES: [&str; 2] = ["tungstenite=debug", "tokio_tungstenite=debug"];

pub(crate) fn env_filter() -> tracing_subscriber::EnvFilter {
    let mut filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    for directive in REDACTING_DIRECTIVES {
        if let Ok(directive) = directive.parse() {
            filter = filter.add_directive(directive);
        }
    }
    filter
}

pub(crate) fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(env_filter())
        .init();
}

pub(crate) fn credentials_from_dotenv(path: &Path) -> Result<Credentials, BoxError> {
    if !path.is_file() {
        return Err("SSI_DOTENV must name a readable file".into());
    }
    let mut key = None;
    let mut secret = None;
    #[cfg(feature = "trading")]
    let mut private_key = None;
    let mut client_id = None;
    let lines = dotenvy::from_path_iter(path).map_err(|_| "cannot read the SSI_DOTENV file")?;
    for line in lines {
        match line {
            Ok((name, value)) => match name.as_str() {
                "SSI_API_KEY" => key = Some(value),
                "SSI_API_SECRET" => secret = Some(value),
                #[cfg(feature = "trading")]
                "SSI_PRIVATE_KEY" => private_key = Some(value),
                "SSI_CLIENT_ID" => client_id = Some(value),
                _ => {}
            },
            // Skip malformed lines without echoing potentially secret values.
            Err(dotenvy::Error::LineParse(..)) => {}
            Err(_) => return Err("cannot read the SSI_DOTENV file".into()),
        }
    }
    let key = key.ok_or("SSI_API_KEY is missing from the dotenv file")?;
    let secret = secret.ok_or("SSI_API_SECRET is missing from the dotenv file")?;
    let mut creds = Credentials::new(key, secret)?;
    #[cfg(feature = "trading")]
    if let Some(pk) = private_key.filter(|s| !s.trim().is_empty()) {
        creds = creds.with_private_key(pk)?;
    }
    if let Some(cid) = client_id.filter(|s| !s.trim().is_empty()) {
        creds = creds.with_client_id(cid);
    }
    Ok(creds)
}

pub(crate) fn client() -> Result<SsiClient, BoxError> {
    let credentials = match std::env::var_os("SSI_DOTENV") {
        Some(path) => credentials_from_dotenv(Path::new(&path))?,
        None => Credentials::from_env()?,
    };
    Ok(SsiClient::new(Config::new(credentials))?)
}

/// Authenticates the example client using `SSI_OTP`, `SSI_READ_ONLY=1`, or Smart OTP polling.
#[cfg(feature = "trading")]
pub(crate) async fn authenticate(client: &SsiClient) -> Result<(), BoxError> {
    if std::env::var("SSI_READ_ONLY").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")) {
        let info = client.auth().authenticate_read_only().await?;
        tracing::info!(?info, "authenticated with read-only token");
        return Ok(());
    }
    if let Ok(otp) = std::env::var("SSI_OTP")
        && !otp.trim().is_empty()
    {
        let info = client.auth().authenticate_with_otp(&otp).await?;
        tracing::info!(?info, "authenticated with OTP");
        return Ok(());
    }
    let otp_resp = client.auth().request_otp().await?;
    tracing::info!(
        is_smart_otp = otp_resp.is_smart_otp(),
        message = %otp_resp.message,
        "requested OTP"
    );
    if let Some(tx_id) = otp_resp.transaction_id() {
        tracing::info!("approve Smart OTP push notification on your device...");
        let cancel = tokio_util::sync::CancellationToken::new();
        let info = client.auth().poll_smart_otp(tx_id, cancel).await?;
        tracing::info!(?info, "authenticated with Smart OTP");
        Ok(())
    } else {
        Err("SSI sent a normal OTP; re-run with SSI_OTP=<6-digit-code>".into())
    }
}
