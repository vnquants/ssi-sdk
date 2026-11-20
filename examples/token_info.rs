//! Obtains a data token, refreshes it once, and logs the observed lifetimes.
//!
//! Only lifetimes and fingerprints are logged, never a token.
//!
//! ```text
//! SSI_DOTENV=/path/to/.env cargo run --example token_info
//! ```

#[path = "common/mod.rs"]
mod common;

use ssi_sdk::auth::TokenInfo;

fn log(label: &str, info: &TokenInfo) {
    tracing::info!(
        label,
        source = ?info.source,
        token_type = %info.token_type,
        epoch_unit = ?info.epoch_unit,
        obtained_at = %info.obtained_at,
        expires_at = %info.expires_at,
        refresh_expires_at = ?info.refresh_expires_at,
        access_lifetime_s = info.access_lifetime.as_secs(),
        refresh_lifetime_s = info.refresh_lifetime.map(|lifetime| lifetime.as_secs()),
        access_fp = format_args!("{:08x}", info.access_fingerprint),
        refresh_fp = ?info.refresh_fingerprint.map(|fp| format!("{fp:08x}")),
        refresh_rotated = ?info.refresh_token_rotated,
        "token lifetime"
    );
}

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let client = common::client()?;
    let issued = client.auth().force_refresh().await?;
    log("issued", &issued);
    let refreshed = client.auth().force_refresh().await?;
    log("refreshed", &refreshed);
    tracing::info!(
        refresh_window_extended = refreshed.refresh_expires_at != issued.refresh_expires_at,
        next_renewal_in_s = client
            .auth()
            .time_until_renewal()
            .map(|wait| wait.as_secs()),
        "refresh summary"
    );
    if let Some(rate) = client.rate_limit() {
        tracing::info!(
            endpoint = %rate.endpoint,
            limit = ?rate.limit,
            remaining = ?rate.remaining,
            reset = ?rate.reset,
            "rate-limit headers"
        );
    }
    Ok(())
}
