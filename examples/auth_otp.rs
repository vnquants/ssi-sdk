//! Demonstrates SSI `FastConnect` V3 OTP / Smart OTP authentication and token inspection.
//!
//! Usage:
//! ```bash
//! SSI_DOTENV=/path/to/.env cargo run --example auth_otp
//! SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --example auth_otp
//! ```

mod common;

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let client = common::client()?;
    common::authenticate(&client).await?;

    if let Some(info) = client.auth().info() {
        tracing::info!(
            source = ?info.source,
            expires_at = %info.expires_at,
            refresh_expires_at = ?info.refresh_expires_at,
            access_lifetime_s = info.access_lifetime.as_secs(),
            "session ready"
        );
    }
    let accounts = client.accounts().await?;
    tracing::info!(count = accounts.len(), "accessible trading accounts");
    for account in &accounts {
        tracing::info!(account = %account.account_no, kind = ?account.account_type, "account");
    }
    Ok(())
}
