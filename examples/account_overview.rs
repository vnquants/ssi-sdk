//! Read-only account overview: lists accounts, cash balances, positions, PPMMR, and today's
//! order book. Places no orders.
//!
//! Usage:
//! ```bash
//! SSI_DOTENV=/path/to/.env SSI_OTP=123456 cargo run --example account_overview
//! ```

mod common;

use ssi_sdk::OrderBookQuery;

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let client = common::client()?;
    common::authenticate(&client).await?;

    let accounts = client.accounts().await?;
    tracing::info!(count = accounts.len(), "loaded accounts");

    for account in &accounts {
        let balance = client.balance(&account.account_no).await?;
        let positions = client.positions(&account.account_no, true).await?;
        let ppmmr = client.ppmmr(&account.account_no).await?;
        let orders = client
            .order_book(&OrderBookQuery::new(account.account_no.clone()))
            .await?;

        tracing::info!(
            account = %account.account_no,
            kind = ?account.account_type,
            has_equity_balance = balance.equity.is_some(),
            has_derivative_balance = balance.derivative.is_some(),
            equity_positions = positions.equity.len(),
            derivative_open = positions
                .derivative
                .as_ref()
                .map_or(0, |d| d.open_positions.len()),
            has_equity_ppmmr = ppmmr.equity.is_some(),
            has_derivative_ppmmr = ppmmr.derivative.is_some(),
            orders = orders.len(),
            "account overview"
        );
    }
    Ok(())
}
