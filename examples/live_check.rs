//! Read-only live check of the REST side (Spike C): token lifetimes, OHLC limits, corporate
//! actions windows, reference data, account queries and rate-limit headers.
//!
//! Logs counts, dates, error codes and header values only: no token, no account number, no
//! balance.
//!
//! ```text
//! SSI_DOTENV=/path/to/.env cargo run --example live_check -- 2026-11-19
//! ```

#[path = "common/mod.rs"]
mod common;

use chrono::{Duration as ChronoDuration, NaiveDate};
use ssi_sdk::{
    Error, SsiClient,
    model::Exchange,
    rest::{CorporateActionsQuery, OhlcQuery, OrderBookQuery, SecuritiesFilter, Timeframe},
};

fn outcome<T>(result: &Result<T, Error>) -> String {
    match result {
        Ok(_) => "ok".to_owned(),
        Err(error) => error
            .api_code()
            .map_or_else(|| error.to_string(), |code| code.to_string()),
    }
}

fn log_rate(client: &SsiClient, step: &str) {
    if let Some(rate) = client.rate_limit() {
        tracing::info!(
            step,
            endpoint = %rate.endpoint,
            limit = ?rate.limit,
            remaining = ?rate.remaining,
            reset = ?rate.reset,
            retry_after = ?rate.retry_after,
            "rate-limit headers"
        );
    }
}

async fn ohlc_checks(client: &SsiClient, day: NaiveDate) -> Result<(), common::BoxError> {
    let year_start = NaiveDate::from_ymd_opt(2025, 1, 1).ok_or("date")?;
    let year_end = NaiveDate::from_ymd_opt(2025, 12, 31).ok_or("date")?;
    for symbol in ["VNM", "FPT"] {
        let bars = client
            .ohlc(&OhlcQuery::days(
                symbol,
                Timeframe::Day1,
                year_start,
                year_end,
            )?)
            .await?;
        tracing::info!(
            symbol,
            bars = bars.len(),
            first = ?bars.first().map(|bar| bar.trading_date),
            last = ?bars.last().map(|bar| bar.trading_date),
            last_close = ?bars.last().map(|bar| bar.close),
            "daily 2025"
        );
        log_rate(client, "ohlc daily");
    }
    for timeframe in Timeframe::ALL {
        let bars = client
            .ohlc(&OhlcQuery::days("VNM", timeframe, day, day)?)
            .await;
        tracing::info!(
            %timeframe,
            %day,
            outcome = %outcome(&bars),
            bars = bars.as_ref().map_or(0, Vec::len),
            first = ?bars.as_ref().ok().and_then(|bars| bars.first().map(|bar| bar.time)),
            last = ?bars.as_ref().ok().and_then(|bars| bars.last().map(|bar| bar.time)),
            "VNM bars for one day"
        );
    }
    let old_day = day - ChronoDuration::days(400);
    let old = client
        .ohlc_page(
            &OhlcQuery::days("VNM", Timeframe::Minute1, old_day, old_day)?,
            1,
        )
        .await;
    tracing::info!(
        %old_day,
        outcome = %outcome(&old),
        bars = old.as_ref().map_or(0, |page| page.bars.len()),
        "1m bars older than one year (unclamped request)"
    );
    let listing = client
        .ohlc(&OhlcQuery::days(
            "VNM",
            Timeframe::Day1,
            NaiveDate::from_ymd_opt(2000, 1, 1).ok_or("date")?,
            day,
        )?)
        .await?;
    tracing::info!(
        bars = listing.len(),
        first = ?listing.first().map(|bar| bar.trading_date),
        first_close = ?listing.first().map(|bar| bar.close),
        "VNM daily history from listing"
    );
    Ok(())
}

async fn corporate_checks(client: &SsiClient, day: NaiveDate) -> Result<(), common::BoxError> {
    let window = CorporateActionsQuery::new(day - ChronoDuration::days(5), day)?;
    let page = client.corporate_actions_page(&window, 1).await?;
    tracing::info!(
        from = %window.from(),
        to = %window.to(),
        items = page.items.len(),
        items_count = ?page.items_count,
        pages = ?page.pages_count,
        with_ratio = page.items.iter().filter(|event| event.ratio.is_some()).count(),
        without_ex_date = page.items.iter().filter(|event| event.ex_right_date.is_none()).count(),
        "corporate actions, one 5-day window"
    );
    log_rate(client, "corporate actions");
    let six_days = client
        .get_raw(
            "/api/v3/data/corporateActions",
            &[
                (
                    "from",
                    (day - ChronoDuration::days(6))
                        .format("%Y/%m/%d")
                        .to_string(),
                ),
                ("to", day.format("%Y/%m/%d").to_string()),
            ],
        )
        .await?;
    tracing::info!(
        status = six_days.status,
        code = ?six_days.json.as_ref().and_then(|body| body.get("code")).cloned(),
        "corporate actions with a 6-day span"
    );
    let month = CorporateActionsQuery::new(day - ChronoDuration::days(18), day)?;
    let events = client.corporate_actions(&month).await?;
    tracing::info!(
        windows = month.windows().len(),
        events = events.len(),
        "corporate actions walker over 19 days"
    );
    Ok(())
}

async fn reference_checks(client: &SsiClient) -> Result<(), common::BoxError> {
    let vnm = client
        .securities(&SecuritiesFilter::Symbol("VNM".to_owned()))
        .await?;
    tracing::info!(
        rows = vnm.len(),
        lot_size = ?vnm.first().and_then(|row| row.lot_size),
        board = ?vnm.first().map(|row| row.board.to_string()),
        "securitiesByBoard VNM"
    );
    let hose = client.indices(Some(&Exchange::Hose)).await?;
    tracing::info!(indices = hose.len(), "indexList HOSE");
    let limits = client.price_limits(None).await?;
    tracing::info!(
        rows = limits.len(),
        date = ?limits.first().map(|row| row.trading_date),
        "masterdata price limits, latest date"
    );
    Ok(())
}

async fn account_checks(client: &SsiClient, day: NaiveDate) {
    let accounts = client.accounts().await;
    tracing::info!(outcome = %outcome(&accounts), "account/info");
    let Ok(accounts) = accounts else { return };
    tracing::info!(
        count = accounts.len(),
        types = ?accounts.iter().map(|account| account.account_type.as_ref().map(ToString::to_string)).collect::<Vec<_>>(),
        "accounts"
    );
    for account in &accounts {
        let positions = client.positions(&account.account_no, false).await;
        let balance = client.balance(&account.account_no).await;
        let query = OrderBookQuery::new(account.account_no.clone())
            .with_date_range(day - ChronoDuration::days(7), day);
        let book = client.order_book(&query).await;
        tracing::info!(
            account_type = ?account.account_type,
            positions = %outcome(&positions),
            balance = %outcome(&balance),
            order_book = %outcome(&book),
            orders = book.as_ref().map_or(0, Vec::len),
            "read-only account queries with the data token"
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), common::BoxError> {
    common::init_tracing();
    let day = std::env::args()
        .nth(1)
        .map(|text| NaiveDate::parse_from_str(&text, "%Y-%m-%d"))
        .transpose()?
        .ok_or("pass the last trading day as YYYY-MM-DD")?;
    let client = common::client()?;

    let issued = client.auth().force_refresh().await?;
    let refreshed = client.auth().force_refresh().await?;
    tracing::info!(
        access_lifetime_s = issued.access_lifetime.as_secs(),
        refresh_lifetime_s = ?issued.refresh_lifetime.map(|lifetime| lifetime.as_secs()),
        epoch_unit = ?issued.epoch_unit,
        refresh_rotated = ?refreshed.refresh_token_rotated,
        refresh_expiry_moved = refreshed.refresh_expires_at != issued.refresh_expires_at,
        "token"
    );
    log_rate(&client, "auth");

    ohlc_checks(&client, day).await?;
    corporate_checks(&client, day).await?;
    reference_checks(&client).await?;
    account_checks(&client, day).await;
    log_rate(&client, "last");
    Ok(())
}
