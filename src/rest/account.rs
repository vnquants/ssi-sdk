//! Read-only account, balance, position, PPMMR, order book, and max buy/sell REST queries.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    client::SsiClient,
    de,
    error::{Error, Result},
    http::Sensitivity,
    model::{AccountNo, AccountType, OrderPrice, OrderSide, OrderStatus, OrderType, check_symbol},
    rest::decode_unwrapped,
    time::format_vn_date,
};

pub(crate) const EP_ACCOUNT_INFO: &str = "/api/v3/account/info";
pub(crate) const EP_ACCOUNT_BALANCE: &str = "/api/v3/trading/accountBalance";
pub(crate) const EP_POSITION: &str = "/api/v3/trading/position";
pub(crate) const EP_PPMMR: &str = "/api/v3/trading/ppmmrAccount";
pub(crate) const EP_ORDER_BOOK: &str = "/api/v3/trading/orderBook";
#[cfg(feature = "trading")]
pub(crate) const EP_MAX_BUY_SELL: &str = "/api/v3/trading/maxBuySell";

/// Safety cap on pages fetched by [`SsiClient::order_book`].
const MAX_ORDER_BOOK_PAGES: u32 = 200;

/// One accessible trading account from `GET /api/v3/account/info`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Account {
    /// Account number (masked in `Debug` / `Display`).
    #[serde(rename = "accountNo")]
    pub account_no: AccountNo,
    /// Account type (`Cash`, `Margin`, `Derivative`, or unknown wire text), if sent.
    #[serde(default, rename = "accountType")]
    pub account_type: Option<AccountType>,
}

/// Equity account cash balance section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EquityAccountBalance {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Account cash balance.
    #[serde(
        default,
        rename = "accountBalance",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_balance: Decimal,
    /// Total margin debt.
    #[serde(
        default,
        rename = "totalDebt",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_debt: Decimal,
    /// Accrued loan interest.
    #[serde(
        default,
        rename = "interestLoan",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub interest_loan: Decimal,
    /// Overdue loan fee.
    #[serde(
        default,
        rename = "overdueFeeLoan",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub overdue_fee_loan: Decimal,
    /// Cash withdrawable.
    #[serde(
        default,
        rename = "withdrawable",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable: Decimal,
    /// Cash on hold.
    #[serde(
        default,
        rename = "onHoldCash",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub on_hold_cash: Decimal,
    /// Unmatched sell value.
    #[serde(
        default,
        rename = "sellUnmatched",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub sell_unmatched: Decimal,
    /// Sell settlement T+0.
    #[serde(default, rename = "sellT0", deserialize_with = "de::decimal_or_zero")]
    pub sell_t0: Decimal,
    /// Sell settlement T+1.
    #[serde(default, rename = "sellT1", deserialize_with = "de::decimal_or_zero")]
    pub sell_t1: Decimal,
    /// Sell settlement T+2.
    #[serde(default, rename = "sellT2", deserialize_with = "de::decimal_or_zero")]
    pub sell_t2: Decimal,
    /// Unmatched buy value.
    #[serde(
        default,
        rename = "buyUnmatched",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub buy_unmatched: Decimal,
    /// Buy settlement T+0.
    #[serde(default, rename = "buyT0", deserialize_with = "de::decimal_or_zero")]
    pub buy_t0: Decimal,
    /// Buy settlement T+1.
    #[serde(default, rename = "buyT1", deserialize_with = "de::decimal_or_zero")]
    pub buy_t1: Decimal,
    /// Buy settlement T+2.
    #[serde(default, rename = "buyT2", deserialize_with = "de::decimal_or_zero")]
    pub buy_t2: Decimal,
    /// Cash advanced T+0 (`advancedCashT0` / `advanceCashT0`).
    #[serde(
        default,
        rename = "advancedCashT0",
        alias = "advanceCashT0",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub advance_cash_t0: Decimal,
    /// Cash advanced T+1 (`advancedCashT1` / `advanceCashT1`).
    #[serde(
        default,
        rename = "advancedCashT1",
        alias = "advanceCashT1",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub advance_cash_t1: Decimal,
    /// Cash held for rights subscription.
    #[serde(
        default,
        rename = "holdSubscription",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub hold_subscription: Decimal,
    /// Pending cash dividend.
    #[serde(default, rename = "dividend", deserialize_with = "de::decimal_or_zero")]
    pub dividend: Decimal,
}

/// Derivative account cash balance section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DerivativeAccountBalance {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Account cash balance.
    #[serde(
        default,
        rename = "accountBalance",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_balance: Decimal,
    /// Exchange/clearing fee.
    #[serde(default, rename = "fee", deserialize_with = "de::decimal_or_zero")]
    pub fee: Decimal,
    /// Broker commission.
    #[serde(
        default,
        rename = "commission",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub commission: Decimal,
    /// Interest.
    #[serde(default, rename = "interest", deserialize_with = "de::decimal_or_zero")]
    pub interest: Decimal,
    /// External interest.
    #[serde(
        default,
        rename = "extInterest",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub ext_interest: Decimal,
    /// Loan amount.
    #[serde(default, rename = "loan", deserialize_with = "de::decimal_or_zero")]
    pub loan: Decimal,
    /// Delivery amount.
    #[serde(
        default,
        rename = "deliveryAmount",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub delivery_amount: Decimal,
    /// Unrealized floating P&L.
    #[serde(
        default,
        rename = "floatingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub floating_pl: Decimal,
    /// Realized trading P&L.
    #[serde(
        default,
        rename = "tradingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub trading_pl: Decimal,
    /// Total P&L.
    #[serde(default, rename = "totalPL", deserialize_with = "de::decimal_or_zero")]
    pub total_pl: Decimal,
    /// Total withdrawable cash.
    #[serde(
        default,
        rename = "withdrawable",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable: Decimal,
    /// Cash held at SSI.
    #[serde(default, rename = "cashSSI", deserialize_with = "de::decimal_or_zero")]
    pub cash_ssi: Decimal,
    /// Valid non-cash collateral at SSI.
    #[serde(
        default,
        rename = "validNonCashSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub valid_non_cash_ssi: Decimal,
    /// Withdrawable cash at SSI (`withdrawableSSI` / `cashWithdrawableSSI`).
    #[serde(
        default,
        rename = "withdrawableSSI",
        alias = "cashWithdrawableSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable_ssi: Decimal,
    /// Cash held at VSDC.
    #[serde(default, rename = "cashVSDC", deserialize_with = "de::decimal_or_zero")]
    pub cash_vsdc: Decimal,
    /// Valid non-cash collateral at VSDC.
    #[serde(
        default,
        rename = "validNonCashVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub valid_non_cash_vsdc: Decimal,
    /// Withdrawable cash at VSDC (`withdrawableVSDC` / `cashWithdrawableVSDC`).
    #[serde(
        default,
        rename = "withdrawableVSDC",
        alias = "cashWithdrawableVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable_vsdc: Decimal,
}

/// Response from `GET /api/v3/trading/accountBalance`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct AccountBalance {
    /// Populated for equity (Cash / Margin) accounts.
    #[serde(default, deserialize_with = "de::opt_non_empty_object")]
    pub equity: Option<EquityAccountBalance>,
    /// Populated for derivative accounts.
    #[serde(default, deserialize_with = "de::opt_non_empty_object")]
    pub derivative: Option<DerivativeAccountBalance>,
    /// Server receipt timestamp (`receivedTime`), if present.
    #[serde(
        default,
        rename = "receivedTime",
        deserialize_with = "de::opt_epoch_millis"
    )]
    pub received_time: Option<DateTime<Utc>>,
}

/// One equity holding row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EquityPosition {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Stock/ETF/CW symbol.
    #[serde(default, deserialize_with = "de::string")]
    pub symbol: String,
    /// Total quantity.
    #[serde(default, deserialize_with = "de::i64_or_zero")]
    pub quantity: i64,
    /// Blocked quantity.
    #[serde(
        default,
        rename = "blockQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub block_quantity: i64,
    /// Pending stock dividend quantity.
    #[serde(
        default,
        rename = "dividendQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub dividend_quantity: i64,
    /// Open buy order quantity.
    #[serde(
        default,
        rename = "buyingQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub buying_quantity: i64,
    /// Bought today quantity.
    #[serde(
        default,
        rename = "boughtQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub bought_quantity: i64,
    /// Open sell order quantity.
    #[serde(
        default,
        rename = "sellingQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub selling_quantity: i64,
    /// Sold today quantity.
    #[serde(default, rename = "soldQuantity", deserialize_with = "de::i64_or_zero")]
    pub sold_quantity: i64,
    /// T+1 pending sell quantity.
    #[serde(
        default,
        rename = "t1SellQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub t1_sell_quantity: i64,
    /// T+2 pending sell quantity.
    #[serde(
        default,
        rename = "t2SellQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub t2_sell_quantity: i64,
    /// T+1 pending buy quantity.
    #[serde(
        default,
        rename = "t1BuyQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub t1_buy_quantity: i64,
    /// T+2 pending buy quantity.
    #[serde(
        default,
        rename = "t2BuyQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub t2_buy_quantity: i64,
    /// Average cost price.
    #[serde(
        default,
        rename = "costPrice",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub cost_price: Decimal,
    /// Mortgaged quantity.
    #[serde(
        default,
        rename = "mortgageQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub mortgage_quantity: i64,
    /// Immediately sellable quantity.
    #[serde(
        default,
        rename = "sellableQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub sellable_quantity: i64,
    /// Transfer-restricted quantity.
    #[serde(
        default,
        rename = "restrictedQuantity",
        deserialize_with = "de::i64_or_zero"
    )]
    pub restricted_quantity: i64,
}

/// One derivative contract position row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DerivativePosition {
    /// Account number (backfilled from the requested account if omitted by SSI).
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Derivative contract symbol (e.g. `VN30F2612`).
    #[serde(default, deserialize_with = "de::string")]
    pub symbol: String,
    /// Long contracts.
    #[serde(default, deserialize_with = "de::i64_or_zero")]
    pub long: i64,
    /// Short contracts.
    #[serde(default, deserialize_with = "de::i64_or_zero")]
    pub short: i64,
    /// Net contracts (`long - short`).
    #[serde(default, deserialize_with = "de::i64_or_zero")]
    pub net: i64,
    /// Average bid (long) price.
    #[serde(
        default,
        rename = "bidAvgPrice",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub bid_avg_price: Decimal,
    /// Average ask (short) price.
    #[serde(
        default,
        rename = "askAvgPrice",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub ask_avg_price: Decimal,
    /// Latest trade price.
    #[serde(
        default,
        rename = "tradePrice",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub trade_price: Decimal,
    /// Unrealized floating P&L.
    #[serde(
        default,
        rename = "floatingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub floating_pl: Decimal,
    /// Realized trading P&L.
    #[serde(
        default,
        rename = "tradingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub trading_pl: Decimal,
}

/// Derivative open and closed positions for an account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DerivativePositions {
    /// Account number.
    pub account_no: AccountNo,
    /// Open derivative positions (`derOpenPositions`, or a bare list).
    pub open_positions: Vec<DerivativePosition>,
    /// Closed derivative positions (`derClosePositions`).
    pub closed_positions: Vec<DerivativePosition>,
}

/// Response from `GET /api/v3/trading/position`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Positions {
    /// Equity positions, if any.
    pub equity: Vec<EquityPosition>,
    /// Derivative positions, if the account has a derivative section.
    pub derivative: Option<DerivativePositions>,
    /// Server receipt timestamp (`receivedTime`), if present.
    pub received_time: Option<DateTime<Utc>>,
}

/// Equity purchasing power and maintenance margin details (`GET /api/v3/trading/ppmmrAccount`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EquityPpmmr {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Cash dividend.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub dividend: Decimal,
    /// Loan value.
    #[serde(
        default,
        rename = "loanValue",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub loan_value: Decimal,
    /// Total debt.
    #[serde(
        default,
        rename = "totalDebt",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_debt: Decimal,
    /// Debt.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub debt: Decimal,
    /// Liability.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub liability: Decimal,
    /// Liability (SSI calculation).
    #[serde(
        default,
        rename = "liabilitySSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub liability_ssi: Decimal,
    /// Net liability.
    #[serde(
        default,
        rename = "netLiability",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub net_liability: Decimal,
    /// Fees.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub fees: Decimal,
    /// Interest (SSI).
    #[serde(
        default,
        rename = "interestSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub interest_ssi: Decimal,
    /// Interest (SPV).
    #[serde(
        default,
        rename = "interestSPV",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub interest_spv: Decimal,
    /// Withdrawable cash.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub withdrawable: Decimal,
    /// Excess equity (`ee`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee: Decimal,
    /// Excess equity at 50% margin (`ee50`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee50: Decimal,
    /// Excess equity at 60% margin (`ee60`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee60: Decimal,
    /// Excess equity at 70% margin (`ee70`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee70: Decimal,
    /// Excess equity at 80% margin (`ee80`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee80: Decimal,
    /// Excess equity at 90% margin (`ee90`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ee90: Decimal,
    /// Action amount.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub action: Decimal,
    /// Action amount (SSI).
    #[serde(
        default,
        rename = "actionSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub action_ssi: Decimal,
    /// Equity.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub equity: Decimal,
    /// Equity (SSI).
    #[serde(
        default,
        rename = "equitySSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub equity_ssi: Decimal,
    /// Cash excess equity (`eeCash`).
    #[serde(default, rename = "eeCash", deserialize_with = "de::decimal_or_zero")]
    pub ee_cash: Decimal,
    /// Hold subscription.
    #[serde(
        default,
        rename = "holdSubscription",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub hold_subscription: Decimal,
    /// Linked bank balance.
    #[serde(
        default,
        rename = "bankBalance",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub bank_balance: Decimal,
    /// On-hold cash.
    #[serde(
        default,
        rename = "onHoldCash",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub on_hold_cash: Decimal,
    /// Overdue debt (`doverdue`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub doverdue: Decimal,
    /// Overdue debt SSI (`doverdue_ssi` / `doverdueSSI`).
    #[serde(
        default,
        rename = "doverdue_ssi",
        alias = "doverdueSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub doverdue_ssi: Decimal,
    /// Account cash balance.
    #[serde(
        default,
        rename = "accountBalance",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_balance: Decimal,
    /// Debt `D` (`D` / `d`).
    #[serde(
        default,
        rename = "D",
        alias = "d",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub d: Decimal,
    /// Debt SPV (`dSPV` / `dspv`).
    #[serde(
        default,
        rename = "dSPV",
        alias = "dspv",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub d_spv: Decimal,
    /// Debt SSI (`dSSI` / `dssi`).
    #[serde(
        default,
        rename = "dSSI",
        alias = "dssi",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub d_ssi: Decimal,
    /// Cash in advance (`cia`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub cia: Decimal,
    /// Collateral asset.
    #[serde(
        default,
        rename = "collateralAsset",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_asset: Decimal,
    /// Collateral asset (SSI).
    #[serde(
        default,
        rename = "collateralAssetSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_asset_ssi: Decimal,
    /// Total assets.
    #[serde(
        default,
        rename = "totalAssets",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_assets: Decimal,
    /// Total equity.
    #[serde(
        default,
        rename = "totalEquity",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_equity: Decimal,
    /// Total equity (SSI).
    #[serde(
        default,
        rename = "totalEquitySSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_equity_ssi: Decimal,
    /// Long market value (`lmv`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub lmv: Decimal,
    /// Marginable LMV.
    #[serde(
        default,
        rename = "lmvMargin",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub lmv_margin: Decimal,
    /// Marginable LMV (SSI).
    #[serde(
        default,
        rename = "lmvMarginSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub lmv_margin_ssi: Decimal,
    /// Call LMV (`callLMV` / `callLmv`).
    #[serde(
        default,
        rename = "callLMV",
        alias = "callLmv",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_lmv: Decimal,
    /// Force-sell LMV (`forceLMV` / `forceLmv`).
    #[serde(
        default,
        rename = "forceLMV",
        alias = "forceLmv",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub force_lmv: Decimal,
    /// Call LMV SSI (`callLMVSSI` / `callLmvSSI`).
    #[serde(
        default,
        rename = "callLMVSSI",
        alias = "callLmvSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_lmv_ssi: Decimal,
    /// Force-sell LMV SSI (`forceLMVSSI` / `forceLmvSSI`).
    #[serde(
        default,
        rename = "forceLMVSSI",
        alias = "forceLmvSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub force_lmv_ssi: Decimal,
    /// Non-marginable LMV.
    #[serde(
        default,
        rename = "lmvNonMarginable",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub lmv_non_marginable: Decimal,
    /// Non-marginable LMV (SSI).
    #[serde(
        default,
        rename = "lmvNonMarginableSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub lmv_non_marginable_ssi: Decimal,
    /// Pre-loan.
    #[serde(default, rename = "preLoan", deserialize_with = "de::decimal_or_zero")]
    pub pre_loan: Decimal,
    /// Margin ratio.
    #[serde(
        default,
        rename = "marginRatio",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_ratio: Decimal,
    /// Margin ratio (SSI).
    #[serde(
        default,
        rename = "marginRatioSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_ratio_ssi: Decimal,
    /// Purchasing power.
    #[serde(
        default,
        rename = "purchasingPower",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub purchasing_power: Decimal,
    /// Original excess equity (`eeOrigin`).
    #[serde(default, rename = "eeOrigin", deserialize_with = "de::decimal_or_zero")]
    pub ee_origin: Decimal,
    /// Unmatched buy value.
    #[serde(
        default,
        rename = "buyUnmatched",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub buy_unmatched: Decimal,
    /// Unmatched sell value.
    #[serde(
        default,
        rename = "sellUnmatched",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub sell_unmatched: Decimal,
    /// Buy T+0.
    #[serde(default, rename = "buyT0", deserialize_with = "de::decimal_or_zero")]
    pub buy_t0: Decimal,
    /// Buy T+1.
    #[serde(default, rename = "buyT1", deserialize_with = "de::decimal_or_zero")]
    pub buy_t1: Decimal,
    /// Buy T+2.
    #[serde(default, rename = "buyT2", deserialize_with = "de::decimal_or_zero")]
    pub buy_t2: Decimal,
    /// Sell T+0.
    #[serde(default, rename = "sellT0", deserialize_with = "de::decimal_or_zero")]
    pub sell_t0: Decimal,
    /// Sell T+1.
    #[serde(default, rename = "sellT1", deserialize_with = "de::decimal_or_zero")]
    pub sell_t1: Decimal,
    /// Sell T+2.
    #[serde(default, rename = "sellT2", deserialize_with = "de::decimal_or_zero")]
    pub sell_t2: Decimal,
    /// Credit limit.
    #[serde(
        default,
        rename = "creditLimit",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub credit_limit: Decimal,
    /// Margin call LMV sold (`marginCallLMVSold` / `marginCallLmvSold`).
    #[serde(
        default,
        rename = "marginCallLMVSold",
        alias = "marginCallLmvSold",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call_lmv_sold: Decimal,
    /// Margin call LMV sold SSI (`marginCallLMVSoldSSI` / `marginCallLmvSoldSSI`).
    #[serde(
        default,
        rename = "marginCallLMVSoldSSI",
        alias = "marginCallLmvSoldSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call_lmv_sold_ssi: Decimal,
    /// Margin call.
    #[serde(
        default,
        rename = "marginCall",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call: Decimal,
    /// Margin call (SSI).
    #[serde(
        default,
        rename = "marginCallSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call_ssi: Decimal,
    /// Collateral A.
    #[serde(
        default,
        rename = "collateralA",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_a: Decimal,
    /// Non-collateral.
    #[serde(
        default,
        rename = "collateralNon",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_non: Decimal,
    /// Collateral A (SSI).
    #[serde(
        default,
        rename = "collateralASSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_a_ssi: Decimal,
    /// Non-collateral (SSI).
    #[serde(
        default,
        rename = "collateralNonSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub collateral_non_ssi: Decimal,
    /// Call margin threshold.
    #[serde(
        default,
        rename = "callMargin",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_margin: Decimal,
    /// Call force-sell threshold (`callForcesell` / `callForceSell`).
    #[serde(
        default,
        rename = "callForcesell",
        alias = "callForceSell",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_force_sell: Decimal,
    /// Call margin threshold (SSI).
    #[serde(
        default,
        rename = "callMarginSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_margin_ssi: Decimal,
    /// Call force-sell threshold SSI (`callForcesellSSI` / `callForceSellSSI`).
    #[serde(
        default,
        rename = "callForcesellSSI",
        alias = "callForceSellSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub call_force_sell_ssi: Decimal,
    /// Account ratio (`ar`).
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub ar: Decimal,
}

/// Derivative purchasing power and margin details (`GET /api/v3/trading/ppmmrAccount`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DerivativePpmmr {
    /// Account number.
    #[serde(default, rename = "accountNo")]
    pub account_no: AccountNo,
    /// Account cash balance.
    #[serde(
        default,
        rename = "accountBalance",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_balance: Decimal,
    /// Fee.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub fee: Decimal,
    /// Commission.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub commission: Decimal,
    /// Interest.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub interest: Decimal,
    /// Loan.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub loan: Decimal,
    /// Delivery amount.
    #[serde(
        default,
        rename = "deliveryAmount",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub delivery_amount: Decimal,
    /// Floating P&L.
    #[serde(
        default,
        rename = "floatingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub floating_pl: Decimal,
    /// Trading P&L.
    #[serde(
        default,
        rename = "tradingPL",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub trading_pl: Decimal,
    /// Total P&L.
    #[serde(default, rename = "totalPL", deserialize_with = "de::decimal_or_zero")]
    pub total_pl: Decimal,
    /// Marginable amount.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub marginable: Decimal,
    /// Depositable amount.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub depositable: Decimal,
    /// RC call.
    #[serde(default, rename = "rcCall", deserialize_with = "de::decimal_or_zero")]
    pub rc_call: Decimal,
    /// Withdrawable cash.
    #[serde(default, deserialize_with = "de::decimal_or_zero")]
    pub withdrawable: Decimal,
    /// Non-cash drawable RC call (`nonCashDrawableRCCall` / `nonCashDrawableRcCall`).
    #[serde(
        default,
        rename = "nonCashDrawableRCCall",
        alias = "nonCashDrawableRcCall",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub non_cash_drawable_rc_call: Decimal,
    /// Cash at SSI.
    #[serde(default, rename = "cashSSI", deserialize_with = "de::decimal_or_zero")]
    pub cash_ssi: Decimal,
    /// Valid non-cash at SSI.
    #[serde(
        default,
        rename = "validNonCashSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub valid_non_cash_ssi: Decimal,
    /// Total asset at SSI.
    #[serde(
        default,
        rename = "totalAssetSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_asset_ssi: Decimal,
    /// Withdrawable at SSI.
    #[serde(
        default,
        rename = "withdrawableSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable_ssi: Decimal,
    /// Excess equity at SSI.
    #[serde(default, rename = "eeSSI", deserialize_with = "de::decimal_or_zero")]
    pub ee_ssi: Decimal,
    /// Cash at VSDC.
    #[serde(default, rename = "cashVSDC", deserialize_with = "de::decimal_or_zero")]
    pub cash_vsdc: Decimal,
    /// Valid non-cash at VSDC.
    #[serde(
        default,
        rename = "validNonCashVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub valid_non_cash_vsdc: Decimal,
    /// Total asset at VSDC.
    #[serde(
        default,
        rename = "totalAssetVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_asset_vsdc: Decimal,
    /// Withdrawable at VSDC.
    #[serde(
        default,
        rename = "withdrawableVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub withdrawable_vsdc: Decimal,
    /// Excess equity at VSDC.
    #[serde(default, rename = "eeVSDC", deserialize_with = "de::decimal_or_zero")]
    pub ee_vsdc: Decimal,
    /// Spread margin at SSI.
    #[serde(
        default,
        rename = "spreadMarginSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub spread_margin_ssi: Decimal,
    /// Delivery margin at SSI.
    #[serde(
        default,
        rename = "deliveryMarginSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub delivery_margin_ssi: Decimal,
    /// Margin requirement at SSI.
    #[serde(
        default,
        rename = "marginReqSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_req_ssi: Decimal,
    /// Account ratio at SSI.
    #[serde(
        default,
        rename = "accountRatioSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_ratio_ssi: Decimal,
    /// Used limit warning level 1 (SSI).
    #[serde(
        default,
        rename = "usedLimitWarningLevel1SSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_1_ssi: Decimal,
    /// Used limit warning level 2 (SSI).
    #[serde(
        default,
        rename = "usedLimitWarningLevel2SSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_2_ssi: Decimal,
    /// Used limit warning level 3 (SSI).
    #[serde(
        default,
        rename = "usedLimitWarningLevel3SSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_3_ssi: Decimal,
    /// Margin call at SSI.
    #[serde(
        default,
        rename = "marginCallSSI",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call_ssi: Decimal,
    /// Spread margin at VSDC.
    #[serde(
        default,
        rename = "spreadMarginVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub spread_margin_vsdc: Decimal,
    /// Delivery margin at VSDC.
    #[serde(
        default,
        rename = "deliveryMarginVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub delivery_margin_vsdc: Decimal,
    /// Margin requirement at VSDC.
    #[serde(
        default,
        rename = "marginReqVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_req_vsdc: Decimal,
    /// Account ratio at VSDC.
    #[serde(
        default,
        rename = "accountRatioVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub account_ratio_vsdc: Decimal,
    /// Used limit warning level 1 (VSDC).
    #[serde(
        default,
        rename = "usedLimitWarningLevel1VSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_1_vsdc: Decimal,
    /// Used limit warning level 2 (VSDC).
    #[serde(
        default,
        rename = "usedLimitWarningLevel2VSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_2_vsdc: Decimal,
    /// Used limit warning level 3 (VSDC).
    #[serde(
        default,
        rename = "usedLimitWarningLevel3VSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub used_limit_warning_level_3_vsdc: Decimal,
    /// Margin call at VSDC.
    #[serde(
        default,
        rename = "marginCallVSDC",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub margin_call_vsdc: Decimal,
    /// Total equity.
    #[serde(
        default,
        rename = "totalEquity",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub total_equity: Decimal,
    /// External interest.
    #[serde(
        default,
        rename = "extInterest",
        deserialize_with = "de::decimal_or_zero"
    )]
    pub ext_interest: Decimal,
}

/// Response from `GET /api/v3/trading/ppmmrAccount`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct Ppmmr {
    /// Populated for equity accounts.
    #[serde(default, deserialize_with = "de::opt_non_empty_object")]
    pub equity: Option<Box<EquityPpmmr>>,
    /// Populated for derivative accounts.
    #[serde(default, deserialize_with = "de::opt_non_empty_object")]
    pub derivative: Option<Box<DerivativePpmmr>>,
    /// Server receipt timestamp (`receivedTime`), if present.
    #[serde(
        default,
        rename = "receivedTime",
        deserialize_with = "de::opt_epoch_millis"
    )]
    pub received_time: Option<DateTime<Utc>>,
}

/// Query filters for `GET /api/v3/trading/orderBook` and Trading WS `query.orderBook`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookQuery {
    /// Trading account number (required).
    pub account_no: AccountNo,
    /// Start date (`YYYY/MM/DD`), optional.
    pub from: Option<NaiveDate>,
    /// End date (`YYYY/MM/DD`), optional; must be `>= from` when both are set.
    pub to: Option<NaiveDate>,
    /// Optional symbol filter.
    pub symbol: Option<String>,
    /// Optional order status filter.
    pub order_status: Option<OrderStatus>,
    /// 1-based page index (default `1`).
    pub page_index: u32,
    /// Page size (default `100`).
    pub page_size: u32,
}

impl OrderBookQuery {
    /// Builds a query for an account with default paging (`page_index = 1`, `page_size = 100`).
    pub const fn new(account_no: AccountNo) -> Self {
        Self {
            account_no,
            from: None,
            to: None,
            symbol: None,
            order_status: None,
            page_index: 1,
            page_size: 100,
        }
    }

    /// Sets the inclusive date range (`from <= to`).
    #[must_use]
    pub const fn with_date_range(mut self, from: NaiveDate, to: NaiveDate) -> Self {
        self.from = Some(from);
        self.to = Some(to);
        self
    }

    /// Filters by symbol.
    #[must_use]
    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    /// Filters by order status.
    #[must_use]
    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.order_status = Some(status);
        self
    }

    /// Sets 1-based pagination (`page_index`, `page_size`).
    #[must_use]
    pub const fn with_page(mut self, page_index: u32, page_size: u32) -> Self {
        self.page_index = page_index;
        self.page_size = page_size;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.account_no.validate()?;
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from > to
        {
            return Err(Error::InvalidRequest(
                "orderBook from date cannot be after to date".to_owned(),
            ));
        }
        if let Some(symbol) = &self.symbol {
            check_symbol(symbol)?;
        }
        if self.page_index == 0 {
            return Err(Error::InvalidRequest("page_index must be >= 1".to_owned()));
        }
        if self.page_size == 0 {
            return Err(Error::InvalidRequest("page_size must be >= 1".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn to_query_pairs(&self) -> Result<Vec<(&'static str, String)>> {
        self.validate()?;
        let mut params = vec![("accountNo", self.account_no.as_str().to_owned())];
        if let Some(from) = self.from {
            params.push(("from", format_vn_date(from)));
        }
        if let Some(to) = self.to {
            params.push(("to", format_vn_date(to)));
        }
        if let Some(symbol) = &self.symbol {
            params.push(("symbol", check_symbol(symbol)?));
        }
        if let Some(status) = &self.order_status {
            params.push(("orderStatus", status.as_str().to_owned()));
        }
        params.push(("pageIndex", self.page_index.to_string()));
        params.push(("pageSize", self.page_size.to_string()));
        Ok(params)
    }
}

/// One order entry in the order book or order history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    /// Account number.
    pub account_no: AccountNo,
    /// Caller-provided or SDK-generated idempotency key.
    pub client_request_id: String,
    /// SSI order id.
    pub order_id: String,
    /// Symbol (`symbol` or `instrumentId`).
    pub symbol: String,
    /// Order side (`side` or `buySell`).
    pub side: OrderSide,
    /// Order type (`LO`, `ATO`, `ATC`, `MTL`, ...).
    pub order_type: OrderType,
    /// Limit price or market-priced word (`ATO`, `ATC`, `MP`, ...).
    pub price: Option<OrderPrice>,
    /// Average filled price.
    pub avg_price: Decimal,
    /// Original order quantity.
    pub quantity: u64,
    /// Outstanding (open) quantity, when sent by the server.
    pub os_quantity: Option<u64>,
    /// Filled quantity (`filledQty` / `filledQuantity`).
    pub filled_quantity: u64,
    /// Cancelled quantity (`cancelQty` / `cancelQuantity`).
    pub cancel_quantity: u64,
    /// Current order status.
    pub status: OrderStatus,
    /// Input time in UTC (parsed from VN local `YYYY/MM/DD HH:MM:SS`).
    pub input_time: Option<DateTime<Utc>>,
    /// Raw `inputTime` string as sent by SSI.
    pub input_time_raw: String,
    /// Last modification time in UTC.
    pub modified_time: Option<DateTime<Utc>>,
    /// Raw `modifiedTime` string as sent by SSI.
    pub modified_time_raw: String,
    /// Server message or rejection reason.
    pub message: String,
}

/// One page of `GET /api/v3/trading/orderBook`.
///
/// `total_records` is informational only (SSI's paging count can be off); page until an empty
/// page is returned, or use [`SsiClient::order_book`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderBook {
    /// Account number echoed by the response, if present.
    pub account_no: Option<AccountNo>,
    /// Orders on this page.
    pub orders: Vec<Order>,
    /// Server-reported `totalRecord` count.
    pub total_records: u64,
}

/// Response from `GET /api/v3/trading/maxBuySell`.
///
/// For derivative accounts SSI returns only `max_buy_qty` and `max_sell_qty`, leaving
/// `margin_ratio` and `purchasing_power` as `None`.
#[cfg(feature = "trading")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxBuySell {
    /// Account number.
    pub account_no: AccountNo,
    /// Symbol queried (upper-cased).
    pub symbol: String,
    /// Maximum buyable quantity / contracts.
    pub max_buy_qty: u64,
    /// Maximum sellable quantity / contracts.
    pub max_sell_qty: u64,
    /// Margin ratio string/decimal (absent on derivative accounts).
    pub margin_ratio: Option<String>,
    /// Purchasing power (absent on derivative accounts).
    pub purchasing_power: Option<Decimal>,
}

// ── wire helpers ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct WirePositions {
    #[serde(default, deserialize_with = "de::null_as_empty")]
    equity: Vec<EquityPosition>,
    #[serde(default)]
    derivative: Option<Box<RawValue>>,
    #[serde(
        default,
        rename = "receivedTime",
        deserialize_with = "de::opt_epoch_millis"
    )]
    received_time: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct WireDerivativeObject {
    #[serde(default, rename = "accountNo")]
    account_no: Option<AccountNo>,
    #[serde(
        default,
        rename = "derOpenPositions",
        deserialize_with = "de::null_as_empty"
    )]
    open: Vec<DerivativePosition>,
    #[serde(
        default,
        rename = "derClosePositions",
        deserialize_with = "de::null_as_empty"
    )]
    closed: Vec<DerivativePosition>,
}

pub(crate) fn parse_positions(bytes: &[u8], fallback_account: &AccountNo) -> Result<Positions> {
    let wire: WirePositions = decode_unwrapped(EP_POSITION, bytes, Sensitivity::Private)?;
    let derivative = match wire.derivative {
        None => None,
        Some(raw) => {
            let text = raw.get().trim();
            let empty_object = text
                .strip_prefix('{')
                .and_then(|s| s.strip_suffix('}'))
                .is_some_and(|inner| inner.trim().is_empty());
            if text == "null" || empty_object || text == "[]" {
                None
            } else if text.starts_with('[') {
                let mut open: Vec<DerivativePosition> =
                    decode_unwrapped(EP_POSITION, text.as_bytes(), Sensitivity::Private)?;
                backfill_derivative_account(&mut open, fallback_account);
                (!open.is_empty()).then(|| DerivativePositions {
                    account_no: fallback_account.clone(),
                    open_positions: open,
                    closed_positions: Vec::new(),
                })
            } else {
                let mut obj: WireDerivativeObject =
                    decode_unwrapped(EP_POSITION, text.as_bytes(), Sensitivity::Private)?;
                let acct = obj
                    .account_no
                    .filter(|acct| !acct.as_str().is_empty())
                    .unwrap_or_else(|| fallback_account.clone());
                backfill_derivative_account(&mut obj.open, &acct);
                backfill_derivative_account(&mut obj.closed, &acct);
                Some(DerivativePositions {
                    account_no: acct,
                    open_positions: obj.open,
                    closed_positions: obj.closed,
                })
            }
        }
    };
    Ok(Positions {
        equity: wire.equity,
        derivative,
        received_time: wire.received_time,
    })
}

fn backfill_derivative_account(rows: &mut [DerivativePosition], fallback: &AccountNo) {
    for row in rows {
        if row.account_no.as_str().is_empty() {
            row.account_no = fallback.clone();
        }
    }
}

#[derive(Deserialize)]
struct WireOrderRow {
    #[serde(default, rename = "accountNo", deserialize_with = "de::opt_string")]
    account_no: Option<String>,
    #[serde(default, rename = "clientRequestId", deserialize_with = "de::string")]
    client_request_id: String,
    #[serde(default, rename = "orderId", deserialize_with = "de::string")]
    order_id: String,
    #[serde(default, alias = "instrumentId", deserialize_with = "de::string")]
    symbol: String,
    #[serde(default, alias = "buySell", deserialize_with = "de::string")]
    side: String,
    #[serde(default, rename = "orderType", deserialize_with = "de::string")]
    order_type: String,
    #[serde(default, deserialize_with = "de::opt_string")]
    price: Option<String>,
    #[serde(default, rename = "avgPrice", deserialize_with = "de::decimal_or_zero")]
    avg_price: Decimal,
    #[serde(default, deserialize_with = "de::quantity_or_zero")]
    quantity: u64,
    #[serde(
        default,
        rename = "osQuantity",
        alias = "osQty",
        deserialize_with = "de::opt_quantity"
    )]
    os_quantity: Option<u64>,
    #[serde(
        default,
        rename = "filledQty",
        alias = "filledQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    filled_qty: u64,
    #[serde(
        default,
        rename = "cancelQty",
        alias = "cancelQuantity",
        deserialize_with = "de::quantity_or_zero"
    )]
    cancel_qty: u64,
    #[serde(default, rename = "orderStatus", deserialize_with = "de::string")]
    order_status: String,
    #[serde(default, rename = "inputTime", deserialize_with = "de::string")]
    input_time: String,
    #[serde(
        default,
        rename = "modifiedTime",
        alias = "modifyTime",
        deserialize_with = "de::string"
    )]
    modified_time: String,
    #[serde(default, deserialize_with = "de::string")]
    message: String,
}

#[derive(Deserialize)]
struct WireOrderBook {
    #[serde(default, rename = "accountNo", deserialize_with = "de::opt_string")]
    account_no: Option<String>,
    #[serde(
        default,
        rename = "totalRecord",
        deserialize_with = "de::quantity_or_zero"
    )]
    total_record: u64,
    #[serde(default, rename = "orderList", deserialize_with = "de::null_as_empty")]
    order_list: Vec<WireOrderRow>,
}

pub(crate) fn parse_order_book(bytes: &[u8], fallback_account: &AccountNo) -> Result<OrderBook> {
    let wire: WireOrderBook = decode_unwrapped(EP_ORDER_BOOK, bytes, Sensitivity::Private)?;
    let book_account = wire
        .account_no
        .as_deref()
        .map(AccountNo::new)
        .filter(|acct| !acct.as_str().is_empty());
    let row_fallback = book_account.as_ref().unwrap_or(fallback_account);
    let orders = wire
        .order_list
        .into_iter()
        .map(|row| {
            let account_no = row
                .account_no
                .as_deref()
                .map(AccountNo::new)
                .filter(|acct| !acct.as_str().is_empty())
                .unwrap_or_else(|| row_fallback.clone());
            Order {
                account_no,
                client_request_id: row.client_request_id,
                order_id: row.order_id,
                symbol: row.symbol,
                side: OrderSide::parse(&row.side),
                order_type: OrderType::parse(&row.order_type),
                price: row.price.as_deref().and_then(OrderPrice::parse_opt),
                avg_price: row.avg_price,
                quantity: row.quantity,
                os_quantity: row.os_quantity,
                filled_quantity: row.filled_qty,
                cancel_quantity: row.cancel_qty,
                status: OrderStatus::parse(&row.order_status),
                input_time: crate::time::parse_vn_datetime(&row.input_time),
                input_time_raw: row.input_time,
                modified_time: crate::time::parse_vn_datetime(&row.modified_time),
                modified_time_raw: row.modified_time,
                message: row.message,
            }
        })
        .collect();
    Ok(OrderBook {
        account_no: book_account,
        orders,
        total_records: wire.total_record,
    })
}

#[cfg(feature = "trading")]
#[derive(Deserialize)]
struct WireMaxBuySell {
    #[serde(default, rename = "accountNo")]
    account_no: AccountNo,
    #[serde(
        default,
        rename = "maxBuyQty",
        deserialize_with = "de::quantity_or_zero"
    )]
    max_buy_qty: u64,
    #[serde(
        default,
        rename = "maxSellQty",
        deserialize_with = "de::quantity_or_zero"
    )]
    max_sell_qty: u64,
    #[serde(default, rename = "marginRatio", deserialize_with = "de::opt_string")]
    margin_ratio: Option<String>,
    #[serde(
        default,
        rename = "purchasingPower",
        alias = "purchasePower",
        deserialize_with = "de::opt_decimal"
    )]
    purchasing_power: Option<Decimal>,
}

#[cfg(feature = "trading")]
pub(crate) fn parse_max_buy_sell(bytes: &[u8], symbol: &str) -> Result<MaxBuySell> {
    let wire: WireMaxBuySell = decode_unwrapped(EP_MAX_BUY_SELL, bytes, Sensitivity::Private)?;
    Ok(MaxBuySell {
        account_no: wire.account_no,
        symbol: symbol.to_ascii_uppercase(),
        max_buy_qty: wire.max_buy_qty,
        max_sell_qty: wire.max_sell_qty,
        margin_ratio: wire.margin_ratio,
        purchasing_power: wire.purchasing_power,
    })
}

impl SsiClient {
    /// Lists the accounts accessible to the current API key (`GET /api/v3/account/info`).
    pub async fn accounts(&self) -> Result<Vec<Account>> {
        let Some(body) = self
            .get_body(EP_ACCOUNT_INFO, &[], Sensitivity::Private)
            .await?
        else {
            return Ok(Vec::new());
        };
        Ok(
            decode_unwrapped::<Option<Vec<Account>>>(EP_ACCOUNT_INFO, &body, Sensitivity::Private)?
                .unwrap_or_default(),
        )
    }

    /// Queries cash balance for an equity or derivative account
    /// (`GET /api/v3/trading/accountBalance`).
    pub async fn balance(&self, account: &AccountNo) -> Result<AccountBalance> {
        account.validate()?;
        let mut query = vec![("accountNo", account.as_str().to_owned())];
        if let Some(client_id) = self.config().credentials.client_id() {
            query.push(("clientId", client_id.to_owned()));
        }
        let Some(body) = self
            .get_body(EP_ACCOUNT_BALANCE, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(AccountBalance::default());
        };
        decode_unwrapped(EP_ACCOUNT_BALANCE, &body, Sensitivity::Private)
    }

    /// Queries holdings / open & closed derivative positions (`GET /api/v3/trading/position`).
    pub async fn positions(&self, account: &AccountNo, query_summary: bool) -> Result<Positions> {
        account.validate()?;
        let mut query = vec![
            ("accountNo", account.as_str().to_owned()),
            ("querySummary", query_summary.to_string()),
        ];
        if let Some(client_id) = self.config().credentials.client_id() {
            query.push(("clientId", client_id.to_owned()));
        }
        let Some(body) = self
            .get_body(EP_POSITION, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(Positions::default());
        };
        parse_positions(&body, account)
    }

    /// Queries purchasing power and maintenance margin ratio (`GET /api/v3/trading/ppmmrAccount`).
    pub async fn ppmmr(&self, account: &AccountNo) -> Result<Ppmmr> {
        account.validate()?;
        let query = vec![("accountNo", account.as_str().to_owned())];
        let Some(body) = self
            .get_body(EP_PPMMR, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(Ppmmr::default());
        };
        decode_unwrapped(EP_PPMMR, &body, Sensitivity::Private)
    }

    /// Queries one page of the order book (`GET /api/v3/trading/orderBook`).
    pub async fn order_book_page(&self, query: &OrderBookQuery) -> Result<OrderBook> {
        let params = query.to_query_pairs()?;
        let Some(body) = self
            .get_body(EP_ORDER_BOOK, &params, Sensitivity::Private)
            .await?
        else {
            return Ok(OrderBook::default());
        };
        parse_order_book(&body, &query.account_no)
    }

    /// Walks every page of `GET /api/v3/trading/orderBook` until an empty or partial page is
    /// returned (bounded at 200 pages).
    pub async fn order_book(&self, query: &OrderBookQuery) -> Result<Vec<Order>> {
        query.validate()?;
        let mut all = Vec::new();
        let mut page = query.clone();
        for _ in 0..MAX_ORDER_BOOK_PAGES {
            let result = self.order_book_page(&page).await?;
            let count = result.orders.len();
            all.extend(result.orders);
            if count < usize::try_from(page.page_size).unwrap_or(usize::MAX) {
                break;
            }
            page.page_index = page.page_index.saturating_add(1);
        }
        Ok(all)
    }

    /// Queries maximum buy/sell quantity (`GET /api/v3/trading/maxBuySell`).
    ///
    /// Pass `price = None` to use the server's reference/market price; when `Some`, price must be
    /// strictly positive.
    #[cfg(feature = "trading")]
    pub async fn max_buy_sell(
        &self,
        account: &AccountNo,
        symbol: &str,
        price: Option<Decimal>,
    ) -> Result<MaxBuySell> {
        account.validate()?;
        let symbol = check_symbol(symbol)?;
        let mut query = vec![
            ("accountNo", account.as_str().to_owned()),
            ("symbol", symbol.clone()),
        ];
        if let Some(price) = price {
            if price <= Decimal::ZERO {
                return Err(Error::InvalidRequest(
                    "maxBuySell price must be strictly positive".to_owned(),
                ));
            }
            query.push(("price", de::format_plain_decimal(price)));
        }
        let Some(body) = self
            .get_body(EP_MAX_BUY_SELL, &query, Sensitivity::Private)
            .await?
        else {
            return Ok(MaxBuySell {
                account_no: account.clone(),
                symbol,
                max_buy_qty: 0,
                max_sell_qty: 0,
                margin_ratio: None,
                purchasing_power: None,
            });
        };
        parse_max_buy_sell(&body, &symbol)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn parses_equity_and_derivative_balances_and_aliases() {
        let eq: AccountBalance = serde_json::from_str(
            r#"{"equity":{"accountNo":"003C123451","accountBalance":"1000000","advancedCashT0":"500","advanceCashT1":"250"},"derivative":{},"receivedTime":1780000000000}"#,
        )
        .expect("decodes");
        let equity = eq.equity.expect("equity");
        assert_eq!(equity.account_balance, dec!(1000000));
        assert_eq!(equity.advance_cash_t0, dec!(500));
        assert_eq!(equity.advance_cash_t1, dec!(250));
        assert!(eq.derivative.is_none());

        let der: AccountBalance = serde_json::from_str(
            r#"{"equity":null,"derivative":{"accountNo":"003C123458","cashWithdrawableSSI":"12345","withdrawableVSDC":"6789"}}"#,
        )
        .expect("decodes");
        let derivative = der.derivative.expect("derivative");
        assert_eq!(derivative.withdrawable_ssi, dec!(12345));
        assert_eq!(derivative.withdrawable_vsdc, dec!(6789));
        assert!(der.equity.is_none());
    }

    #[test]
    fn parses_positions_both_derivative_shapes() {
        let acct = AccountNo::new("003C123458");
        let obj = parse_positions(
            br#"{"derivative":{"derOpenPositions":[{"symbol":"VN30F2612","long":2,"short":0,"net":2}],"derClosePositions":[]}}"#,
            &acct,
        )
        .expect("decodes");
        let der = obj.derivative.expect("der");
        assert_eq!(der.open_positions.len(), 1);
        assert_eq!(der.open_positions.first().expect("pos").account_no, acct);

        let list = parse_positions(
            br#"{"derivative":[{"symbol":"VN30F2612","long":1,"short":1,"net":0}]}"#,
            &acct,
        )
        .expect("decodes");
        assert_eq!(list.derivative.expect("der").open_positions.len(), 1);
    }

    #[test]
    fn parses_order_book_market_price_and_aliases() {
        let acct = AccountNo::new("003C123451");
        let book = parse_order_book(
            br#"{"accountNo":"003C123451","totalRecord":1,"orderList":[{"orderId":"O1","clientRequestId":"req1","instrumentId":"VNM","buySell":"B","orderType":"ATO","price":"ATO","quantity":"100","filledQuantity":"0","cancelQty":0,"orderStatus":"QU","inputTime":"2026/07/29 09:01:00"}]}"#,
            &acct,
        )
        .expect("decodes");
        assert_eq!(book.orders.len(), 1);
        let order = book.orders.first().expect("order");
        assert_eq!(order.symbol, "VNM");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.price, Some(OrderPrice::Market("ATO".to_owned())));
        assert_eq!(order.status, OrderStatus::Queued);
        assert!(order.input_time.is_some());
    }
}
