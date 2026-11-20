//! Shared vocabulary: exchanges, accounts, sides, order types, statuses, prices and (with the
//! `trading` feature) request ids and FCO enums.
//!
//! Unknown server strings are preserved verbatim (`Other(String)`) so new SSI wire values never
//! break decoding.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

#[cfg(feature = "trading")]
use crate::de::format_plain_decimal;
use crate::{
    de::parse_decimal,
    error::{Error, Result},
    secret::mask_account,
};

/// Maximum length of a `clientRequestId` / `clientModifyId` / `clientCancelId` accepted by SSI.
#[cfg(feature = "trading")]
pub const MAX_CLIENT_REQUEST_ID: usize = 20;

/// Alphabet used by [`ClientRequestId::generate`].
#[cfg(feature = "trading")]
const ID_ALPHABET: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Defines a string-backed enum that preserves unknown wire values.
macro_rules! text_enum {
    (
        $(#[$meta:meta])*
        $name:ident { $($(#[$vmeta:meta])* $variant:ident => $text:literal),* $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $($(#[$vmeta])* $variant,)*
            /// A value this crate does not know, kept verbatim as sent.
            Other(String),
        }

        impl $name {
            /// The wire text.
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $text,)*
                    Self::Other(text) => text,
                }
            }

            /// Parses wire text (case-insensitive); unknown text becomes `Other`.
            pub fn parse(text: &str) -> Self {
                let trimmed = text.trim();
                $(if trimmed.eq_ignore_ascii_case($text) { return Self::$variant; })*
                Self::Other(trimmed.to_owned())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::Other(String::new())
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(
                &self,
                serializer: S,
            ) -> ::std::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> ::std::result::Result<Self, D::Error> {
                Ok(Self::parse(&crate::de::string(deserializer)?))
            }
        }
    };
}

text_enum! {
    /// Exchange / board.
    Exchange {
        /// Ho Chi Minh City Stock Exchange.
        Hose => "HOSE",
        /// Hanoi Stock Exchange.
        Hnx => "HNX",
        /// Unlisted Public Company Market.
        Upcom => "UPCOM",
        /// Derivatives board (SSI also sends `DER`).
        Derivatives => "DERIVATIVES",
    }
}

impl Exchange {
    /// Maps SSI's alternative spellings (`DER`, `DERIVATIVE`, `HNXUPCOM`) as well.
    pub fn normalize(text: &str) -> Self {
        match text.trim().to_ascii_uppercase().as_str() {
            "DER" | "DERIVATIVE" => Self::Derivatives,
            "HNXUPCOM" => Self::Upcom,
            _ => Self::parse(text),
        }
    }
}

/// A trading account number. `Debug` and `Display` mask it (`003C123456` -> `*******456`);
/// call [`AccountNo::as_str`] to access the raw value.
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct AccountNo(String);

impl AccountNo {
    /// Wraps an account number (trimmed).
    pub fn new(account: impl Into<String>) -> Self {
        Self(account.into().trim().to_owned())
    }

    /// Validated account number (non-empty ASCII letters/digits).
    pub fn validated(account: impl Into<String>) -> Result<Self> {
        let value = Self::new(account);
        value.validate()?;
        Ok(value)
    }

    /// Validates that the account number is non-empty and ASCII alphanumeric.
    pub fn validate(&self) -> Result<()> {
        if self.0.is_empty() || !self.0.chars().all(|ch| ch.is_ascii_alphanumeric()) {
            return Err(Error::InvalidRequest("invalid account number".to_owned()));
        }
        Ok(())
    }

    /// The unmasked account number.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccountNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AccountNo({})", mask_account(&self.0))
    }
}

impl fmt::Display for AccountNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&mask_account(&self.0))
    }
}

impl<'de> Deserialize<'de> for AccountNo {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::new(crate::de::string(deserializer)?))
    }
}

/// Idempotency key for an order, modify, cancel or batch request (1–20 ASCII letters, digits,
/// `-` or `_`).
///
/// SSI rejects a repeat within the trading day with HTTP 409 (`409100`), so a request whose
/// outcome is unknown must be reconciled through the order book, never re-sent with a new id.
#[cfg(feature = "trading")]
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct ClientRequestId(String);

#[cfg(feature = "trading")]
impl ClientRequestId {
    /// Validates an id: 1–20 ASCII letters, digits, `-` or `_`.
    pub fn new(id: &str) -> Result<Self> {
        let valid = !id.is_empty()
            && id.len() <= MAX_CLIENT_REQUEST_ID
            && id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'));
        if valid {
            Ok(Self(id.to_owned()))
        } else {
            Err(Error::InvalidRequest(format!(
                "clientRequestId must be 1-{MAX_CLIENT_REQUEST_ID} letters, digits, '-' or '_'"
            )))
        }
    }

    /// Generates a cryptographically random 20-character alphanumeric id.
    pub fn generate() -> Self {
        use rand::Rng as _;
        let mut rng = rand::rngs::OsRng;
        let mut out = String::with_capacity(MAX_CLIENT_REQUEST_ID);
        for _ in 0..MAX_CLIENT_REQUEST_ID {
            let idx = rng.gen_range(0..ID_ALPHABET.len());
            if let Some(byte) = ID_ALPHABET.get(idx) {
                out.push(char::from(*byte));
            }
        }
        Self(out)
    }

    /// The id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "trading")]
impl fmt::Debug for ClientRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientRequestId({})", self.0)
    }
}

#[cfg(feature = "trading")]
impl fmt::Display for ClientRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "trading")]
impl<'de> Deserialize<'de> for ClientRequestId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self(crate::de::string(deserializer)?))
    }
}

text_enum! {
    /// Account type.
    AccountType {
        /// Cash account (number ends in 1).
        Cash => "Cash",
        /// Margin account (ends in 6).
        Margin => "Margin",
        /// Derivatives account (ends in 8).
        Derivative => "Derivative",
    }
}

text_enum! {
    /// Order side.
    OrderSide {
        /// Buy (`B`).
        Buy => "B",
        /// Sell (`S`).
        Sell => "S",
    }
}

text_enum! {
    /// Order type. `LO` and `PLO` carry a limit price; all other types are market-priced (`"0"`).
    OrderType {
        /// Limit order.
        Lo => "LO",
        /// At-the-open auction order.
        Ato => "ATO",
        /// At-the-close auction order.
        Atc => "ATC",
        /// Market price (HOSE).
        Mp => "MP",
        /// Market-and-kill (HNX).
        Mak => "MAK",
        /// Market-or-kill (HNX).
        Mok => "MOK",
        /// Market-to-limit (HOSE / HNX).
        Mtl => "MTL",
        /// Post-close limit order (HNX).
        Plo => "PLO",
    }
}

impl OrderType {
    /// Whether this order type requires a strictly positive limit price (`LO`, `PLO`).
    pub const fn is_priced(&self) -> bool {
        matches!(self, Self::Lo | Self::Plo)
    }

    /// Whether this order type is a market-priced word (carries no slip in FCO orders).
    pub const fn is_market_word(&self) -> bool {
        matches!(
            self,
            Self::Ato | Self::Atc | Self::Mp | Self::Mak | Self::Mok | Self::Mtl
        )
    }
}

text_enum! {
    /// Order status as reported by SSI `FastConnect` V3.
    OrderStatus {
        /// `PD` pending: accepted by SSI, not yet at the exchange.
        Pending => "PD",
        /// `WA` waiting approval.
        WaitingApproval => "WA",
        /// `RS` ready to send.
        ReadyToSend => "RS",
        /// `SD` sent to the exchange.
        Sent => "SD",
        /// `QU` queued at the exchange.
        Queued => "QU",
        /// `FF` fully filled (final).
        FullyFilled => "FF",
        /// `PF` partially filled.
        PartiallyFilled => "PF",
        /// `FFPC` partially filled, remainder cancelled (final).
        FilledPartiallyCancelled => "FFPC",
        /// `WM` waiting modify.
        WaitingModify => "WM",
        /// `WC` waiting cancel.
        WaitingCancel => "WC",
        /// `CL` cancelled (final).
        Cancelled => "CL",
        /// `RJ` rejected (final).
        Rejected => "RJ",
        /// `EX` expired or rejected by the exchange (final).
        Expired => "EX",
        /// `IAV` pre-session order.
        PreSession => "IAV",
        /// `SOI` pre-session stop order.
        PreSessionStop => "SOI",
        /// `PAS` stop order waiting approval.
        WaitingApprovalStop => "PAS",
        /// `CPL` completed (order engine status).
        Completed => "CPL",
        /// `RQ` request received (FCO engine).
        Requested => "RQ",
        /// `ERR` engine error.
        Error => "ERR",
    }
}

impl OrderStatus {
    /// Whether no further state transition can occur.
    pub const fn is_final(&self) -> bool {
        matches!(
            self,
            Self::FullyFilled
                | Self::FilledPartiallyCancelled
                | Self::Cancelled
                | Self::Rejected
                | Self::Expired
                | Self::Completed
                | Self::Error
        )
    }

    /// Whether the order is typically open and cancellable.
    pub const fn is_cancellable(&self) -> bool {
        matches!(
            self,
            Self::WaitingApproval
                | Self::ReadyToSend
                | Self::Sent
                | Self::Queued
                | Self::PartiallyFilled
        )
    }
}

/// Price of an order as returned by SSI: a numeric limit price or a market-priced word
/// (`ATO`, `ATC`, `MP`, `MTL`, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OrderPrice {
    /// Numeric limit price.
    Limit(Decimal),
    /// Market-priced order word, as sent.
    Market(String),
}

impl OrderPrice {
    /// Numeric price if this is a limit price.
    pub const fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Limit(price) => Some(*price),
            Self::Market(_) => None,
        }
    }

    /// Parses a wire string into [`OrderPrice`]. Returns `None` for empty strings.
    pub fn parse_opt(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(parse_decimal(trimmed).map_or_else(
            || {
                let order_type = OrderType::parse(trimmed);
                if matches!(order_type, OrderType::Other(_)) {
                    Self::Market(trimmed.to_owned())
                } else {
                    Self::Market(order_type.as_str().to_owned())
                }
            },
            Self::Limit,
        ))
    }
}

impl<'de> Deserialize<'de> for OrderPrice {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = crate::de::string(deserializer)?;
        Ok(Self::parse_opt(&text).unwrap_or_else(|| Self::Market(String::new())))
    }
}

#[cfg(feature = "trading")]
text_enum! {
    /// Flexible Conditional Order (FCO) strategy type.
    FcoType {
        /// Good-till-date (`gtd`).
        Gtd => "gtd",
        /// Stop market (`stop`).
        Stop => "stop",
        /// Stop limit (`stop_limit`).
        StopLimit => "stop_limit",
        /// Trailing stop market (`trailing_stop`).
        TrailingStop => "trailing_stop",
        /// Trailing stop limit (`trailing_stop_limit`).
        TrailingStopLimit => "trailing_stop_limit",
        /// One-cancels-the-other (`oco`).
        Oco => "oco",
        /// Bull-bear bracket (`bullbear`).
        BullBear => "bullbear",
    }
}

#[cfg(feature = "trading")]
text_enum! {
    /// Processing status of a Flexible Conditional Order (FCO).
    FcoStatus {
        /// `INIT` initialized.
        Init => "INIT",
        /// `WAIT` waiting for trigger.
        Wait => "WAIT",
        /// `TRI` triggered.
        Tri => "TRI",
        /// `TRIT` triggered (trailing).
        Trit => "TRIT",
        /// `TER` terminated / cancelled.
        Ter => "TER",
        /// `FIS` finished.
        Fis => "FIS",
        /// `WC` waiting cancel.
        Wc => "WC",
        /// `EXP` expired.
        Exp => "EXP",
        /// `ERR` error.
        Err => "ERR",
    }
}

/// Comparison operator that triggers an FCO stop order.
///
/// Requests send the canonical `snake_case` name (`"greater_or_equal"`, etc.); responses may send
/// either the name or SSI's numeric code (`0`..=`4`).
#[cfg(feature = "trading")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FcoOperator {
    /// Price > trigger (response code `2`).
    Greater,
    /// Price >= trigger (response code `0`).
    GreaterOrEqual,
    /// Price < trigger (response code `3`).
    Lesser,
    /// Price <= trigger (response code `4`).
    LesserOrEqual,
    /// Price == trigger (response code `1`).
    Equal,
    /// Any unknown operator value, kept verbatim.
    Other(String),
}

#[cfg(feature = "trading")]
impl FcoOperator {
    /// Canonical wire name sent in requests.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Greater => "greater",
            Self::GreaterOrEqual => "greater_or_equal",
            Self::Lesser => "lesser",
            Self::LesserOrEqual => "lesser_or_equal",
            Self::Equal => "equal",
            Self::Other(text) => text,
        }
    }

    /// Parses either an operator name or SSI's numeric response code (`0`..=`4`).
    pub fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        match trimmed {
            "0" => Self::GreaterOrEqual,
            "1" => Self::Equal,
            "2" => Self::Greater,
            "3" => Self::Lesser,
            "4" => Self::LesserOrEqual,
            _ if trimmed.eq_ignore_ascii_case("greater") => Self::Greater,
            _ if trimmed.eq_ignore_ascii_case("greater_or_equal") => Self::GreaterOrEqual,
            _ if trimmed.eq_ignore_ascii_case("lesser") => Self::Lesser,
            _ if trimmed.eq_ignore_ascii_case("lesser_or_equal") => Self::LesserOrEqual,
            _ if trimmed.eq_ignore_ascii_case("equal") => Self::Equal,
            other => Self::Other(other.to_owned()),
        }
    }
}

#[cfg(feature = "trading")]
impl fmt::Display for FcoOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(feature = "trading")]
impl Serialize for FcoOperator {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "trading")]
impl<'de> Deserialize<'de> for FcoOperator {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::parse(&crate::de::string(deserializer)?))
    }
}

/// Price for an FCO order leg: either a numeric limit price or an order-type word (`MTL`, `MP`,
/// `ATO`, `ATC`, `MOK`, `MAK`, `LO`, `PLO`).
#[cfg(feature = "trading")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcoPrice {
    /// Numeric price; keeps any configured `price_slip`.
    Limit(Decimal),
    /// Order-type word; market words (`ATO`, `ATC`, `MTL`, `MP`, `MOK`, `MAK`) zero `price_slip`.
    OrderType(OrderType),
    /// Escape hatch for a raw price word.
    Raw(String),
}

#[cfg(feature = "trading")]
impl FcoPrice {
    /// Serializes the price and returns the effective `price_slip` (market-priced words force 0).
    pub(crate) fn wire_and_slip(&self, slip: Decimal) -> Result<(String, Decimal)> {
        match self {
            Self::Limit(price) => {
                if *price < Decimal::ZERO {
                    return Err(Error::InvalidRequest(
                        "price must be non-negative".to_owned(),
                    ));
                }
                Ok((format_plain_decimal(*price), slip))
            }
            Self::OrderType(order_type) => {
                let effective_slip = if order_type.is_market_word() {
                    Decimal::ZERO
                } else {
                    slip
                };
                Ok((order_type.as_str().to_owned(), effective_slip))
            }
            Self::Raw(word) => {
                let trimmed = word.trim();
                let parsed = OrderType::parse(trimmed);
                let effective_slip = if parsed.is_market_word() {
                    Decimal::ZERO
                } else {
                    slip
                };
                Ok((trimmed.to_owned(), effective_slip))
            }
        }
    }
}

/// Validates and upper-cases a symbol (1–20 ASCII letters or digits).
pub(crate) fn check_symbol(symbol: &str) -> Result<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 20
        || !symbol.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        return Err(Error::InvalidRequest(format!(
            "symbol must be 1-20 ASCII letters or digits, got {symbol:?}"
        )));
    }
    Ok(symbol)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_and_enums() {
        let acc = AccountNo::validated("003C123451").expect("valid");
        assert!(!format!("{acc:?}").contains("123451"));
        assert!(!format!("{acc}").contains("123451"));
        assert!(format!("{acc}").ends_with("451"));
        assert_eq!(acc.as_str(), "003C123451");
        assert!(AccountNo::validated("bad acct").is_err());

        assert_eq!(Exchange::normalize("DER"), Exchange::Derivatives);
        assert_eq!(Exchange::normalize("HNXUPCOM"), Exchange::Upcom);
        assert_eq!(
            OrderStatus::parse("ffpc"),
            OrderStatus::FilledPartiallyCancelled
        );
        assert!(OrderStatus::parse("CL").is_final());
    }

    #[cfg(feature = "trading")]
    #[test]
    fn trading_ids_and_fco_rules() {
        use rust_decimal::dec;

        let generated = ClientRequestId::generate();
        assert_eq!(generated.as_str().len(), MAX_CLIENT_REQUEST_ID);
        assert!(ClientRequestId::new(generated.as_str()).is_ok());
        assert!(ClientRequestId::new("").is_err());
        assert!(ClientRequestId::new("this-id-is-way-too-long-for-ssi").is_err());
        assert!(ClientRequestId::new("bad id").is_err());

        assert_eq!(FcoOperator::parse("0"), FcoOperator::GreaterOrEqual);
        assert_eq!(FcoOperator::parse("4"), FcoOperator::LesserOrEqual);
        assert_eq!(
            FcoPrice::OrderType(OrderType::Mtl)
                .wire_and_slip(dec!(500))
                .expect("ok"),
            ("MTL".to_owned(), dec!(0))
        );
        assert_eq!(
            FcoPrice::Limit(dec!(26000.00))
                .wire_and_slip(dec!(500))
                .expect("ok"),
            ("26000".to_owned(), dec!(500))
        );
    }
}
