//! Validated `TRADING` channel topics (`order.<account>`, `order.*`, `portfolio.<account>`,
//! `portfolio.*`).
//!
//! `Display` and `Debug` always mask the account number (`order.*******456`) so topics can be
//! logged safely.

use std::fmt;

use crate::{
    error::{Error, Result},
    model::AccountNo,
    secret::mask_topic,
};

/// Kind of a trading stream topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TradingTopicKind {
    /// Order status, fill, and FCO updates (`order.<account>` / `order.*`).
    Order,
    /// Derivative position updates (`portfolio.<account>` / `portfolio.*`).
    Portfolio,
}

impl TradingTopicKind {
    /// Wire prefix before the dot.
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Order => "order",
            Self::Portfolio => "portfolio",
        }
    }
}

/// One validated `TRADING` channel topic.
///
/// `Debug` and `Display` mask account numbers; call [`TradingTopic::as_str`] for the wire string.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TradingTopic {
    raw: String,
    kind: TradingTopicKind,
}

impl TradingTopic {
    /// Order, fill, and FCO events for one account (`order.<account>`).
    pub fn order(account: &AccountNo) -> Result<Self> {
        account.validate()?;
        Ok(Self {
            raw: format!("order.{}", account.as_str()),
            kind: TradingTopicKind::Order,
        })
    }

    /// Order, fill, and FCO events for all accounts owned by the token (`order.*`).
    ///
    /// Supported on `wss://stream.ssi.com.vn/ws/v3` (not on `/ws/v3/trading`, which requires a
    /// single account).
    pub fn all_orders() -> Self {
        Self {
            raw: "order.*".to_owned(),
            kind: TradingTopicKind::Order,
        }
    }

    /// Derivative portfolio events for one account (`portfolio.<account>`).
    pub fn portfolio(account: &AccountNo) -> Result<Self> {
        account.validate()?;
        Ok(Self {
            raw: format!("portfolio.{}", account.as_str()),
            kind: TradingTopicKind::Portfolio,
        })
    }

    /// Derivative portfolio events for all accounts owned by the token (`portfolio.*`).
    pub fn all_portfolios() -> Self {
        Self {
            raw: "portfolio.*".to_owned(),
            kind: TradingTopicKind::Portfolio,
        }
    }

    /// Parses a wire topic string (`order.<account>`, `order.*`, `portfolio.<account>`,
    /// `portfolio.*`).
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        let Some((prefix, target)) = trimmed.split_once('.') else {
            return Err(Error::InvalidRequest(
                "trading topic must be 'order.<account|*|>' or 'portfolio.<account|*>'".to_owned(),
            ));
        };
        let kind = if prefix.eq_ignore_ascii_case("order") {
            TradingTopicKind::Order
        } else if prefix.eq_ignore_ascii_case("portfolio") {
            TradingTopicKind::Portfolio
        } else {
            return Err(Error::InvalidRequest(format!(
                "unsupported trading topic prefix {prefix:?}"
            )));
        };
        if target == "*" {
            return Ok(Self {
                raw: format!("{}.*", kind.prefix()),
                kind,
            });
        }
        let account = AccountNo::validated(target)?;
        Ok(Self {
            raw: format!("{}.{}", kind.prefix(), account.as_str()),
            kind,
        })
    }

    /// The topic kind (`Order` or `Portfolio`).
    pub const fn kind(&self) -> TradingTopicKind {
        self.kind
    }

    /// Whether this topic uses the `*` wildcard.
    pub fn is_wildcard(&self) -> bool {
        self.raw.ends_with(".*")
    }

    /// Raw wire topic string (contains the unmasked account number; do not log).
    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for TradingTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&mask_topic(&self.raw))
    }
}

impl fmt::Debug for TradingTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TradingTopic({})", mask_topic(&self.raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_masks_topics() {
        let acc = AccountNo::validated("003C123451").expect("valid");
        let order = TradingTopic::order(&acc).expect("valid");
        assert_eq!(order.as_str(), "order.003C123451");
        assert_eq!(order.to_string(), "order.*******451");
        assert!(!format!("{order:?}").contains("123451"));

        let wild = TradingTopic::parse("PORTFOLIO.*").expect("wild");
        assert_eq!(wild.as_str(), "portfolio.*");
        assert!(wild.is_wildcard());

        assert!(TradingTopic::parse("trade.VNM").is_err());
        assert!(TradingTopic::parse("order.bad acct").is_err());
    }
}
