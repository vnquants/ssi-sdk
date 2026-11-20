//! Masking helpers so keys, tokens, OTPs and account numbers never reach logs in clear.

use std::{
    fmt,
    hash::{DefaultHasher, Hash as _, Hasher as _},
};

/// Display wrapper that prints a secret as its length and a short fingerprint.
///
/// The fingerprint lets two log lines be compared ("did the refresh token change?") without
/// revealing the value. It is a truncated `SipHash` of the secret, stable within one build.
#[derive(Clone, Copy)]
pub struct Masked<'a>(&'a str);

impl<'a> Masked<'a> {
    /// Wraps a secret for display.
    pub const fn new(secret: &'a str) -> Self {
        Self(secret)
    }
}

impl fmt::Display for Masked<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("<empty>");
        }
        write!(
            f,
            "***(len={}, fp={:08x})",
            self.0.len(),
            fingerprint(self.0)
        )
    }
}

impl fmt::Debug for Masked<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Short non-reversible fingerprint of a secret, for comparing values in logs.
pub fn fingerprint(secret: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    secret.hash(&mut hasher);
    u32::try_from(hasher.finish() >> 32).unwrap_or_default()
}

/// Masks an account number for logs: keeps the last three characters.
///
/// `003C123456` becomes `*******456`.
pub fn mask_account(account: &str) -> String {
    let count = account.chars().count();
    account
        .chars()
        .enumerate()
        .map(|(index, ch)| if index + 3 >= count { ch } else { '*' })
        .collect()
}

/// Masks the account number of a trading topic (`order.<account>`, `portfolio.<account>`).
pub fn mask_topic(topic: &str) -> String {
    for prefix in ["order.", "portfolio."] {
        if let Some(account) = topic
            .get(..prefix.len())
            .filter(|head| head.eq_ignore_ascii_case(prefix))
            .and_then(|_| topic.get(prefix.len()..))
            && account != "*"
        {
            return format!(
                "{}{}",
                topic.get(..prefix.len()).unwrap_or(prefix),
                mask_account(account)
            );
        }
    }
    topic.to_owned()
}

/// Masks trading topics inside free server text.
pub fn mask_topics_in(text: &str) -> String {
    text.split(' ')
        .map(|word| {
            let trimmed = word.trim_end_matches([',', ';', '.']);
            let tail = word.get(trimmed.len()..).unwrap_or_default();
            format!("{}{tail}", mask_topic(trimmed))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_never_contains_the_secret() {
        let secret = "eyJhbGciOiJIUzI1NiJ9.payload.signature";
        let shown = Masked::new(secret).to_string();
        assert!(!shown.contains("payload"));
        assert!(shown.starts_with("***(len=38, fp="));
        assert_eq!(Masked::new("").to_string(), "<empty>");
    }

    #[test]
    fn fingerprint_distinguishes_values() {
        assert_eq!(fingerprint("a"), fingerprint("a"));
        assert_ne!(fingerprint("a"), fingerprint("b"));
    }

    #[test]
    fn account_and_topic_masking() {
        assert_eq!(mask_account("003C123456"), "*******456");
        assert_eq!(mask_account("12"), "12");
        assert_eq!(mask_topic("order.003C123456"), "order.*******456");
        assert_eq!(mask_topic("portfolio.003C123456"), "portfolio.*******456");
        assert_eq!(mask_topic("order.*"), "order.*");
        assert_eq!(
            mask_topics_in("Denied: order.003C123456."),
            "Denied: order.*******456."
        );
    }
}
