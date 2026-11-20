# Changelog

All notable changes to `ssi-sdk` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-20

### Added

- Unified SSI FastConnect V3 Rust SDK (`ssi-sdk`, library `ssi_sdk`) on Rust `1.100` (`edition = "2024"`), merging `ssi-fc-data` and `ssi-fc-trading` behind one `SsiClient` with `data` (default) and `trading` Cargo features.
- **Core (always enabled):**
  - Unified `TokenManager` supporting automatic read-only data login with failure/revoked backoff, single-use `/refresh` rotation, sticky OTP/Smart OTP trading sessions (never silently downgrading on dead refresh), and `StoredToken` export/import.
  - Per-endpoint rate pacing honoring `X-RATELIMIT-LIMIT` / `Retry-After`, `Accept-Language` configuration, host routing (`api_url` / `trading_url`), and full V3 `ApiErrorCode` classification.
  - Read-only account REST queries: `accounts`, `balance`, `positions`, `ppmmr`, `order_book_page`, `order_book`.
- **Feature `data` (default):**
  - Market-data REST endpoints: `ohlc_page`, `ohlc`, `ohlc_today`, `corporate_actions_page`, `corporate_actions` (with `ratio_text` and decimal `ratio`), `securities`, `price_limits_page`, `price_limits`, `indices`, `index_summary`, `securities_summary_page`, `securities_summary`, `market_files`, `download_market_file`, `download_market_file_by_token`.
  - Supervised `DATA` WebSocket stream (`MarketStream`) with exponential backoff, staleness detection, board/basket `@*` fan-out topics, `unsubscribe_all()`, 6-digit rejection normalization, `retryAfter`, and `IndexTick` / `IndexSummary` decoding.
  - Zero RSA or XML dependencies compiled when `trading` is off.
- **Feature `trading` (opt-in):**
  - Normal OTP and Smart OTP push-approval authentication (`request_otp`, `authenticate_with_otp`, `authenticate_with_transaction_id`, `poll_smart_otp`).
  - RSA PKCS#1 v1.5 SHA-256 request signing (base64 XML `RSAKeyValue` and PEM PKCS#1/PKCS#8 keys) with `zeroize` cleanup.
  - `max_buy_sell`, signed single & batch orders (`place_order`, `modify_order`, `cancel_order`, `place_batch_orders`, `cancel_batch_orders`), and all 7 Flexible Conditional Order (FCO) strategies plus list, order book, and status history.
  - Supervised `TRADING` event stream (`TradingStream`) with `unsubscribe_all()` and masked account topics.
  - Explicit opt-in Trading WebSocket RPC client (`TradingWsClient`) with verbatim params signing, `Auto` base64/hex fallback on pre-execution signature errors, post-upgrade refusal grace window, and rate-limit pacing.
