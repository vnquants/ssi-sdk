# SSI FastConnect V3 Protocol Reference (`ssi-sdk`)

This document summarizes the **SSI FastConnect V3** wire contract implemented by `ssi-sdk`,
combining market data (`feature = "data"`) and trading (`feature = "trading"`) behind one
[`SsiClient`].

## Upstream References

- **Official API Reference:** <https://developers.ssi.com.vn/docs/api-reference>
- **Official V3 Tutorials Repository (Go / Python / Node.js / .NET):** <https://github.com/SSI-Securities-Inc/ssi-fastconnect-v3-tutorials>
- **Official Go SDK v3:** <https://github.com/SSI-Securities-Inc/ssi-sdk-go>
- **Release Notes:** <https://developers.ssi.com.vn/docs/api-reference/release-notes>

## Base Endpoints

| Surface | Production URL | Environment Override | Feature |
|---|---|---|---|
| Auth & Market Data REST | `https://api.ssi.com.vn` | `SSI_V3_API_URL` | core |
| Trading / Account REST | `https://api.ssi.com.vn` (defaults to `SSI_V3_API_URL`) | `SSI_V3_TRADING_URL` | core |
| Streaming WebSocket (`DATA` & `TRADING` channels) | `wss://stream.ssi.com.vn/ws/v3` | `SSI_V3_STREAM_URL` | core |
| Trading WebSocket RPC | `wss://api.ssi.com.vn/ws/v3/trading` | `SSI_V3_TRADING_WS_URL` | `trading` |

## Authentication & Session Lifecycle (Core + `trading`)

| Endpoint | Method | Feature | `ssi-sdk` API |
|---|---|---|---|
| `/api/v3/auth/requestOtp` | `POST` | `trading` | `TokenManager::request_otp` |
| `/api/v3/auth/token` | `POST` | core / `trading` | Auto-login in `access_token()`, `authenticate_read_only`, `authenticate_with_otp`, `authenticate_with_transaction_id`, `poll_smart_otp` |
| `/api/v3/auth/refresh` | `POST` | core | `TokenManager::refresh`, `force_refresh`, automatic single-flight renewal |

Key rules:

- **Automatic data / read-only mode (default):** `access_token()` logs in with `apiKey` +
  `apiSecret` (no OTP), caches the token, refreshes inside the lead window (default 5 min), and
  re-logs in if `/refresh` fails or the 8-hour refresh chain ends. Failed logins back off (1 s
  doubling to 60 s; 10 min on `401104` / `401105`).
- **Sticky trading mode (`feature = "trading"`):** Authenticating via `authenticate_with_otp`,
  `authenticate_with_transaction_id`, `poll_smart_otp`, or importing a `StoredToken` with
  `is_trading_session = true` locks the manager into sticky trading mode. It refreshes via
  single-use `/refresh` and **never** silently downgrades to an OTP-less token; a dead or expired
  refresh clears the session and returns `Error::ReauthenticationRequired`.
- **Smart OTP push polling:** `request_otp()` returns `transaction_id` only for Smart OTP
  accounts. Polling `/api/v3/auth/token` with `transactionId` returns **HTTP 202 + `401114`**
  (`Error::SmartOtpPending`) while waiting and `401113` / `401115` / `401116`
  (`Error::SmartOtpRejected`) on terminal failure.
- **Single-use refresh tokens:** Refresh tokens are removed from state before the HTTP request so
  no concurrent caller or cancelled future can ever send the same refresh token twice.
- **Observed lifetimes (2026-11-20):** access tokens live 900 s (15 min), refresh tokens live
  28 800 s (8 h) anchored at login (`refreshExpiresAt` does not extend on refresh); both epoch
  seconds and epoch milliseconds (`>= 10^11`) are accepted.

## Account Read-Only REST Queries (Core, always enabled)

Work with either a read-only data token or an OTP trading token.

| Endpoint | Method | `ssi-sdk` API |
|---|---|---|
| `/api/v3/account/info` | `GET` | `SsiClient::accounts` |
| `/api/v3/trading/accountBalance` | `GET` | `SsiClient::balance` |
| `/api/v3/trading/position` | `GET` | `SsiClient::positions` |
| `/api/v3/trading/ppmmrAccount` | `GET` | `SsiClient::ppmmr` |
| `/api/v3/trading/orderBook` | `GET` | `SsiClient::order_book_page`, `SsiClient::order_book` |

## Market Data REST & WebSocket (`feature = "data"`, default)

### REST Endpoints (`GET`)

| Endpoint | `ssi-sdk` API | Notes |
|---|---|---|
| `/api/v3/data/ohlc` | `ohlc_page`, `ohlc`, `ohlc_today` | `from`/`to` require `YYYY/MM/DD HH:mm:ss` for every timeframe; `30m` rejected live (`400210`); intraday covers 365 days; daily prices adjusted. |
| `/api/v3/data/corporateActions` | `corporate_actions_page`, `corporate_actions` | Max 5-day span per request (walker splits automatically); `X-RATELIMIT-LIMIT: 1`; preserves decimal `ratio` and raw `ratio_text` (e.g. `"10:1"`). |
| `/api/v3/data/securitiesByBoard` | `securities` | Exactly one of `symbol`, `board`, or `index`. |
| `/api/v3/data/masterdata` | `price_limits_page`, `price_limits` | Ceiling, floor, reference price per symbol/date (`board` / `exchange`, `referencePrice` / `refPrice` aliases handled). |
| `/api/v3/data/indexList` | `indices` | Index codes trimmed of trailing spaces. |
| `/api/v3/data/indexSummary` | `index_summary` | Exactly one of `board` or `index` (`504207` otherwise). |
| `/api/v3/data/securitiesSummary` | `securities_summary_page`, `securities_summary` | Exactly one of `symbol` or `index`, plus `from`/`to`. |
| `/api/v3/data/file` | `market_files` | Signed Parquet links (`1m` max 2-month span, `15m` unlimited, `1d` no dates). Requires `data:file:get` scope. |
| `/api/v3/data/download` | `download_market_file`, `download_market_file_by_token` | Raw Parquet bytes (`application/octet-stream`), same Bearer token, single-use 30-min link, origin validated. |

### `DATA` WebSocket (`MarketStream`)

- Topics: `trade.<filter>[@tick|@1m|@5m]`, `trade.index.<index>[@tick|@1m|@5m]`, `quote.<filter>`,
  `room.<filter>`, `put.<filter>`, `oddlot.<filter>`, `market.<filter>`, `market.flag`,
  `indexsummary.<index|*>`, plus `@*` board/basket fan-out (`Topic::board_fanout`,
  `Topic::basket_fanout`, `Topic::for_board`, `Topic::for_basket`).
- Heartbeat: client `PING_PONG` on `HEARTBEAT` every 25 s, answers server `PING`, idle timeout
  75 s, optional `StalePolicy`.
- Rejections: both 3-digit HTTP (`401`, `403`, `429`, `5xx`) and 6-digit V3 `ErrCode` (`401004`,
  `429000`) normalized; honors `retryAfter`.

## Trading REST, Event Stream & Trading WS (`feature = "trading"`)

### REST Queries & Signed Commands

| Endpoint | Method | Signed | `ssi-sdk` API |
|---|---|---|---|
| `/api/v3/trading/maxBuySell` | `GET` | No | `SsiClient::max_buy_sell` |
| `/api/v3/trading/order` | `POST` | Yes | `SsiClient::place_order` |
| `/api/v3/trading/order` | `PUT` | Yes | `SsiClient::modify_order` |
| `/api/v3/trading/order` | `DELETE` | Yes | `SsiClient::cancel_order` |
| `/api/v3/trading/order/batch` | `POST` | Yes | `SsiClient::place_batch_orders` |
| `/api/v3/trading/order/batch` | `DELETE` | Yes | `SsiClient::cancel_batch_orders` |
| `/api/v3/trading/fco/order` | `POST` | Yes | `SsiClient::place_fco` |
| `/api/v3/trading/fco/order` | `DELETE` | Yes | `SsiClient::cancel_fco` |
| `/api/v3/trading/fco/list` | `GET` | No | `SsiClient::fco_list` |
| `/api/v3/trading/fco/orderbook` | `GET` | No | `SsiClient::fco_order_book` |
| `/api/v3/trading/fco/statusHistory` | `GET` | No | `SsiClient::fco_status_history` |

### `TRADING` Event Stream (`TradingStream`)

- Topics: `order.<account>`, `order.*`, `portfolio.<account>`, `portfolio.*` (masked in `Display`
  and `Debug`).
- Events: `OrderEvent`, `OrderMatchEvent`, `PortfolioEvent`, `FcoEvent`, `Unknown`.
- Supports `subscribe`, `unsubscribe`, `unsubscribe_all`, `request_subscriptions`.
- Requires an OTP / Smart OTP trading session (`trading_access_token`); never silently logs in
  without OTP.

### Trading WebSocket RPC (`TradingWsClient`)

- Opened via `SsiClient::connect_trading_ws(TradingWsConfig)`.
- Verbatim RSA-SHA256 signing over serialized `"params"`; `SignatureEncoding::Auto` starts with
  base64 and retries once with hex only on pre-execution signature errors (`401006` / `401008` /
  `401013`).
- Post-upgrade `handshake_grace` window catches immediate `{"code":401|403|429|500}` refusals.
- Covers `order.{place,amend,cancel,batchNew,batchCancel}`, `fco.{place,cancel,list,orderbook,statusHistory}`,
  `query.{accountBalance,position,ppmmrAccount,orderBook,maxBuySell}`, and single-account push
  subscriptions (`subscribe_order_events`, `subscribe_portfolio_events`, `unsubscribe_all_events`).

## Request Signing Details (`feature = "trading"`)

- **Algorithm:** RSA PKCS#1 v1.5 with SHA-256 over the exact UTF-8 JSON bytes sent on the wire.
- **Accepted key formats (`SSI_PRIVATE_KEY`):**
  1. Base64-encoded `<RSAKeyValue>` XML (SSI portal export, full CRT or minimal `Modulus`/`D`).
  2. Raw `<RSAKeyValue>` XML.
  3. PEM PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`) and PEM PKCS#8 (`-----BEGIN PRIVATE KEY-----`).
- **REST header:** `X-Signature: <lowercase hex>` over raw request body bytes.
- **Trading WS field:** `"signature": "<base64|hex>"` over exact verbatim `"params"` JSON bytes.
