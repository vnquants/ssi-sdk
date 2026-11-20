//! Async client for the SSI `FastConnect` V3 Trading WebSocket (`/ws/v3/trading`).

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _};
use rust_decimal::Decimal;
use secrecy::ExposeSecret as _;
use serde_json::{Map, Value};
use tokio::{net::TcpStream, sync::mpsc, time::Instant};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        self, Message,
        client::IntoClientRequest as _,
        http::{
            HeaderValue,
            header::{AUTHORIZATION, USER_AGENT},
        },
    },
};
use tokio_util::sync::CancellationToken;

use super::{
    config::{SignatureEncoding, TradingWsConfig},
    protocol::{
        self, WireEncoding, WsRateLimit, WsResponse, build_command_frame, is_mutating_method,
        is_rejection, is_signature_rejection_code, parse_rejection, unwrap_response_frame,
    },
};
use crate::{
    client::SsiClient,
    de::format_plain_decimal,
    error::{Error, Result, TradingWsError},
    model::{AccountNo, ClientRequestId, check_symbol},
    rest::{
        AccountBalance, BatchCancelOrderItem, BatchOrderResponse, BatchPlaceOrderItem,
        CancelOrderRequest, CancelOrderResponse, FcoCancelRequest, FcoCancelResponse, FcoListPage,
        FcoListQuery, FcoOrderBookPage, FcoPlaceRequest, FcoPlaceResponse, FcoStatusHistoryItem,
        MaxBuySell, ModifyOrderRequest, ModifyOrderResponse, OrderBook, OrderBookQuery,
        PlaceOrderRequest, PlaceOrderResponse, Positions, Ppmmr,
        account::{parse_max_buy_sell, parse_order_book, parse_positions},
        fco::{
            build_fco_cancel_json, build_fco_place_json, parse_fco_id, parse_fco_list_page,
            parse_fco_order_book_page, parse_fco_status_history,
        },
        orders::{
            build_batch_cancel_json, build_batch_place_json, build_cancel_order_json,
            build_modify_order_json, build_place_order_json,
        },
    },
    secret::mask_topic,
    stream::{
        SubscriptionAck,
        common::warn_if_handshake_is_traced,
        trading::{
            TradingEvent,
            message::decode_text,
            protocol::{Inbound, classify},
        },
    },
    time::format_vn_date,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Unsolicited frames emitted on the Trading WebSocket event receiver (frames with no matching
/// in-flight command `id`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TradingWsEvent {
    /// Real-time trading event (`orderEvent`, `orderMatchEvent`, `clientPortfolioEvent`,
    /// `fcoEvent`) from a subscription on this socket.
    Event(Box<TradingEvent>),
    /// Subscription acknowledgement (`SUBSCRIBE` / `UNSUBSCRIBE`).
    Ack(SubscriptionAck),
    /// Server-initiated or late frame that did not match any pending command.
    Other {
        /// Shape description (top-level keys only, never values).
        shape: String,
    },
}

enum CallItem {
    Frame(String),
    Rejected(TradingWsError),
    Closed,
}

struct SharedState {
    pending: HashMap<String, mpsc::Sender<CallItem>>,
    encoding: WireEncoding,
    rejection: Option<TradingWsError>,
    rate_limit: Option<WsRateLimit>,
    blocked_until: Option<Instant>,
    connected: bool,
}

struct Inner {
    client: SsiClient,
    ws_config: TradingWsConfig,
    outbound: mpsc::UnboundedSender<String>,
    state: Mutex<SharedState>,
    cancel: CancellationToken,
}

/// Handle to one in-flight Trading WebSocket command that may receive multiple frames with the
/// same correlation `id` (for example an immediate `PD` ack followed by an order update).
///
/// Dropping `WsCall` automatically deregisters the correlation id; any subsequent frames with
/// that id are delivered to the [`TradingWsEvent`] channel.
pub struct WsCall {
    inner: Arc<Inner>,
    id: String,
    rx: mpsc::Receiver<CallItem>,
    fixed_hint: Option<WireEncoding>,
}

impl fmt::Debug for WsCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsCall")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl WsCall {
    /// Command correlation id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Awaits the next response frame for this command.
    ///
    /// Pass `timeout = None` to use [`TradingWsConfig::request_timeout`]. A timeout returns
    /// [`Error::Timeout`] and **never retries**; reconcile through the order book or order
    /// events before acting again.
    pub async fn next(&mut self, timeout: Option<Duration>) -> Result<WsResponse> {
        let limit = timeout.unwrap_or(self.inner.ws_config.request_timeout);
        let item = tokio::time::timeout(limit, self.rx.recv())
            .await
            .map_err(|_| Error::Timeout(format!("Trading WS command {} timed out", self.id)))?;
        match item {
            None | Some(CallItem::Closed) => Err(Error::StreamClosed),
            Some(CallItem::Rejected(err)) => Err(Error::TradingWs(err)),
            Some(CallItem::Frame(raw)) => {
                let response =
                    unwrap_response_frame(&raw, self.fixed_hint).map_err(Error::TradingWs)?;
                if let Some(rl) = response.rate_limit {
                    self.inner.note_rate_limit(rl);
                }
                Ok(response)
            }
        }
    }
}

impl Drop for WsCall {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.pending.remove(&self.id);
    }
}

/// Connected client for the SSI `FastConnect` V3 Trading WebSocket (`/ws/v3/trading`).
///
/// Explicit opt-in: opened via [`SsiClient::connect_trading_ws`]. Cloning is cheap (`Arc`).
#[derive(Clone)]
pub struct TradingWsClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for TradingWsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TradingWsClient")
            .field("connected", &self.is_connected())
            .field("rate_limit", &self.rate_limit())
            .finish_non_exhaustive()
    }
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, SharedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn note_rate_limit(&self, rl: WsRateLimit) {
        let mut state = self.state();
        state.rate_limit = Some(rl);
        if rl.remaining == Some(0)
            && let Some(reset_ms) = rl.reset_ms
        {
            let wait = Duration::from_millis(reset_ms).min(self.ws_config.max_rate_limit_wait);
            state.blocked_until = Some(Instant::now() + wait);
        }
    }

    fn fail_pending(&self, rejection: Option<&TradingWsError>) {
        let waiters = {
            let mut state = self.state();
            state.connected = false;
            if let Some(rej) = rejection {
                state.rejection = Some(rej.clone());
            }
            std::mem::take(&mut state.pending)
        };
        for (_, tx) in waiters {
            let item = rejection
                .cloned()
                .map_or(CallItem::Closed, CallItem::Rejected);
            let _ = tx.try_send(item);
        }
    }
}

impl TradingWsClient {
    pub(crate) async fn connect(
        client: SsiClient,
        ws_config: TradingWsConfig,
    ) -> Result<(Self, mpsc::Receiver<TradingWsEvent>)> {
        let token = client.auth().trading_access_token().await?;
        let mut request = client
            .config()
            .trading_ws_url()
            .as_str()
            .into_client_request()
            .map_err(|err| Error::WebSocket(Box::new(err)))?;
        let mut auth_header = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
            .map_err(|_| {
            Error::Authentication("access token is not a valid header value".to_owned())
        })?;
        auth_header.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, auth_header);
        if let Ok(ua) = HeaderValue::from_str(&client.config().user_agent) {
            request.headers_mut().insert(USER_AGENT, ua);
        }
        warn_if_handshake_is_traced();

        let connect_timeout = client.config().connect_timeout;
        let (socket, _) = tokio::time::timeout(connect_timeout, connect_async(request))
            .await
            .map_err(|_| Error::Timeout("Trading WebSocket handshake timed out".to_owned()))?
            .map_err(|err| {
                if let tungstenite::Error::Http(ref resp) = err
                    && matches!(resp.status().as_u16(), 401 | 403)
                {
                    client.auth().invalidate(&token);
                }
                Error::WebSocket(Box::new(err))
            })?;

        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(ws_config.event_channel_capacity);
        let cancel = CancellationToken::new();

        let initial_encoding = match ws_config.signature_encoding {
            SignatureEncoding::Auto | SignatureEncoding::Base64 => WireEncoding::Base64,
            SignatureEncoding::Hex => WireEncoding::Hex,
        };
        let inner = Arc::new(Inner {
            client: client.clone(),
            ws_config,
            outbound: out_tx,
            state: Mutex::new(SharedState {
                pending: HashMap::new(),
                encoding: initial_encoding,
                rejection: None,
                rate_limit: None,
                blocked_until: None,
                connected: true,
            }),
            cancel: cancel.clone(),
        });

        spawn_io_loop(socket, Arc::clone(&inner), out_rx, event_tx, token, cancel);

        // Wait up to handshake_grace to catch SSI's post-101 refusal frame.
        if !ws_config.handshake_grace.is_zero() {
            let deadline = Instant::now() + ws_config.handshake_grace;
            let step = Duration::from_millis(25);
            while Instant::now() < deadline {
                {
                    let state = inner.state();
                    if let Some(rej) = state.rejection.clone() {
                        drop(state);
                        inner.cancel.cancel();
                        return Err(Error::TradingWs(rej));
                    }
                    if !state.connected {
                        break;
                    }
                }
                tokio::time::sleep(step.min(deadline.saturating_duration_since(Instant::now())))
                    .await;
            }
        }
        let post_grace_rejection = inner.state().rejection.clone();
        if let Some(rej) = post_grace_rejection {
            inner.cancel.cancel();
            return Err(Error::TradingWs(rej));
        }

        if let Some(interval) = ws_config.ping_interval {
            let ping_inner = Arc::clone(&inner);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        () = ping_inner.cancel.cancelled() => return,
                        _ = ticker.tick() => {
                            if ping_inner.outbound.send(protocol::ping_frame()).is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }

        Ok((Self { inner }, event_rx))
    }

    /// Whether the underlying WebSocket connection is open.
    pub fn is_connected(&self) -> bool {
        self.inner.state().connected
    }

    /// Post-upgrade refusal error sent by SSI, if any.
    pub fn rejection(&self) -> Option<TradingWsError> {
        self.inner.state().rejection.clone()
    }

    /// Latest quota snapshot reported in a response frame (`"rateLimit"`).
    pub fn rate_limit(&self) -> Option<WsRateLimit> {
        self.inner.state().rate_limit
    }

    /// Closes the Trading WebSocket connection and wakes all in-flight calls.
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }

    /// Sends a raw `PING_PONG` keep-alive frame.
    pub fn ping(&self) -> Result<()> {
        self.send_raw_frame(protocol::ping_frame())
    }

    /// Sends an arbitrary RPC command with pre-serialized `params_json` and returns a multi-frame
    /// [`WsCall`] handle.
    pub async fn command_stream(&self, method: &str, params_json: &str) -> Result<WsCall> {
        let encoding = self.inner.state().encoding;
        self.send_command(method, params_json, encoding).await
    }

    /// Sends one RPC command and awaits its first response frame.
    ///
    /// Never retried on timeout or server error, with one safe exception: in
    /// [`SignatureEncoding::Auto`] mode, if a mutating command's signature is rejected before
    /// execution (`401006` / `401008` / `401013`), the exact same `params_json` is sent once
    /// more with the alternate encoding and the accepted encoding is remembered.
    pub async fn command_raw(&self, method: &str, params_json: &str) -> Result<WsResponse> {
        let used = self.inner.state().encoding;
        let first = {
            let mut call = self.send_command(method, params_json, used).await?;
            call.next(None).await
        };
        match first {
            Ok(resp) => Ok(resp),
            Err(Error::TradingWs(err))
                if self.inner.ws_config.signature_encoding == SignatureEncoding::Auto
                    && is_mutating_method(method)
                    && is_signature_rejection_code(&err.code) =>
            {
                let alt = used.other();
                let mut call = self.send_command(method, params_json, alt).await?;
                match call.next(None).await {
                    Ok(resp) => {
                        self.inner.state().encoding = alt;
                        Ok(resp)
                    }
                    Err(Error::TradingWs(mut second)) => {
                        second.message.push_str(
                            " (refused with both base64 and hex signatures: check SSI_PRIVATE_KEY)",
                        );
                        Err(Error::TradingWs(second))
                    }
                    Err(other) => Err(other),
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Sends one RPC command with a `serde_json::Value` payload.
    pub async fn command(&self, method: &str, params: &Value) -> Result<WsResponse> {
        let text = serde_json::to_string(params).map_err(|_| {
            Error::InvalidRequest("command params could not be serialized".to_owned())
        })?;
        self.command_raw(method, &text).await
    }

    async fn send_command(
        &self,
        method: &str,
        params_json: &str,
        encoding: WireEncoding,
    ) -> Result<WsCall> {
        let wait_until = {
            let state = self.inner.state();
            if let Some(rej) = state.rejection.clone() {
                return Err(Error::TradingWs(rej));
            }
            if !state.connected {
                return Err(Error::StreamClosed);
            }
            self.inner
                .ws_config
                .respect_rate_limit
                .then_some(state.blocked_until)
                .flatten()
        };
        if let Some(until) = wait_until
            && until > Instant::now()
        {
            tokio::time::sleep_until(until).await;
        }

        let id = self.allocate_id();
        let signer = self.inner.client.signer();
        let timestamp_ms = self.inner.client.now().timestamp_millis();
        let frame = build_command_frame(method, params_json, signer, &id, timestamp_ms, encoding)?;

        let (tx, rx) = mpsc::channel(16);
        {
            let mut state = self.inner.state();
            if !state.connected {
                return Err(Error::StreamClosed);
            }
            state.pending.insert(id.clone(), tx);
        }

        let fixed_hint = match self.inner.ws_config.signature_encoding {
            SignatureEncoding::Auto => None,
            SignatureEncoding::Base64 => Some(WireEncoding::Base64),
            SignatureEncoding::Hex => Some(WireEncoding::Hex),
        };
        let call = WsCall {
            inner: Arc::clone(&self.inner),
            id,
            rx,
            fixed_hint,
        };
        if self.inner.outbound.send(frame).is_err() {
            return Err(Error::StreamClosed);
        }
        Ok(call)
    }

    fn allocate_id(&self) -> String {
        use rand::RngCore as _;
        let mut buf = [0_u8; 8];
        loop {
            rand::rngs::OsRng.fill_bytes(&mut buf);
            let candidate = hex::encode(buf);
            if !self.inner.state().pending.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    fn send_raw_frame(&self, frame: String) -> Result<()> {
        {
            let state = self.inner.state();
            if let Some(rej) = state.rejection.clone() {
                return Err(Error::TradingWs(rej));
            }
            if !state.connected {
                return Err(Error::StreamClosed);
            }
        }
        self.inner
            .outbound
            .send(frame)
            .map_err(|_| Error::StreamClosed)
    }

    // ── typed orders (signed, never retried on timeout) ─────────────────────

    /// Places a single order (`order.place`).
    pub async fn place_order(&self, request: &PlaceOrderRequest) -> Result<PlaceOrderResponse> {
        let params = build_place_order_json(self.inner.client.config(), request)?;
        let resp = self.command_raw("order.place", &params).await?;
        resp.decode_result()
    }

    /// Amends an order's price OR quantity (`order.amend`).
    pub async fn amend_order(&self, request: &ModifyOrderRequest) -> Result<ModifyOrderResponse> {
        let params = build_modify_order_json(self.inner.client.config(), request)?;
        let resp = self.command_raw("order.amend", &params).await?;
        resp.decode_result()
    }

    /// Cancels a single order (`order.cancel`).
    pub async fn cancel_order(&self, request: &CancelOrderRequest) -> Result<CancelOrderResponse> {
        let params = build_cancel_order_json(self.inner.client.config(), request)?;
        let resp = self.command_raw("order.cancel", &params).await?;
        resp.decode_result()
    }

    /// Places a batch of orders (`order.batchNew`).
    pub async fn place_batch_orders(
        &self,
        items: &[BatchPlaceOrderItem],
        batch_id: Option<ClientRequestId>,
    ) -> Result<BatchOrderResponse> {
        let params = build_batch_place_json(self.inner.client.config(), items, batch_id)?;
        let resp = self.command_raw("order.batchNew", &params).await?;
        resp.decode_result()
    }

    /// Cancels a batch of orders (`order.batchCancel`).
    pub async fn cancel_batch_orders(
        &self,
        items: &[BatchCancelOrderItem],
        batch_id: Option<ClientRequestId>,
    ) -> Result<BatchOrderResponse> {
        let params = build_batch_cancel_json(self.inner.client.config(), items, batch_id)?;
        let resp = self.command_raw("order.batchCancel", &params).await?;
        resp.decode_result()
    }

    // ── typed FCO ───────────────────────────────────────────────────────────

    /// Places a Flexible Conditional Order (`fco.place`).
    pub async fn place_fco(&self, request: &FcoPlaceRequest) -> Result<FcoPlaceResponse> {
        let params = build_fco_place_json(self.inner.client.config(), request)?;
        let resp = self.command_raw("fco.place", &params).await?;
        Ok(FcoPlaceResponse {
            fco_id: parse_fco_id("tradingws:fco.place", resp.result_raw.as_bytes())?,
        })
    }

    /// Cancels a Flexible Conditional Order (`fco.cancel`).
    pub async fn cancel_fco(&self, request: &FcoCancelRequest) -> Result<FcoCancelResponse> {
        let params = build_fco_cancel_json(self.inner.client.config(), request)?;
        let resp = self.command_raw("fco.cancel", &params).await?;
        Ok(FcoCancelResponse {
            fco_id: parse_fco_id("tradingws:fco.cancel", resp.result_raw.as_bytes())?,
        })
    }

    /// Lists Flexible Conditional Orders (`fco.list`; accepts at most one `processStatus`).
    pub async fn fco_list(&self, query: &FcoListQuery) -> Result<FcoListPage> {
        let params = query.to_ws_params()?;
        let resp = self.command("fco.list", &params).await?;
        parse_fco_list_page(resp.result_raw.as_bytes())
    }

    /// Queries an FCO's child order book (`fco.orderbook`).
    pub async fn fco_order_book(
        &self,
        fco_id: &str,
        page_index: u32,
        page_size: u32,
    ) -> Result<FcoOrderBookPage> {
        let fco_id = fco_id.trim();
        if fco_id.is_empty() {
            return Err(Error::InvalidRequest("fcoId cannot be empty".to_owned()));
        }
        let params = serde_json::json!({
            "fcoId": fco_id,
            "pageIndex": page_index.max(1),
            "pageSize": page_size.max(1),
        });
        let resp = self.command("fco.orderbook", &params).await?;
        parse_fco_order_book_page(resp.result_raw.as_bytes())
    }

    /// Queries an FCO's status transitions (`fco.statusHistory`).
    pub async fn fco_status_history(&self, fco_id: &str) -> Result<Vec<FcoStatusHistoryItem>> {
        let fco_id = fco_id.trim();
        if fco_id.is_empty() {
            return Err(Error::InvalidRequest("fcoId cannot be empty".to_owned()));
        }
        let params = serde_json::json!({ "fcoId": fco_id });
        let resp = self.command("fco.statusHistory", &params).await?;
        parse_fco_status_history(resp.result_raw.as_bytes())
    }

    // ── typed queries (unsigned) ────────────────────────────────────────────

    /// Queries account balance (`query.accountBalance`).
    pub async fn balance(&self, account: &AccountNo) -> Result<AccountBalance> {
        account.validate()?;
        let mut map = Map::new();
        map.insert(
            "accountNo".to_owned(),
            Value::String(account.as_str().to_owned()),
        );
        if let Some(client_id) = self.inner.client.config().credentials.client_id() {
            map.insert("clientId".to_owned(), Value::String(client_id.to_owned()));
        }
        let resp = self
            .command("query.accountBalance", &Value::Object(map))
            .await?;
        resp.decode_result()
    }

    /// Queries account positions (`query.position`).
    pub async fn positions(&self, account: &AccountNo, query_summary: bool) -> Result<Positions> {
        account.validate()?;
        let mut map = Map::new();
        map.insert(
            "accountNo".to_owned(),
            Value::String(account.as_str().to_owned()),
        );
        map.insert("querySummary".to_owned(), Value::Bool(query_summary));
        if let Some(client_id) = self.inner.client.config().credentials.client_id() {
            map.insert("clientId".to_owned(), Value::String(client_id.to_owned()));
        }
        let resp = self.command("query.position", &Value::Object(map)).await?;
        parse_positions(resp.result_raw.as_bytes(), account)
    }

    /// Queries purchasing power & margin ratio (`query.ppmmrAccount`).
    pub async fn ppmmr(&self, account: &AccountNo) -> Result<Ppmmr> {
        account.validate()?;
        let params = serde_json::json!({ "accountNo": account.as_str() });
        let resp = self.command("query.ppmmrAccount", &params).await?;
        resp.decode_result()
    }

    /// Queries one page of the order book (`query.orderBook`).
    pub async fn order_book(&self, query: &OrderBookQuery) -> Result<OrderBook> {
        query.validate()?;
        let mut map = Map::new();
        map.insert(
            "accountNo".to_owned(),
            Value::String(query.account_no.as_str().to_owned()),
        );
        if let Some(from) = query.from {
            map.insert("from".to_owned(), Value::String(format_vn_date(from)));
        }
        if let Some(to) = query.to {
            map.insert("to".to_owned(), Value::String(format_vn_date(to)));
        }
        if let Some(symbol) = &query.symbol {
            map.insert("symbol".to_owned(), Value::String(check_symbol(symbol)?));
        }
        if let Some(status) = &query.order_status {
            map.insert(
                "orderStatus".to_owned(),
                Value::String(status.as_str().to_owned()),
            );
        }
        map.insert("pageIndex".to_owned(), Value::from(query.page_index));
        map.insert("pageSize".to_owned(), Value::from(query.page_size));
        let resp = self.command("query.orderBook", &Value::Object(map)).await?;
        parse_order_book(resp.result_raw.as_bytes(), &query.account_no)
    }

    /// Queries max buy/sell quantity (`query.maxBuySell`).
    pub async fn max_buy_sell(
        &self,
        account: &AccountNo,
        symbol: &str,
        price: Option<Decimal>,
    ) -> Result<MaxBuySell> {
        account.validate()?;
        let symbol = check_symbol(symbol)?;
        let params = match price {
            None => serde_json::json!({
                "accountNo": account.as_str(),
                "symbol": symbol,
            })
            .to_string(),
            Some(value) => {
                if value < Decimal::ZERO {
                    return Err(Error::InvalidRequest("price cannot be negative".to_owned()));
                }
                // Trading WS query.maxBuySell expects price as a JSON number.
                let num_text = format_plain_decimal(value);
                let acc_json = serde_json::to_string(account.as_str())
                    .map_err(|_| Error::InvalidRequest("invalid account".to_owned()))?;
                let sym_json = serde_json::to_string(&symbol)
                    .map_err(|_| Error::InvalidRequest("invalid symbol".to_owned()))?;
                format!(r#"{{"accountNo":{acc_json},"symbol":{sym_json},"price":{num_text}}}"#)
            }
        };
        let resp = self.command_raw("query.maxBuySell", &params).await?;
        parse_max_buy_sell(resp.result_raw.as_bytes(), &symbol)
    }

    // ── subscriptions on Trading WS (single account only, no wildcards) ─────

    /// Subscribes to `order.<account>` push events on this Trading WebSocket.
    pub fn subscribe_order_events(&self, account: &AccountNo) -> Result<()> {
        let topic = single_account_topic("order", account)?;
        self.send_raw_frame(protocol::subscribe_frame(&[&topic]))
    }

    /// Unsubscribes from `order.<account>` push events on this Trading WebSocket.
    pub fn unsubscribe_order_events(&self, account: &AccountNo) -> Result<()> {
        let topic = single_account_topic("order", account)?;
        self.send_raw_frame(protocol::unsubscribe_frame(Some(&[&topic])))
    }

    /// Subscribes to `portfolio.<account>` push events (derivative accounts) on this socket.
    pub fn subscribe_portfolio_events(&self, account: &AccountNo) -> Result<()> {
        let topic = single_account_topic("portfolio", account)?;
        self.send_raw_frame(protocol::subscribe_frame(&[&topic]))
    }

    /// Unsubscribes from `portfolio.<account>` push events on this socket.
    pub fn unsubscribe_portfolio_events(&self, account: &AccountNo) -> Result<()> {
        let topic = single_account_topic("portfolio", account)?;
        self.send_raw_frame(protocol::unsubscribe_frame(Some(&[&topic])))
    }

    /// Drops all `TRADING` subscriptions on this Trading WebSocket.
    pub fn unsubscribe_all_events(&self) -> Result<()> {
        self.send_raw_frame(protocol::unsubscribe_frame(None))
    }
}

fn single_account_topic(kind: &str, account: &AccountNo) -> Result<String> {
    account.validate()?;
    let raw = account.as_str();
    if raw.contains('*') || raw.contains(',') {
        return Err(Error::InvalidRequest(
            "Trading WS subscriptions require one account at a time (no wildcards or commas)"
                .to_owned(),
        ));
    }
    Ok(format!("{kind}.{raw}"))
}

fn spawn_io_loop(
    mut socket: Socket,
    inner: Arc<Inner>,
    mut out_rx: mpsc::UnboundedReceiver<String>,
    event_tx: mpsc::Sender<TradingWsEvent>,
    token: secrecy::SecretString,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    let _ = socket.close(None).await;
                    inner.fail_pending(None);
                    return;
                }
                outbound = out_rx.recv() => {
                    let Some(text) = outbound else {
                        let _ = socket.close(None).await;
                        inner.fail_pending(None);
                        return;
                    };
                    if let Err(error) = socket.send(Message::text(text)).await {
                        tracing::warn!(%error, "Trading WebSocket write failed");
                        inner.fail_pending(None);
                        return;
                    }
                }
                inbound = socket.next() => {
                    let Some(frame) = inbound else {
                        inner.fail_pending(None);
                        return;
                    };
                    let message = match frame {
                        Ok(msg) => msg,
                        Err(error) => {
                            tracing::warn!(%error, "Trading WebSocket read failed");
                            inner.fail_pending(None);
                            return;
                        }
                    };
                    match message {
                        Message::Text(text) => {
                            if handle_inbound_text(&inner, &event_tx, &token, text.as_str()) {
                                return;
                            }
                        }
                        Message::Close(_) => {
                            inner.fail_pending(None);
                            return;
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    });
}

/// Returns `true` when the connection was fatally rejected and the loop should exit.
fn handle_inbound_text(
    inner: &Inner,
    event_tx: &mpsc::Sender<TradingWsEvent>,
    token: &secrecy::SecretString,
    raw: &str,
) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        tracing::warn!(
            bytes = raw.len(),
            "dropped non-JSON Trading WebSocket frame"
        );
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if is_rejection(object) {
        let rej = parse_rejection(object);
        if matches!(rej.status, 401 | 403) {
            inner.client.auth().invalidate(token);
        }
        inner.fail_pending(Some(&rej));
        return true;
    }

    if let Some(id) = object.get("id").and_then(Value::as_str) {
        let sender = inner.state().pending.get(id).cloned();
        if let Some(tx) = sender {
            if tx.try_send(CallItem::Frame(raw.to_owned())).is_err() {
                tracing::warn!(
                    id,
                    "dropped Trading WebSocket response frame: per-command buffer full"
                );
            }
            return false;
        }
    }

    let received_at = inner.client.now();
    match classify(raw) {
        Inbound::Trading { topic, payloads } => {
            let masked = mask_topic(&topic);
            for payload in payloads {
                let data = decode_text(&topic, &payload);
                let _ = event_tx.try_send(TradingWsEvent::Event(Box::new(TradingEvent {
                    topic: masked.clone(),
                    data,
                    received_at,
                })));
            }
        }
        Inbound::Ack(ack) => {
            let _ = event_tx.try_send(TradingWsEvent::Ack(ack));
        }
        Inbound::PingAck => {}
        _ => {
            let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
            keys.sort_unstable();
            let shape = format!("object{{{}}}", keys.join(","));
            let _ = event_tx.try_send(TradingWsEvent::Other { shape });
        }
    }
    false
}
