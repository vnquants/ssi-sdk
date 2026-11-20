//! The reconnecting supervisor behind [`super::MarketStream`].
//!
//! Events go through a local queue. The socket is only read while the queue is empty, and the
//! queue is drained by reserving channel permits inside the same `select!`, so a slow consumer
//! holds back reading (TCP back-pressure) but never pings, commands, cancellation or the
//! watchdog.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use secrecy::{ExposeSecret as _, SecretString};
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
        protocol::frame::coding::CloseCode,
    },
};
use tokio_util::sync::CancellationToken;

use super::{
    Command, DataEvent, DisconnectReason, HeartbeatKind, MarketStreamEvent,
    config::MarketStreamConfig,
    message::{self, MarketData},
    protocol::{self, Inbound},
    topic::Topic,
};
use crate::{
    SsiClient,
    error::Error,
    stream::common::{
        Counters, STALL_THRESHOLD, is_fatal_rejection, list_frame, millis, ping_frame, pong_frame,
        rejection_http_status, warn_if_handshake_is_traced,
    },
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Why a connection attempt or session ended, and the least time to wait before retrying.
struct Ending {
    reason: DisconnectReason,
    min_wait: Duration,
}

enum SessionEnd {
    Cancelled,
    Ended(Ending),
}

const fn ended(reason: DisconnectReason) -> SessionEnd {
    SessionEnd::Ended(Ending {
        reason,
        min_wait: Duration::ZERO,
    })
}

/// Events waiting for the consumer, and how long the consumer has been holding them up.
struct Outbox {
    events: mpsc::Sender<MarketStreamEvent>,
    queue: VecDeque<MarketStreamEvent>,
    blocked_since: Option<Instant>,
    counters: Arc<Counters>,
}

impl Outbox {
    fn push(&mut self, event: MarketStreamEvent) {
        if self.queue.is_empty() {
            self.blocked_since = Some(Instant::now());
        }
        self.queue.push_back(event);
    }

    /// Hands one queued event to a reserved permit. Returns the length of a stall that just
    /// ended, if the queue is now empty.
    fn deliver(&mut self, permit: mpsc::Permit<'_, MarketStreamEvent>) -> Option<Duration> {
        if let Some(event) = self.queue.pop_front() {
            permit.send(event);
        }
        if !self.queue.is_empty() {
            return None;
        }
        let stalled = self.blocked_since.take()?.elapsed();
        if stalled >= STALL_THRESHOLD {
            self.counters.record_stall(stalled);
            if stalled >= Duration::from_secs(1) {
                tracing::warn!(
                    stalled_ms = millis(stalled),
                    "market stream consumer is slow; socket reads were held back"
                );
            }
            return Some(stalled);
        }
        None
    }

    /// Delivers everything queued, waiting for the consumer. `false` when cancelled or the
    /// handle is gone.
    async fn drain(&mut self, cancel: &CancellationToken) -> bool {
        let events = self.events.clone();
        while !self.queue.is_empty() {
            let permit = tokio::select! {
                () = cancel.cancelled() => return false,
                permit = events.reserve() => permit,
            };
            let Ok(permit) = permit else { return false };
            self.deliver(permit);
        }
        true
    }
}

pub(crate) struct Supervisor {
    client: SsiClient,
    config: MarketStreamConfig,
    outbox: Outbox,
    commands: mpsc::UnboundedReceiver<Command>,
    cancel: CancellationToken,
    topics: Vec<Topic>,
    connections: u64,
    last_data: Instant,
    last_data_at: Option<DateTime<Utc>>,
    window_open_since: Option<Instant>,
    stale_reported: bool,
    unknown_topics: HashSet<String>,
    unknown_shapes: HashSet<String>,
}

impl Supervisor {
    pub(crate) fn new(
        client: SsiClient,
        mut config: MarketStreamConfig,
        events: mpsc::Sender<MarketStreamEvent>,
        commands: mpsc::UnboundedReceiver<Command>,
        cancel: CancellationToken,
        counters: Arc<Counters>,
    ) -> Self {
        let initial = std::mem::take(&mut config.topics);
        let mut supervisor = Self {
            client,
            config,
            outbox: Outbox {
                events,
                queue: VecDeque::new(),
                blocked_since: None,
                counters,
            },
            commands,
            cancel,
            topics: Vec::new(),
            connections: 0,
            last_data: Instant::now(),
            last_data_at: None,
            window_open_since: None,
            stale_reported: false,
            unknown_topics: HashSet::new(),
            unknown_shapes: HashSet::new(),
        };
        supervisor.add_topics(initial);
        supervisor
    }

    fn now(&self) -> DateTime<Utc> {
        self.client.now()
    }

    pub(crate) async fn run(mut self) {
        let cancel = self.cancel.clone();
        let mut attempt: u32 = 0;
        loop {
            self.drain_offline_commands();
            let connected = tokio::select! {
                () = cancel.cancelled() => break,
                result = self.connect() => result,
            };
            let ending = match connected {
                Err(ending) => ending,
                Ok((socket, token)) => {
                    self.connections += 1;
                    self.outbox
                        .counters
                        .connections
                        .fetch_add(1, Ordering::Relaxed);
                    let opened = Instant::now();
                    match self.session(socket, &token).await {
                        SessionEnd::Cancelled => break,
                        SessionEnd::Ended(ending) => {
                            if opened.elapsed() >= self.config.backoff.stable_after {
                                attempt = 0;
                            }
                            ending
                        }
                    }
                }
            };
            let retry_in = self.config.backoff.delay(attempt).max(ending.min_wait);
            attempt = attempt.saturating_add(1);
            tracing::warn!(
                reason = ?ending.reason,
                retry_in_ms = millis(retry_in),
                attempt,
                "SSI market stream disconnected; reconnecting"
            );
            let at = self.now();
            self.outbox.push(MarketStreamEvent::Disconnected {
                reason: ending.reason,
                retry_in,
                at,
            });
            if !self.outbox.drain(&cancel).await {
                break;
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(retry_in) => {}
            }
        }
        tracing::info!(connections = self.connections, "SSI market stream stopped");
    }

    async fn connect(&self) -> Result<(Socket, SecretString), Ending> {
        let token = self
            .client
            .auth()
            .access_token()
            .await
            .map_err(|error| match error {
                Error::ReauthenticationRequired(api) => Ending {
                    reason: DisconnectReason::ReauthenticationRequired(api.to_string()),
                    min_wait: self.config.backoff.reauth_wait,
                },
                Error::AuthBackoff { retry_in, .. } => Ending {
                    min_wait: retry_in,
                    reason: DisconnectReason::TokenUnavailable(error.to_string()),
                },
                other => Ending {
                    min_wait: Duration::from_secs(5),
                    reason: DisconnectReason::TokenUnavailable(other.to_string()),
                },
            })?;
        let failed = |message: String| Ending {
            reason: DisconnectReason::ConnectFailed {
                status: None,
                message,
            },
            min_wait: Duration::ZERO,
        };
        let mut request = self
            .client
            .config()
            .stream_url()
            .as_str()
            .into_client_request()
            .map_err(|error| failed(error.to_string()))?;
        let mut header = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
            .map_err(|_| failed("token is not a valid header value".to_owned()))?;
        header.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, header);
        if let Ok(ua) = HeaderValue::from_str(&self.client.config().user_agent) {
            request.headers_mut().insert(USER_AGENT, ua);
        }
        warn_if_handshake_is_traced();
        let connecting = tokio::time::timeout(self.config.connect_timeout, connect_async(request));
        match connecting.await {
            Err(_) => Err(failed(format!(
                "connect timed out after {:?}",
                self.config.connect_timeout
            ))),
            Ok(Err(tungstenite::Error::Http(response))) => {
                let status = response.status().as_u16();
                if matches!(status, 401 | 403) {
                    self.client.auth().invalidate(&token);
                }
                Err(Ending {
                    reason: DisconnectReason::ConnectFailed {
                        status: Some(status),
                        message: format!("upgrade refused with HTTP {status}"),
                    },
                    min_wait: if status == 429 {
                        self.config.backoff.rate_limited
                    } else {
                        Duration::ZERO
                    },
                })
            }
            Ok(Err(error)) => Err(failed(error.to_string())),
            Ok(Ok((socket, _response))) => Ok((socket, token)),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one select loop; splitting it would scatter the back-pressure logic"
    )]
    async fn session(&mut self, mut socket: Socket, token: &SecretString) -> SessionEnd {
        let cancel = self.cancel.clone();
        let events = self.outbox.events.clone();
        let connection = self.connections;
        if let Err(error) = self
            .send_requests(&mut socket, "SUBSCRIBE", &self.topics.clone())
            .await
        {
            return ended(DisconnectReason::Transport(error.to_string()));
        }
        tracing::info!(
            connection,
            topics = self.topics.len(),
            "SSI market stream connected"
        );
        // A fresh connection starts a fresh silence measurement.
        self.last_data = Instant::now();
        let at = self.now();
        self.outbox
            .push(MarketStreamEvent::Connected { connection, at });
        if connection > 1 && !self.topics.is_empty() {
            let topics = self.topics.len();
            self.outbox.push(MarketStreamEvent::Resubscribed {
                connection,
                topics,
                at,
            });
        }
        let mut last_frame = Instant::now();
        let mut ping = self.config.ping_interval.map(|period| {
            let mut interval = tokio::time::interval_at(Instant::now() + period, period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        let shortest = self
            .config
            .stale
            .after()
            .map_or(self.config.idle_timeout, |after| {
                after.min(self.config.idle_timeout)
            });
        let check_every = (shortest / 4).clamp(Duration::from_millis(20), Duration::from_secs(1));
        let mut watchdog = tokio::time::interval(check_every);
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let backlog = !self.outbox.queue.is_empty();
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Err(error) = socket.close(None).await {
                        tracing::debug!(%error, "closing the market stream socket failed");
                    }
                    return SessionEnd::Cancelled;
                }
                permit = events.reserve(), if backlog => {
                    let Ok(permit) = permit else { return SessionEnd::Cancelled };
                    if self.outbox.deliver(permit).is_some_and(|stall| stall >= Duration::from_secs(1)) {
                        // Frames waited in the socket buffer; do not count that as silence.
                        last_frame = Instant::now();
                    }
                }
                command = self.commands.recv() => {
                    let Some(command) = command else { return SessionEnd::Cancelled };
                    if let Err(error) = self.apply_online(&mut socket, command).await {
                        return ended(DisconnectReason::Transport(error.to_string()));
                    }
                }
                () = tick(ping.as_mut()) => {
                    if let Err(error) = socket.send(Message::text(ping_frame())).await {
                        return ended(DisconnectReason::Transport(error.to_string()));
                    }
                }
                _ = watchdog.tick() => {
                    // While the consumer holds the queue nothing is read, so silence says
                    // nothing about the connection or the feed.
                    if !backlog {
                        let silent = last_frame.elapsed();
                        if silent >= self.config.idle_timeout {
                            return ended(DisconnectReason::IdleTimeout(silent));
                        }
                        if let Some(event) = self.stale_check() {
                            self.outbox.push(event);
                        }
                    }
                }
                frame = socket.next(), if !backlog => {
                    let Some(frame) = frame else {
                        return ended(DisconnectReason::Closed { code: None, reason: "end of stream".to_owned() });
                    };
                    let message = match frame {
                        Ok(message) => message,
                        Err(error) => return ended(DisconnectReason::Transport(error.to_string())),
                    };
                    last_frame = Instant::now();
                    if let Some(end) = self.handle_frame(&mut socket, message, token).await {
                        return end;
                    }
                }
            }
        }
    }

    async fn handle_frame(
        &mut self,
        socket: &mut Socket,
        message: Message,
        token: &SecretString,
    ) -> Option<SessionEnd> {
        match message {
            Message::Text(text) => self.handle_text(socket, text.as_str(), token).await,
            Message::Ping(_) => {
                self.heartbeat(HeartbeatKind::TransportPing);
                None
            }
            Message::Pong(_) => {
                self.heartbeat(HeartbeatKind::TransportPong);
                None
            }
            Message::Close(frame) => {
                let code = frame.as_ref().map(|frame| frame.code);
                if code == Some(CloseCode::Policy) {
                    // SSI closes with 1008 after rejecting a token; a bare 1008 means the same.
                    self.client.auth().invalidate(token);
                }
                Some(ended(DisconnectReason::Closed {
                    code: code.map(u16::from),
                    reason: frame.map_or_default(|frame| frame.reason.to_string()),
                }))
            }
            Message::Binary(bytes) => {
                tracing::debug!(bytes = bytes.len(), "ignoring a binary frame");
                None
            }
            Message::Frame(_) => None,
        }
    }

    async fn handle_text(
        &mut self,
        socket: &mut Socket,
        text: &str,
        token: &SecretString,
    ) -> Option<SessionEnd> {
        let received_at = self.now();
        self.outbox.counters.frames.fetch_add(1, Ordering::Relaxed);
        if self.config.raw_frames {
            self.outbox.push(MarketStreamEvent::RawFrame {
                text: text.to_owned(),
                received_at,
            });
        }
        match protocol::classify(text) {
            Inbound::Data { topic, payloads } => {
                if let Some(event) = self.note_data(received_at) {
                    self.outbox.push(event);
                }
                for payload in payloads {
                    let data = message::decode_text(&topic, &payload);
                    self.count_data(&topic, &data);
                    self.outbox
                        .push(MarketStreamEvent::Data(Box::new(DataEvent {
                            topic: topic.clone(),
                            data,
                            received_at,
                        })));
                }
                None
            }
            Inbound::Ack(ack) => {
                if !ack.is_clean() {
                    tracing::warn!(
                        method = %ack.method,
                        status = %ack.status,
                        invalid = ?ack.invalid_topics,
                        "SSI did not accept every topic"
                    );
                }
                self.outbox.push(MarketStreamEvent::Ack(ack));
                None
            }
            Inbound::PingAck => {
                self.heartbeat(HeartbeatKind::PingAck);
                None
            }
            Inbound::ServerPing { time } => {
                let pong = pong_frame(time.as_deref());
                if let Err(error) = socket.send(Message::text(pong)).await {
                    return Some(ended(DisconnectReason::Transport(error.to_string())));
                }
                self.heartbeat(HeartbeatKind::ServerPing);
                None
            }
            Inbound::Rejection {
                code,
                message,
                retry_after,
            } => self.handle_rejection(code, message, retry_after, token),
            Inbound::Subscriptions { data, trading } => {
                self.outbox
                    .push(MarketStreamEvent::Subscriptions { data, trading });
                None
            }
            Inbound::Other { shape } => {
                self.outbox
                    .counters
                    .unrecognised_frames
                    .fetch_add(1, Ordering::Relaxed);
                if self.unknown_shapes.insert(shape.clone()) {
                    tracing::warn!(
                        %shape,
                        bytes = text.len(),
                        "SSI sent a frame this client does not recognise; ignored \
                         (logged once per shape, counted in stats)"
                    );
                }
                None
            }
        }
    }

    fn handle_rejection(
        &self,
        code: i64,
        message: String,
        retry_after: Option<Duration>,
        token: &SecretString,
    ) -> Option<SessionEnd> {
        if !is_fatal_rejection(code) {
            tracing::warn!(
                code,
                %message,
                "SSI sent an error frame; the connection stays up"
            );
            return None;
        }
        let status = rejection_http_status(code);
        if matches!(status, 401 | 403) {
            self.client.auth().invalidate(token);
        }
        let min_wait = if status == 429 {
            self.config
                .backoff
                .rate_limited
                .max(retry_after.unwrap_or(Duration::ZERO))
        } else {
            retry_after.unwrap_or(Duration::ZERO)
        };
        Some(SessionEnd::Ended(Ending {
            reason: DisconnectReason::Rejected { code, message },
            min_wait,
        }))
    }

    fn count_data(&mut self, topic: &str, data: &MarketData) {
        let counters = &self.outbox.counters;
        counters.messages.fetch_add(1, Ordering::Relaxed);
        if let MarketData::Unknown { reason, .. } = data {
            counters.unknown_messages.fetch_add(1, Ordering::Relaxed);
            if self.unknown_topics.insert(topic.to_owned()) {
                tracing::warn!(
                    topic,
                    %reason,
                    "SSI market data did not match the documented shape; kept as Unknown \
                     (logged once per topic)"
                );
            }
        }
    }

    fn heartbeat(&mut self, kind: HeartbeatKind) {
        if self.config.emit_heartbeats {
            let at = self.now();
            self.outbox.push(MarketStreamEvent::Heartbeat { kind, at });
        }
    }

    fn note_data(&mut self, at: DateTime<Utc>) -> Option<MarketStreamEvent> {
        let silent_for = self.last_data.elapsed();
        self.last_data = Instant::now();
        self.last_data_at = Some(at);
        std::mem::take(&mut self.stale_reported)
            .then_some(MarketStreamEvent::Resumed { silent_for, at })
    }

    /// Silence counts from the later of the last data and the moment the activity window
    /// opened, so the night (or lunch) never makes a `Stale` fire at the opening bell.
    fn stale_check(&mut self) -> Option<MarketStreamEvent> {
        let now = self.now();
        let Some(threshold) = self.config.stale.threshold(now) else {
            self.window_open_since = None;
            return None;
        };
        let opened = *self.window_open_since.get_or_insert_with(Instant::now);
        if self.stale_reported {
            return None;
        }
        let silent_for = self.last_data.max(opened).elapsed();
        if silent_for < threshold {
            return None;
        }
        self.stale_reported = true;
        tracing::warn!(silent_ms = millis(silent_for), "SSI market data is stale");
        Some(MarketStreamEvent::Stale {
            silent_for,
            last_data_at: self.last_data_at,
            at: now,
        })
    }

    async fn send_requests(
        &self,
        socket: &mut Socket,
        method: &str,
        topics: &[Topic],
    ) -> Result<(), tungstenite::Error> {
        for frame in protocol::request_frames(method, topics, self.config.max_frame_bytes) {
            socket.send(Message::text(frame)).await?;
        }
        Ok(())
    }

    async fn apply_online(
        &mut self,
        socket: &mut Socket,
        command: Command,
    ) -> Result<(), tungstenite::Error> {
        match command {
            Command::Subscribe(topics) => {
                let added = self.add_topics(topics);
                if !added.is_empty() {
                    self.send_requests(socket, "SUBSCRIBE", &added).await?;
                }
            }
            Command::Unsubscribe(topics) => {
                let removed = self.remove_topics(&topics);
                if !removed.is_empty() {
                    self.send_requests(socket, "UNSUBSCRIBE", &removed).await?;
                }
            }
            Command::UnsubscribeAll => {
                let removed = std::mem::take(&mut self.topics);
                if !removed.is_empty() {
                    self.send_requests(socket, "UNSUBSCRIBE", &removed).await?;
                }
            }
            Command::ListSubscriptions => {
                socket.send(Message::text(list_frame())).await?;
            }
        }
        Ok(())
    }

    fn drain_offline_commands(&mut self) {
        while let Ok(command) = self.commands.try_recv() {
            match command {
                Command::Subscribe(topics) => {
                    self.add_topics(topics);
                }
                Command::Unsubscribe(topics) => {
                    self.remove_topics(&topics);
                }
                Command::UnsubscribeAll => {
                    self.topics.clear();
                }
                Command::ListSubscriptions => {
                    tracing::debug!("subscription list requested while disconnected; skipped");
                }
            }
        }
    }

    fn add_topics(&mut self, topics: Vec<Topic>) -> Vec<Topic> {
        let mut added = Vec::new();
        for topic in topics {
            if !self.topics.contains(&topic) {
                self.topics.push(topic.clone());
                added.push(topic);
            }
        }
        added
    }

    fn remove_topics(&mut self, topics: &[Topic]) -> Vec<Topic> {
        let removed: Vec<Topic> = self
            .topics
            .iter()
            .filter(|topic| topics.contains(topic))
            .cloned()
            .collect();
        self.topics.retain(|topic| !topics.contains(topic));
        removed
    }
}

async fn tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}
