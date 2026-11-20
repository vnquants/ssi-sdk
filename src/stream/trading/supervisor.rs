//! Reconnecting supervisor behind [`super::TradingStream`].

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
    Command, DisconnectReason, HeartbeatKind, TradingEvent, TradingStreamEvent,
    config::TradingStreamConfig,
    message::{self, TradingEventData},
    protocol::{self, Inbound},
    topic::TradingTopic,
};
use crate::{
    SsiClient,
    error::Error,
    secret::mask_topic,
    stream::common::{
        Counters, STALL_THRESHOLD, is_fatal_rejection, list_frame, millis, ping_frame, pong_frame,
        rejection_http_status, warn_if_handshake_is_traced,
    },
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

struct Outbox {
    events: mpsc::Sender<TradingStreamEvent>,
    queue: VecDeque<TradingStreamEvent>,
    blocked_since: Option<Instant>,
    counters: Arc<Counters>,
}

impl Outbox {
    fn push(&mut self, event: TradingStreamEvent) {
        if self.queue.is_empty() {
            self.blocked_since = Some(Instant::now());
        }
        self.queue.push_back(event);
    }

    fn deliver(&mut self, permit: mpsc::Permit<'_, TradingStreamEvent>) -> Option<Duration> {
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
                    "trading stream consumer is slow; socket reads were held back"
                );
            }
            return Some(stalled);
        }
        None
    }

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
    config: TradingStreamConfig,
    outbox: Outbox,
    commands: mpsc::UnboundedReceiver<Command>,
    cancel: CancellationToken,
    topics: Vec<TradingTopic>,
    connections: u64,
    unknown_topics: HashSet<String>,
    unknown_shapes: HashSet<String>,
}

impl Supervisor {
    pub(crate) fn new(
        client: SsiClient,
        mut config: TradingStreamConfig,
        events: mpsc::Sender<TradingStreamEvent>,
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
                "SSI trading stream disconnected; reconnecting"
            );
            let at = self.now();
            self.outbox.push(TradingStreamEvent::Disconnected {
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
        tracing::info!(connections = self.connections, "SSI trading stream stopped");
    }

    async fn connect(&self) -> Result<(Socket, SecretString), Ending> {
        let token =
            self.client
                .auth()
                .trading_access_token()
                .await
                .map_err(|error| match error {
                    Error::ReauthenticationRequired(api) => Ending {
                        reason: DisconnectReason::ReauthenticationRequired(api.to_string()),
                        min_wait: self.config.backoff.reauth_wait,
                    },
                    other => Ending {
                        reason: DisconnectReason::TokenUnavailable(other.to_string()),
                        min_wait: Duration::from_secs(5),
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
        reason = "single select loop keeping back-pressure, pings, commands, and watchdog atomic"
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
            "SSI trading stream connected"
        );
        let at = self.now();
        self.outbox
            .push(TradingStreamEvent::Connected { connection, at });
        if connection > 1 && !self.topics.is_empty() {
            let topics = self.topics.len();
            self.outbox.push(TradingStreamEvent::Resubscribed {
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
        let check_every =
            (self.config.idle_timeout / 4).clamp(Duration::from_millis(20), Duration::from_secs(1));
        let mut watchdog = tokio::time::interval(check_every);
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let backlog = !self.outbox.queue.is_empty();
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Err(error) = socket.close(None).await {
                        tracing::debug!(%error, "closing trading stream socket failed");
                    }
                    return SessionEnd::Cancelled;
                }
                permit = events.reserve(), if backlog => {
                    let Ok(permit) = permit else { return SessionEnd::Cancelled };
                    if self.outbox.deliver(permit).is_some_and(|stall| stall >= Duration::from_secs(1)) {
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
                    if !backlog {
                        let silent = last_frame.elapsed();
                        if silent >= self.config.idle_timeout {
                            return ended(DisconnectReason::IdleTimeout(silent));
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
                    self.client.auth().invalidate(token);
                }
                Some(ended(DisconnectReason::Closed {
                    code: code.map(u16::from),
                    reason: frame.map_or_default(|frame| frame.reason.to_string()),
                }))
            }
            Message::Binary(bytes) => {
                tracing::debug!(
                    bytes = bytes.len(),
                    "ignoring binary frame on trading stream"
                );
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
            self.outbox.push(TradingStreamEvent::RawFrame {
                text: text.to_owned(),
                received_at,
            });
        }
        match protocol::classify(text) {
            Inbound::Trading { topic, payloads } => {
                let masked = mask_topic(&topic);
                for payload in payloads {
                    let data = message::decode_text(&topic, &payload);
                    self.count_event(&masked, &data);
                    self.outbox
                        .push(TradingStreamEvent::Event(Box::new(TradingEvent {
                            topic: masked.clone(),
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
                        "SSI did not accept every trading topic"
                    );
                }
                self.outbox.push(TradingStreamEvent::Ack(ack));
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
            } if is_fatal_rejection(code) => {
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
            Inbound::Rejection { code, message, .. } => {
                tracing::warn!(
                    code,
                    %message,
                    "SSI sent a non-fatal error frame on trading stream"
                );
                None
            }
            Inbound::Subscriptions { data, trading } => {
                self.outbox
                    .push(TradingStreamEvent::Subscriptions { data, trading });
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
                        "SSI sent an unrecognised frame on trading stream; ignored"
                    );
                }
                None
            }
        }
    }

    fn count_event(&mut self, masked_topic: &str, data: &TradingEventData) {
        let counters = &self.outbox.counters;
        counters.messages.fetch_add(1, Ordering::Relaxed);
        if let TradingEventData::Unknown { reason, .. } = data {
            counters.unknown_messages.fetch_add(1, Ordering::Relaxed);
            if self.unknown_topics.insert(masked_topic.to_owned()) {
                tracing::warn!(
                    topic = masked_topic,
                    %reason,
                    "SSI trading event did not match documented schema; kept as Unknown"
                );
            }
        }
    }

    fn heartbeat(&mut self, kind: HeartbeatKind) {
        if self.config.emit_heartbeats {
            let at = self.now();
            self.outbox.push(TradingStreamEvent::Heartbeat { kind, at });
        }
    }

    async fn send_requests(
        &self,
        socket: &mut Socket,
        method: &str,
        topics: &[TradingTopic],
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

    fn add_topics(&mut self, topics: Vec<TradingTopic>) -> Vec<TradingTopic> {
        let mut added = Vec::new();
        for topic in topics {
            if !self.topics.contains(&topic) {
                self.topics.push(topic.clone());
                added.push(topic);
            }
        }
        added
    }

    fn remove_topics(&mut self, topics: &[TradingTopic]) -> Vec<TradingTopic> {
        let removed: Vec<TradingTopic> = self
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
