#![doc = "Market stream supervisor tests against a local mock WebSocket server."]
#![allow(clippy::expect_used, clippy::indexing_slicing)]

mod common;

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::{config, mount_auth, token_body};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use ssi_sdk::{
    SsiClient,
    stream::{
        ActivityWindow, Backoff, DisconnectReason, HeartbeatKind, MarketData, MarketStream,
        MarketStreamConfig, MarketStreamEvent, StalePolicy, Topic,
    },
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

type Socket = WebSocketStream<TcpStream>;
type Script =
    Arc<dyn Fn(usize, Socket, Log) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What the mock server saw.
#[derive(Clone, Default)]
struct Log {
    frames: Arc<Mutex<Vec<(usize, Value)>>>,
    authorization: Arc<Mutex<Vec<String>>>,
}

impl Log {
    fn record(&self, connection: usize, text: &str) {
        let value = serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()));
        self.frames.lock().expect("lock").push((connection, value));
    }

    fn frames(&self) -> Vec<(usize, Value)> {
        self.frames.lock().expect("lock").clone()
    }

    fn authorization(&self) -> Vec<String> {
        self.authorization.lock().expect("lock").clone()
    }
}

/// Starts a WebSocket server running `script` for each accepted connection.
async fn ws_server(script: Script) -> (String, Log) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("ws://{}", listener.local_addr().expect("addr"));
    let log = Log::default();
    let server_log = log.clone();
    tokio::spawn(async move {
        let connections = AtomicUsize::new(0);
        while let Ok((stream, _)) = listener.accept().await {
            let index = connections.fetch_add(1, Ordering::SeqCst);
            let log = server_log.clone();
            let script = Arc::clone(&script);
            tokio::spawn(async move {
                let headers = log.clone();
                #[allow(
                    clippy::result_large_err,
                    reason = "the callback signature is fixed by tungstenite"
                )]
                let callback = move |request: &Request, response: Response| {
                    let value = request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    headers.authorization.lock().expect("lock").push(value);
                    Ok(response)
                };
                if let Ok(socket) = tokio_tungstenite::accept_hdr_async(stream, callback).await {
                    script(index, socket, log).await;
                }
            });
        }
    });
    (url, log)
}

/// Reads frames until the next text frame, recording it.
async fn next_text(socket: &mut Socket, connection: usize, log: &Log) -> Option<Value> {
    while let Some(Ok(message)) = socket.next().await {
        if let Message::Text(text) = message {
            log.record(connection, text.as_str());
            return serde_json::from_str(text.as_str()).ok();
        }
    }
    None
}

/// Keeps reading (and recording) until the client goes away.
async fn drain(mut socket: Socket, connection: usize, log: Log) {
    while next_text(&mut socket, connection, &log).await.is_some() {}
}

fn ack(count: usize) -> Message {
    Message::text(
        json!({"method": "SUBSCRIBE", "channel": "DATA", "status": "ok",
               "message": format!("Subscribed to {count} topic(s)")})
        .to_string(),
    )
}

fn trade(price: &str, volume: &str) -> Message {
    Message::text(
        json!({"channel": "DATA", "topic": "trade.VNM",
               "data": {"s": "VNM", "t": "2026/11/20 09:15:01", "p": price, "q": "100",
                        "a": price, "si": "B", "o": price, "h": price, "l": price, "v": volume}})
        .to_string(),
    )
}

const fn fast_backoff() -> Backoff {
    Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(50),
        rate_limited: Duration::from_millis(50),
        reauth_wait: Duration::from_millis(50),
        stable_after: Duration::from_secs(10),
    }
}

async fn client(stream_url: &str) -> (SsiClient, MockServer) {
    let http = MockServer::start().await;
    mount_auth(&http, "access-1").await;
    let config = config(&http.uri())
        .with_stream_url(stream_url)
        .expect("stream url");
    (SsiClient::new(config).expect("client"), http)
}

fn topics() -> Vec<Topic> {
    vec![
        Topic::trade("VNM").expect("topic"),
        Topic::quote("VNM").expect("topic"),
    ]
}

async fn next_event(stream: &mut MarketStream) -> MarketStreamEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("event in time")
        .expect("stream open")
}

/// Skips acknowledgements and heartbeats.
async fn next_significant(stream: &mut MarketStream) -> MarketStreamEvent {
    loop {
        match next_event(stream).await {
            MarketStreamEvent::Ack(_) | MarketStreamEvent::Heartbeat { .. } => {}
            other => return other,
        }
    }
}

fn subscribed_topics(frame: &Value) -> Vec<String> {
    frame
        .get("topics")
        .and_then(Value::as_array)
        .map_or_default(|topics| {
            topics
                .iter()
                .filter_map(|topic| topic.as_str().map(ToOwned::to_owned))
                .collect()
        })
}

#[tokio::test]
async fn reconnects_and_resubscribes_after_the_server_closes() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            let subscribe = next_text(&mut socket, index, &log)
                .await
                .expect("subscribe");
            let count = subscribed_topics(&subscribe).len();
            socket.send(ack(count)).await.expect("ack");
            socket.send(trade("51100", "1000")).await.expect("data");
            if index == 0 {
                socket.close(None).await.expect("close");
            } else {
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );

    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    let MarketStreamEvent::Data(first) = next_significant(&mut stream).await else {
        panic!("data expected")
    };
    assert_eq!(first.topic, "trade.VNM");
    let MarketData::Trade(trade) = &first.data else {
        panic!("trade expected")
    };
    assert_eq!(trade.total_volume, 1000);
    assert_eq!(trade.time.to_rfc3339(), "2026-11-20T02:15:01+00:00");
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Disconnected {
            reason: DisconnectReason::Closed { .. },
            ..
        }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Resubscribed {
            connection: 2,
            topics: 2,
            ..
        }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Data(_)
    ));
    stream.shutdown().await;

    let frames = log.frames();
    let per_connection: Vec<Vec<String>> = (0..2)
        .map(|connection| {
            frames
                .iter()
                .find(|(index, _)| *index == connection)
                .map(|(_, frame)| subscribed_topics(frame))
                .expect("subscribe frame")
        })
        .collect();
    assert_eq!(per_connection[0], vec!["trade.VNM", "quote.VNM"]);
    assert_eq!(
        per_connection[0], per_connection[1],
        "same topics after reconnect"
    );
    assert_eq!(
        log.authorization(),
        vec!["Bearer access-1", "Bearer access-1"]
    );
}

#[tokio::test]
async fn silent_connections_are_dropped_after_the_idle_timeout() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            // Say nothing at all; just hold the socket.
            tokio::time::sleep(Duration::from_secs(5)).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_idle_timeout(Duration::from_millis(200)),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    let MarketStreamEvent::Disconnected {
        reason, retry_in, ..
    } = next_significant(&mut stream).await
    else {
        panic!("disconnect expected")
    };
    assert!(
        matches!(reason, DisconnectReason::IdleTimeout(silent) if silent >= Duration::from_millis(200))
    );
    assert!(retry_in <= Duration::from_millis(50));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    stream.shutdown().await;
}

#[tokio::test]
async fn stale_data_is_reported_while_the_transport_stays_alive() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            socket.send(ack(2)).await.expect("ack");
            // Keep-alive pongs (as SSI sends every 30 s) but no market data for a while.
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                socket
                    .send(Message::Pong(Vec::new().into()))
                    .await
                    .expect("pong");
            }
            socket.send(trade("51200", "2000")).await.expect("data");
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_idle_timeout(Duration::from_secs(2))
            .with_stale_policy(StalePolicy::Always(Duration::from_millis(150))),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let MarketStreamEvent::Stale {
        silent_for,
        last_data_at,
        ..
    } = next_significant(&mut stream).await
    else {
        panic!("stale expected")
    };
    assert!(silent_for >= Duration::from_millis(150));
    assert!(last_data_at.is_none());
    let MarketStreamEvent::Resumed { silent_for, .. } = next_significant(&mut stream).await else {
        panic!("resumed expected")
    };
    assert!(silent_for >= Duration::from_millis(350));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Data(_)
    ));
    stream.shutdown().await;
}

#[tokio::test]
async fn a_rejected_token_is_renewed_before_reconnecting() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            if index == 0 {
                let rejection = json!({"code": 401, "msg": "Invalid or expired token"});
                socket
                    .send(Message::text(rejection.to_string()))
                    .await
                    .expect("reject");
                socket.close(None).await.ok();
            } else {
                next_text(&mut socket, index, &log).await;
                socket.send(ack(2)).await.expect("ack");
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, http) = client(&url).await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("access-2")))
        .expect(1)
        .mount(&http)
        .await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Disconnected {
            reason: DisconnectReason::Rejected { code: 401, .. },
            ..
        }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    stream.shutdown().await;
    assert_eq!(
        log.authorization(),
        vec!["Bearer access-1", "Bearer access-2"]
    );
}

#[tokio::test]
async fn runtime_subscriptions_pings_and_server_pings() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            socket.send(ack(2)).await.expect("ack");
            let ping =
                json!({"method": "PING", "channel": "HEARTBEAT", "time": "20-11-2026 09-00-00"});
            socket
                .send(Message::text(ping.to_string()))
                .await
                .expect("ping");
            drain(socket, index, log).await;
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(Some(Duration::from_millis(100)))
            .with_heartbeat_events(true),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    stream
        .subscribe([Topic::room("VNM").expect("topic"), Topic::market_flag()])
        .expect("subscribe");
    stream
        .unsubscribe([Topic::trade("VNM").expect("topic")])
        .expect("unsubscribe");
    stream.request_subscriptions().expect("list");
    tokio::time::sleep(Duration::from_millis(350)).await;
    stream.shutdown().await;

    let frames: Vec<Value> = log.frames().into_iter().map(|(_, frame)| frame).collect();
    let method_of = |frame: &Value| {
        frame
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let pong = frames
        .iter()
        .find(|frame| method_of(frame) == "PONG")
        .expect("answered the server PING");
    assert_eq!(
        pong.get("time").and_then(Value::as_str),
        Some("20-11-2026 09-00-00")
    );
    let subscribe = frames
        .iter()
        .filter(|frame| method_of(frame) == "SUBSCRIBE")
        .nth(1)
        .expect("runtime subscribe");
    assert_eq!(
        subscribed_topics(subscribe),
        vec!["room.VNM", "market.flag"]
    );
    let unsubscribe = frames
        .iter()
        .find(|frame| method_of(frame) == "UNSUBSCRIBE")
        .expect("unsubscribe");
    assert_eq!(subscribed_topics(unsubscribe), vec!["trade.VNM"]);
    assert!(
        frames
            .iter()
            .any(|frame| method_of(frame) == "LIST_SUBSCRIPTION")
    );
    let pings = frames
        .iter()
        .filter(|frame| method_of(frame) == "PING_PONG")
        .count();
    assert!(pings >= 2, "client pinged every 100 ms: {pings}");
}

#[tokio::test]
async fn refused_upgrades_are_retried_with_backoff() {
    // Nothing listens on this port after the listener is dropped: every connect fails.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("ws://{}", listener.local_addr().expect("addr"));
    drop(listener);
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    for _ in 0..3 {
        let MarketStreamEvent::Disconnected { reason, .. } = next_significant(&mut stream).await
        else {
            panic!("disconnect expected")
        };
        assert!(matches!(reason, DisconnectReason::ConnectFailed { .. }));
    }
    stream.shutdown().await;
}

#[tokio::test]
async fn captured_control_frames_are_typed() {
    let frames: Vec<String> = common::fixture_document("stream_frames")
        .get("frames")
        .and_then(Value::as_array)
        .expect("frames")
        .iter()
        .filter_map(|frame| frame.as_str().map(ToOwned::to_owned))
        .collect();
    let script: Script = Arc::new(move |index, mut socket, log| {
        let frames = frames.clone();
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            if index == 0 {
                for frame in frames {
                    socket.send(Message::text(frame)).await.expect("send");
                }
            }
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_heartbeat_events(true),
    );
    assert!(matches!(
        next_event(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let MarketStreamEvent::Ack(clean) = next_event(&mut stream).await else {
        panic!("ack expected")
    };
    assert!(clean.is_clean());
    let MarketStreamEvent::Ack(partial) = next_event(&mut stream).await else {
        panic!("ack expected")
    };
    assert_eq!(partial.invalid_topics, vec!["bogus.VNM"]);
    let MarketStreamEvent::Subscriptions { data, trading } = next_event(&mut stream).await else {
        panic!("subscription list expected")
    };
    assert_eq!(data.len(), 4);
    assert_eq!(trading, Vec::<String>::new());
    assert!(matches!(
        next_event(&mut stream).await,
        MarketStreamEvent::Heartbeat {
            kind: HeartbeatKind::PingAck,
            ..
        }
    ));
    assert!(matches!(
        next_event(&mut stream).await,
        MarketStreamEvent::Disconnected {
            reason: DisconnectReason::Rejected { code: 401, .. },
            ..
        }
    ));
    stream.shutdown().await;
}

/// Replays every frame of the 2026-11-20 market-hours capture (first 31 s: trade and quote for
/// three HOSE, two HNX and one UPCOM symbol, plus the ack, subscription list and ping ack).
#[tokio::test]
async fn the_market_hours_capture_replays_without_unknown_or_unrecognised_frames() {
    let frames: Vec<String> = common::fixture_lines("stream_market_hours_20261120")
        .iter()
        .filter_map(|line| line.get("frame").map(Value::to_string))
        .collect();
    let sent = u64::try_from(frames.len()).expect("count");
    let script: Script = Arc::new(move |index, mut socket, log| {
        let frames = frames.clone();
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            if index == 0 {
                for frame in frames {
                    socket.send(Message::text(frame)).await.expect("send");
                }
            }
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_heartbeat_events(true),
    );
    let mut families = BTreeMap::<&str, u64>::new();
    let mut control = (0, 0, 0);
    while families.values().sum::<u64>() < 401 {
        match next_event(&mut stream).await {
            MarketStreamEvent::Data(data) => *families.entry(data.data.family()).or_default() += 1,
            MarketStreamEvent::Ack(ack) => {
                assert!(ack.is_clean(), "{ack:?}");
                control.0 += 1;
            }
            MarketStreamEvent::Subscriptions { data, trading } => {
                assert_eq!((data.len(), trading.len()), (19, 0));
                control.1 += 1;
            }
            MarketStreamEvent::Heartbeat {
                kind: HeartbeatKind::PingAck,
                ..
            } => control.2 += 1,
            MarketStreamEvent::Connected { .. } => {}
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert_eq!(families, BTreeMap::from([("quote", 282), ("trade", 119)]));
    assert_eq!(control, (1, 1, 1), "ack, subscription list, ping ack");
    let stats = stream.stats();
    assert_eq!(stats.frames, sent);
    assert_eq!(stats.messages, 401);
    assert_eq!(stats.unknown_messages, 0);
    assert_eq!(stats.unrecognised_frames, 0);
    stream.shutdown().await;
}

#[tokio::test]
async fn stale_waits_for_the_threshold_after_the_window_opens() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            socket.send(ack(2)).await.ok();
            // The transport stays alive; no market data at all.
            for _ in 0..60 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if socket.send(Message::Pong(Vec::new().into())).await.is_err() {
                    return;
                }
            }
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let started = std::time::Instant::now();
    let opens_after = Duration::from_millis(800);
    let after = Duration::from_millis(600);
    // The window opens late, after a silence longer than `after` (09:00 after the night).
    let window: ActivityWindow = Arc::new(move |_now| started.elapsed() >= opens_after);
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_idle_timeout(Duration::from_secs(10))
            .with_stale_policy(StalePolicy::During { window, after }),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let event = next_significant(&mut stream).await;
    let at = started.elapsed();
    let MarketStreamEvent::Stale { silent_for, .. } = event else {
        panic!("stale expected, got {event:?}")
    };
    assert!(
        at >= opens_after + after,
        "Stale fired {at:?} after start, before window opening + threshold"
    );
    assert!(silent_for >= after && silent_for < after + Duration::from_millis(400));
    stream.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_consumer_does_not_stop_pings() {
    let script: Script = Arc::new(move |index, socket, log| {
        Box::pin(async move {
            let (mut sink, mut source) = socket.split();
            let reader_log = log.clone();
            let reader = tokio::spawn(async move {
                while let Some(Ok(message)) = source.next().await {
                    if let Message::Text(text) = message {
                        reader_log.record(index, text.as_str());
                    }
                }
            });
            let mut volume = 0_u64;
            'flood: loop {
                for _ in 0..20 {
                    volume += 100;
                    if sink
                        .send(trade("51100", &volume.to_string()))
                        .await
                        .is_err()
                    {
                        break 'flood;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            reader.abort();
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_channel_capacity(8)
            .with_ping_interval(Some(Duration::from_millis(100))),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let pings = |log: &Log| {
        log.frames()
            .iter()
            .filter(|(_, frame)| frame.get("method").and_then(Value::as_str) == Some("PING_PONG"))
            .count()
    };
    let before = pings(&log);
    // The consumer stops reading for 2 s.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let during = pings(&log) - before;
    assert!(
        during >= 10,
        "pings kept going while the consumer stalled: {during}"
    );
    // Reading resumes and the stall shows in the statistics.
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while std::time::Instant::now() < deadline {
        if let Ok(Some(MarketStreamEvent::Disconnected { reason, .. })) =
            tokio::time::timeout(Duration::from_millis(50), stream.recv()).await
        {
            panic!("the connection dropped: {reason:?}");
        }
    }
    let stats = stream.stats();
    assert!(stats.consumer_stalls >= 1, "{stats:?}");
    assert!(
        stats.longest_consumer_stall >= Duration::from_millis(1500),
        "{stats:?}"
    );
    stream.shutdown().await;
}

#[tokio::test]
async fn a_zero_ping_interval_disables_pings() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            socket.send(ack(2)).await.ok();
            drain(socket, index, log).await;
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(Some(Duration::ZERO)),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stream.is_finished(), "the supervisor is alive");
    stream.shutdown().await;
    assert!(
        log.frames()
            .iter()
            .all(|(_, frame)| frame.get("method").and_then(Value::as_str) != Some("PING_PONG"))
    );
}

#[tokio::test]
async fn non_fatal_error_frames_keep_the_connection() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            let frame = json!({"code": 400, "msg": "Invalid request"});
            socket.send(Message::text(frame.to_string())).await.ok();
            socket.send(trade("51100", "100")).await.ok();
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Data(_)
    ));
    stream.shutdown().await;
}

#[tokio::test]
async fn a_bare_policy_close_renews_the_token() {
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            if index == 0 {
                let close = CloseFrame {
                    code: CloseCode::Policy,
                    reason: "Invalid or expired token".into(),
                };
                socket.close(Some(close)).await.ok();
            } else {
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, http) = client(&url).await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("access-2")))
        .expect(1)
        .mount(&http)
        .await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Disconnected {
            reason: DisconnectReason::Closed {
                code: Some(1008),
                ..
            },
            ..
        }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    stream.shutdown().await;
    assert_eq!(
        log.authorization(),
        vec!["Bearer access-1", "Bearer access-2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_blocked_handle_closes_the_socket() {
    let closed = Arc::new(AtomicUsize::new(0));
    let closed_server = Arc::clone(&closed);
    let script: Script = Arc::new(move |index, mut socket, log| {
        let closed = Arc::clone(&closed_server);
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            for volume in 0..200_u64 {
                if socket
                    .send(trade("51100", &volume.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            drain(socket, index, log).await;
            closed.fetch_add(1, Ordering::SeqCst);
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_channel_capacity(4),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    // Nobody reads: the supervisor fills the channel and its queue.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(stream);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while closed.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "the server saw the socket close"
    );
}

#[tokio::test]
async fn subscription_changes_while_disconnected_apply_on_reconnect() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            if index == 0 {
                next_text(&mut socket, index, &log).await;
                socket.close(None).await.ok();
            } else {
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_ping_interval(None)
            .with_backoff(Backoff {
                initial: Duration::from_millis(600),
                max: Duration::from_millis(600),
                rate_limited: Duration::from_millis(600),
                reauth_wait: Duration::from_millis(600),
                stable_after: Duration::from_secs(30),
            }),
    );
    loop {
        if let MarketStreamEvent::Disconnected { .. } = next_significant(&mut stream).await {
            break;
        }
    }
    // In the backoff sleep now.
    stream
        .unsubscribe([Topic::trade("VNM").expect("topic")])
        .expect("command");
    stream
        .subscribe([
            Topic::room("VNM").expect("topic"),
            Topic::quote("VNM").expect("topic"),
        ])
        .expect("command");
    stream
        .subscribe([Topic::room("VNM").expect("topic")])
        .expect("command");
    loop {
        if let MarketStreamEvent::Connected { connection: 2, .. } =
            next_significant(&mut stream).await
        {
            break;
        }
    }
    stream
        .subscribe([Topic::quote("VNM").expect("topic")])
        .expect("command");
    tokio::time::sleep(Duration::from_millis(200)).await;
    stream.shutdown().await;
    let second: Vec<Value> = log
        .frames()
        .into_iter()
        .filter(|(index, _)| *index == 1)
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(
        second.len(),
        1,
        "one subscribe and no duplicate: {second:?}"
    );
    assert_eq!(subscribed_topics(&second[0]), vec!["quote.VNM", "room.VNM"]);
}

#[tokio::test]
async fn raw_frames_and_untyped_payloads_are_reported() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            let bad = json!({"channel": "DATA", "topic": "trade.VNM",
                             "data": {"s": "VNM", "t": "not a time", "p": "1"}});
            socket.send(Message::text(bad.to_string())).await.ok();
            socket.send(trade("51100", "100")).await.ok();
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None)
            .with_raw_frames(true),
    );
    assert!(matches!(
        next_event(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let MarketStreamEvent::RawFrame { text, .. } = next_event(&mut stream).await else {
        panic!("raw frame expected")
    };
    assert!(text.contains("not a time"));
    let MarketStreamEvent::Data(bad) = next_event(&mut stream).await else {
        panic!("data expected")
    };
    assert!(matches!(bad.data, MarketData::Unknown { .. }));
    assert!(matches!(
        next_event(&mut stream).await,
        MarketStreamEvent::RawFrame { .. }
    ));
    let MarketStreamEvent::Data(good) = next_event(&mut stream).await else {
        panic!("data expected")
    };
    assert!(matches!(good.data, MarketData::Trade(_)));
    let stats = stream.stats();
    assert_eq!(stats.unknown_messages, 1);
    assert_eq!(stats.messages, 2);
    assert_eq!(stats.frames, 2);
    stream.shutdown().await;
}

#[tokio::test]
async fn malformed_book_levels_are_counted_as_unknown() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            for levels in [
                json!([["51100", "abc"]]),
                json!([{"p": "51100", "q": "100"}]),
                json!(["51100", "100"]),
                json!([["51100", "100"], ["", ""]]),
            ] {
                let frame = json!({"channel": "DATA", "topic": "quote.VNM",
                                   "data": {"s": "VNM", "t": "2026/11/20 09:15:01",
                                            "bids": levels, "asks": []}});
                socket.send(Message::text(frame.to_string())).await.ok();
            }
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    let mut kinds = Vec::new();
    while kinds.len() < 4 {
        if let MarketStreamEvent::Data(data) = next_significant(&mut stream).await {
            kinds.push(data.data.family());
        }
    }
    assert_eq!(kinds, vec!["unknown", "unknown", "unknown", "quote"]);
    assert_eq!(stream.stats().unknown_messages, 3);
    stream.shutdown().await;
}

#[tokio::test]
async fn unrecognised_frames_are_counted_and_never_pass_as_subscription_lists() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            next_text(&mut socket, index, &log).await;
            for frame in [
                json!({"type": "snapshot", "data": {"s": "VNM", "p": "51100"}}),
                json!({"s": "VNM", "p": "51100"}),
                json!({"type": "snapshot", "data": {"s": "FPT"}}),
            ] {
                socket.send(Message::text(frame.to_string())).await.ok();
            }
            socket.send(trade("51100", "100")).await.ok();
            drain(socket, index, log).await;
        })
    });
    let (url, _log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { .. }
    ));
    let event = next_significant(&mut stream).await;
    assert!(matches!(event, MarketStreamEvent::Data(_)), "got {event:?}");
    assert_eq!(stream.stats().unrecognised_frames, 3);
    stream.shutdown().await;
}

#[tokio::test]
async fn six_digit_token_rejection_renews_and_retry_after_is_honored() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            if index == 0 {
                let rejection = json!({"code": 401_004, "msg": "Access Token Expired"});
                socket
                    .send(Message::text(rejection.to_string()))
                    .await
                    .expect("reject");
                socket.close(None).await.ok();
            } else if index == 1 {
                let rate_limit =
                    json!({"code": 429_000, "msg": "Too many connections", "retryAfter": 0.2});
                socket
                    .send(Message::text(rate_limit.to_string()))
                    .await
                    .expect("429");
                socket.close(None).await.ok();
            } else {
                next_text(&mut socket, index, &log).await;
                socket.send(ack(2)).await.expect("ack");
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, http) = client(&url).await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("access-2")))
        .expect(1)
        .mount(&http)
        .await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Disconnected {
            reason: DisconnectReason::Rejected { code: 401_004, .. },
            ..
        }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Resubscribed { connection: 2, .. }
    ));
    let MarketStreamEvent::Disconnected {
        reason: DisconnectReason::Rejected { code: 429_000, .. },
        retry_in,
        ..
    } = next_significant(&mut stream).await
    else {
        panic!("429000 disconnect expected")
    };
    assert!(
        retry_in >= Duration::from_millis(200),
        "retryAfter 0.2s honored: {retry_in:?}"
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 3, .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Resubscribed { connection: 3, .. }
    ));
    stream.shutdown().await;
    assert_eq!(
        log.authorization(),
        vec!["Bearer access-1", "Bearer access-2", "Bearer access-2"]
    );
}

#[tokio::test]
async fn unsubscribe_all_clears_online_and_reconnect_subscriptions() {
    let script: Script = Arc::new(|index, mut socket, log| {
        Box::pin(async move {
            if index == 0 {
                next_text(&mut socket, index, &log).await;
                socket.send(ack(2)).await.expect("ack");
                // Wait for UNSUBSCRIBE, then drop the socket to force a reconnect.
                let unsub = next_text(&mut socket, index, &log).await;
                assert!(unsub.is_some());
                socket.close(None).await.ok();
            } else {
                drain(socket, index, log).await;
            }
        })
    });
    let (url, log) = ws_server(script).await;
    let (client, _http) = client(&url).await;
    let mut stream = client.market_stream(
        MarketStreamConfig::new(topics())
            .with_backoff(fast_backoff())
            .with_ping_interval(None),
    );
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 1, .. }
    ));
    stream.unsubscribe_all().expect("unsubscribe_all");
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Disconnected { .. }
    ));
    assert!(matches!(
        next_significant(&mut stream).await,
        MarketStreamEvent::Connected { connection: 2, .. }
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    stream.shutdown().await;

    let frames = log.frames();
    let unsub = frames
        .iter()
        .find(|(conn, frame)| {
            *conn == 0 && frame.get("method").and_then(Value::as_str) == Some("UNSUBSCRIBE")
        })
        .expect("unsubscribe frame sent");
    assert_eq!(subscribed_topics(&unsub.1), vec!["trade.VNM", "quote.VNM"]);
    assert_eq!(
        frames.iter().filter(|(conn, _)| *conn == 1).count(),
        0,
        "nothing re-subscribed after unsubscribe_all"
    );
}
