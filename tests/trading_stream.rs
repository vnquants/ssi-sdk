#![doc = "Offline integration tests for the supervised V3 TRADING WebSocket event stream."]
#![allow(clippy::expect_used, clippy::too_many_lines, clippy::result_large_err)]

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt as _, StreamExt as _};
use ssi_sdk::{
    AccountNo, Backoff, OrderStatus, SsiClient, TradingEventData, TradingStreamConfig,
    TradingStreamEvent, TradingTopic,
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn trading_stream_delivers_events_and_reconnects_after_401_refresh() {
    let http = MockServer::start().await;
    let now = Utc::now().timestamp();
    common::mount_token_login(&http, "tok-old", "ref-1").await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": "tok-fresh",
            "refreshToken": "ref-2",
            "expiresAt": now + 900,
            "refreshExpiresAt": now + 28_800,
        })))
        .mount(&http)
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let ws_url = format!("ws://{}", listener.local_addr().expect("addr"));
    let conn_count = Arc::new(AtomicUsize::new(0));
    let conn_count_server = Arc::clone(&conn_count);

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let n = conn_count_server.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let mut auth_seen = String::new();
                let mut ws = tokio_tungstenite::accept_hdr_async(
                    stream,
                    |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        if let Some(h) = req.headers().get("authorization") {
                            auth_seen = h.to_str().unwrap_or_default().to_owned();
                        }
                        Ok(resp)
                    },
                )
                .await
                .expect("accept");

                if n == 1 {
                    assert_eq!(auth_seen, "Bearer tok-old");
                    // Consume initial SUBSCRIBE, then send 401 rejection + close 1008.
                    let _ = ws.next().await;
                    let _ = ws
                        .send(Message::text(
                            r#"{"code":401,"msg":"Invalid or expired token"}"#,
                        ))
                        .await;
                    let _ = ws
                        .send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Policy,
                            reason: "expired".into(),
                        })))
                        .await;
                } else {
                    assert_eq!(auth_seen, "Bearer tok-fresh");
                    let sub = ws.next().await.expect("sub").expect("ok");
                    let sub_text = sub.to_text().expect("text");
                    assert!(sub_text.contains(r#""channel":"TRADING""#));
                    let _ = ws
                        .send(Message::text(
                            r#"{"method":"SUBSCRIBE","channel":"TRADING","status":"ok","message":"Subscribed to 1 topic(s)"}"#,
                        ))
                        .await;
                    let _ = ws
                        .send(Message::text(
                            r#"{"channel":"TRADING","topic":"order.003C123451","data":{"eventType":"orderEvent","accountNo":"003C123451","orderId":"O-55","symbol":"VNM","side":"B","orderType":"LO","price":"65000","quantity":100,"osQty":100,"orderStatus":"QU"}}"#,
                        ))
                        .await;
                    let _ = ws
                        .send(Message::text(
                            r#"{"channel":"TRADING","topic":"order.003C123451","data":{"eventType":"orderMatchEvent","accountNo":"003C123451","orderId":"O-55","symbol":"VNM","side":"B","matchPrice":"65000","matchQty":100}}"#,
                        ))
                        .await;
                    while ws.next().await.is_some() {}
                }
            });
        }
    });

    let cfg = common::trading_config(&http.uri())
        .with_stream_url(&ws_url)
        .expect("stream url");
    let client = SsiClient::new(cfg).expect("client");
    client
        .auth()
        .authenticate_with_otp("123456")
        .await
        .expect("otp");

    let acct = AccountNo::new(common::TEST_ACCOUNT);
    let stream_cfg = TradingStreamConfig::new([TradingTopic::order(&acct).expect("topic")])
        .with_backoff(Backoff {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(50),
            rate_limited: Duration::from_millis(50),
            reauth_wait: Duration::from_millis(50),
            stable_after: Duration::from_secs(30),
        });
    let mut stream = client.trading_stream(stream_cfg);

    let mut saw_resubscribed = false;
    let mut saw_order = false;
    let mut saw_match = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline && !(saw_resubscribed && saw_order && saw_match) {
        let ev = tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("event in time")
            .expect("open");
        match ev {
            TradingStreamEvent::Resubscribed { connection, .. } => {
                assert_eq!(connection, 2);
                saw_resubscribed = true;
            }
            TradingStreamEvent::Event(trading) => {
                assert!(!trading.topic.contains("123451"));
                match trading.data {
                    TradingEventData::Order(order) => {
                        assert_eq!(order.order_id, "O-55");
                        assert_eq!(order.status, OrderStatus::Queued);
                        saw_order = true;
                    }
                    TradingEventData::OrderMatch(fill) => {
                        assert_eq!(fill.match_qty, 100);
                        saw_match = true;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    assert!(saw_resubscribed, "must emit Resubscribed after reconnect");
    assert!(saw_order, "must decode orderEvent");
    assert!(saw_match, "must decode orderMatchEvent");
    stream.shutdown().await;
}
