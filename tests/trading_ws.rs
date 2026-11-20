#![doc = "Offline integration tests for the V3 Trading WebSocket RPC client (/ws/v3/trading)."]
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::too_many_lines)]

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _};
use rust_decimal::dec;
use serde_json::{Value, value::RawValue};
use ssi_sdk::{
    AccountNo, Error, OrderSide, OrderStatus, PlaceOrderRequest, SignatureEncoding, SsiClient,
    TradingEventData, TradingWsConfig, TradingWsEvent,
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use wiremock::MockServer;

/// Extracts the exact raw JSON substring of `"params"` from a command frame so tests verify
/// signatures over the exact wire bytes.
fn extract_raw_params(frame: &str) -> String {
    #[derive(serde::Deserialize)]
    struct Peek<'a> {
        #[serde(borrow)]
        params: &'a RawValue,
    }
    serde_json::from_str::<Peek<'_>>(frame)
        .expect("valid frame")
        .params
        .get()
        .to_owned()
}

#[tokio::test]
async fn trading_ws_catches_post_upgrade_refusal_during_handshake_grace() {
    let http = MockServer::start().await;
    let client_base = common::authenticated_client(&http, "tok-ws-refuse").await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let ws_url = format!("ws://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept");
            let _ = ws
                .send(Message::text(
                    r#"{"code":403,"msg":"Missing scope tradingws:order:post"}"#,
                ))
                .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let cfg = common::trading_config(&http.uri())
        .with_trading_ws_url(&ws_url)
        .expect("ws url");
    let client = SsiClient::new(cfg).expect("client");
    client
        .auth()
        .set_token(client_base.auth().export_token().expect("tok"))
        .expect("set");

    let err = client
        .connect_trading_ws(
            TradingWsConfig::default().with_handshake_grace(Duration::from_millis(300)),
        )
        .await
        .expect_err("must detect 403 refusal");
    assert!(matches!(err, Error::TradingWs(e) if e.status == 403));
}

#[tokio::test]
async fn trading_ws_auto_signature_fallback_queries_push_and_no_timeout_retry() {
    let http = MockServer::start().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let ws_url = format!("ws://{}", listener.local_addr().expect("addr"));

    let order_attempts = Arc::new(AtomicUsize::new(0));
    let timeout_attempts = Arc::new(AtomicUsize::new(0));
    let order_attempts_srv = Arc::clone(&order_attempts);
    let timeout_attempts_srv = Arc::clone(&timeout_attempts);

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept");
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                let val: Value = serde_json::from_str(text.as_str()).expect("json");
                let method = val
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                match method {
                    "order.place" => {
                        let id = val["id"].as_str().expect("id");
                        let sig = val["signature"].as_str().expect("sig");
                        let raw_params = extract_raw_params(text.as_str());
                        let attempt = order_attempts_srv.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt == 1 {
                            // First attempt uses base64; simulate gateway demanding hex (401013).
                            assert!(common::verify_base64_signature(raw_params.as_bytes(), sig));
                            let _ = ws
                                .send(Message::text(
                                    serde_json::json!({
                                        "id": id,
                                        "method": "order.place",
                                        "status": 401,
                                        "error": { "code": "401013", "message": "Signature Not Valid" }
                                    })
                                    .to_string(),
                                ))
                                .await;
                        } else {
                            // Auto fallback retries once with hex over identical params.
                            assert!(common::verify_hex_signature(raw_params.as_bytes(), sig));
                            let _ = ws
                                .send(Message::text(
                                    serde_json::json!({
                                        "id": id,
                                        "method": "order.place",
                                        "status": 200,
                                        "result": {
                                            "orderId": "WS-ORD-1",
                                            "clientRequestId": "ws-req-1",
                                            "orderStatus": "PD"
                                        },
                                        "rateLimit": { "limit": 50, "remaining": 49, "resetMs": 1000 }
                                    })
                                    .to_string(),
                                ))
                                .await;
                        }
                    }
                    "order.cancel" => {
                        // Never reply: test must time out and NOT retry!
                        timeout_attempts_srv.fetch_add(1, Ordering::SeqCst);
                    }
                    "query.maxBuySell" => {
                        let id = val["id"].as_str().expect("id");
                        assert!(val.get("signature").is_none());
                        assert!(val["params"]["price"].is_number());
                        let _ = ws
                            .send(Message::text(
                                serde_json::json!({
                                    "id": id,
                                    "method": "query.maxBuySell",
                                    "status": 200,
                                    "result": {
                                        "accountNo": common::TEST_ACCOUNT,
                                        "maxBuyQty": 2000,
                                        "maxSellQty": 500
                                    }
                                })
                                .to_string(),
                            ))
                            .await;
                    }
                    "SUBSCRIBE" => {
                        let _ = ws
                            .send(Message::text(
                                r#"{"channel":"TRADING","topic":"order.003C123451","data":{"eventType":"orderEvent","accountNo":"003C123451","orderId":"WS-ORD-1","symbol":"VNM","side":"B","orderStatus":"QU"}}"#,
                            ))
                            .await;
                    }
                    _ => {}
                }
            }
        }
    });

    common::mount_token_login(&http, "tok-ws", "ref-ws").await;
    let cfg = common::trading_config(&http.uri())
        .with_trading_ws_url(&ws_url)
        .expect("ws url");
    let client = SsiClient::new(cfg).expect("client");
    client
        .auth()
        .authenticate_with_otp("123456")
        .await
        .expect("otp");

    let ws_cfg = TradingWsConfig::default()
        .with_signature_encoding(SignatureEncoding::Auto)
        .with_handshake_grace(Duration::from_millis(50))
        .with_request_timeout(Duration::from_millis(200))
        .with_ping_interval(None);
    let (ws, mut events) = client.connect_trading_ws(ws_cfg).await.expect("connect");

    let acct = AccountNo::new(common::TEST_ACCOUNT);
    let placed = ws
        .place_order(&PlaceOrderRequest::limit(
            acct.clone(),
            "VNM",
            OrderSide::Buy,
            100,
            dec!(65500),
        ))
        .await
        .expect("placed via hex fallback");
    assert_eq!(placed.order_id, "WS-ORD-1");
    assert_eq!(placed.status, OrderStatus::Pending);
    assert_eq!(order_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(ws.rate_limit().expect("rl").remaining, Some(49));

    // Query (unsigned, numeric price).
    let mbs = ws
        .max_buy_sell(&acct, "VNM", Some(dec!(65500)))
        .await
        .expect("maxBuySell");
    assert_eq!(mbs.max_buy_qty, 2000);

    // Push subscription on Trading WS.
    ws.subscribe_order_events(&acct).expect("sub");
    let pushed = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("in time")
        .expect("event");
    assert!(matches!(
        pushed,
        TradingWsEvent::Event(ev) if matches!(ev.data, TradingEventData::Order(ref o) if o.order_id == "WS-ORD-1")
    ));

    // Wildcard account rejected locally.
    assert!(ws.subscribe_order_events(&AccountNo::new("*")).is_err());

    // Timeout NEVER retries.
    let err = ws
        .cancel_order(&ssi_sdk::CancelOrderRequest::new(
            acct,
            ssi_sdk::OrderTarget::OrderId("WS-ORD-1".to_owned()),
        ))
        .await
        .expect_err("timeout");
    assert!(matches!(err, Error::Timeout(_)));
    assert_eq!(timeout_attempts.load(Ordering::SeqCst), 1);

    ws.close();
}
