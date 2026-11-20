#![doc = "Offline integration tests for V3 OTP, Smart OTP, and single-use refresh flows."]
#![allow(clippy::expect_used)]

mod common;

use std::time::Duration;

use chrono::Utc;
use secrecy::ExposeSecret as _;
use ssi_sdk::{ApiErrorCode, Error, SsiClient, TokenSource};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, method, path},
};

#[tokio::test]
async fn request_otp_and_smart_otp_polling_flow() {
    let server = MockServer::start().await;
    let now = Utc::now().timestamp();

    Mock::given(method("POST"))
        .and(path("/api/v3/auth/requestOtp"))
        .and(body_json(serde_json::json!({
            "apiKey": common::TEST_API_KEY,
            "apiSecret": common::TEST_API_SECRET,
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": "Smart OTP push sent",
            "transactionId": "tx-smart-001",
        })))
        .mount(&server)
        .await;

    // First poll returns HTTP 202 + 401114 (pending).
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .and(body_json(serde_json::json!({
            "apiKey": common::TEST_API_KEY,
            "apiSecret": common::TEST_API_SECRET,
            "transactionId": "tx-smart-001",
        })))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "code": 401_114,
            "msg": "Push approval pending",
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second poll succeeds.
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": "smart-access-token",
            "refreshToken": "smart-refresh-token",
            "tokenType": "Bearer",
            "expiresAt": now + 900,
            "refreshExpiresAt": now + 28_800,
        })))
        .mount(&server)
        .await;

    let cfg = common::trading_config(&server.uri())
        .with_otp_poll(Duration::from_millis(50), Duration::from_secs(2));
    let client = SsiClient::new(cfg).expect("client");

    let otp = client.auth().request_otp().await.expect("requestOtp");
    assert!(otp.is_smart_otp());
    let tx_id = otp.transaction_id().expect("tx");
    let info = client
        .auth()
        .poll_smart_otp(tx_id, CancellationToken::new())
        .await
        .expect("approved");
    assert_eq!(info.source, TokenSource::SmartOtp);
    assert_eq!(
        client
            .auth()
            .access_token()
            .await
            .expect("tok")
            .expose_secret(),
        "smart-access-token"
    );
}

#[tokio::test]
async fn smart_otp_terminal_rejection_stops_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "code": 401_115,
            "msg": "User rejected push approval",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = common::trading_config(&server.uri())
        .with_otp_poll(Duration::from_millis(50), Duration::from_secs(2));
    let client = SsiClient::new(cfg).expect("client");

    let err = client
        .auth()
        .poll_smart_otp("tx-reject", CancellationToken::new())
        .await
        .expect_err("must stop immediately");
    assert!(matches!(err, Error::SmartOtpRejected(_)));
    assert_eq!(err.api_code(), Some(ApiErrorCode::PushApprovalRejected));
}

#[tokio::test]
async fn dead_refresh_clears_session_and_requires_reauth() {
    let server = MockServer::start().await;
    common::mount_token_login(&server, "acc-1", "ref-1").await;

    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .and(body_json(serde_json::json!({ "refreshToken": "ref-1" })))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "code": 401_103,
            "msg": "Refresh token revoked or expired",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = SsiClient::new(common::trading_config(&server.uri())).expect("client");
    client
        .auth()
        .authenticate_with_otp("654321")
        .await
        .expect("login");

    let err = client
        .auth()
        .refresh()
        .await
        .expect_err("refresh must fail with ReauthenticationRequired");
    assert!(matches!(err, Error::ReauthenticationRequired(_)));
    assert!(!client.auth().is_authenticated());
    assert!(client.auth().export_token().is_none());
}

#[tokio::test]
async fn explicit_read_only_token_works_when_requested() {
    let server = MockServer::start().await;
    let now = Utc::now().timestamp();
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .and(body_json(serde_json::json!({
            "apiKey": common::TEST_API_KEY,
            "apiSecret": common::TEST_API_SECRET,
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": "readonly-tok",
            "refreshToken": "readonly-ref",
            "expiresAt": now + 900,
        })))
        .mount(&server)
        .await;

    let client = SsiClient::new(common::trading_config(&server.uri())).expect("client");
    let info = client
        .auth()
        .authenticate_read_only()
        .await
        .expect("read only");
    assert_eq!(info.source, TokenSource::ReadOnly);
}
