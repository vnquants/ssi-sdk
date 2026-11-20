#![doc = "Offline integration tests for V3 REST queries, signed orders, batch orders, and FCO."]
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::too_many_lines)]

mod common;

use chrono::NaiveDate;
use rust_decimal::dec;
use ssi_sdk::{
    AccountNo, AccountType, ApiErrorCode, CancelOrderRequest, ClientRequestId, Config, Credentials,
    Error, FcoCancelRequest, FcoListQuery, FcoOperator, FcoPlaceRequest, FcoPrice, FcoStatus,
    FcoStrategy, FcoType, FcoWindow, GtdParams, ModifyOrderRequest, OrderBookQuery, OrderPrice,
    OrderSide, OrderStatus, OrderTarget, OrderType, PlaceOrderRequest, SsiClient, StopLimitParams,
    parse_vn_datetime,
};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

#[tokio::test]
async fn account_balance_positions_ppmmr_orderbook_maxbuysell_and_host_routing() {
    let auth_server = MockServer::start().await;
    let trading_server = MockServer::start().await;
    common::mount_token_login(&auth_server, "tok-rest", "ref-rest").await;

    Mock::given(method("GET"))
        .and(path("/api/v3/account/info"))
        .and(header("authorization", "Bearer tok-rest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"accountNo": common::TEST_ACCOUNT, "accountType": "Cash"},
            {"accountNo": common::TEST_DERIV_ACCOUNT, "accountType": "Derivative"}
        ])))
        .mount(&trading_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/accountBalance"))
        .and(query_param("accountNo", common::TEST_ACCOUNT))
        .and(query_param("clientId", "CLIENT01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "equity": {
                "accountNo": common::TEST_ACCOUNT,
                "accountBalance": "150000000.5",
                "withdrawable": "100000000",
                "advancedCashT0": "5000000"
            },
            "derivative": {}
        })))
        .mount(&trading_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/position"))
        .and(query_param("accountNo", common::TEST_DERIV_ACCOUNT))
        .and(query_param("querySummary", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "equity": [],
            "derivative": {
                "derOpenPositions": [{
                    "symbol": "VN30F2612",
                    "long": 3,
                    "short": 1,
                    "net": 2,
                    "floatingPL": "2500000"
                }],
                "derClosePositions": []
            }
        })))
        .mount(&trading_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/ppmmrAccount"))
        .and(query_param("accountNo", common::TEST_ACCOUNT))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "equity": {
                "accountNo": common::TEST_ACCOUNT,
                "purchasingPower": "250000000",
                "marginRatio": "55.5",
                "doverdue_ssi": "0",
                "callForcesell": "12000000"
            }
        })))
        .mount(&trading_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/orderBook"))
        .and(query_param("accountNo", common::TEST_ACCOUNT))
        .and(query_param("from", "2026/07/29"))
        .and(query_param("to", "2026/07/29"))
        .and(query_param("pageIndex", "1"))
        .and(query_param("pageSize", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accountNo": common::TEST_ACCOUNT,
            "totalRecord": 1,
            "orderList": [{
                "orderId": "ORD-01",
                "clientRequestId": "req-01",
                "symbol": "VNM",
                "side": "B",
                "orderType": "LO",
                "price": "65500",
                "quantity": 100,
                "filledQty": 100,
                "orderStatus": "FF",
                "inputTime": "2026/07/29 09:15:00"
            }]
        })))
        .mount(&trading_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/maxBuySell"))
        .and(query_param("accountNo", common::TEST_ACCOUNT))
        .and(query_param("symbol", "VNM"))
        .and(query_param("price", "65500"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accountNo": common::TEST_ACCOUNT,
            "maxBuyQty": 3800,
            "maxSellQty": 500,
            "marginRatio": "0.5",
            "purchasingPower": "250000000"
        })))
        .mount(&trading_server)
        .await;

    let cfg = common::trading_config(&auth_server.uri())
        .with_trading_url(&trading_server.uri())
        .expect("trading url");
    let client = SsiClient::new(cfg).expect("client");
    client
        .auth()
        .authenticate_with_otp("123456")
        .await
        .expect("otp");

    let accounts = client.accounts().await.expect("accounts");
    assert_eq!(accounts.len(), 2);
    assert_eq!(accounts[0].account_type, Some(AccountType::Cash));
    assert_eq!(accounts[1].account_type, Some(AccountType::Derivative));

    let eq_acct = AccountNo::new(common::TEST_ACCOUNT);
    let der_acct = AccountNo::new(common::TEST_DERIV_ACCOUNT);

    let bal = client.balance(&eq_acct).await.expect("balance");
    assert_eq!(bal.equity.expect("eq").account_balance, dec!(150000000.5));

    let pos = client.positions(&der_acct, true).await.expect("positions");
    let der = pos.derivative.expect("der");
    assert_eq!(der.open_positions[0].net, 2);
    assert_eq!(der.open_positions[0].account_no, der_acct);

    let ppmmr = client.ppmmr(&eq_acct).await.expect("ppmmr");
    let eq_ppmmr = ppmmr.equity.expect("eq ppmmr");
    assert_eq!(eq_ppmmr.purchasing_power, dec!(250000000));
    assert_eq!(eq_ppmmr.call_force_sell, dec!(12000000));

    let date = NaiveDate::from_ymd_opt(2026, 7, 29).expect("date");
    let orders = client
        .order_book(&OrderBookQuery::new(eq_acct.clone()).with_date_range(date, date))
        .await
        .expect("order book");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].price, Some(OrderPrice::Limit(dec!(65500))));
    assert_eq!(orders[0].status, OrderStatus::FullyFilled);

    let mbs = client
        .max_buy_sell(&eq_acct, "vnm", Some(dec!(65500.00)))
        .await
        .expect("max buy sell");
    assert_eq!(mbs.max_buy_qty, 3800);
    assert_eq!(mbs.purchasing_power, Some(dec!(250000000)));
}

#[tokio::test]
async fn signed_orders_verify_x_signature_and_never_retry() {
    let server = MockServer::start().await;
    let client = common::authenticated_client(&server, "tok-sign").await;

    // Place order: verify X-Signature cryptographically against raw HTTP body.
    Mock::given(method("POST"))
        .and(path("/api/v3/trading/order"))
        .respond_with(|req: &Request| {
            let sig = req
                .headers
                .get("x-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert!(
                common::verify_hex_signature(&req.body, sig),
                "X-Signature must verify over exact raw request bytes"
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "orderId": "ORD-100",
                "clientRequestId": "my-req-01",
                "orderStatus": "PD"
            }))
        })
        .expect(1)
        .mount(&server)
        .await;

    let acct = AccountNo::new(common::TEST_ACCOUNT);
    let req = PlaceOrderRequest::limit(acct.clone(), "VNM", OrderSide::Buy, 100, dec!(65500))
        .with_client_request_id(ClientRequestId::new("my-req-01").expect("id"));
    let placed = client.place_order(&req).await.expect("placed");
    assert_eq!(placed.order_id, "ORD-100");
    assert_eq!(placed.status, OrderStatus::Pending);

    // Modify order: verifies signature.
    Mock::given(method("PUT"))
        .and(path("/api/v3/trading/order"))
        .respond_with(|req: &Request| {
            let sig = req
                .headers
                .get("x-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert!(common::verify_hex_signature(&req.body, sig));
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "clientModifyId": "mod-1",
                "orderId": "ORD-100",
                "clientRequestId": "my-req-01",
                "orderStatus": "WM"
            }))
        })
        .expect(1)
        .mount(&server)
        .await;

    let modified = client
        .modify_order(&ModifyOrderRequest::price(
            acct.clone(),
            OrderTarget::OrderId("ORD-100".to_owned()),
            dec!(66000),
        ))
        .await
        .expect("modified");
    assert_eq!(modified.status, OrderStatus::WaitingModify);

    // Cancel order returning 503 MUST NOT RETRY (expect(1)).
    Mock::given(method("DELETE"))
        .and(path("/api/v3/trading/order"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "code": 503_000,
            "msg": "Service Unavailable"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let cancel_err = client
        .cancel_order(&CancelOrderRequest::new(
            acct.clone(),
            OrderTarget::OrderId("ORD-100".to_owned()),
        ))
        .await
        .expect_err("must fail without retry");
    assert_eq!(
        cancel_err.api_code(),
        Some(ApiErrorCode::ServiceUnavailable)
    );

    // Batch place returning 409 / 409100 -> Error::DuplicateRequest (zero retries).
    Mock::given(method("POST"))
        .and(path("/api/v3/trading/order/batch"))
        .respond_with(|req: &Request| {
            let sig = req
                .headers
                .get("x-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert!(common::verify_hex_signature(&req.body, sig));
            ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "code": 409_100,
                "msg": "Duplicate batchRequestId"
            }))
        })
        .expect(1)
        .mount(&server)
        .await;

    let dup = client
        .place_batch_orders(
            &[PlaceOrderRequest::market(
                acct,
                "VNM",
                OrderSide::Buy,
                100,
                OrderType::Ato,
            )],
            Some(ClientRequestId::new("batch-dup-1").expect("id")),
        )
        .await
        .expect_err("duplicate");
    assert!(matches!(dup, Error::DuplicateRequest(_)));
}

#[tokio::test]
async fn missing_private_key_fails_closed_without_network_call() {
    let server = MockServer::start().await;
    common::mount_token_login(&server, "tok-nopk", "ref-nopk").await;

    // Zero order POSTs allowed.
    Mock::given(method("POST"))
        .and(path("/api/v3/trading/order"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let creds = Credentials::new(common::TEST_API_KEY, common::TEST_API_SECRET).expect("creds");
    let cfg = Config::new(creds)
        .allow_insecure(true)
        .with_api_url(&server.uri())
        .expect("url");
    let client = SsiClient::new(cfg).expect("client");
    client
        .auth()
        .authenticate_with_otp("123456")
        .await
        .expect("otp");

    let err = client
        .place_order(&PlaceOrderRequest::limit(
            AccountNo::new(common::TEST_ACCOUNT),
            "VNM",
            OrderSide::Buy,
            100,
            dec!(65000),
        ))
        .await
        .expect_err("must fail closed");
    assert!(matches!(err, Error::InvalidPrivateKey(_)));
}

#[tokio::test]
async fn fco_place_cancel_list_orderbook_and_lowercase_fcoid_history() {
    let server = MockServer::start().await;
    let client = common::authenticated_client(&server, "tok-fco").await;
    let acct = AccountNo::new(common::TEST_ACCOUNT);
    let window = FcoWindow::new(
        parse_vn_datetime("2026/07/01 09:00:00").expect("from"),
        parse_vn_datetime("2026/07/10 15:00:00").expect("to"),
    )
    .expect("window");

    Mock::given(method("POST"))
        .and(path("/api/v3/trading/fco/order"))
        .respond_with(|req: &Request| {
            let sig = req
                .headers
                .get("x-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert!(common::verify_hex_signature(&req.body, sig));
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "fcoId": "FCO-777" }
            }))
        })
        .expect(2)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/api/v3/trading/fco/order"))
        .respond_with(|req: &Request| {
            let sig = req
                .headers
                .get("x-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert!(common::verify_hex_signature(&req.body, sig));
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "fcoId": "FCO-777" }))
        })
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/fco/list"))
        .and(query_param("accountNo", common::TEST_ACCOUNT))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "pageIndex": 1,
            "pageSize": 10,
            "itemsCount": 1,
            "pagesCount": 1,
            "data": [{
                "fcoId": "FCO-777",
                "accountNo": common::TEST_ACCOUNT,
                "symbol": "VNM",
                "quantity": 100,
                "matchedQuality": 0,
                "type": "stop_limit",
                "status": "WAIT",
                "params": { "stopPrice": 64000, "operator": "4" }
            }]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/trading/fco/orderbook"))
        .and(query_param("fcoId", "FCO-777"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "pageIndex": 1,
            "pageSize": 10,
            "itemsCount": 1,
            "pagesCount": 1,
            "data": [{
                "fcoId": "FCO-777",
                "accountNo": common::TEST_ACCOUNT,
                "symbol": "VNM",
                "quantity": 100,
                "uniquedId": "U-1",
                "orderId": "ORD-9",
                "status": "QU",
                "type": "stop_limit"
            }]
        })))
        .mount(&server)
        .await;

    // Must use lowercase `fcoid` query parameter on REST!
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/fco/statusHistory"))
        .and(query_param("fcoid", "FCO-777"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"state": "INIT", "time": "2026/07/01 09:00:01", "detail": "created"},
                {"state": "WAIT", "time": "2026/07/01 09:00:02", "code": "0", "detail": "active"}
            ]
        })))
        .mount(&server)
        .await;

    let gtd = client
        .place_fco(&FcoPlaceRequest::new(FcoStrategy::Gtd(GtdParams {
            account_no: acct.clone(),
            symbol: "VNM".to_owned(),
            side: OrderSide::Buy,
            price: FcoPrice::Limit(dec!(65000)),
            price_slip: dec!(0),
            quantity: 100,
            window,
        })))
        .await
        .expect("gtd");
    assert_eq!(gtd.fco_id, "FCO-777");

    let sl = client
        .place_fco(&FcoPlaceRequest::new(FcoStrategy::StopLimit(
            StopLimitParams {
                account_no: acct.clone(),
                symbol: "VNM".to_owned(),
                side: OrderSide::Sell,
                price: FcoPrice::Limit(dec!(63900)),
                price_slip: dec!(100),
                stop_price: dec!(64000),
                operator: FcoOperator::LesserOrEqual,
                quantity: 100,
                window,
            },
        )))
        .await
        .expect("stop limit");
    assert_eq!(sl.fco_id, "FCO-777");

    let list = client
        .fco_list(&FcoListQuery::new(acct).with_statuses([FcoStatus::Wait, FcoStatus::Init]))
        .await
        .expect("list");
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].fco_type, FcoType::StopLimit);
    assert_eq!(
        list.items[0]
            .params
            .as_ref()
            .and_then(|p| p.operator.clone()),
        Some(FcoOperator::LesserOrEqual)
    );

    let ob = client.fco_order_book("FCO-777", 1, 10).await.expect("ob");
    assert_eq!(ob.orders[0].unique_id, "U-1");

    let hist = client.fco_status_history("FCO-777").await.expect("history");
    assert_eq!(hist.len(), 2);
    assert_eq!(hist[1].state, "WAIT");

    let cancelled = client
        .cancel_fco(&FcoCancelRequest::new("FCO-777"))
        .await
        .expect("cancel");
    assert_eq!(cancelled.fco_id, "FCO-777");
}
