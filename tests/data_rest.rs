#![doc = "REST contract tests: captured live responses replayed by a mock server."]
#![allow(clippy::expect_used, clippy::indexing_slicing)]

mod common;

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use common::{client, config, fixture, mount_auth, response, token_body};
use rust_decimal::dec;
use serde_json::{Value, json};
use ssi_sdk::{
    ApiErrorCode, Error, SsiClient,
    model::{AccountNo, AccountType, Exchange, OrderStatus},
    rest::{
        CorporateActionsPageSize, CorporateActionsQuery, IndexSummaryFilter, MarketFile,
        MarketFileQuery, OhlcQuery, OrderBookQuery, SecuritiesFilter, SecuritiesSummaryQuery,
        Timeframe,
    },
    time::Clock,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("date")
}

async fn server() -> MockServer {
    let server = MockServer::start().await;
    mount_auth(&server, "access-1").await;
    server
}

fn bars(count: usize, newest_day: u32) -> Value {
    let data: Vec<Value> = (0..count)
        .map(|offset| {
            let day = newest_day - u32::try_from(offset).expect("small");
            json!({"symbol": "VNM", "tradingDate": format!("2025/03/{day:02}"),
                   "open": "1", "high": "2", "low": "1", "close": "2",
                   "volume": "100", "value": "200"})
        })
        .collect();
    json!({"code": 200, "msg": "", "pageIndex": 1, "pageSize": 2, "data": data})
}

#[tokio::test]
async fn ohlc_contract_daily_and_intraday() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .and(query_param("timeFrame", "1d"))
        .and(query_param("from", "2025/01/01 00:00:00"))
        .and(query_param("to", "2025/12/31 23:59:59"))
        .and(header("authorization", "Bearer access-1"))
        .respond_with(response(&fixture("ohlc_daily_fpt_2025")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .and(query_param("timeFrame", "1m"))
        .respond_with(response(&fixture("ohlc_1m_vnm_20261119")))
        .mount(&server)
        .await;
    let client = client(&server.uri());

    let daily = OhlcQuery::days(
        "FPT",
        Timeframe::Day1,
        date("2025-01-01"),
        date("2025-12-31"),
    )
    .expect("query");
    let page = client.ohlc_page(&daily, 1).await.expect("page");
    assert_eq!(page.bars.len(), 5);
    let newest = &page.bars[0];
    assert_eq!(newest.trading_date, date("2025-12-31"));
    assert_eq!(newest.close, dec!(85907.332));
    assert_eq!(newest.volume, 3_082_600);
    assert_eq!(newest.value, dec!(264817941623.2));
    let all = client.ohlc(&daily).await.expect("walk");
    assert!(
        all.windows(2).all(|pair| pair[0].time < pair[1].time),
        "oldest first"
    );

    let intraday = OhlcQuery::days(
        "VNM",
        Timeframe::Minute1,
        date("2026-11-19"),
        date("2026-11-19"),
    )
    .expect("query");
    let minute = client.ohlc_page(&intraday, 1).await.expect("page");
    let last = &minute.bars[0];
    assert_eq!(last.time.to_rfc3339(), "2026-11-19T07:45:00+00:00");
    assert_eq!(last.close, dec!(51100));
    let rate = client.rate_limit().expect("headers seen");
    assert_eq!(rate.limit, Some(20));
}

#[tokio::test]
async fn ohlc_walker_pages_until_a_short_page() {
    let server = server().await;
    for (index, count, newest) in [("1", 2, 20), ("2", 2, 18), ("3", 1, 16)] {
        Mock::given(method("GET"))
            .and(path("/api/v3/data/ohlc"))
            .and(query_param("pageIndex", index))
            .and(query_param("pageSize", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(bars(count, newest)))
            .expect(1)
            .mount(&server)
            .await;
    }
    let client = client(&server.uri());
    let query = OhlcQuery::days(
        "VNM",
        Timeframe::Day1,
        date("2025-03-01"),
        date("2025-03-31"),
    )
    .expect("query")
    .with_page_size(2)
    .expect("size");
    let bars = client.ohlc(&query).await.expect("walk");
    let days: Vec<u32> = bars
        .iter()
        .map(|bar| chrono::Datelike::day(&bar.trading_date))
        .collect();
    assert_eq!(days, vec![16, 17, 18, 19, 20]);
}

#[tokio::test]
async fn error_codes_are_typed() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .respond_with(response(&fixture("ohlc_error_bare_date")))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = OhlcQuery::days(
        "VNM",
        Timeframe::Day1,
        date("2025-01-01"),
        date("2025-01-31"),
    )
    .expect("q");
    let error = client.ohlc_page(&query, 1).await.expect_err("400213");
    assert_eq!(error.api_code(), Some(ApiErrorCode::DateFormatInvalid));
    let Error::Api(api) = error else {
        panic!("API error expected")
    };
    assert_eq!(api.http_status, 400);
    assert_eq!(api.message, "Invalid Date/Timestamp Format");
}

#[tokio::test]
async fn corporate_actions_contract() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/14"))
        .and(query_param("to", "2026/11/19"))
        .and(query_param("pageSize", "100"))
        .respond_with(response(&fixture("corporate_actions_20261114_20261119")))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-14"), date("2026-11-19")).expect("query");
    let page = client
        .corporate_actions_page(&query, 1)
        .await
        .expect("page");
    assert_eq!(page.items_count, Some(76));
    assert_eq!(page.items.len(), 8);
    let dividend = page
        .items
        .iter()
        .find(|event| event.symbol == "S4A")
        .expect("S4A dividend");
    assert_eq!(dividend.ratio, Some(dec!(0.9355)));
    assert_eq!(dividend.ex_right_date, Some(date("2026-11-19")));
    assert_eq!(dividend.record_date, Some(date("2026-11-20")));
    let notice = &page.items[0];
    assert_eq!(notice.ex_right_date, None);
    assert_eq!(notice.exchange, None);

    let too_long = CorporateActionsQuery::new(date("2026-11-13"), date("2026-11-19")).expect("q");
    assert!(matches!(
        client.corporate_actions_page(&too_long, 1).await,
        Err(Error::InvalidRequest(_))
    ));
}

#[tokio::test]
async fn corporate_actions_walker_uses_five_day_windows() {
    let server = server().await;
    let body = fixture("corporate_actions_20261114_20261119").body;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-19")).expect("query");
    let events = client.corporate_actions(&query).await.expect("walk");
    assert_eq!(
        events.len(),
        8,
        "identical events across windows are kept once"
    );

    let requests = server.received_requests().await.expect("recorded");
    let spans: Vec<(NaiveDate, NaiveDate)> = requests
        .iter()
        .filter(|request| request.url.path() == "/api/v3/data/corporateActions")
        .map(|request| {
            let get = |key: &str| {
                request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == key)
                    .map(|(_, value)| NaiveDate::parse_from_str(&value, "%Y/%m/%d").expect("date"))
                    .expect("param")
            };
            (get("from"), get("to"))
        })
        .collect();
    assert_eq!(spans.len(), 4);
    assert!(spans.iter().all(|(from, to)| (*to - *from).num_days() <= 5));
    assert_eq!(spans[0].0, date("2026-11-01"));
    assert_eq!(spans[3].1, date("2026-11-19"));
}

#[tokio::test]
async fn no_content_is_an_empty_result() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-14"), date("2026-11-19")).expect("query");
    assert_eq!(
        client.corporate_actions(&query).await.expect("empty"),
        Vec::new()
    );
}

#[tokio::test]
async fn throttled_requests_wait_for_retry_after() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(response(&fixture("corporate_actions_429")))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(2)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-14"), date("2026-11-19")).expect("query");
    let started = std::time::Instant::now();
    let page = client
        .corporate_actions_page(&query, 1)
        .await
        .expect("retried");
    assert_eq!(page.items, Vec::new());
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "honoured Retry-After: 1"
    );
}

#[tokio::test]
async fn throttling_beyond_the_accepted_wait_is_reported() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(response(&fixture("corporate_actions_429")))
        .mount(&server)
        .await;
    let client = SsiClient::new(
        config(&server.uri()).with_rate_limit_retries(3, Duration::from_millis(100)),
    )
    .expect("client");
    let query = CorporateActionsQuery::new(date("2026-11-14"), date("2026-11-19")).expect("query");
    let error = client
        .corporate_actions_page(&query, 1)
        .await
        .expect_err("429");
    let Error::RateLimited(limited) = error else {
        panic!("rate limit expected")
    };
    assert_eq!(limited.retry_after, Some(Duration::from_secs(1)));
    assert_eq!(limited.snapshot.as_ref().and_then(|s| s.limit), Some(1));
    assert_eq!(
        limited.message,
        "Too many requests. Please try again later."
    );
}

#[tokio::test]
async fn business_limits_under_http_429_are_not_retried() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(json!({"code": 429_100, "msg": "Daily Order Limit Exceeded"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let error = client.indices(None).await.expect_err("limit");
    assert_eq!(
        error.api_code(),
        Some(ApiErrorCode::DailyOrderLimitExceeded)
    );
    assert!(matches!(error, Error::Api(_)));
}

#[tokio::test]
async fn rejected_access_token_is_renewed_once() {
    let server = MockServer::start().await;
    mount_auth(&server, "access-1").await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("access-2")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .and(header("authorization", "Bearer access-1"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(json!({"code": 401_004, "msg": "Access Token Expired"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .and(header("authorization", "Bearer access-2"))
        .respond_with(response(&fixture("index_list_hose")))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let indices = client
        .indices(Some(&Exchange::Hose))
        .await
        .expect("renewed");
    assert_eq!(indices.len(), 28);
    let info = client.auth().info().expect("token info");
    assert_eq!(info.refresh_token_rotated, Some(true));
}

#[tokio::test]
async fn reference_data_contracts() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/securitiesByBoard"))
        .and(query_param("symbol", "VNM"))
        .respond_with(response(&fixture("securities_vnm")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/masterdata"))
        .respond_with(response(&fixture("masterdata_page1")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .and(query_param("board", "HOSE"))
        .respond_with(response(&fixture("index_list_hose")))
        .mount(&server)
        .await;
    let client = client(&server.uri());

    let vnm = client
        .securities(&SecuritiesFilter::Symbol("vnm".to_owned()))
        .await
        .expect("securities");
    assert_eq!(vnm.len(), 1);
    assert_eq!(vnm[0].board, Exchange::Hose);
    assert_eq!(vnm[0].lot_size, Some(100));
    assert_eq!(vnm[0].stock_type.as_deref(), Some("Stock"));
    assert_eq!(vnm[0].first_trading_date, None);

    let page = client
        .price_limits_page(None, 1, 10)
        .await
        .expect("masterdata");
    assert_eq!(page.items.len(), 10);
    assert_eq!(page.items_count, Some(2120));
    assert_eq!(page.items[0].trading_date, date("2026-11-19"));
    assert_eq!(page.items[0].board, Exchange::Hnx);

    let indices = client
        .indices(Some(&Exchange::Hose))
        .await
        .expect("indices");
    assert!(
        indices
            .iter()
            .all(|index| index.index.trim() == index.index)
    );
    assert!(indices.iter().any(|index| index.index == "VNDIVIDEND"));
}

#[tokio::test]
async fn account_contracts_mask_account_numbers() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/account/info"))
        .respond_with(response(&fixture("account_info")))
        .mount(&server)
        .await;
    for (account, name) in [("003C000001", "cash"), ("003C000008", "derivative")] {
        Mock::given(method("GET"))
            .and(path("/api/v3/trading/position"))
            .and(query_param("accountNo", account))
            .respond_with(response(&fixture(&format!("position_{name}"))))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v3/trading/accountBalance"))
            .and(query_param("accountNo", account))
            .respond_with(response(&fixture(&format!("balance_{name}"))))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/orderBook"))
        .and(query_param("from", "2026/11/01"))
        .and(query_param("to", "2026/11/19"))
        .respond_with(response(&fixture("orderbook_cash")))
        .mount(&server)
        .await;
    let client = client(&server.uri());

    let accounts = client.accounts().await.expect("accounts");
    assert_eq!(
        accounts
            .iter()
            .map(|a| a.account_type.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(AccountType::Cash),
            Some(AccountType::Margin),
            Some(AccountType::Derivative)
        ]
    );
    let shown = format!("{accounts:?}");
    assert!(!shown.contains("003C000001"), "Debug masks account numbers");

    let cash = AccountNo::new("003C000001");
    let positions = client.positions(&cash, false).await.expect("positions");
    assert_eq!(positions.equity, Vec::new());
    assert!(positions.derivative.is_none());
    assert!(positions.received_time.is_some());
    let balance = client.balance(&cash).await.expect("balance");
    assert!(balance.equity.is_some());
    assert!(balance.derivative.is_none());

    let derivative = AccountNo::new("003C000008");
    let positions = client
        .positions(&derivative, false)
        .await
        .expect("positions");
    let der = positions.derivative.expect("derivative block");
    assert!(der.open_positions.is_empty() && der.closed_positions.is_empty());
    let balance = client.balance(&derivative).await.expect("balance");
    assert!(balance.derivative.is_some());

    let book = client
        .order_book_page(
            &OrderBookQuery::new(cash).with_date_range(date("2026-11-01"), date("2026-11-19")),
        )
        .await
        .expect("order book");
    assert_eq!(book.orders, Vec::new());
    assert_eq!(book.total_records, 0);
    assert!(!OrderStatus::Queued.is_final());
}

#[tokio::test]
async fn login_redirects_are_not_followed() {
    let other = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/capture"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("stolen")))
        .expect(0)
        .mount(&other)
        .await;
    let login = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("Location", format!("{}/capture", other.uri()).as_str()),
        )
        .mount(&login)
        .await;
    let client = client(&login.uri());
    let error = client
        .auth()
        .access_token()
        .await
        .expect_err("307 is an error");
    assert!(
        matches!(error, Error::Api(ref api) if api.http_status == 307),
        "{error}"
    );
    assert_eq!(other.received_requests().await.expect("recorded").len(), 0);
}

#[tokio::test]
async fn raw_requests_cannot_leave_api_v3() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/internal/admin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .expect(0)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    for path in [
        "/api/v3/../../internal/admin",
        "/api/v3/%2e%2e/%2e%2e/internal/admin",
    ] {
        assert!(matches!(
            client.get_raw(path, &[]).await,
            Err(Error::InvalidRequest(_))
        ));
    }
}

#[tokio::test]
async fn auth_does_not_queue_behind_paced_requests() {
    let server = server().await;
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("access-2")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .respond_with(response(&fixture("index_list_hose")))
        .mount(&server)
        .await;
    let client = SsiClient::new(
        config(&server.uri()).with_requests_per_second(std::num::NonZeroU32::new(5)),
    )
    .expect("client");
    client.auth().access_token().await.expect("login");
    // 15 paced GETs reserve 3 s of slots on indexList.
    let tasks: Vec<_> = (0..15)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.indices(None).await.map(|rows| rows.len()) })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    client.auth().force_refresh().await.expect("refresh");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    let started = std::time::Instant::now();
    client.auth().access_token().await.expect("token");
    assert!(started.elapsed() < Duration::from_millis(50));
    for task in tasks {
        task.await.expect("join").expect("rows");
    }
}

#[tokio::test]
async fn one_exhausted_endpoint_does_not_pause_the_others() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(response(&fixture("corporate_actions_20261114_20261119")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexList"))
        .respond_with(response(&fixture("index_list_hose")))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-14"), date("2026-11-19")).expect("query");
    // The fixture says X-RATELIMIT-REMAINING: 0 for corporateActions.
    client
        .corporate_actions_page(&query, 1)
        .await
        .expect("page");
    let started = std::time::Instant::now();
    client.indices(None).await.expect("indices");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    let started = std::time::Instant::now();
    client
        .corporate_actions_page(&query, 1)
        .await
        .expect("page");
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "corporateActions itself waits"
    );
}

#[tokio::test]
async fn ohlc_walker_follows_a_server_capped_page_size() {
    let server = server().await;
    for (index, newest) in [("1", 20), ("2", 18), ("3", 16)] {
        Mock::given(method("GET"))
            .and(path("/api/v3/data/ohlc"))
            .and(query_param("pageIndex", index))
            .respond_with(ResponseTemplate::new(200).set_body_json(bars(2, newest)))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .and(query_param("pageIndex", "4"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"code": 200, "msg": "", "pageIndex": 4, "pageSize": 2, "data": []}),
        ))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    // 1000 requested; the server answers pages of 2 and echoes pageSize: 2.
    let query = OhlcQuery::days(
        "VNM",
        Timeframe::Day1,
        date("2025-03-01"),
        date("2025-03-31"),
    )
    .expect("query");
    assert_eq!(client.ohlc(&query).await.expect("walk").len(), 6);
}

#[tokio::test]
async fn numeric_prices_keep_every_digit() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"code":200,"msg":"","pageIndex":1,"pageSize":1000,"data":[{"symbol":"FPT","tradingDate":"2025/12/31","open":86804.068,"high":86983.415,"low":85369.29,"close":85907.33212345678912,"volume":3082600,"value":264817941623.2}]}"#,
            "application/json",
        ))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = OhlcQuery::days(
        "FPT",
        Timeframe::Day1,
        date("2025-12-31"),
        date("2025-12-31"),
    )
    .expect("query");
    let page = client.ohlc_page(&query, 1).await.expect("page");
    assert_eq!(page.bars[0].close.to_string(), "85907.33212345678912");
    assert_eq!(page.bars[0].value.to_string(), "264817941623.2");
}

#[tokio::test]
async fn pages_without_a_count_are_walked_until_empty() {
    let server = server().await;
    let row = |symbol: &str| {
        json!({"board": "HOSE", "ceiling": 2, "floor": 1, "refPrice": 1.5,
               "symbol": symbol, "tradingDate": "2026/11/19"})
    };
    for (index, data) in [
        ("1", json!([row("AAA"), row("BBB")])),
        ("2", json!([row("CCC")])),
        ("3", json!([])),
    ] {
        Mock::given(method("GET"))
            .and(path("/api/v3/data/masterdata"))
            .and(query_param("pageIndex", index))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"code": 200, "data": data})),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let client = client(&server.uri());
    let rows = client.price_limits(None).await.expect("rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].reference, dec!(1.5));
}

fn corporate_event(symbol: &str, number: u32) -> Value {
    json!({"eventTitle": format!("{symbol} - event {number}"), "exchange": "",
           "exrightDate": "2026/11/05", "issueDate": "2026/11/20", "ratio": "",
           "recordDate": "", "symbol": symbol})
}

fn corporate_body(data: &[Value], page: u32, pages: Option<u32>) -> Value {
    let mut body = json!({"code": 200, "msg": "", "pageIndex": page, "pageSize": 10,
                          "itemsCount": 13, "data": data});
    if let Some(pages) = pages {
        body["pagesCount"] = json!(pages);
    }
    body
}

#[tokio::test]
async fn corporate_walker_keeps_pages_that_repeat_earlier_windows() {
    let server = server().await;
    let old: Vec<Value> = (0..10).map(|n| corporate_event("AAA", n)).collect();
    let new: Vec<Value> = (0..3).map(|n| corporate_event("BBB", n)).collect();
    // Window 1 (11/01..11/06): ten AAA events on one page.
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&old, 1, Some(1))))
        .mount(&server)
        .await;
    // Window 2 (11/07..11/12): page 1 repeats those ten, page 2 has three new ones.
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/07"))
        .and(query_param("pageIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&old, 1, Some(2))))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/07"))
        .and(query_param("pageIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&new, 2, Some(2))))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-12"))
        .expect("query")
        .with_page_size(CorporateActionsPageSize::Ten);
    let events = client.corporate_actions(&query).await.expect("walk");
    assert_eq!(events.len(), 13);
    assert!(events.iter().any(|event| event.symbol == "BBB"));
}

#[tokio::test]
async fn corporate_walker_stops_when_a_window_repeats_its_own_page() {
    let server = server().await;
    let events: Vec<Value> = (0..10).map(|n| corporate_event("AAA", n)).collect();
    // The server ignores pageIndex but claims 50 pages.
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(
            &events,
            1,
            Some(50),
        )))
        .expect(2)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-06"))
        .expect("query")
        .with_page_size(CorporateActionsPageSize::Ten);
    assert_eq!(
        client.corporate_actions(&query).await.expect("walk").len(),
        10
    );
}

#[tokio::test]
async fn corporate_walker_without_a_page_count_stops_on_a_repeated_page() {
    let server = server().await;
    let events: Vec<Value> = (0..10).map(|n| corporate_event("AAA", n)).collect();
    let more: Vec<Value> = (0..2).map(|n| corporate_event("CCC", n)).collect();
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("pageIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&events, 1, None)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("pageIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&more, 2, None)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("pageIndex", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&more, 3, None)))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-06"))
        .expect("query")
        .with_page_size(CorporateActionsPageSize::Ten);
    assert_eq!(
        client.corporate_actions(&query).await.expect("walk").len(),
        12
    );
}

#[tokio::test]
async fn raw_paths_with_whitespace_or_controls_never_leave_api_v3() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/internal/admin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .expect(0)
        .mount(&server)
        .await;
    let client = client(&server.uri());
    for candidate in [
        "/api/v3/../../internal/admin",
        "/api/v3/%2e%2e/%2e%2e/internal/admin",
        "/api/v3/.\t./.\t./internal/admin",
        "/api/v3/.\n./.\n./internal/admin",
        "/api/v3/.\r./.\r./internal/admin",
        "/api/v3/%2e\t%2e/.%2\te/internal/admin",
        "/api/v3/x/..;/..;/internal/admin",
    ] {
        assert!(
            matches!(
                client.get_raw(candidate, &[]).await,
                Err(Error::InvalidRequest(_))
            ),
            "{candidate:?} must be rejected"
        );
    }
    let hits = server
        .received_requests()
        .await
        .expect("recorded")
        .iter()
        .filter(|request| request.url.path() != "/api/v3/auth/token")
        .count();
    assert_eq!(hits, 0);
}

#[tokio::test]
async fn private_error_messages_stay_out_of_debug_output() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/position"))
        .respond_with(ResponseTemplate::new(403).set_body_json(
            json!({"code": 403_101, "msg": "Forbidden: account '003C123456' is not authorized"}),
        ))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let error = client
        .positions(&AccountNo::new("003C123456"), false)
        .await
        .expect_err("forbidden");
    assert!(!error.to_string().contains("003C123456"));
    assert!(!format!("{error:?}").contains("003C123456"));
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
    assert!(
        !format!("{boxed:?}").contains("003C123456"),
        "what `main() -> Result` prints"
    );
}

#[tokio::test]
async fn corporate_walker_without_a_page_count_keeps_pages_that_repeat_earlier_windows() {
    let server = server().await;
    let old: Vec<Value> = (0..10).map(|n| corporate_event("AAA", n)).collect();
    let new: Vec<Value> = (0..3).map(|n| corporate_event("BBB", n)).collect();
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/01"))
        .and(query_param("pageIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&old, 1, None)))
        .mount(&server)
        .await;
    // Window 2, no pagesCount: page 1 repeats window 1, page 2 is new, page 3 is empty.
    for (index, data) in [("1", old.clone()), ("2", new.clone()), ("3", Vec::new())] {
        Mock::given(method("GET"))
            .and(path("/api/v3/data/corporateActions"))
            .and(query_param("from", "2026/11/07"))
            .and(query_param("pageIndex", index))
            .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&data, 1, None)))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/api/v3/data/corporateActions"))
        .and(query_param("from", "2026/11/01"))
        .and(query_param("pageIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(corporate_body(&[], 2, None)))
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let query = CorporateActionsQuery::new(date("2026-11-01"), date("2026-11-12"))
        .expect("query")
        .with_page_size(CorporateActionsPageSize::Ten);
    let events = client.corporate_actions(&query).await.expect("walk");
    assert_eq!(events.len(), 13);
    assert!(events.iter().any(|event| event.symbol == "BBB"));
}

#[derive(Debug)]
struct FixedClock(DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[tokio::test]
async fn index_summary_and_securities_summary_endpoints() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/indexSummary"))
        .and(query_param("index", "VN30"))
        .and(query_param("tradingDate", "2026/11/19"))
        .and(header("accept-language", "vi"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "tradingDate": "2026/11/19",
                "totalTrade": "500000",
                "totalTradeValue": "12000000000000",
                "indexValue": "1315.42",
                "indexChange": "8.25",
                "indexChangePercentage": "0.63",
                "totalAdvanceStock": "21",
                "totalDeclineStock": "7",
                "totalNoChangeStock": "2",
                "netPurchasesForeignVolume": "-12000"
            }
        ])))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v3/data/securitiesSummary"))
        .and(query_param("symbol", "VNM"))
        .and(query_param("from", "2026/11/18"))
        .and(query_param("to", "2026/11/19"))
        .and(query_param("pageIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pageIndex": 1,
            "pageSize": 1,
            "pagesCount": 2,
            "itemsCount": 2,
            "data": [{
                "symbol": "VNM",
                "tradingDate": "2026/11/19",
                "close": "51100",
                "totalMatch": "1,234,500",
                "remainForeignRoom": "5000000"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/securitiesSummary"))
        .and(query_param("symbol", "VNM"))
        .and(query_param("pageIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pageIndex": 2,
            "pageSize": 1,
            "pagesCount": 2,
            "itemsCount": 2,
            "data": [{
                "symbol": "VNM",
                "tradingDate": "2026/11/18",
                "close": "51600",
                "totalMatch": "980000",
                "remainForeignRoom": "5100000"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = SsiClient::new(config(&server.uri()).with_accept_language("vi")).expect("client");
    let summary = client
        .index_summary(
            &IndexSummaryFilter::Index("vn30".to_owned()),
            Some(date("2026-11-19")),
        )
        .await
        .expect("index summary");
    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].index_value, dec!(1315.42));
    assert_eq!(summary[0].net_purchases_foreign_volume, Some(dec!(-12000)));

    let sec_query =
        SecuritiesSummaryQuery::for_symbol("vnm", date("2026-11-18"), date("2026-11-19"))
            .expect("query")
            .with_page_size(1)
            .expect("size");
    let rows = client
        .securities_summary(&sec_query)
        .await
        .expect("securities summary");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].total_match, 1_234_500);
    assert_eq!(rows[1].close, dec!(51600));
}

#[tokio::test]
async fn market_files_and_parquet_download_contract() {
    let server = server().await;
    let download_url = format!(
        "{}/api/v3/data/download?token=ONE_TIME_PARQUET_TOKEN",
        server.uri()
    );
    Mock::given(method("GET"))
        .and(path("/api/v3/data/file"))
        .and(query_param("symbol", "VNM"))
        .and(query_param("timeFrame", "1d"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "files": [{"date": "", "dataLink": download_url}],
            "code": 200,
            "msg": "OK"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // First download gets 503 (link not consumed per SSI docs), second succeeds with raw bytes.
    Mock::given(method("GET"))
        .and(path("/api/v3/data/download"))
        .and(query_param("token", "ONE_TIME_PARQUET_TOKEN"))
        .and(header("authorization", "Bearer access-1"))
        .and(header("accept", "application/octet-stream"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/download"))
        .and(query_param("token", "ONE_TIME_PARQUET_TOKEN"))
        .and(header("authorization", "Bearer access-1"))
        .and(header("accept", "application/octet-stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(b"PAR1\x00\x01\x02PAR1".to_vec(), "application/octet-stream"),
        )
        .with_priority(2)
        .mount(&server)
        .await;

    // 403 on a spent token must NOT invalidate the Bearer token or retry.
    Mock::given(method("GET"))
        .and(path("/api/v3/data/download"))
        .and(query_param("token", "SPENT_TOKEN"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_json(json!({"code": "400", "msg": "Invalid request"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let fixed_now = DateTime::parse_from_rfc3339("2026-11-20T03:00:00Z")
        .expect("time")
        .with_timezone(&Utc);
    let client = SsiClient::new(
        config(&server.uri())
            .with_clock(Arc::new(FixedClock(fixed_now)))
            .with_transient_retries(1),
    )
    .expect("client");

    // Future toDate rejected client-side before sending.
    let future = MarketFileQuery::minute1("VNM", date("2026-11-19"), date("2026-11-21"))
        .expect("query valid");
    assert!(matches!(
        client.market_files(&future).await,
        Err(Error::InvalidRequest(_))
    ));

    let files = client
        .market_files(&MarketFileQuery::daily("vnm").expect("daily"))
        .await
        .expect("files");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].date, None);
    assert!(!format!("{:?}", files[0]).contains("ONE_TIME_PARQUET_TOKEN"));

    let bytes = client
        .download_market_file(&files[0])
        .await
        .expect("parquet bytes");
    assert_eq!(bytes, b"PAR1\x00\x01\x02PAR1");

    // Off-origin dataLink rejected.
    let evil = MarketFile::new(None, "https://evil.example/api/v3/data/download?token=abc");
    assert!(matches!(
        client.download_market_file(&evil).await,
        Err(Error::InvalidRequest(_))
    ));

    // 403 fails immediately and preserves valid Bearer token.
    let err = client
        .download_market_file_by_token("SPENT_TOKEN")
        .await
        .expect_err("403");
    assert!(matches!(err, Error::Api(ref api) if api.http_status == 403));
    assert!(client.auth().has_valid_token());
}

#[tokio::test]
async fn ppmmr_positions_summary_order_book_status_and_ohlc_today() {
    let server = server().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/ppmmrAccount"))
        .and(query_param("accountNo", "003C000006"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "equity": {
                "accountNo": "003C000006",
                "purchasingPower": "250000000",
                "marginRatio": "50.5",
                "callForceSell": "1200000"
            },
            "derivative": {}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/position"))
        .and(query_param("accountNo", "003C000006"))
        .and(query_param("querySummary", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "equity": [],
            "derivative": {}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/trading/orderBook"))
        .and(query_param("accountNo", "003C000006"))
        .and(query_param("orderStatus", "QU"))
        .and(query_param("pageIndex", "1"))
        .and(query_param("pageSize", "20"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accountNo": "003C000006",
            "totalRecord": 0,
            "orderList": []
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/data/ohlc"))
        .and(query_param("symbol", "VNM"))
        .and(query_param("from", "2026/11/20 00:00:00"))
        .and(query_param("to", "2026/11/20 23:59:59"))
        .and(query_param("timeFrame", "1m"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let fixed_now = DateTime::parse_from_rfc3339("2026-11-20T03:00:00Z")
        .expect("time")
        .with_timezone(&Utc);
    let client = SsiClient::new(
        config(&server.uri())
            .with_clock(Arc::new(FixedClock(fixed_now)))
            .with_transient_retries(1),
    )
    .expect("client");
    let margin = AccountNo::new("003C000006");
    let ppmmr = client.ppmmr(&margin).await.expect("ppmmr");
    assert_eq!(ppmmr.derivative, None);
    let eq = ppmmr.equity.expect("equity");
    assert_eq!(eq.purchasing_power, dec!(250000000));
    assert_eq!(eq.call_force_sell, dec!(1200000));

    let pos = client.positions(&margin, true).await.expect("positions");
    assert_eq!(pos.derivative, None);

    let ob_query = OrderBookQuery::new(margin)
        .with_date_range(date("2026-11-20"), date("2026-11-20"))
        .with_status(OrderStatus::Queued)
        .with_page(1, 20);
    let page = client.order_book_page(&ob_query).await.expect("page");
    assert_eq!(page.orders, Vec::new());

    let today_bars = client
        .ohlc_today("VNM", Timeframe::Minute1)
        .await
        .expect("today");
    assert_eq!(today_bars, Vec::new());
}
