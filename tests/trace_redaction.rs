#![doc = "The bearer token, API secret, private key, and account numbers must not reach logs."]
#![allow(clippy::expect_used)]

mod common;
#[path = "../examples/common/mod.rs"]
#[allow(unreachable_pub, reason = "only the filter helpers are used here")]
mod example_common;

#[cfg(any(feature = "data", feature = "trading"))]
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(any(feature = "data", feature = "trading"))]
use futures_util::StreamExt as _;
#[cfg(any(feature = "data", feature = "trading"))]
use ssi_sdk::SsiClient;
#[cfg(any(feature = "data", feature = "trading"))]
use tokio::net::TcpListener;
#[cfg(any(feature = "data", feature = "trading"))]
use tracing_subscriber::{EnvFilter, util::SubscriberInitExt as _};
#[cfg(any(feature = "data", feature = "trading"))]
use wiremock::MockServer;

#[cfg(any(feature = "data", feature = "trading"))]
#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

#[cfg(any(feature = "data", feature = "trading"))]
impl io::Write for Buffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(any(feature = "data", feature = "trading"))]
impl Buffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("lock")).into_owned()
    }
}

#[cfg(feature = "data")]
mod market_stream_redaction {
    use ssi_sdk::stream::{MarketStreamConfig, MarketStreamEvent, Topic};

    use super::*;

    const TOKEN: &str = "SECRET-ACCESS-TOKEN-0123456789";

    async fn logs_of_one_connect(filter: EnvFilter) -> String {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let _guard = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .set_default();

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await {
                        while socket.next().await.is_some() {}
                    }
                });
            }
        });
        let http = MockServer::start().await;
        common::mount_auth(&http, TOKEN).await;
        let client = SsiClient::new(
            common::config(&http.uri())
                .with_stream_url(&url)
                .expect("stream url"),
        )
        .expect("client");
        let mut stream = client.market_stream(MarketStreamConfig::new([
            Topic::trade("VNM").expect("topic")
        ]));
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .expect("event in time")
                .expect("stream open");
            if matches!(event, MarketStreamEvent::Connected { .. }) {
                break;
            }
        }
        stream.shutdown().await;
        buffer.text()
    }

    #[tokio::test]
    async fn the_example_filter_keeps_the_data_token_out_of_trace_logs() {
        let mut filter = EnvFilter::new("trace");
        for directive in example_common::REDACTING_DIRECTIVES {
            filter = filter.add_directive(directive.parse().expect("directive"));
        }
        let logs = logs_of_one_connect(filter).await;
        assert!(logs.lines().count() > 5, "trace logging was active");
        assert!(!logs.contains(TOKEN), "the bearer token reached the logs");
    }

    #[tokio::test]
    async fn plain_trace_logging_is_flagged_on_market_stream() {
        let logs = logs_of_one_connect(EnvFilter::new("trace")).await;
        assert!(
            logs.contains(TOKEN),
            "expected tungstenite to log the request at TRACE"
        );
        assert!(
            logs.contains("add `tungstenite=debug` to the log filter"),
            "the client did not warn about TRACE logging"
        );
    }
}

#[cfg(feature = "trading")]
mod trading_stream_redaction {
    use ssi_sdk::{
        AccountNo,
        stream::{TradingStreamConfig, TradingStreamEvent, TradingTopic},
    };

    use super::*;

    const TOKEN: &str = "SECRET-TRADING-TOKEN-9876543210";

    async fn logs_of_one_connect(filter: EnvFilter) -> String {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let _guard = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .set_default();

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await {
                        while socket.next().await.is_some() {}
                    }
                });
            }
        });

        let http = MockServer::start().await;
        common::mount_token_login(&http, TOKEN, "SECRET-REFRESH-TOKEN-5555").await;
        let client = SsiClient::new(
            common::trading_config(&http.uri())
                .with_stream_url(&url)
                .expect("stream url"),
        )
        .expect("client");
        client
            .auth()
            .authenticate_with_otp("999111")
            .await
            .expect("otp");

        let acct = AccountNo::new(common::TEST_ACCOUNT);
        let mut stream = client
            .trading_stream(TradingStreamConfig::new([
                TradingTopic::order(&acct).expect("topic")
            ]));
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .expect("event in time")
                .expect("stream open");
            if matches!(event, TradingStreamEvent::Connected { .. }) {
                break;
            }
        }
        stream.shutdown().await;
        buffer.text()
    }

    #[tokio::test]
    async fn example_filter_keeps_trading_secrets_out_of_trace_logs() {
        let mut filter = EnvFilter::new("trace");
        for directive in example_common::REDACTING_DIRECTIVES {
            filter = filter.add_directive(directive.parse().expect("directive"));
        }
        let logs = logs_of_one_connect(filter).await;
        assert!(logs.lines().count() > 5, "trace logging was active");
        assert!(!logs.contains(TOKEN), "bearer token leaked into logs");
        assert!(
            !logs.contains(common::TEST_API_SECRET),
            "api secret leaked into logs"
        );
        assert!(
            !logs.contains("SECRET-REFRESH-TOKEN-5555"),
            "refresh token leaked into logs"
        );
        assert!(
            !logs.contains(common::TEST_ACCOUNT),
            "account number leaked into logs"
        );
    }
}
