#![doc = "Helpers shared by the examples: dotenv loading and the raw-frame dump."]
#![allow(clippy::expect_used, clippy::indexing_slicing)]

#[path = "../examples/common/dump.rs"]
#[allow(dead_code, unreachable_pub, reason = "only some helpers are exercised")]
mod dump;
#[path = "../examples/common/mod.rs"]
#[allow(unreachable_pub, reason = "only some helpers are exercised")]
mod example_common;

use std::{sync::mpsc, time::Duration};

use serde_json::{Value, json};
use ssi_sdk::stream::MarketStreamEvent;

#[test]
fn a_dotenv_directory_is_an_error_not_a_hang() {
    let directory = std::env::temp_dir();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let outcome = example_common::credentials_from_dotenv(&directory).map(|_| ());
        sender.send(outcome.map_err(|error| error.to_string())).ok();
    });
    let outcome = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("returns quickly");
    assert!(outcome.is_err());
}

#[test]
fn a_dotenv_file_yields_only_the_ssi_keys() {
    let path = std::env::temp_dir().join(format!("ssi-fc-data-dotenv-{}", std::process::id()));
    std::fs::write(
        &path,
        "OTHER_SECRET=has spaces and is not quoted\nSSI_API_KEY=key-123\nSSI_API_SECRET=secret-456\n",
    )
    .expect("write");
    let credentials = example_common::credentials_from_dotenv(&path).expect("credentials");
    std::fs::remove_file(&path).ok();
    let shown = format!("{credentials:?}");
    assert!(!shown.contains("key-123") && !shown.contains("secret-456"));
}

#[test]
fn the_dump_redacts_account_data_and_trading_topics() {
    let event = MarketStreamEvent::RawFrame {
        text: json!({
            "channel": "TRADING",
            "topic": "order.003C123456",
            "data": {"accountNo": "003C123456", "symbol": "VNM", "nested": [{"subAccount": "x"}]}
        })
        .to_string(),
        received_at: chrono::Utc::now(),
    };
    let line = dump::line_for(&event).expect("a line");
    let text = line.to_string();
    assert!(!text.contains("123456"), "{text}");
    assert_eq!(line["kind"], "frame");
    assert_eq!(line["frame"]["data"]["symbol"], "VNM");

    let market = MarketStreamEvent::RawFrame {
        text: r#"{"channel":"DATA","topic":"trade.VNM","data":{"p":"51100"}}"#.to_owned(),
        received_at: chrono::Utc::now(),
    };
    let line = dump::line_for(&market).expect("a line");
    assert_eq!(
        line["frame"]["data"]["p"],
        Value::String("51100".to_owned())
    );
}

#[test]
fn the_dump_writes_one_line_per_frame() {
    let path = std::env::temp_dir().join(format!("ssi-fc-data-dump-{}.jsonl", std::process::id()));
    std::fs::remove_file(&path).ok();
    let mut writer = dump::Dump::open(&path).expect("open");
    for index in 0..3 {
        writer
            .write(&MarketStreamEvent::RawFrame {
                text: format!(r#"{{"n":{index}}}"#),
                received_at: chrono::Utc::now(),
            })
            .expect("write");
    }
    assert_eq!(writer.lines(), 3);
    let text = std::fs::read_to_string(&path).expect("read");
    std::fs::remove_file(&path).ok();
    let lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("json line"))
        .collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[2]["frame"]["n"], 2);
}
