//! JSON-lines capture of the raw market feed (`examples/stream --dump`).
//!
//! One line per received text frame, `{"received_at", "kind": "frame", "frame"}`, plus
//! heartbeat and connection events. Lines are flushed one by one so a killed run keeps what it
//! read. Account-like fields and trading topics are redacted before writing.

use std::{
    io::{LineWriter, Write as _},
    path::Path,
};

use serde_json::{Value, json};
use ssi_sdk::stream::MarketStreamEvent;

/// Replaces account-like values and trading topics in a parsed frame.
pub(crate) fn redact(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map.iter_mut() {
                let lower = key.to_ascii_lowercase();
                if lower.contains("account") || lower == "clientid" {
                    *inner = Value::String("REDACTED".to_owned());
                } else {
                    redact(inner);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact),
        Value::String(text) => {
            let lower = text.to_ascii_lowercase();
            if lower.contains("order.") || lower.contains("portfolio.") {
                "REDACTED-TRADING-TOPIC".clone_into(text);
            }
        }
        _ => {}
    }
}

/// The JSON line for one stream event, or `None` for events the dump skips (typed data, acks
/// and subscription lists are already in the raw frames).
pub(crate) fn line_for(event: &MarketStreamEvent) -> Option<Value> {
    match event {
        MarketStreamEvent::RawFrame { text, received_at } => {
            let mut frame =
                serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone()));
            redact(&mut frame);
            Some(json!({"received_at": received_at, "kind": "frame", "frame": frame}))
        }
        MarketStreamEvent::Heartbeat { kind, at } => {
            Some(json!({"received_at": at, "kind": "heartbeat", "heartbeat": format!("{kind:?}")}))
        }
        MarketStreamEvent::Data(_)
        | MarketStreamEvent::Ack(_)
        | MarketStreamEvent::Subscriptions { .. } => None,
        other => Some(json!({
            "received_at": chrono::Utc::now(),
            "kind": "event",
            "event": format!("{other:?}"),
        })),
    }
}

/// Appends JSON lines to a file.
pub(crate) struct Dump {
    file: LineWriter<std::fs::File>,
    lines: u64,
}

impl Dump {
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: LineWriter::new(file),
            lines: 0,
        })
    }

    pub(crate) fn write(&mut self, event: &MarketStreamEvent) -> std::io::Result<()> {
        let Some(line) = line_for(event) else {
            return Ok(());
        };
        self.lines += 1;
        writeln!(self.file, "{line}")
    }

    pub(crate) const fn lines(&self) -> u64 {
        self.lines
    }
}
