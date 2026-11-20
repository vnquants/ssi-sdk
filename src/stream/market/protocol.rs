//! Wire frames for the `DATA` channel on `wss://stream.ssi.com.vn/ws/v3`.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

use super::topic::Topic;
use crate::{
    de::raw_scalar,
    secret::mask_topics_in,
    stream::common::{
        SubscriptionAck, code, invalid_topics, retry_after, shape_of, split_topics, text,
    },
};

/// A classified inbound text frame on the `DATA` channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Inbound {
    Data {
        topic: String,
        /// Each `data` payload as JSON text (an array payload is split).
        payloads: Vec<String>,
    },
    Ack(SubscriptionAck),
    PingAck,
    ServerPing {
        time: Option<String>,
    },
    Rejection {
        code: i64,
        message: String,
        retry_after: Option<Duration>,
    },
    Subscriptions {
        data: Vec<String>,
        trading: Vec<String>,
    },
    /// Anything else, described by its shape only (top-level keys, never values).
    Other {
        shape: String,
    },
}

/// A DATA frame read without materialising its payload, so numbers keep their text.
#[derive(Deserialize)]
struct DataEnvelope<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a RawValue>,
    #[serde(borrow, default)]
    topic: Option<&'a RawValue>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
}

fn data_frame(frame: &str) -> Option<Inbound> {
    let envelope: DataEnvelope<'_> = serde_json::from_str(frame).ok()?;
    let channel = raw_scalar(envelope.channel?.get()).ok().flatten()?;
    if !channel.eq_ignore_ascii_case("DATA") {
        return None;
    }
    let topic = raw_scalar(envelope.topic?.get()).ok().flatten()?;
    let payloads = match envelope.data {
        None => Vec::new(),
        Some(data) if data.get().trim_start().starts_with('[') => {
            serde_json::from_str::<Vec<&RawValue>>(data.get())
                .ok()?
                .into_iter()
                .map(|item| item.get().to_owned())
                .collect()
        }
        Some(data) => vec![data.get().to_owned()],
    };
    Some(Inbound::Data { topic, payloads })
}

/// Classifies one text frame. Never fails: anything unexpected is `Other`.
pub(crate) fn classify(frame: &str) -> Inbound {
    if let Some(data) = data_frame(frame) {
        return data;
    }
    let Ok(value) = serde_json::from_str::<Value>(frame) else {
        return Inbound::Other {
            shape: "not JSON".to_owned(),
        };
    };
    let Value::Object(object) = &value else {
        return Inbound::Other {
            shape: shape_of(&value),
        };
    };
    let channel = text(object, "channel").map(str::to_ascii_uppercase);
    let method = text(object, "method").unwrap_or_default();
    match channel.as_deref() {
        Some("HEARTBEAT") if method.eq_ignore_ascii_case("ping") => Inbound::ServerPing {
            time: text(object, "time").map(ToOwned::to_owned),
        },
        Some("HEARTBEAT") => Inbound::PingAck,
        Some(channel) if object.contains_key("status") && !method.is_empty() => {
            let message = mask_topics_in(text(object, "message").unwrap_or_default());
            Inbound::Ack(SubscriptionAck {
                method: method.to_owned(),
                channel: channel.to_owned(),
                status: text(object, "status").unwrap_or_default().to_owned(),
                invalid_topics: invalid_topics(&message),
                message,
            })
        }
        Some(_) => Inbound::Other {
            shape: shape_of(&value),
        },
        None => {
            if let Some(code) = code(object)
                && (object.contains_key("msg") || object.contains_key("message"))
            {
                let message = mask_topics_in(
                    text(object, "msg")
                        .or_else(|| text(object, "message"))
                        .unwrap_or_default(),
                );
                return Inbound::Rejection {
                    code,
                    message,
                    retry_after: retry_after(object),
                };
            }
            let subscription_list = !object.is_empty()
                && object.iter().all(|(key, value)| {
                    matches!(key.as_str(), "trading" | "data") && value.is_string()
                });
            if subscription_list {
                return Inbound::Subscriptions {
                    data: split_topics(text(object, "data").unwrap_or_default()),
                    trading: split_topics(text(object, "trading").unwrap_or_default()),
                };
            }
            Inbound::Other {
                shape: shape_of(&value),
            }
        }
    }
}

/// `SUBSCRIBE` / `UNSUBSCRIBE` frames for the DATA channel, split so each stays under
/// `max_bytes`.
pub(crate) fn request_frames(method: &str, topics: &[Topic], max_bytes: usize) -> Vec<String> {
    let encode = |chunk: &[&str]| {
        json!({ "method": method, "channel": "DATA", "topics": chunk }).to_string()
    };
    let mut frames = Vec::new();
    let mut chunk: Vec<&str> = Vec::new();
    for topic in topics {
        chunk.push(topic.as_str());
        if chunk.len() > 1 && encode(&chunk).len() > max_bytes {
            let last = chunk.pop();
            frames.push(encode(&chunk));
            chunk = last.into_iter().collect();
        }
    }
    if !chunk.is_empty() {
        frames.push(encode(&chunk));
    }
    frames
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        secret::mask_topic,
        stream::common::{DEFAULT_MAX_FRAME_BYTES, is_fatal_rejection},
    };

    #[test]
    fn classifies_live_frames() {
        let ack = classify(
            r#"{"method":"subscribe","channel":"DATA","status":"ok","message":"Subscribed to 9 topic(s). Invalid: bogus.VNM"}"#,
        );
        let Inbound::Ack(ack) = ack else {
            panic!("ack expected")
        };
        assert_eq!(ack.invalid_topics, vec!["bogus.VNM".to_owned()]);
        assert!(!ack.is_clean());

        assert_eq!(
            classify(
                r#"{"method":"ping_pong","channel":"HEARTBEAT","status":"ok","message":"pong"}"#
            ),
            Inbound::PingAck
        );
        assert_eq!(
            classify(r#"{"trading":"","data":"trade.VNM;quote.VNM"}"#),
            Inbound::Subscriptions {
                data: vec!["trade.VNM".to_owned(), "quote.VNM".to_owned()],
                trading: Vec::new()
            }
        );
        assert_eq!(
            classify(r#"{"code":401,"msg":"Invalid or expired token"}"#),
            Inbound::Rejection {
                code: 401,
                message: "Invalid or expired token".to_owned(),
                retry_after: None,
            }
        );
        assert_eq!(
            classify(r#"{"code":429000,"msg":"Too many sockets","retryAfter":12}"#),
            Inbound::Rejection {
                code: 429_000,
                message: "Too many sockets".to_owned(),
                retry_after: Some(Duration::from_secs(12)),
            }
        );
        assert_eq!(
            classify(r#"{"method":"PING","channel":"HEARTBEAT","time":"20-11-2026 09-00-00"}"#),
            Inbound::ServerPing {
                time: Some("20-11-2026 09-00-00".to_owned())
            }
        );
        let data = classify(r#"{"channel":"DATA","topic":"trade.VNM","data":{"s":"VNM"}}"#);
        assert!(
            matches!(data, Inbound::Data { ref topic, ref payloads } if topic == "trade.VNM" && payloads.len() == 1)
        );
        assert!(matches!(classify("not json"), Inbound::Other { .. }));
        assert_eq!(
            classify(r#"{"type":"snapshot","data":{"s":"VNM"}}"#),
            Inbound::Other {
                shape: "object{data,type}".to_owned()
            }
        );
        assert_eq!(
            classify(r#"{"data":"trade.VNM","extra":1}"#),
            Inbound::Other {
                shape: "object{data,extra}".to_owned()
            }
        );
        assert_eq!(
            classify("[1,2]"),
            Inbound::Other {
                shape: "array".to_owned()
            }
        );
    }

    #[test]
    fn data_payloads_keep_their_text_and_arrays_split() {
        let Inbound::Data { topic, payloads } =
            classify(r#"{"channel":"DATA","topic":"trade.VNM","data":{"p":85907.33212345678912}}"#)
        else {
            panic!("data expected")
        };
        assert_eq!(topic, "trade.VNM");
        assert_eq!(payloads, vec![r#"{"p":85907.33212345678912}"#.to_owned()]);
        let Inbound::Data { payloads, .. } =
            classify(r#"{"channel":"data","topic":"quote.X","data":[{"a":1},{"a":2}]}"#)
        else {
            panic!("data expected")
        };
        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn trading_topics_are_masked() {
        assert_eq!(mask_topic("portfolio.003C123456"), "portfolio.*******456");
        assert_eq!(mask_topic("order.003C123456"), "order.*******456");
        assert_eq!(mask_topic("order.*"), "order.*");
        assert_eq!(mask_topic("trade.VNM"), "trade.VNM");
        let Inbound::Subscriptions { trading, .. } =
            classify(r#"{"trading":"order.003C123456;portfolio.003C123456","data":""}"#)
        else {
            panic!("list expected")
        };
        assert!(trading.iter().all(|topic| !topic.contains("123456")));
        let Inbound::Ack(ack) = classify(
            r#"{"method":"subscribe","channel":"TRADING","status":"error","message":"Denied (account): order.003C123456."}"#,
        ) else {
            panic!("ack expected")
        };
        assert!(!ack.message.contains("123456"), "{}", ack.message);
        assert!(is_fatal_rejection(401) && is_fatal_rejection(503) && !is_fatal_rejection(400));
        assert!(
            is_fatal_rejection(401_004)
                && is_fatal_rejection(403_001)
                && is_fatal_rejection(429_000)
                && is_fatal_rejection(500_000)
                && !is_fatal_rejection(400_001)
        );
    }

    #[test]
    fn splits_large_requests() {
        let topics: Vec<Topic> = (0..600)
            .map(|index| Topic::trade(&format!("S{index:04}")).expect("valid"))
            .collect();
        let frames = request_frames("SUBSCRIBE", &topics, DEFAULT_MAX_FRAME_BYTES);
        assert!(frames.len() > 1);
        assert!(
            frames
                .iter()
                .all(|frame| frame.len() <= DEFAULT_MAX_FRAME_BYTES)
        );
        let total: usize = frames
            .iter()
            .map(|frame| {
                serde_json::from_str::<Value>(frame)
                    .expect("json")
                    .get("topics")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len)
            })
            .sum();
        assert_eq!(total, 600);
    }
}
