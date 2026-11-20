//! Wire frames for the `TRADING` channel on `wss://stream.ssi.com.vn/ws/v3`.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

use super::topic::TradingTopic;
use crate::{
    de::raw_scalar,
    secret::mask_topics_in,
    stream::common::{
        SubscriptionAck, code, invalid_topics, retry_after, shape_of, split_topics, text,
    },
};

/// A classified inbound text frame on the `TRADING` channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Inbound {
    Trading {
        topic: String,
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
    Other {
        shape: String,
    },
}

#[derive(Deserialize)]
struct TradingEnvelope<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a RawValue>,
    #[serde(borrow, default)]
    topic: Option<&'a RawValue>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
}

fn trading_frame(frame: &str) -> Option<Inbound> {
    let envelope: TradingEnvelope<'_> = serde_json::from_str(frame).ok()?;
    let channel = raw_scalar(envelope.channel?.get()).ok().flatten()?;
    if !channel.eq_ignore_ascii_case("TRADING") {
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
    Some(Inbound::Trading { topic, payloads })
}

/// Classifies one inbound text frame. Never fails.
pub(crate) fn classify(frame: &str) -> Inbound {
    if let Some(trading) = trading_frame(frame) {
        return trading;
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

/// `SUBSCRIBE` / `UNSUBSCRIBE` frames for the `TRADING` channel, split under `max_bytes`.
pub(crate) fn request_frames(
    method: &str,
    topics: &[TradingTopic],
    max_bytes: usize,
) -> Vec<String> {
    let encode = |chunk: &[&str]| {
        json!({ "method": method, "channel": "TRADING", "topics": chunk }).to_string()
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

    #[test]
    fn classifies_trading_frames_and_masks_accounts() {
        let frame = r#"{"channel":"TRADING","topic":"order.003C123451","data":{"eventType":"orderEvent","orderId":"O1"}}"#;
        let Inbound::Trading { topic, payloads } = classify(frame) else {
            panic!("trading expected");
        };
        assert_eq!(topic, "order.003C123451");
        assert_eq!(payloads.len(), 1);

        let Inbound::Ack(ack) = classify(
            r#"{"method":"SUBSCRIBE","channel":"TRADING","status":"error","message":"Denied: order.003C123451."}"#,
        ) else {
            panic!("ack expected");
        };
        assert!(!ack.message.contains("123451"));

        let Inbound::Rejection {
            code, retry_after, ..
        } = classify(r#"{"code":429000,"msg":"Too many sockets","retryAfter":15}"#)
        else {
            panic!("rejection expected");
        };
        assert_eq!(code, 429_000);
        assert_eq!(retry_after, Some(Duration::from_secs(15)));
    }
}
