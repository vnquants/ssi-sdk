//! Wire protocol for SSI `FastConnect` V3 Trading WebSocket (`/ws/v3/trading`).

use std::fmt::Write as _;

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json, value::RawValue};

use crate::{
    crypto::RequestSigner,
    de,
    error::{ApiErrorCode, Error, Result, TradingWsError},
    secret::mask_topics_in,
};

/// Active wire encoding used for one signed command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireEncoding {
    Base64,
    Hex,
}

impl WireEncoding {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Base64 => "base64",
            Self::Hex => "hex",
        }
    }

    pub(crate) const fn other(self) -> Self {
        match self {
            Self::Base64 => Self::Hex,
            Self::Hex => Self::Base64,
        }
    }
}

/// Reports whether a Trading WebSocket RPC method mutates state and therefore requires an RSA
/// signature over the exact serialized `params` text.
pub fn is_mutating_method(method: &str) -> bool {
    matches!(
        method.to_ascii_lowercase().as_str(),
        "order.place"
            | "order.amend"
            | "order.cancel"
            | "order.batchnew"
            | "order.batchcancel"
            | "fco.place"
            | "fco.cancel"
    )
}

/// Reports whether an error code from the Trading WebSocket indicates a pre-execution signature
/// encoding rejection (`401006`, `401008`, `401013`).
pub(crate) fn is_signature_rejection_code(code: &str) -> bool {
    matches!(code.trim(), "401006" | "401008" | "401013")
}

/// Quota reported inside a Trading WebSocket response frame (`"rateLimit"`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct WsRateLimit {
    /// Window request cap (50 for orders, 100 for queries).
    #[serde(default, deserialize_with = "de::opt_quantity")]
    pub limit: Option<u64>,
    /// Remaining commands in the current window.
    #[serde(default, deserialize_with = "de::opt_quantity")]
    pub remaining: Option<u64>,
    /// Milliseconds until the window resets (`resetMs`).
    #[serde(default, rename = "resetMs", deserialize_with = "de::opt_quantity")]
    pub reset_ms: Option<u64>,
}

/// A decoded success response frame from `/ws/v3/trading`.
///
/// `result_raw` retains the exact JSON bytes of `"result"` so decimals decode via `RawValue`
/// without `f64` rounding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsResponse {
    /// Command correlation id.
    pub id: String,
    /// Echoed RPC method name.
    pub method: String,
    /// Numeric response status (normally `200`).
    pub status: u16,
    /// Exact JSON text of `"result"` (defaults to `"{}"` when omitted).
    pub result_raw: String,
    /// Latest quota reported by the server, if present.
    pub rate_limit: Option<WsRateLimit>,
    /// Optional server receipt timestamp (`receivedTime`).
    pub received_time: Option<String>,
}

impl WsResponse {
    /// Decodes `"result"` into a typed struct without leaking raw response bytes on error.
    pub fn decode_result<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(&self.result_raw).map_err(|_| Error::Decode {
            endpoint: format!("tradingws:{}", self.method),
            detail: "Trading WebSocket result did not match expected schema".to_owned(),
        })
    }

    /// Parses `"result"` into a dynamic `serde_json::Value`.
    pub fn result_value(&self) -> Result<Value> {
        serde_json::from_str(&self.result_raw).map_err(|_| Error::Decode {
            endpoint: format!("tradingws:{}", self.method),
            detail: "Trading WebSocket result is not valid JSON".to_owned(),
        })
    }
}

/// Builds one Trading WebSocket command frame.
///
/// `params_json` is embedded verbatim without re-serialization; for mutating methods those exact
/// bytes are signed with `signer` (failing closed if no signer is configured).
pub(crate) fn build_command_frame(
    method: &str,
    params_json: &str,
    signer: Option<&RequestSigner>,
    request_id: &str,
    timestamp_ms: i64,
    encoding: WireEncoding,
) -> Result<String> {
    if request_id.is_empty() {
        return Err(Error::InvalidRequest(
            "Trading WebSocket command requires a non-empty id".to_owned(),
        ));
    }
    if serde_json::from_str::<&RawValue>(params_json).is_err() {
        return Err(Error::InvalidRequest(
            "Trading WebSocket params must be valid JSON".to_owned(),
        ));
    }
    let id_json = serde_json::to_string(request_id)
        .map_err(|_| Error::InvalidRequest("invalid command id".to_owned()))?;
    let method_json = serde_json::to_string(method)
        .map_err(|_| Error::InvalidRequest("invalid method name".to_owned()))?;

    let mut frame = format!(r#"{{"id":{id_json},"method":{method_json},"params":{params_json}"#);
    if is_mutating_method(method) {
        let Some(signer) = signer else {
            return Err(Error::InvalidPrivateKey(
                "private key is required for state-changing Trading WebSocket commands".to_owned(),
            ));
        };
        let signature = match encoding {
            WireEncoding::Base64 => signer.sign_base64(params_json.as_bytes())?,
            WireEncoding::Hex => signer.sign_hex(params_json.as_bytes())?,
        };
        let sig_json = serde_json::to_string(&signature)
            .map_err(|_| Error::Authentication("could not encode signature".to_owned()))?;
        frame.push_str(r#","signature":"#);
        frame.push_str(&sig_json);
    }
    frame.push_str(r#","timestamp":"#);
    frame.push_str(&timestamp_ms.to_string());
    frame.push('}');
    Ok(frame)
}

/// Reports whether a decoded JSON object is a post-upgrade refusal frame
/// (`{"code": 401|403|429|500, "msg": ...}` without `"id"`).
pub(crate) fn is_rejection(object: &Map<String, Value>) -> bool {
    !object.contains_key("id")
        && object.contains_key("code")
        && (object.contains_key("msg") || object.contains_key("message"))
}

/// Converts a refusal frame into a [`TradingWsError`].
pub(crate) fn parse_rejection(object: &Map<String, Value>) -> TradingWsError {
    let code_str = value_as_string(object.get("code"));
    let numeric = code_str.trim().parse::<i64>().ok();
    let status = numeric.and_then(|n| u16::try_from(n).ok()).unwrap_or(0);
    let message = mask_topics_in(
        object
            .get("msg")
            .or_else(|| object.get("message"))
            .and_then(Value::as_str)
            .unwrap_or_default(),
    );
    TradingWsError {
        status,
        code: code_str,
        api_code: numeric.map(ApiErrorCode::from_code),
        message,
    }
}

#[derive(Deserialize)]
struct CommandFrameEnvelope<'a> {
    #[serde(borrow, default)]
    id: Option<&'a RawValue>,
    #[serde(borrow, default)]
    method: Option<&'a RawValue>,
    #[serde(borrow, default)]
    status: Option<&'a RawValue>,
    #[serde(borrow, default)]
    result: Option<&'a RawValue>,
    #[serde(borrow, default)]
    error: Option<&'a RawValue>,
    #[serde(default, rename = "rateLimit")]
    rate_limit: Option<WsRateLimit>,
    #[serde(borrow, default, rename = "receivedTime")]
    received_time: Option<&'a RawValue>,
}

/// Unwraps one command response frame (matching a known `"id"`), returning `Ok(WsResponse)` or
/// `Err(TradingWsError)`.
pub(crate) fn unwrap_response_frame(
    raw_frame: &str,
    fixed_encoding_hint: Option<WireEncoding>,
) -> std::result::Result<WsResponse, TradingWsError> {
    let Ok(env) = serde_json::from_str::<CommandFrameEnvelope<'_>>(raw_frame) else {
        return Err(TradingWsError {
            status: 0,
            code: "DECODE_ERROR".to_owned(),
            api_code: None,
            message: "Trading WebSocket response frame is not valid JSON".to_owned(),
        });
    };

    let status_text = env
        .status
        .and_then(|raw| de::raw_scalar(raw.get()).ok().flatten());
    let status: u16 = status_text
        .as_deref()
        .map_or(200, |text| text.parse::<u16>().unwrap_or(0));

    let has_error = env.error.is_some_and(|raw| {
        let trimmed = raw.get().trim();
        !matches!(trimmed, "null" | "false" | "\"\"" | "{}")
    });

    if has_error || status >= 400 || (status == 0 && status_text.is_some()) {
        let mut code = String::new();
        let mut message = String::new();
        if let Some(raw_err) = env.error
            && let Ok(val) = serde_json::from_str::<Value>(raw_err.get())
        {
            if let Some(obj) = val.as_object() {
                code = value_as_string(obj.get("code"));
                message = mask_topics_in(
                    obj.get("message")
                        .or_else(|| obj.get("msg"))
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                );
            } else if let Some(text) = val.as_str() {
                message = mask_topics_in(text);
            }
        }
        if let Some(used) = fixed_encoding_hint
            && is_signature_rejection_code(&code)
        {
            let _ = write!(
                message,
                " (signed as {}; try SignatureEncoding::{} if the gateway expects it)",
                used.as_str(),
                used.other().as_str()
            );
        }
        let api_code = code.trim().parse::<i64>().ok().map(ApiErrorCode::from_code);
        return Err(TradingWsError {
            status,
            code,
            api_code,
            message,
        });
    }

    let id = env
        .id
        .and_then(|raw| de::raw_scalar(raw.get()).ok().flatten())
        .unwrap_or_default();
    let method = env
        .method
        .and_then(|raw| de::raw_scalar(raw.get()).ok().flatten())
        .unwrap_or_default();
    let result_raw = env
        .result
        .map_or_else(|| "{}".to_owned(), |raw| raw.get().to_owned());
    let received_time = env
        .received_time
        .and_then(|raw| de::raw_scalar(raw.get()).ok().flatten());

    Ok(WsResponse {
        id,
        method,
        status,
        result_raw,
        rate_limit: env.rate_limit,
        received_time,
    })
}

pub(crate) fn subscribe_frame(topics: &[&str]) -> String {
    json!({
        "method": "SUBSCRIBE",
        "channel": "TRADING",
        "topics": topics,
    })
    .to_string()
}

pub(crate) fn unsubscribe_frame(topics: Option<&[&str]>) -> String {
    topics.map_or_else(
        || {
            json!({
                "method": "UNSUBSCRIBE",
                "channel": "TRADING",
            })
            .to_string()
        },
        |list| {
            json!({
                "method": "UNSUBSCRIBE",
                "channel": "TRADING",
                "topics": list,
            })
            .to_string()
        },
    )
}

pub(crate) fn ping_frame() -> String {
    json!({ "method": "PING_PONG" }).to_string()
}

fn value_as_string(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(num)) => num.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::OsRng;
    use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey as _};

    use super::*;

    #[test]
    fn command_embeds_params_verbatim_and_signs_exact_bytes() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).expect("key");
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).expect("pem");
        let signer = RequestSigner::parse(&pem).expect("signer");

        let params = r#"{"b":1,"a":"26000"}"#;
        let frame = build_command_frame(
            "order.place",
            params,
            Some(&signer),
            "cmd-1",
            1_780_000_000_000,
            WireEncoding::Base64,
        )
        .expect("frame");
        assert!(frame.contains(r#""params":{"b":1,"a":"26000"}"#));
        assert!(frame.contains(r#""signature":""#));

        // Mutating without signer fails closed.
        assert!(
            build_command_frame(
                "order.place",
                params,
                None,
                "cmd-2",
                1_780_000_000_000,
                WireEncoding::Base64,
            )
            .is_err()
        );

        // Queries do not require a signer and omit signature.
        let query = build_command_frame(
            "query.accountBalance",
            r#"{"accountNo":"003C123451"}"#,
            None,
            "cmd-3",
            1_780_000_000_000,
            WireEncoding::Base64,
        )
        .expect("query");
        assert!(!query.contains("signature"));
    }

    #[test]
    fn unwraps_success_and_error_frames() {
        let ok = unwrap_response_frame(
            r#"{"id":"1","method":"order.place","status":200,"result":{"orderId":"O1","orderStatus":"PD"},"rateLimit":{"limit":50,"remaining":49,"resetMs":1000}}"#,
            None,
        )
        .expect("ok");
        assert_eq!(ok.status, 200);
        assert_eq!(ok.rate_limit.expect("rl").remaining, Some(49));

        let err = unwrap_response_frame(
            r#"{"id":"2","method":"fco.place","status":"error","error":{"code":"FCO_ERROR","message":"invalid window"}}"#,
            None,
        )
        .expect_err("err");
        assert_eq!(err.code, "FCO_ERROR");
        assert_eq!(err.status, 0);
    }
}
