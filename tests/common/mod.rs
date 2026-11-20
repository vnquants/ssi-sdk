//! Shared helpers for offline integration tests (fixtures, wiremock auth, RSA test key, config).
#![allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::expect_used,
    reason = "each integration test binary uses a subset"
)]

use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use serde_json::Value;
use ssi_sdk::{Config, Credentials, SsiClient};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

pub(crate) const TEST_API_KEY: &str = "test-api-key-secret-001";
pub(crate) const TEST_API_SECRET: &str = "test-api-secret-value-002";
pub(crate) const TEST_ACCOUNT: &str = "003C123451";
pub(crate) const TEST_DERIV_ACCOUNT: &str = "003C123458";

/// A captured response: status, headers and body.
pub struct Fixture {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Value,
}

/// The whole fixture document.
pub fn fixture_document(name: &str) -> Value {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.json"));
    let text = std::fs::read_to_string(&file).expect("fixture exists");
    serde_json::from_str(&text).expect("fixture is JSON")
}

/// Every line of a JSON-lines fixture (`tests/fixtures/<name>.jsonl`), parsed.
pub fn fixture_lines(name: &str) -> Vec<Value> {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.jsonl"));
    let text = std::fs::read_to_string(&file).expect("fixture exists");
    text.lines()
        .map(|line| serde_json::from_str(line).expect("fixture line is JSON"))
        .collect()
}

pub fn fixture(name: &str) -> Fixture {
    let doc = fixture_document(name);
    let status = doc
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .unwrap_or(200);
    let headers = doc
        .get("headers")
        .and_then(Value::as_object)
        .map_or_default(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
                .collect()
        });
    let body = doc.get("body").cloned().unwrap_or(Value::Null);
    Fixture {
        status,
        headers,
        body,
    }
}

pub fn response(fixture: &Fixture) -> ResponseTemplate {
    let mut template = ResponseTemplate::new(fixture.status).set_body_json(&fixture.body);
    for (name, value) in &fixture.headers {
        template = template.insert_header(name.as_str(), value.as_str());
    }
    template
}

/// A token response that expires 15 minutes from now (the live lifetime).
pub fn token_body(access: &str) -> Value {
    let now = chrono::Utc::now().timestamp();
    serde_json::json!({
        "tokenType": "Bearer",
        "accessToken": access,
        "expiresAt": now + 900,
        "refreshToken": format!("refresh-for-{access}"),
        "refreshExpiresAt": now + 28_800,
    })
}

/// Mounts `POST /api/v3/auth/token` answering with `access` tokens.
pub async fn mount_auth(server: &MockServer, access: &str) {
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body(access)))
        .mount(server)
        .await;
}

/// Mounts `POST /api/v3/auth/token` answering with explicit access and refresh tokens.
pub async fn mount_token_login(server: &MockServer, access: &str, refresh: &str) {
    let now = chrono::Utc::now().timestamp();
    Mock::given(method("POST"))
        .and(path("/api/v3/auth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": access,
            "refreshToken": refresh,
            "tokenType": "Bearer",
            "expiresAt": now + 900,
            "refreshExpiresAt": now + 28_800,
        })))
        .mount(server)
        .await;
}

/// Default data-test config against a local mock HTTP server.
pub fn config(api_url: &str) -> Config {
    Config::new(Credentials::new(TEST_API_KEY, TEST_API_SECRET).expect("credentials"))
        .allow_insecure(true)
        .with_api_url(api_url)
        .expect("api url")
        .with_requests_per_second(NonZeroU32::new(1000))
        .with_rate_limit_retries(3, Duration::from_secs(5))
        .with_transient_retries(0)
}

/// Default data-test client against a local mock HTTP server.
pub fn client(api_url: &str) -> SsiClient {
    SsiClient::new(config(api_url)).expect("client")
}

#[cfg(feature = "trading")]
pub(crate) mod trading_helpers {
    use std::sync::OnceLock;

    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use rand::rngs::OsRng;
    use rsa::{
        RsaPrivateKey,
        pkcs1v15::{Signature, VerifyingKey},
        signature::Verifier as _,
        traits::{PrivateKeyParts as _, PublicKeyParts as _},
    };
    use sha2::Sha256;

    use super::*;

    pub(crate) struct TestKeyPair {
        pub(crate) xml_base64: String,
        pub(crate) verifying: VerifyingKey<Sha256>,
    }

    pub(crate) fn test_keypair() -> &'static TestKeyPair {
        static KEY: OnceLock<TestKeyPair> = OnceLock::new();
        KEY.get_or_init(|| {
            let key = RsaPrivateKey::new(&mut OsRng, 2048).expect("generate test RSA key");
            let primes = key.primes();
            let xml = format!(
                "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent><P>{}</P><Q>{}</Q><D>{}</D></RSAKeyValue>",
                STANDARD.encode(key.n().to_bytes_be()),
                STANDARD.encode(key.e().to_bytes_be()),
                STANDARD.encode(primes.first().expect("p").to_bytes_be()),
                STANDARD.encode(primes.get(1).expect("q").to_bytes_be()),
                STANDARD.encode(key.d().to_bytes_be()),
            );
            TestKeyPair {
                xml_base64: STANDARD.encode(xml.as_bytes()),
                verifying: VerifyingKey::<Sha256>::new(key.to_public_key()),
            }
        })
    }

    pub(crate) fn verify_hex_signature(body: &[u8], sig_hex: &str) -> bool {
        let Ok(bytes) = hex::decode(sig_hex) else {
            return false;
        };
        let Ok(sig) = Signature::try_from(bytes.as_slice()) else {
            return false;
        };
        test_keypair().verifying.verify(body, &sig).is_ok()
    }

    pub(crate) fn verify_base64_signature(body: &[u8], sig_b64: &str) -> bool {
        let Ok(bytes) = STANDARD.decode(sig_b64) else {
            return false;
        };
        let Ok(sig) = Signature::try_from(bytes.as_slice()) else {
            return false;
        };
        test_keypair().verifying.verify(body, &sig).is_ok()
    }

    pub(crate) fn credentials_with_key() -> Credentials {
        Credentials::new(TEST_API_KEY, TEST_API_SECRET)
            .expect("creds")
            .with_private_key(&test_keypair().xml_base64)
            .expect("pk")
            .with_client_id("CLIENT01")
    }

    pub(crate) fn trading_config(base_uri: &str) -> Config {
        Config::new(credentials_with_key())
            .allow_insecure(true)
            .with_api_url(base_uri)
            .expect("api url")
            .with_device_id("test-device-id")
            .with_requests_per_second(None)
    }

    pub(crate) async fn authenticated_client(server: &MockServer, access: &str) -> SsiClient {
        mount_token_login(server, access, "refresh-tok-1").await;
        let client = SsiClient::new(trading_config(&server.uri())).expect("client");
        client
            .auth()
            .authenticate_with_otp("123456")
            .await
            .expect("otp login");
        client
    }
}

#[cfg(feature = "trading")]
pub(crate) use trading_helpers::{
    authenticated_client, credentials_with_key, test_keypair, trading_config,
    verify_base64_signature, verify_hex_signature,
};
