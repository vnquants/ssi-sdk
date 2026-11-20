//! RSA PKCS#1 v1.5 SHA-256 request signing.
//!
//! SSI `FastConnect` V3 requires mutating trading commands (order place/modify/cancel, batch, FCO)
//! to be signed over the exact JSON payload bytes:
//!
//! - **REST `X-Signature` header**: lowercase hex encoding;
//! - **Trading WebSocket `signature` field**: standard base64 encoding (with `auto` fallback to
//!   hex if the gateway rejects the encoding before execution).
//!
//! Private key formats accepted:
//!
//! 1. Base64-encoded `<RSAKeyValue>` XML (the standard SSI `FastConnect` format), with either full
//!    CRT components (`Modulus`, `Exponent`, `D`, `P`, `Q`) or minimal components (`Modulus`,
//!    `D`, optional `Exponent`).
//! 2. Raw `<RSAKeyValue>` XML.
//! 3. PEM PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`) and PEM PKCS#8
//!    (`-----BEGIN PRIVATE KEY-----`).
//!
//! Key material is never echoed in errors or `Debug` output, and temporary decoded XML buffers
//! are zeroized on drop.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use quick_xml::de::from_str;
use rsa::{
    BigUint, RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pkcs1v15::SigningKey,
    pkcs8::DecodePrivateKey as _,
    signature::{SignatureEncoding as _, Signer as _},
    traits::PublicKeyParts as _,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use crate::error::{Error, Result};

const MAX_ENCODED_KEY_BYTES: usize = 128 * 1024;
const MINIMUM_RSA_BITS: usize = 512;

/// ASN.1 DER `DigestInfo` prefix for SHA-256 (RFC 8017 §9.2).
const SHA256_DER_PREFIX: [u8; 19] = [
    0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01, 0x05,
    0x00, 0x04, 0x20,
];

/// Parsed RSA private key ready to sign request payloads.
pub(crate) struct RequestSigner {
    backend: SignerBackend,
}

enum SignerBackend {
    /// Full RSA private key (PEM or XML with P/Q primes), signed via `rsa::pkcs1v15::SigningKey`.
    Full(Box<SigningKey<Sha256>>),
    /// Minimal XML key (`Modulus`, `D`, optional `Exponent`) without `P`/`Q`.
    Minimal {
        n: BigUint,
        e: Option<BigUint>,
        d: BigUint,
        key_len: usize,
    },
}

impl fmt::Debug for RequestSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSigner").finish_non_exhaustive()
    }
}

impl RequestSigner {
    /// Parses a private key from base64-XML, raw XML, PEM PKCS#1, or PEM PKCS#8.
    pub(crate) fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidPrivateKey("private key is empty".to_owned()));
        }
        if trimmed.len() > MAX_ENCODED_KEY_BYTES {
            return Err(Error::InvalidPrivateKey(
                "private key exceeds maximum size".to_owned(),
            ));
        }

        if trimmed.starts_with("-----BEGIN ") {
            return Self::from_pem(trimmed);
        }
        if trimmed.starts_with('<') {
            return Self::from_xml(trimmed);
        }
        Self::from_base64_xml(trimmed)
    }

    fn from_pem(pem: &str) -> Result<Self> {
        let mut private_key = RsaPrivateKey::from_pkcs1_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(pem))
            .map_err(|_| {
                Error::InvalidPrivateKey("PEM private key could not be decoded".to_owned())
            })?;
        validate_full_key(&mut private_key)?;
        Ok(Self {
            backend: SignerBackend::Full(Box::new(SigningKey::new(private_key))),
        })
    }

    fn from_base64_xml(encoded: &str) -> Result<Self> {
        let compact: Zeroizing<String> = Zeroizing::new(
            encoded
                .chars()
                .filter(|ch| !ch.is_ascii_whitespace())
                .collect(),
        );
        let decoded = Zeroizing::new(STANDARD.decode(compact.as_bytes()).map_err(|_| {
            Error::InvalidPrivateKey("private key is not valid base64 XML or PEM".to_owned())
        })?);
        let xml = Zeroizing::new(String::from_utf8(decoded.to_vec()).map_err(|_| {
            Error::InvalidPrivateKey("decoded private key XML is not valid UTF-8".to_owned())
        })?);
        Self::from_xml(&xml)
    }

    fn from_xml(xml: &str) -> Result<Self> {
        let components: RsaKeyValue = from_str(xml)
            .map_err(|_| Error::InvalidPrivateKey("private key XML is malformed".to_owned()))?;
        let modulus = decode_component(&components.modulus, "Modulus")?;
        let private_exp = decode_component(&components.d, "D")?;
        let public_exp = components
            .exponent
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| decode_component(value, "Exponent"))
            .transpose()?;

        if modulus.bits() < MINIMUM_RSA_BITS {
            return Err(Error::InvalidPrivateKey(
                "RSA modulus is too small".to_owned(),
            ));
        }

        if let (Some(e_val), Some(p_raw), Some(q_raw)) = (
            public_exp.clone(),
            components
                .p
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
            components
                .q
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
        ) {
            let prime_p = decode_component(p_raw, "P")?;
            let prime_q = decode_component(q_raw, "Q")?;
            let mut private_key =
                RsaPrivateKey::from_components(modulus, e_val, private_exp, vec![prime_p, prime_q])
                    .map_err(|_| {
                        Error::InvalidPrivateKey("RSA key components are inconsistent".to_owned())
                    })?;
            validate_full_key(&mut private_key)?;
            return Ok(Self {
                backend: SignerBackend::Full(Box::new(SigningKey::new(private_key))),
            });
        }

        let key_len = modulus.bits().div_ceil(8);
        if key_len < SHA256_DER_PREFIX.len() + 32 + 11 {
            return Err(Error::InvalidPrivateKey(
                "RSA modulus is too small for SHA-256 PKCS#1 v1.5".to_owned(),
            ));
        }
        Ok(Self {
            backend: SignerBackend::Minimal {
                n: modulus,
                e: public_exp,
                d: private_exp,
                key_len,
            },
        })
    }

    /// Signs `body` with PKCS#1 v1.5 SHA-256 and returns lowercase hex (for REST `X-Signature`).
    pub(crate) fn sign_hex(&self, body: &[u8]) -> Result<String> {
        Ok(hex::encode(self.sign_bytes(body)?))
    }

    /// Signs `body` with PKCS#1 v1.5 SHA-256 and returns standard base64 (for Trading WebSocket).
    pub(crate) fn sign_base64(&self, body: &[u8]) -> Result<String> {
        Ok(STANDARD.encode(self.sign_bytes(body)?))
    }

    fn sign_bytes(&self, body: &[u8]) -> Result<Vec<u8>> {
        match &self.backend {
            SignerBackend::Full(signing_key) => {
                let signature = signing_key
                    .try_sign(body)
                    .map_err(|_| Error::Authentication("RSA request signing failed".to_owned()))?;
                Ok(signature.to_bytes().to_vec())
            }
            SignerBackend::Minimal { n, e, d, key_len } => {
                let digest = Sha256::digest(body);
                let digest_len = SHA256_DER_PREFIX.len() + digest.len();
                let Some(pad_len) = key_len.checked_sub(digest_len + 3) else {
                    return Err(Error::InvalidPrivateKey(
                        "RSA modulus is too small".to_owned(),
                    ));
                };
                if pad_len < 8 {
                    return Err(Error::InvalidPrivateKey(
                        "RSA modulus is too small".to_owned(),
                    ));
                }
                let mut padded = Vec::with_capacity(*key_len);
                padded.push(0x00);
                padded.push(0x01);
                padded.resize(2 + pad_len, 0xff);
                padded.push(0x00);
                padded.extend_from_slice(&SHA256_DER_PREFIX);
                padded.extend_from_slice(&digest);

                let m = BigUint::from_bytes_be(&padded);
                let s = m.modpow(d, n);
                if let Some(exp) = e
                    && s.modpow(exp, n) != m
                {
                    return Err(Error::Authentication(
                        "RSA signature failed internal consistency check".to_owned(),
                    ));
                }
                let raw = s.to_bytes_be();
                let mut out = vec![0_u8; *key_len];
                let Some(offset) = key_len.checked_sub(raw.len()) else {
                    return Err(Error::Authentication(
                        "RSA signature exceeded modulus length".to_owned(),
                    ));
                };
                if let Some(target) = out.get_mut(offset..) {
                    target.copy_from_slice(&raw);
                }
                Ok(out)
            }
        }
    }
}

fn validate_full_key(key: &mut RsaPrivateKey) -> Result<()> {
    key.validate()
        .map_err(|_| Error::InvalidPrivateKey("RSA key validation failed".to_owned()))?;
    if key.n().bits() < MINIMUM_RSA_BITS {
        return Err(Error::InvalidPrivateKey(
            "RSA key modulus is too small".to_owned(),
        ));
    }
    key.precompute()
        .map_err(|_| Error::InvalidPrivateKey("RSA key precomputation failed".to_owned()))?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename = "RSAKeyValue")]
struct RsaKeyValue {
    #[serde(rename = "Modulus")]
    modulus: String,
    #[serde(rename = "Exponent", default)]
    exponent: Option<String>,
    #[serde(rename = "P", default)]
    p: Option<String>,
    #[serde(rename = "Q", default)]
    q: Option<String>,
    #[serde(rename = "D")]
    d: String,
}

fn decode_component(value: &str, name: &str) -> Result<BigUint> {
    let bytes = STANDARD.decode(value.trim()).map_err(|_| {
        Error::InvalidPrivateKey(format!("RSA component {name} is not valid base64"))
    })?;
    if bytes.is_empty() {
        return Err(Error::InvalidPrivateKey(format!(
            "RSA component {name} is empty"
        )));
    }
    Ok(BigUint::from_bytes_be(&bytes))
}

#[cfg(test)]
mod tests {
    use rand::rngs::OsRng;
    use rsa::{
        pkcs1::EncodeRsaPrivateKey as _,
        pkcs1v15::{Signature, VerifyingKey},
        pkcs8::EncodePrivateKey as _,
        signature::Verifier as _,
        traits::PrivateKeyParts as _,
    };

    use super::*;

    fn generate_key() -> RsaPrivateKey {
        RsaPrivateKey::new(&mut OsRng, 2048).expect("generate 2048-bit key")
    }

    fn xml_full(key: &RsaPrivateKey) -> String {
        let primes = key.primes();
        let xml = format!(
            "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent><P>{}</P><Q>{}</Q><D>{}</D></RSAKeyValue>",
            STANDARD.encode(key.n().to_bytes_be()),
            STANDARD.encode(key.e().to_bytes_be()),
            STANDARD.encode(primes.first().expect("p").to_bytes_be()),
            STANDARD.encode(primes.get(1).expect("q").to_bytes_be()),
            STANDARD.encode(key.d().to_bytes_be()),
        );
        STANDARD.encode(xml.as_bytes())
    }

    fn xml_minimal(key: &RsaPrivateKey, with_exponent: bool) -> String {
        use std::fmt::Write as _;
        let mut xml = format!(
            "<RSAKeyValue><Modulus>{}</Modulus>",
            STANDARD.encode(key.n().to_bytes_be())
        );
        if with_exponent {
            let _ = write!(
                xml,
                "<Exponent>{}</Exponent>",
                STANDARD.encode(key.e().to_bytes_be())
            );
        }
        let _ = write!(
            xml,
            "<D>{}</D></RSAKeyValue>",
            STANDARD.encode(key.d().to_bytes_be())
        );
        STANDARD.encode(xml.as_bytes())
    }

    #[test]
    fn all_key_formats_produce_identical_verifiable_signatures() {
        let key = generate_key();
        let verifying = VerifyingKey::<Sha256>::new(key.to_public_key());
        let payload = br#"{"accountNo":"003C000001","symbol":"VNM","side":"B","quantity":100}"#;

        let pkcs1_pem = key
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .expect("pkcs1 pem");
        let pkcs8_pem = key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .expect("pkcs8 pem");

        let signers = [
            RequestSigner::parse(&xml_full(&key)).expect("full xml"),
            RequestSigner::parse(&xml_minimal(&key, true)).expect("minimal xml + e"),
            RequestSigner::parse(&xml_minimal(&key, false)).expect("minimal xml no e"),
            RequestSigner::parse(&pkcs1_pem).expect("pkcs1 pem"),
            RequestSigner::parse(&pkcs8_pem).expect("pkcs8 pem"),
        ];

        let expected_hex = signers[0].sign_hex(payload).expect("sign hex");
        let expected_b64 = signers[0].sign_base64(payload).expect("sign b64");

        for signer in &signers {
            let hex_sig = signer.sign_hex(payload).expect("sign hex");
            let b64_sig = signer.sign_base64(payload).expect("sign b64");
            assert_eq!(hex_sig, expected_hex, "PKCS#1 v1.5 is deterministic");
            assert_eq!(b64_sig, expected_b64);

            let raw = hex::decode(&hex_sig).expect("hex");
            let sig = Signature::try_from(raw.as_slice()).expect("sig");
            verifying.verify(payload, &sig).expect("verifies");
        }
    }

    #[test]
    fn bad_key_never_leaks_key_material() {
        let secret = "c2VjcmV0LW5vdC1hLWtleQ==";
        for bad in ["", "!!!not-base64!!!", secret, "<RSAKeyValue/>"] {
            let error = RequestSigner::parse(bad).expect_err("must fail");
            assert!(matches!(error, Error::InvalidPrivateKey(_)));
            if !bad.is_empty() {
                assert!(!error.to_string().contains(bad));
            }
        }
    }
}
