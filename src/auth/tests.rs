//! Unified token manager tests on a paused Tokio clock with an in-memory backend.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret as _, SecretString};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "trading")]
use super::OtpResponse;
use super::{
    AuthBackend, AuthMode, EpochUnit, RawToken, StoredToken, TokenManager, TokenSource,
    backend::BoxFuture,
};
use crate::{
    config::Credentials,
    error::{ApiError, ApiErrorCode, Error, Result},
    time::Clock,
};

/// Wall clock that follows Tokio's (pausable) clock.
#[derive(Debug)]
struct TestClock {
    base: DateTime<Utc>,
    start: tokio::time::Instant,
}

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: DateTime::from_timestamp(1_795_000_000, 0).expect("valid"),
            start: tokio::time::Instant::now(),
        })
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        self.base + chrono::Duration::from_std(self.start.elapsed()).expect("small")
    }
}

/// Mimics SSI: 15-minute access tokens, an 8-hour refresh window fixed at login, rotation on
/// every refresh, and a reused refresh token counted as a protocol violation.
struct FakeBackend {
    clock: Arc<TestClock>,
    access_lifetime: i64,
    refresh_lifetime: i64,
    millis: bool,
    issues: AtomicU32,
    refreshes: AtomicU32,
    fail_refresh: AtomicBool,
    /// Login fails with this code (0 = succeeds).
    fail_issue: AtomicI64,
    hang_refresh: AtomicBool,
    /// Every round trip takes this long (milliseconds).
    slow_ms: AtomicU64,
    issue_attempts: AtomicU32,
    reused: AtomicBool,
    live_refresh: Mutex<HashMap<String, i64>>,
    spent_refresh: Mutex<Vec<String>>,
    #[cfg(feature = "trading")]
    otp_reply: Mutex<Option<Result<OtpResponse>>>,
    #[cfg(feature = "trading")]
    scripted_issues: Mutex<std::collections::VecDeque<Result<RawToken>>>,
    #[cfg(feature = "trading")]
    scripted_refreshes: Mutex<std::collections::VecDeque<Result<RawToken>>>,
}

impl FakeBackend {
    fn new(clock: Arc<TestClock>) -> Arc<Self> {
        Arc::new(Self {
            clock,
            access_lifetime: 900,
            refresh_lifetime: 28_800,
            millis: false,
            issues: AtomicU32::new(0),
            refreshes: AtomicU32::new(0),
            fail_refresh: AtomicBool::new(false),
            fail_issue: AtomicI64::new(0),
            hang_refresh: AtomicBool::new(false),
            slow_ms: AtomicU64::new(0),
            issue_attempts: AtomicU32::new(0),
            reused: AtomicBool::new(false),
            live_refresh: Mutex::new(HashMap::new()),
            spent_refresh: Mutex::new(Vec::new()),
            #[cfg(feature = "trading")]
            otp_reply: Mutex::new(None),
            #[cfg(feature = "trading")]
            scripted_issues: Mutex::new(std::collections::VecDeque::new()),
            #[cfg(feature = "trading")]
            scripted_refreshes: Mutex::new(std::collections::VecDeque::new()),
        })
    }

    const fn epoch(&self, seconds: i64) -> i64 {
        if self.millis { seconds * 1000 } else { seconds }
    }

    fn token(&self, label: &str, number: u32, refresh_expiry: i64) -> RawToken {
        let now = self.clock.now().timestamp();
        let refresh = format!("refresh-{label}-{number}");
        self.live_refresh
            .lock()
            .expect("lock")
            .insert(refresh.clone(), refresh_expiry);
        RawToken {
            token_type: Some("Bearer".to_owned()),
            access_token: SecretString::from(format!("access-{label}-{number}")),
            expires_at: self.epoch(now + self.access_lifetime),
            refresh_token: Some(SecretString::from(refresh)),
            refresh_expires_at: Some(self.epoch(refresh_expiry)),
        }
    }
}

fn rejected(code: ApiErrorCode) -> Error {
    Error::Api(ApiError {
        http_status: 401,
        code,
        message: "rejected".to_owned(),
        endpoint: "/api/v3/auth/refresh".to_owned(),
        private: true,
    })
}

impl AuthBackend for FakeBackend {
    #[cfg(feature = "trading")]
    fn request_otp<'a>(
        &'a self,
        _credentials: &'a Credentials,
    ) -> BoxFuture<'a, Result<OtpResponse>> {
        Box::pin(async move {
            self.otp_reply
                .lock()
                .expect("lock")
                .take()
                .unwrap_or_else(|| Ok(OtpResponse::new("OTP sent", Some("tx-123".to_owned()))))
        })
    }

    fn issue<'a>(
        &'a self,
        _credentials: &'a Credentials,
        _mode: &'a AuthMode,
    ) -> BoxFuture<'a, Result<RawToken>> {
        Box::pin(async move {
            self.issue_attempts.fetch_add(1, Ordering::SeqCst);
            let slow = self.slow_ms.load(Ordering::SeqCst);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
            #[cfg(feature = "trading")]
            {
                let scripted = self.scripted_issues.lock().expect("lock").pop_front();
                if let Some(reply) = scripted {
                    if reply.is_ok() {
                        self.issues.fetch_add(1, Ordering::SeqCst);
                    }
                    return reply;
                }
            }
            let failure = self.fail_issue.load(Ordering::SeqCst);
            if failure != 0 {
                return Err(Error::Api(ApiError {
                    http_status: u16::try_from(failure / 1000).unwrap_or(500),
                    code: ApiErrorCode::from_code(failure),
                    message: "login failed".to_owned(),
                    endpoint: "/api/v3/auth/token".to_owned(),
                    private: true,
                }));
            }
            let number = self.issues.fetch_add(1, Ordering::SeqCst) + 1;
            let expiry = self.clock.now().timestamp() + self.refresh_lifetime;
            Ok(self.token("issued", number, expiry))
        })
    }

    fn refresh<'a>(&'a self, refresh_token: &'a SecretString) -> BoxFuture<'a, Result<RawToken>> {
        Box::pin(async move {
            let number = self.refreshes.fetch_add(1, Ordering::SeqCst) + 1;
            let slow = self.slow_ms.load(Ordering::SeqCst);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
            let text = refresh_token.expose_secret().to_owned();
            {
                let mut spent = self.spent_refresh.lock().expect("lock");
                if spent.contains(&text) {
                    self.reused.store(true, Ordering::SeqCst);
                    return Err(rejected(ApiErrorCode::RefreshTokenRevokedOrExpired));
                }
                spent.push(text.clone());
            }
            if self.hang_refresh.load(Ordering::SeqCst) {
                // The server rotated the token but the answer never arrives.
                std::future::pending::<()>().await;
            }
            #[cfg(feature = "trading")]
            {
                let scripted = self.scripted_refreshes.lock().expect("lock").pop_front();
                if let Some(reply) = scripted {
                    return reply;
                }
            }
            let expiry = self.live_refresh.lock().expect("lock").remove(&text);
            if self.fail_refresh.load(Ordering::SeqCst) {
                return Err(rejected(ApiErrorCode::InvalidRefreshToken));
            }
            let Some(expiry) = expiry else {
                return Err(rejected(ApiErrorCode::InvalidRefreshToken));
            };
            Ok(self.token("refreshed", number, expiry))
        })
    }
}

fn manager(backend: &Arc<FakeBackend>, clock: &Arc<TestClock>) -> TokenManager {
    TokenManager::new(
        Arc::clone(backend) as Arc<dyn AuthBackend>,
        Credentials::new("key", "secret").expect("valid"),
        Arc::clone(clock) as Arc<dyn Clock>,
        Duration::from_mins(5),
        #[cfg(feature = "trading")]
        Duration::from_millis(50),
        #[cfg(feature = "trading")]
        Duration::from_millis(500),
    )
}

#[tokio::test(start_paused = true)]
async fn caches_then_refreshes_inside_the_lead_window() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);

    let first = manager.access_token().await.expect("issued");
    tokio::time::advance(Duration::from_mins(1)).await;
    let cached = manager.access_token().await.expect("cached");
    assert_eq!(first.expose_secret(), cached.expose_secret());
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(manager.time_until_renewal(), Some(Duration::from_mins(9)));

    // 15-minute token, 5-minute lead: renewal is due 10 minutes after issue.
    tokio::time::advance(Duration::from_mins(9) + Duration::from_secs(1)).await;
    let renewed = manager.access_token().await.expect("refreshed");
    assert_ne!(first.expose_secret(), renewed.expose_secret());
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    let info = manager.info().expect("info");
    assert_eq!(info.source, TokenSource::Refreshed);
    assert_eq!(info.refresh_token_rotated, Some(true));
    assert_eq!(info.access_lifetime, Duration::from_mins(15));
    assert_eq!(info.epoch_unit, EpochUnit::Seconds);
    assert_eq!(manager.history().len(), 2);
    assert!(!backend.reused.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn failed_refresh_logs_in_again_and_never_reuses_a_refresh_token() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    manager.access_token().await.expect("issued");

    backend.fail_refresh.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_mins(11)).await;
    manager.access_token().await.expect("fallback login");
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(backend.issues.load(Ordering::SeqCst), 2);
    assert_eq!(manager.info().expect("info").source, TokenSource::ReadOnly);

    backend.fail_refresh.store(false, Ordering::SeqCst);
    tokio::time::advance(Duration::from_mins(11)).await;
    manager.access_token().await.expect("refreshed");
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 2);
    assert_eq!(backend.issues.load(Ordering::SeqCst), 2);
    assert!(
        !backend.reused.load(Ordering::SeqCst),
        "a refresh token was sent twice"
    );
}

#[tokio::test(start_paused = true)]
async fn refresh_window_is_fixed_at_login_so_the_chain_ends() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    manager.access_token().await.expect("issued");
    let login_window = manager.info().and_then(|info| info.refresh_expires_at);

    // Renew every 11 minutes for 8 hours: refreshes until the 8-hour window closes, then
    // exactly one new login.
    for _ in 0..44 {
        tokio::time::advance(Duration::from_mins(11)).await;
        manager.access_token().await.expect("renewed");
        let info = manager.info().expect("info");
        if info.source == TokenSource::Refreshed && backend.issues.load(Ordering::SeqCst) == 1 {
            assert_eq!(info.refresh_expires_at, login_window);
        }
    }
    assert!(backend.refreshes.load(Ordering::SeqCst) >= 40);
    assert_eq!(
        backend.issues.load(Ordering::SeqCst),
        2,
        "one re-login after 8 hours"
    );
    assert!(!backend.reused.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn auto_refresh_renews_before_expiry() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    manager.access_token().await.expect("issued");
    let cancel = CancellationToken::new();
    let task = manager.spawn_auto_refresh(cancel.clone());

    tokio::time::sleep(Duration::from_mins(46)).await;
    cancel.cancel();
    task.await.expect("task ends");

    let history = manager.history();
    assert!(
        history.len() >= 5,
        "renewed about every 10 minutes: {}",
        history.len()
    );
    for (before, after) in history.iter().zip(history.iter().skip(1)) {
        assert!(
            after.obtained_at < before.expires_at,
            "renewed before expiry"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn invalidate_forces_renewal_of_that_token_only() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let first = manager.access_token().await.expect("issued");

    manager.invalidate(&SecretString::from("some-other-token".to_owned()));
    let same = manager.access_token().await.expect("unchanged");
    assert_eq!(first.expose_secret(), same.expose_secret());

    manager.invalidate(&first);
    let renewed = manager.access_token().await.expect("renewed");
    assert_ne!(first.expose_secret(), renewed.expose_secret());
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn concurrent_callers_share_one_login() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let manager = manager.clone();
            tokio::spawn(async move { manager.access_token().await })
        })
        .collect();
    for task in tasks {
        task.await.expect("joined").expect("token");
    }
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn millisecond_epochs_are_understood() {
    let clock = TestClock::new();
    let mut backend = FakeBackend::new(Arc::clone(&clock));
    Arc::get_mut(&mut backend).expect("unique").millis = true;
    let manager = manager(&backend, &clock);
    manager.access_token().await.expect("issued");
    let info = manager.info().expect("info");
    assert_eq!(info.epoch_unit, EpochUnit::Milliseconds);
    assert_eq!(info.access_lifetime, Duration::from_mins(15));
    assert_eq!(info.refresh_lifetime, Some(Duration::from_hours(8)));
}

#[test]
fn token_payloads_wrapped_or_not() {
    use super::backend::parse_token_response;

    let bare = parse_token_response(
        "/t",
        br#"{"tokenType":"Bearer","accessToken":"a","expiresAt":1779806203,"refreshToken":"r","refreshExpiresAt":1779892603}"#,
    )
    .expect("bare");
    assert_eq!(bare.expires_at, 1_779_806_203);
    let wrapped = parse_token_response(
        "/t",
        br#"{"data":{"accessToken":"a","expiresAt":"1772075293023"}}"#,
    )
    .expect("wrapped");
    assert_eq!(wrapped.expires_at, 1_772_075_293_023);
    assert!(wrapped.refresh_token.is_none());
    assert!(parse_token_response("/t", br#"{"expiresAt":1}"#).is_err());
}

#[tokio::test(start_paused = true)]
async fn failed_renewal_in_the_lead_window_keeps_serving_the_valid_token() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let first = manager.access_token().await.expect("issued");
    let expires_at = manager.info().expect("info").expires_at;

    tokio::time::advance(Duration::from_mins(11)).await;
    backend.fail_refresh.store(true, Ordering::SeqCst);
    backend.fail_issue.store(503_000, Ordering::SeqCst);
    for _ in 0..5 {
        let token = manager.access_token().await.expect("still valid");
        assert_eq!(token.expose_secret(), first.expose_secret());
        tokio::time::advance(Duration::from_millis(100)).await;
    }
    assert!(clock.now() < expires_at);
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(backend.issue_attempts.load(Ordering::SeqCst), 2);

    tokio::time::advance(Duration::from_mins(5)).await;
    assert!(manager.access_token().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn on_demand_failures_back_off() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    backend.fail_issue.store(503_000, Ordering::SeqCst);
    let mut backoff_errors = 0;
    for _ in 0..100 {
        match manager.access_token().await {
            Err(Error::AuthBackoff { .. }) => backoff_errors += 1,
            Err(_) => {}
            Ok(_) => panic!("login cannot succeed"),
        }
        tokio::time::advance(Duration::from_millis(100)).await;
    }
    let attempts = backend.issue_attempts.load(Ordering::SeqCst);
    assert!(
        (3..=5).contains(&attempts),
        "login attempts in 10 s: {attempts}"
    );
    assert!(backoff_errors >= 90);

    backend.fail_issue.store(0, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(20)).await;
    manager
        .access_token()
        .await
        .expect("recovers after the backoff");
}

#[tokio::test(start_paused = true)]
async fn a_revoked_key_backs_off_for_minutes() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    backend.fail_issue.store(401_105, Ordering::SeqCst);
    for _ in 0..300 {
        let _ = manager.access_token().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(
        backend.issue_attempts.load(Ordering::SeqCst),
        1,
        "no retry within 5 minutes of 401105"
    );
    tokio::time::advance(Duration::from_mins(6)).await;
    let _ = manager.access_token().await;
    assert_eq!(backend.issue_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn other_callers_get_the_valid_token_while_a_renewal_hangs() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let first = manager.access_token().await.expect("issued");
    tokio::time::advance(Duration::from_mins(11)).await;
    backend.hang_refresh.store(true, Ordering::SeqCst);

    let renewing = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.access_token().await })
    };
    tokio::task::yield_now().await;
    let started = tokio::time::Instant::now();
    for _ in 0..10 {
        let token = manager.access_token().await.expect("served");
        assert_eq!(token.expose_secret(), first.expose_secret());
    }
    assert_eq!(started.elapsed(), Duration::ZERO);
    renewing.abort();
}

#[tokio::test(start_paused = true)]
async fn cancelled_refresh_never_resends_the_refresh_token() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    manager.access_token().await.expect("issued");
    tokio::time::advance(Duration::from_mins(11)).await;

    backend.hang_refresh.store(true, Ordering::SeqCst);
    let timed_out = tokio::time::timeout(Duration::from_secs(5), manager.force_refresh()).await;
    assert!(timed_out.is_err());
    backend.hang_refresh.store(false, Ordering::SeqCst);

    manager.access_token().await.expect("recovered");
    assert!(!backend.reused.load(Ordering::SeqCst));
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(manager.info().expect("info").source, TokenSource::ReadOnly);
}

#[tokio::test(start_paused = true)]
async fn concurrent_force_refresh_invalidate_and_auto_refresh_for_nine_hours() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let first = manager.access_token().await.expect("issued");
    let cancel = CancellationToken::new();
    let auto = manager.spawn_auto_refresh(cancel.clone());
    let mut tasks = Vec::new();
    for worker in 0..8_u64 {
        let manager = manager.clone();
        let first = first.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..(9 * 60_u64) {
                tokio::time::sleep(Duration::from_mins(1) + Duration::from_millis(worker * 7))
                    .await;
                match (round + worker) % 4 {
                    0 => {
                        manager.force_refresh().await.expect("force");
                    }
                    1 => {
                        let token = manager.access_token().await.expect("token");
                        manager.invalidate(&token);
                    }
                    2 => manager.invalidate(&first),
                    _ => {
                        manager.access_token().await.expect("token");
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("worker");
    }
    cancel.cancel();
    auto.await.expect("auto refresh");
    assert!(!backend.reused.load(Ordering::SeqCst));
    assert!(backend.issues.load(Ordering::SeqCst) >= 2);
}

#[tokio::test(start_paused = true)]
async fn a_skewed_clock_still_never_reuses_refresh_tokens() {
    let clock = TestClock::new();
    let mut backend = FakeBackend::new(Arc::clone(&clock));
    Arc::get_mut(&mut backend).expect("unique").access_lifetime = -300;
    let manager = manager(&backend, &clock);
    for _ in 0..120 {
        manager.access_token().await.expect("token");
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert!(!backend.reused.load(Ordering::SeqCst));
    assert!(super::lifetime_is_suspect(chrono::Duration::seconds(-300)));
    assert!(!super::lifetime_is_suspect(chrono::Duration::seconds(899)));
}

#[test]
fn the_token_fixture_matches_the_observed_lifetimes() {
    use serde_json::Value;

    let fixture: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/auth_token.json")).expect("json");
    let body = serde_json::to_vec(fixture.get("body").expect("body")).expect("bytes");
    let raw = super::backend::parse_token_response("/api/v3/auth/token", &body).expect("token");
    let refresh = raw.refresh_expires_at.expect("refresh expiry");
    assert_eq!(refresh - (raw.expires_at - 900), 28_800);
    assert!(raw.expires_at < MILLIS_THRESHOLD_FOR_TESTS, "epoch seconds");
}

const MILLIS_THRESHOLD_FOR_TESTS: i64 = 100_000_000_000;

#[tokio::test(start_paused = true)]
async fn a_token_rejected_during_a_failed_renewal_is_not_served() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let manager = manager(&backend, &clock);
    let first = manager.access_token().await.expect("issued");
    tokio::time::advance(Duration::from_mins(11)).await;
    backend.slow_ms.store(2_000, Ordering::SeqCst);
    backend.fail_refresh.store(true, Ordering::SeqCst);
    backend.fail_issue.store(503_000, Ordering::SeqCst);

    let renewing = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.access_token().await })
    };
    tokio::task::yield_now().await;
    manager.invalidate(&first);
    let waiting = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.access_token().await })
    };
    let served = |result: &Result<SecretString>| {
        result
            .as_ref()
            .is_ok_and(|token| token.expose_secret() == first.expose_secret())
    };
    let renewer = renewing.await.expect("join");
    let waiter = waiting.await.expect("join");
    assert!(!served(&renewer));
    assert!(!served(&waiter));
    assert!(renewer.is_err() && waiter.is_err());
    let next = manager.access_token().await;
    assert!(!served(&next));
}

#[tokio::test(start_paused = true)]
async fn export_import_and_clear_token_round_trip() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let first_mgr = manager(&backend, &clock);

    assert!(!first_mgr.has_valid_token());
    assert!(first_mgr.export_token().is_none());

    let issued = first_mgr.access_token().await.expect("issued");
    assert!(first_mgr.has_valid_token());
    let exported = first_mgr.export_token().expect("exported");
    assert_eq!(exported.access_token(), issued.expose_secret());
    assert!(exported.refresh_token().is_some());
    assert!(!exported.is_trading_session);
    assert!(!format!("{exported:?}").contains(issued.expose_secret()));

    let second_mgr = manager(&backend, &clock);
    let info = second_mgr.set_token(exported.clone()).expect("imported");
    assert_eq!(info.source, TokenSource::Imported);
    assert!(second_mgr.has_valid_token());
    let got = second_mgr.access_token().await.expect("from cache");
    assert_eq!(got.expose_secret(), issued.expose_secret());
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);

    let expired_stored = StoredToken::new(
        "expired-access",
        exported.refresh_token().map(ToOwned::to_owned),
        clock.now() - chrono::Duration::seconds(60),
        exported.refresh_expires_at,
    );
    let third_mgr = manager(&backend, &clock);
    third_mgr.set_token(expired_stored).expect("imported");
    assert!(!third_mgr.has_valid_token());
    let refreshed = third_mgr.access_token().await.expect("refreshed");
    assert_ne!(refreshed.expose_secret(), "expired-access");
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);

    let empty = StoredToken::new("   ", None, clock.now(), None);
    assert!(third_mgr.set_token(empty).is_err());
    third_mgr.clear();
    assert!(!third_mgr.has_valid_token());
    assert!(third_mgr.export_token().is_none());
    assert!(third_mgr.info().is_none());
}

#[cfg(feature = "trading")]
#[tokio::test(start_paused = true)]
async fn trading_access_token_never_auto_logs_in_without_otp() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let tm = manager(&backend, &clock);

    let err = tm
        .trading_access_token()
        .await
        .expect_err("must require OTP");
    assert!(matches!(err, Error::Authentication(_)));
    assert_eq!(backend.issue_attempts.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "trading")]
#[tokio::test(start_paused = true)]
async fn otp_session_is_sticky_and_dead_refresh_requires_reauth() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let tm = manager(&backend, &clock);

    let info = tm.authenticate_with_otp("123456").await.expect("otp");
    assert_eq!(info.source, TokenSource::Otp);
    assert!(tm.export_token().expect("exported").is_trading_session);
    assert!(tm.trading_access_token().await.is_ok());

    // Enter 5-minute refresh lead window: refreshes via /refresh.
    tokio::time::advance(Duration::from_mins(11)).await;
    let refreshed = tm.trading_access_token().await.expect("refreshed");
    assert!(refreshed.expose_secret().starts_with("access-refreshed-"));
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);

    // Next refresh fails with 401103 -> clears token, keeps TradingOtp mode, NEVER downgrades!
    backend
        .scripted_refreshes
        .lock()
        .expect("lock")
        .push_back(Err(rejected(ApiErrorCode::RefreshTokenRevokedOrExpired)));
    tokio::time::advance(Duration::from_mins(16)).await;
    let err = tm.access_token().await.expect_err("must require reauth");
    assert!(matches!(err, Error::ReauthenticationRequired(_)));
    assert!(!tm.is_authenticated());
    assert!(tm.info().is_none());
    // Still refuses to downgrade on subsequent access_token() or trading_access_token() calls.
    assert!(matches!(
        tm.trading_access_token().await,
        Err(Error::ReauthenticationRequired(_) | Error::Authentication(_))
    ));
    assert_eq!(backend.issues.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "trading")]
#[tokio::test(start_paused = true)]
async fn smart_otp_polling_succeeds_after_pending_and_stops_on_rejected() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let pending = Error::SmartOtpPending(ApiError {
        http_status: 202,
        code: ApiErrorCode::PushApprovalPending,
        message: "pending".to_owned(),
        endpoint: "/api/v3/auth/token".to_owned(),
        private: true,
    });
    let approved = backend.token("smart", 1, clock.now().timestamp() + 28_800);
    {
        let mut queue = backend.scripted_issues.lock().expect("lock");
        queue.push_back(Err(pending));
        queue.push_back(Ok(approved));
    }
    let tm = manager(&backend, &clock);
    let info = tm
        .poll_smart_otp("tx-123", CancellationToken::new())
        .await
        .expect("approved");
    assert_eq!(info.source, TokenSource::SmartOtp);
    assert_eq!(backend.issue_attempts.load(Ordering::SeqCst), 2);

    backend
        .scripted_issues
        .lock()
        .expect("lock")
        .push_back(Err(Error::SmartOtpRejected(ApiError {
            http_status: 401,
            code: ApiErrorCode::PushApprovalRejected,
            message: "rejected".to_owned(),
            endpoint: "/api/v3/auth/token".to_owned(),
            private: true,
        })));
    let err = tm
        .poll_smart_otp("tx-456", CancellationToken::new())
        .await
        .expect_err("rejected");
    assert!(matches!(err, Error::SmartOtpRejected(_)));
    assert_eq!(backend.issue_attempts.load(Ordering::SeqCst), 3);
}

#[cfg(feature = "trading")]
#[tokio::test(start_paused = true)]
async fn imported_trading_token_preserves_sticky_trading_mode() {
    let clock = TestClock::new();
    let backend = FakeBackend::new(Arc::clone(&clock));
    let tm = manager(&backend, &clock);

    let stored = StoredToken::new(
        "persisted-trading-access",
        Some("persisted-trading-refresh".to_owned()),
        clock.now() + chrono::Duration::seconds(900),
        Some(clock.now() + chrono::Duration::seconds(28_800)),
    )
    .with_trading_session(true);
    tm.set_token(stored).expect("set");
    assert_eq!(
        tm.trading_access_token().await.expect("ok").expose_secret(),
        "persisted-trading-access"
    );

    // When its refresh fails, it returns ReauthenticationRequired instead of OTP-less login.
    backend.fail_refresh.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_mins(16)).await;
    let err = tm.access_token().await.expect_err("reauth");
    assert!(matches!(err, Error::ReauthenticationRequired(_)));
    assert_eq!(backend.issues.load(Ordering::SeqCst), 0);
}
