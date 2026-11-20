//! Error types.
//!
//! [`ApiErrorCode`] maps every code of the V3 `ErrCode` table
//! (<https://developers.ssi.com.vn/docs/api-reference/appendix/error-codes>) plus the server
//! codes observed by the official V3 SDKs (`400171`, `401006`, `401008`, `4002xx`, `50420x`).
//! Codes follow `{HTTP_STATUS}{xxx}`; any code not in the table is preserved verbatim as
//! [`ApiErrorCode::Other`].

use std::{fmt, time::Duration};

use thiserror::Error;

use crate::http::RateLimitSnapshot;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every failure this crate reports.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The configuration is incomplete or invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// A request failed local validation; nothing was sent to SSI.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The RSA private key is missing or malformed; nothing was signed or sent.
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(String),
    /// Authentication state is missing or invalid (for example, no trading OTP session yet).
    #[error("authentication error: {0}")]
    Authentication(String),
    /// No usable token: the last auto-login failed and the manager is backing off before retrying.
    #[error("SSI login is backing off for {retry_in:?} after: {reason}")]
    AuthBackoff {
        /// Time until the next login attempt.
        retry_in: Duration,
        /// Why the last attempt failed.
        reason: String,
    },
    /// The HTTP transport failed (connect, TLS, timeout, body read).
    #[error("HTTP transport error: {0}")]
    Http(#[source] reqwest::Error),
    /// SSI answered with an error code.
    #[error(transparent)]
    Api(ApiError),
    /// SSI rejected a duplicate `clientRequestId` or `batchRequestId` (HTTP 409 / `409100`).
    ///
    /// Never re-submit the order under a fresh id without first reconciling via the order book:
    /// the original order may already be live.
    #[error("duplicate request: {0}")]
    DuplicateRequest(ApiError),
    /// Smart OTP push-approval has not been confirmed on the user's device yet
    /// (HTTP 202 with code `401114`). Poll again with the same `transactionId`.
    #[error("Smart OTP approval pending: {0}")]
    SmartOtpPending(ApiError),
    /// Smart OTP push-approval reached a terminal failure (`401113`, `401115`, `401116`).
    #[error("Smart OTP approval failed: {0}")]
    SmartOtpRejected(ApiError),
    /// A trading session cannot be refreshed (`401101`–`401104`, `401106`–`401108`, or expired
    /// refresh token). The stored token has been cleared; authenticate again with a new OTP.
    #[error("re-authentication with a new OTP is required: {0}")]
    ReauthenticationRequired(ApiError),
    /// SSI throttled the request (HTTP 429 with code `429000` or no code).
    #[error(transparent)]
    RateLimited(Box<RateLimited>),
    /// A response body did not have the documented shape.
    #[error("could not decode the {endpoint} response: {detail}")]
    Decode {
        /// Endpoint path (or stream topic / WS method).
        endpoint: String,
        /// Decoder message. The body is never included; private endpoints redact details too.
        detail: String,
    },
    /// The WebSocket transport failed.
    #[error("WebSocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    /// The Trading WebSocket (`/ws/v3/trading`) rejected the connection or a command.
    #[error(transparent)]
    TradingWs(TradingWsError),
    /// The stream supervisor or WebSocket connection has stopped.
    #[error("the stream has stopped")]
    StreamClosed,
    /// A command timed out waiting for a response. Mutating commands are never retried;
    /// reconcile through the order book or order events before acting again.
    #[error("request timed out (not retried): {0}")]
    Timeout(String),
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.without_url())
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}

impl Error {
    /// The SSI error code, when SSI sent one.
    pub const fn api_code(&self) -> Option<ApiErrorCode> {
        match self {
            Self::Api(error)
            | Self::DuplicateRequest(error)
            | Self::SmartOtpPending(error)
            | Self::SmartOtpRejected(error)
            | Self::ReauthenticationRequired(error) => Some(error.code),
            Self::RateLimited(limited) => limited.code,
            Self::TradingWs(error) => error.api_code,
            _ => None,
        }
    }

    /// Whether repeating an **idempotent read (`GET`)** request later may succeed.
    ///
    /// Mutating trading commands (`POST`/`PUT`/`DELETE` orders, batch, FCO) are never retried
    /// automatically by this crate regardless of what this method returns.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(error) => error.is_timeout() || error.is_connect() || error.is_request(),
            Self::RateLimited(_) | Self::WebSocket(_) | Self::AuthBackoff { .. } => true,
            Self::Api(error) => error.is_transient(),
            _ => false,
        }
    }
}

/// An error answer from SSI.
///
/// For private endpoints (`private == true`) neither `Display` nor `Debug` prints `message`,
/// which can quote account numbers; the field itself stays readable to callers.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status of the response (SSI sometimes reports errors with HTTP 200 or 202).
    pub http_status: u16,
    /// Typed `ErrCode`.
    pub code: ApiErrorCode,
    /// `msg` / `message` text from SSI.
    pub message: String,
    /// Endpoint path that failed.
    pub endpoint: String,
    /// Whether the endpoint deals with private account/trading data.
    pub private: bool,
}

impl fmt::Debug for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message: &dyn fmt::Debug = if self.private {
            &"<withheld: private endpoint>"
        } else {
            &self.message
        };
        f.debug_struct("ApiError")
            .field("http_status", &self.http_status)
            .field("code", &self.code)
            .field("message", message)
            .field("endpoint", &self.endpoint)
            .field("private", &self.private)
            .finish()
    }
}

impl ApiError {
    /// Whether a retry of an identical read-only request may succeed.
    pub const fn is_transient(&self) -> bool {
        self.code.is_transient() || matches!(self.http_status, 502..=504)
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SSI {} returned HTTP {} code {} ({})",
            self.endpoint,
            self.http_status,
            self.code.code(),
            self.code.name(),
        )?;
        if self.private {
            f.write_str(": <message withheld: private endpoint>")
        } else {
            write!(f, ": {}", self.message)
        }
    }
}

impl std::error::Error for ApiError {}

/// An error or post-upgrade refusal frame from the Trading WebSocket (`/ws/v3/trading`).
///
/// Never carries signed request parameters, tokens or account numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingWsError {
    /// Numeric status (`401`, `403`, `409`, `429`, `500`, or `0` when the server sent `"error"`).
    pub status: u16,
    /// Raw error code string from the server (e.g. `"401013"`, `"FCO_ERROR"`, `"TOO_MANY"`).
    pub code: String,
    /// Parsed `ApiErrorCode` if `code` is numeric.
    pub api_code: Option<ApiErrorCode>,
    /// Server message text (with any account-bearing trading topics masked).
    pub message: String,
}

impl fmt::Display for TradingWsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SSI Trading WebSocket error status={} code={}: {}",
            self.status, self.code, self.message
        )
    }
}

impl std::error::Error for TradingWsError {}

/// SSI throttled a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimited {
    /// Endpoint path.
    pub endpoint: String,
    /// `ErrCode` if SSI sent one (normally `429000`).
    pub code: Option<ApiErrorCode>,
    /// Wait advised by `Retry-After` or `X-RATELIMIT-RESET`, if present.
    pub retry_after: Option<Duration>,
    /// Rate-limit headers of the throttled response.
    pub snapshot: Option<RateLimitSnapshot>,
    /// `msg` text from SSI.
    pub message: String,
}

impl fmt::Display for RateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SSI {} rate limited the request", self.endpoint)?;
        if let Some(wait) = self.retry_after {
            write!(f, "; retry after {wait:?}")?;
        }
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for RateLimited {}

macro_rules! api_error_codes {
    ($($variant:ident = $code:literal, $name:literal;)*) => {
        /// A V3 `ErrCode` from the error-code appendix and official SDKs.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ApiErrorCode {
            $(
                #[doc = concat!("`", stringify!($code), "` ", $name, ".")]
                $variant,
            )*
            /// A code that is not in the table (kept verbatim).
            Other(i64),
        }

        impl ApiErrorCode {
            /// Maps a numeric code.
            pub const fn from_code(code: i64) -> Self {
                match code {
                    $($code => Self::$variant,)*
                    other => Self::Other(other),
                }
            }

            /// The numeric code.
            pub const fn code(self) -> i64 {
                match self {
                    $(Self::$variant => $code,)*
                    Self::Other(code) => code,
                }
            }

            /// The English name from the appendix.
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)*
                    Self::Other(_) => "Unknown",
                }
            }
        }
    };
}

api_error_codes! {
    InvalidRequest = 400_000, "Invalid Request";
    MalformedJsonBody = 400_001, "Malformed JSON Body";
    MissingRequiredField = 400_002, "Missing Required Field";
    InvalidFieldValue = 400_003, "Invalid Field Value";
    InvalidFieldFormat = 400_004, "Invalid Field Format";
    InvalidField = 400_005, "Invalid Field Error";
    MissingRequestBody = 400_006, "Missing Request Body";
    DataOnlyForLatestYears = 400_007, "Data is available only for the latest x years";
    RangeExceedsMonths = 400_008, "Query time range cannot exceed y months";
    FromAfterTo = 400_009, "From Date cannot be greater than To Date";
    RangeExceedsYears = 400_010, "Query time range cannot exceed z years";
    LookupTimeNotPast = 400_011, "The lookup time must be earlier than the current date";
    InvalidDateTime = 400_012, "Invalid Date/Timestamp";
    InvalidTimezone = 400_013, "Invalid Timezone";
    InvalidPageNumber = 400_014, "Invalid PageNumber";
    InvalidPageSize = 400_015, "Invalid PageSize";
    CredentialHasNoClient = 400_101, "Credential has no associated client";
    TwoFactorInfoFailed = 400_102, "Failed to get 2FA info";
    TwoFactorInfoUnavailable = 400_103, "2FA info not available for this account";
    OtpSendFailed = 400_104, "Failed to send OTP / Symbol Required";
    UnsupportedCoreType = 400_105, "Unsupported Core_Type for account";
    DisclosurePolicyViolation = 400_106, "Order rejected: stock disclosure policy violation";
    PriceAndQuantityModified = 400_107, "Do not edit price and quantity at the same time";
    OrderTypeNotAllowed = 400_108, "OrderType is not allowed (batch: orders empty or over limit)";
    ModifyDisclosurePolicyViolation = 400_109,
        "Modify order rejected: stock disclosure policy violation (batch: missing batch id or time)";
    NoAccountsForClient = 400_110, "No accounts found for client";
    SymbolNotTradable = 400_111, "Symbol Not Tradable (batch: request time outside replay window)";
    PriceOutOfRange = 400_112, "Price Out of Range";
    InvalidLotSize = 400_113, "Invalid Lot Size";
    PriceMustBePositive = 400_114, "Price Must Be Positive";
    PriceMustBeZero = 400_115, "Price Must Be Zero";
    PriceMustBeZeroForMarketOrders = 400_116, "Price Must Be Zero For Market Orders";
    OrderTypeNotAllowedInSession = 400_117, "Order Type Not Allowed In Current Session";
    PriceNotDivisibleByTick = 400_118, "Price Not Divisible By Tick Size";
    SellNotAllowedInSession = 400_119, "Sell Not Allowed In This Session";
    QuantityBelowMinimum = 400_120, "Quantity Below Minimum";
    QuantityExceedsMaximum = 400_121, "Quantity Exceeds Maximum";
    DuplicateOrderInPeriodicSession = 400_122, "Duplicate Order In Periodic Session";
    SymbolNotRegistered = 400_123, "Symbol Not Registered With Exchange";
    SymbolRuleNotConfigured = 400_124, "Symbol Rule Not Configured";
    EvenLotRuleNotConfigured = 400_125, "Symbol Even Lot Rule Not Configured";
    OddLotRuleNotConfigured = 400_126, "Symbol Odd Lot Rule Not Configured";
    CannotChangeEvenToOddLot = 400_127, "Cannot Change From Even To Odd Lot";
    CannotChangeOddToEvenLot = 400_128, "Cannot Change From Odd To Even Lot";
    OrderQuantityMustMatch = 400_129, "OrderQty Must Match Original";
    InvalidLotQuantity = 400_130, "Invalid Lot Quantity";
    OrderCannotBeCancelled = 400_131, "Order Cannot Be Cancelled";
    OrderCannotBeModified = 400_132, "Order Cannot Be Modified";
    NoChangesDetected = 400_133, "No Changes Detected";
    InvalidSymbolFormat = 400_134, "Invalid Symbol Format";
    PreSessionOrderTypeNotBound = 400_135, "Pre-session Order Type Not Bound";
    CannotModifyMpInContinuous = 400_136, "Cannot Modify MP In Continuous Session";
    CannotModifyMokInContinuous = 400_137, "Cannot Modify MOK In Continuous Session";
    CannotModifyMtlInContinuous = 400_138, "Cannot Modify MTL In Continuous Session";
    CannotModifyMakInContinuous = 400_139, "Cannot Modify MAK In Continuous Session";
    DerivativeAccountRequired = 400_140, "Derivative Account Required";
    SymbolBuyRestricted = 400_141, "Symbol Buy Restricted";
    SymbolSellRestricted = 400_142, "Symbol Sell Restricted";
    SymbolNotOnDerivativeExchange = 400_143, "Symbol Not On Derivative Exchange";
    AccountCannotTradeDerivative = 400_144, "Account Cannot Trade Derivative Symbol";
    AccountSuspended = 400_145, "Account Suspended";
    SymbolRequired = 400_146, "Symbol Required";
    QuantityMustBePositive = 400_147, "Quantity Must Be Positive";
    QuantityExceedsLimit = 400_148, "Quantity Exceeds Limit";
    BeforeTradingHours = 400_149, "Before Trading Hours";
    AfterTradingHours = 400_150, "After Trading Hours / Trading Account Required";
    SymbolMissing = 400_151, "Symbol Required";
    InvalidSymbol = 400_152, "Invalid Symbol";
    OrderSideRequired = 400_153, "Order Side Required";
    InvalidOrderSide = 400_154, "Invalid Order Side";
    OrderTypeRequired = 400_155, "Order Type Required";
    InvalidOrderType = 400_156, "Invalid Order Type";
    QuantityOrPriceRequired = 400_157, "Quantity Required / Price Required";
    InvalidQuantity = 400_158, "Invalid Quantity";
    InvalidPrice = 400_159, "Invalid Price";
    OperationNotPermitted = 400_160, "Operation Not Permitted";
    FcoStrategyNotSupported = 400_161, "FCO Strategy Not Supported";
    FcoCannotSetStopOrder = 400_162, "FCO Cannot Set StopOrder";
    FcoCannotSetMainOrderPrice = 400_163, "FCO Cannot Set Price Main Order";
    FcoActivationLimitReached = 400_164, "FCO Activation Limit Reached";
    FcoIndicatorTypeMismatch = 400_165, "FCO Indicator Type Mismatch";
    PushApprovalCreateFailed = 400_171, "Push-approval creation failed";
    FromAfterToDate = 400_202, "from is later than to (market data)";
    BoardNotAllowed = 400_208, "Board not allowed (market data)";
    TimeframeNotSupported = 400_210, "Timeframe not supported (market data)";
    DateFormatInvalid = 400_213, "Invalid Date/Timestamp Format (market data)";
    MissingAuthorizationHeader = 401_000, "Missing Authorization Header";
    Unauthorized = 401_001, "Unauthorized";
    AccessTokenMissing = 401_002, "Access Token Missing";
    AccessTokenInvalid = 401_003, "Access Token Invalid";
    AccessTokenExpired = 401_004, "Access Token Expired";
    SdkSignatureMissing = 401_006, "Signature Missing (gateway code)";
    SdkSignatureBodyEmpty = 401_008, "Signature Body Empty (gateway code)";
    AccessTokenNotYetValid = 401_009, "Access Token Not Yet Valid";
    TokenSignatureInvalid = 401_010, "Token Signature Invalid";
    MissingSignatureHeader = 401_011, "Missing Signature Header";
    SignatureNotRecognized = 401_012, "Signature Not Recognized";
    SignatureNotValid = 401_013, "Signature Not Valid / Signature Mismatch";
    SignatureNotMatched = 401_014, "Signature Not Matched";
    SignatureBodyEmpty = 401_015, "Signature Body Empty";
    TokenNotIncludeOtp = 401_016, "Token Not Include OTP";
    OtpVerificationFailed = 401_100, "OTP Verification Failed";
    InvalidRefreshToken = 401_101, "Invalid Refresh Token";
    RefreshTokenMissingClient = 401_102, "Refresh Token Missing Client";
    RefreshTokenRevokedOrExpired = 401_103, "Refresh Token Revoked or Expired";
    InvalidApiKeyReference = 401_104, "Invalid API Key Reference";
    InvalidApiKeyOrSecret = 401_105, "Invalid API Key or Secret / Token Without OTP";
    InvalidTokenType = 401_106, "Invalid Token Type";
    RefreshTokenMissingJti = 401_107, "Refresh Token Missing JTI";
    RefreshTokenMissingKeyId = 401_108, "Refresh Token Missing Key ID";
    PushApprovalNotFound = 401_113, "Push-approval not found";
    PushApprovalPending = 401_114, "Push-approval is pending";
    PushApprovalRejected = 401_115, "Push-approval rejected";
    PushApprovalForeignTransaction = 401_116, "Push-approval belongs to another transaction";
    Forbidden = 403_000, "Forbidden";
    InsufficientScope = 403_001, "Insufficient Scope";
    NoScopesGranted = 403_100, "No Scopes Granted";
    AccountNotAuthorized = 403_101, "Account Not Authorized";
    ProductNotEnabled = 403_102, "Product Not Enabled For Account";
    ResourceNotFound = 404_000, "Resource Not Found";
    ClientRequestIdNotFound = 404_100, "ClientRequestId Not Found";
    ClientRequestIdHasNoOrderId = 404_101, "ClientRequestId Has No OrderId";
    OrderIdNotFound = 404_102, "OrderId Not Found";
    AccountNotFound = 404_103, "Account Not Found";
    SymbolNotFound = 404_204, "Symbol Not Found";
    MethodNotAllowed = 405_000, "Method Not Allowed";
    GetNotAllowed = 405_001, "GET Not Allowed";
    PostNotAllowed = 405_002, "POST Not Allowed";
    PutNotAllowed = 405_003, "PUT Not Allowed";
    PatchNotAllowed = 405_004, "PATCH Not Allowed";
    DeleteNotAllowed = 405_005, "DELETE Not Allowed";
    OptionsNotAllowed = 405_006, "OPTIONS Not Allowed";
    RequestTimeout = 408_000, "Request Timeout";
    MarketDataExportTimeout = 408_200, "Market Data Export Request Timeout";
    DuplicateBatchRequest = 409, "Duplicate Batch Request";
    Conflict = 409_000, "Conflict";
    DuplicateRequest = 409_100, "Duplicate Request";
    ResourceAlreadyExists = 409_101, "Resource Already Exists";
    InsufficientBuyingPower = 409_102, "Insufficient Buying Power";
    InsufficientAccountBalance = 409_103, "Insufficient Account Balance";
    RateLimitExceeded = 429_000, "Rate Limit Exceeded";
    DailyOrderLimitExceeded = 429_100, "Daily Order Limit Exceeded";
    VolumeExceedsCurrentRoom = 429_101, "Volume Exceeds Current Room";
    DepositExceedsApprovalLimit = 429_102, "Deposit Exceeds Approval Limit";
    ExceedsAllowableInputLimit = 429_103, "Exceeds Allowable Input Limit";
    ExceedsAllowableApprovalLimit = 429_104, "Exceeds Allowable Approval Limit";
    OverrideExceedsWithdrawableAmount = 429_105, "Override Exceeds Withdrawable Amount";
    ExceedsWithdrawableAmount = 429_106, "Exceeds Withdrawable Amount";
    ExceedsDailyTransactionLimit = 429_107, "Exceeds Daily Transaction Limit";
    ExceedsMaximumWithdrawableAmount = 429_108, "Exceeds Maximum Withdrawable Amount";
    ExceedsAccountTransactionLimit = 429_109, "Exceeds Account Transaction Limit";
    ExceedsMaximumWithdrawableBalance = 429_110, "Exceeds Maximum Withdrawable Balance";
    ExceedsAllowableInputLimit111 = 429_111, "Exceeds Allowable Input Limit (429111)";
    ExceedsAllowableInputAmount = 429_112, "Exceeds Allowable Input Amount";
    ExceedsDailyTransactionLimit113 = 429_113, "Exceeds Daily Transaction Limit (429113)";
    ExceedsTransferableAmount = 429_114, "Exceeds Transferable Amount";
    ExceedsMaximumTransferableAmount = 429_115, "Exceeds Maximum Transferable Amount";
    ExceedsDailyTransferableLimit = 429_116, "Exceeds Daily Transferable Limit";
    TransferExceedsNapasLimit = 429_117, "Transfer Exceeds Napas Limit";
    ExceedsMaximumWithdrawableBalance118 = 429_118,
        "Exceeds Maximum Withdrawable Balance (429118)";
    CashAdvanceExceedsMaximum = 429_119, "Cash Advance Exceeds Maximum Limit";
    ReceivedCashAdvanceExceedsMaximum = 429_120, "Received Cash Advance Exceeds Maximum";
    BlockedVolumeExceedsAvailable = 429_121, "Blocked Volume Exceeds Available Balance";
    ReleasedVolumeExceedsBlocked = 429_122, "Released Volume Exceeds Blocked Volume";
    ExceedsAllowableInputLimit123 = 429_123, "Exceeds Allowable Input Limit (429123)";
    ExceedsAutoApprovalCashAdvance = 429_124, "Exceeds Auto-Approval Cash Advance";
    RejectionReasonTooLong = 429_125, "Rejection Reason Length Exceeded";
    VolumeExceedsLimit = 429_126, "Volume Exceeds Limit";
    InternalServerError = 500_000, "Internal Server Error";
    DatabaseExecutionError = 500_002, "Database Execution Error";
    TokenClientIdMismatch = 500_100, "Token Verification Failed: Client ID Mismatch";
    TokenScopesMissing = 500_101, "Token Verification Failed: Scopes Missing";
    MarketDataPersistenceError = 500_200, "Market Data Persistence Error";
    BadGateway = 502_000, "Bad Gateway";
    CashPositionNoData = 502_100, "CashPosition No Data";
    CashBalanceNoData = 502_101, "GetCashBalance No Data";
    AssetsExtendsFailed = 502_102, "GetAssetsExtends Failed";
    ServiceUnavailable = 503_000, "Service Unavailable";
    SystemMaintenance = 503_001, "System Maintenance Window";
    GatewayTimeout = 504_000, "Gateway Timeout";
    IndexSummaryNeedsOneOf = 504_207, "indexSummary needs exactly one of board or index";
    SecuritiesNeedsOneOf = 504_208, "securitiesByBoard needs exactly one of symbol, board, index";
    SecuritiesSummaryNeedsOneOf = 504_209, "securitiesSummary needs exactly one of symbol, index";
    CorporateActionsRangeMissing = 504_210, "Corporate actions: from or to missing";
    CorporateActionsRangeTooLong = 504_211, "Corporate actions: range longer than 5 days";
    CorporateActionsInvalidLanguage = 504_212, "Corporate actions: language must be vn or en";
    CorporateActionsInvalidPageIndex = 504_213, "Corporate actions: pageIndex below 1";
    CorporateActionsInvalidPageSize = 504_214, "Corporate actions: pageSize not 10, 20, 50, 100";
}

impl ApiErrorCode {
    /// The access token is missing, invalid or expired: refresh and retry once on reads.
    pub const fn is_access_token_error(self) -> bool {
        matches!(
            self,
            Self::MissingAuthorizationHeader
                | Self::Unauthorized
                | Self::AccessTokenMissing
                | Self::AccessTokenInvalid
                | Self::AccessTokenExpired
                | Self::AccessTokenNotYetValid
                | Self::TokenSignatureInvalid
        )
    }

    /// The refresh token is dead (`401101`–`401104`, `401106`–`401108`).
    pub const fn is_refresh_token_error(self) -> bool {
        matches!(
            self,
            Self::InvalidRefreshToken
                | Self::RefreshTokenMissingClient
                | Self::RefreshTokenRevokedOrExpired
                | Self::InvalidApiKeyReference
                | Self::InvalidTokenType
                | Self::RefreshTokenMissingJti
                | Self::RefreshTokenMissingKeyId
        )
    }

    /// Smart OTP push-approval is still awaiting user confirmation (`401114`).
    pub const fn is_smart_otp_pending(self) -> bool {
        matches!(self, Self::PushApprovalPending)
    }

    /// Smart OTP push-approval can no longer succeed (`401113`, `401115`, `401116`).
    pub const fn is_smart_otp_terminal(self) -> bool {
        matches!(
            self,
            Self::PushApprovalNotFound
                | Self::PushApprovalRejected
                | Self::PushApprovalForeignTransaction
        )
    }

    /// The server rejected the request signature (`401006`, `401008`, `401011`–`401015`).
    pub const fn is_signature_error(self) -> bool {
        matches!(
            self,
            Self::SdkSignatureMissing
                | Self::SdkSignatureBodyEmpty
                | Self::MissingSignatureHeader
                | Self::SignatureNotRecognized
                | Self::SignatureNotValid
                | Self::SignatureNotMatched
                | Self::SignatureBodyEmpty
        )
    }

    /// A temporary server-side condition; an idempotent read may succeed later.
    pub const fn is_transient(self) -> bool {
        matches!(
            self,
            Self::RateLimitExceeded
                | Self::RequestTimeout
                | Self::MarketDataExportTimeout
                | Self::InternalServerError
                | Self::BadGateway
                | Self::ServiceUnavailable
                | Self::SystemMaintenance
                | Self::GatewayTimeout
        )
    }

    /// The request id was already used today (`409100`, `409`).
    pub const fn is_duplicate(self) -> bool {
        matches!(
            self,
            Self::DuplicateRequest | Self::DuplicateBatchRequest | Self::Conflict
        )
    }
}

impl fmt::Display for ApiErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.code(), self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_and_classification() {
        for code in [
            400_171, 401_004, 401_006, 401_008, 408_200, 409_100, 429_100, 500_200, 504_211,
        ] {
            let typed = ApiErrorCode::from_code(code);
            assert!(!matches!(typed, ApiErrorCode::Other(_)), "{code} is mapped");
            assert_eq!(typed.code(), code);
        }
        for code in 429_100..=429_126 {
            assert!(
                !matches!(ApiErrorCode::from_code(code), ApiErrorCode::Other(_)),
                "{code} is mapped"
            );
        }
        assert!(ApiErrorCode::DuplicateRequest.is_duplicate());
        assert!(ApiErrorCode::PushApprovalPending.is_smart_otp_pending());
        assert!(ApiErrorCode::PushApprovalRejected.is_smart_otp_terminal());
        assert!(ApiErrorCode::RefreshTokenRevokedOrExpired.is_refresh_token_error());
        assert!(ApiErrorCode::MarketDataExportTimeout.is_transient());
        assert!(ApiErrorCode::SignatureNotValid.is_signature_error());
        assert_eq!(
            ApiErrorCode::from_code(999_999),
            ApiErrorCode::Other(999_999)
        );
    }

    #[test]
    fn private_messages_stay_out_of_debug_and_display() {
        let error = Error::Api(ApiError {
            http_status: 403,
            code: ApiErrorCode::AccountNotAuthorized,
            message: "Forbidden: account '003C123456' is not authorized".to_owned(),
            endpoint: "/api/v3/trading/position".to_owned(),
            private: true,
        });
        assert!(!format!("{error}").contains("003C123456"));
        assert!(!format!("{error:?}").contains("003C123456"));
    }
}
