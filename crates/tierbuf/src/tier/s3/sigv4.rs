//! AWS Signature Version 4 request signing.
//!
//! The signer is deliberately independent of an HTTP client. Callers provide
//! the request components exactly as they will be sent and add the returned
//! authorization value to the outgoing request.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{Result, TierBufError};

type HmacSha256 = Hmac<Sha256>;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const TERMINATOR: &str = "aws4_request";
const SECONDS_PER_DAY: i64 = 86_400;
const MIN_AMZ_UNIX_SECONDS: i64 = -62_167_219_200;
const MAX_AMZ_UNIX_SECONDS: i64 = 253_402_300_799;
const REDACTED: &str = "<redacted>";

/// Immutable AWS credentials used to sign requests.
#[derive(Clone)]
pub struct Credentials {
    /// AWS access-key identifier.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: String,
    /// Optional session token for temporary credentials.
    pub session_token: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let session_token = self.session_token.as_ref().map(|_| REDACTED);
        formatter
            .debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &REDACTED)
            .field("session_token", &session_token)
            .finish()
    }
}

/// One header name/value pair included in the signature.
pub type Header = (String, String);

/// Inputs for signing one HTTP request.
pub struct SignRequest<'a> {
    /// HTTP method, such as `GET`, `PUT`, or `DELETE`.
    pub method: &'a str,
    /// Authority value sent in the HTTP `Host` header.
    pub host: &'a str,
    /// URI-encoded absolute request path, beginning with `/`.
    pub path: &'a str,
    /// Unencoded query-string name/value pairs.
    pub query: &'a [(String, String)],
    /// Headers covered by the signature, excluding `Host`.
    pub headers: &'a [Header],
    /// Lowercase SHA-256 payload digest or `UNSIGNED-PAYLOAD`.
    pub payload_sha256_hex: &'a str,
    /// AWS region used in the credential scope.
    pub region: &'a str,
    /// AWS service name used in the credential scope.
    pub service: &'a str,
    /// UTC request timestamp.
    pub timestamp: AmzTimestamp,
}

/// UTC timestamp pre-formatted for Signature Version 4.
#[derive(Clone, Copy, Debug)]
pub struct AmzTimestamp {
    bytes: [u8; 16],
}

impl AmzTimestamp {
    /// Formats a system timestamp as `YYYYMMDDTHHMMSSZ`.
    ///
    /// Leap seconds are ignored. Values outside the four-digit year range
    /// representable by SigV4 are clamped to that range.
    #[must_use]
    pub fn from_system_time(time: SystemTime) -> Self {
        let unix_seconds = unix_seconds(time).clamp(MIN_AMZ_UNIX_SECONDS, MAX_AMZ_UNIX_SECONDS);
        let days = unix_seconds.div_euclid(SECONDS_PER_DAY);
        let second_of_day = unix_seconds.rem_euclid(SECONDS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        let hour = (second_of_day / 3_600) as u32;
        let minute = (second_of_day % 3_600 / 60) as u32;
        let second = (second_of_day % 60) as u32;

        Self::from_components(year as u32, month, day, hour, minute, second)
    }

    /// Parses a test timestamp in `YYYYMMDDTHHMMSSZ` form.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source if the value
    /// is not an exact, valid UTC calendar timestamp.
    pub fn from_amz_date_str(value: &str) -> Result<Self> {
        let bytes = value.as_bytes();
        if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
            return Err(invalid_input(
                "SigV4 timestamp must use YYYYMMDDTHHMMSSZ format",
            ));
        }

        let year = parse_decimal(&bytes[0..4])?;
        let month = parse_decimal(&bytes[4..6])?;
        let day = parse_decimal(&bytes[6..8])?;
        let hour = parse_decimal(&bytes[9..11])?;
        let minute = parse_decimal(&bytes[11..13])?;
        let second = parse_decimal(&bytes[13..15])?;
        validate_utc_components(year, month, day, hour, minute, second)?;

        let mut timestamp = [0_u8; 16];
        timestamp.copy_from_slice(bytes);
        Ok(Self { bytes: timestamp })
    }

    /// Returns the full timestamp in `YYYYMMDDTHHMMSSZ` form.
    #[must_use]
    pub fn amz_date(&self) -> &str {
        ascii_str(&self.bytes)
    }

    /// Returns the calendar date in `YYYYMMDD` form.
    #[must_use]
    pub fn date(&self) -> &str {
        ascii_str(&self.bytes[..8])
    }

    fn from_components(
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> Self {
        let mut bytes = [b'0'; 16];
        bytes[8] = b'T';
        bytes[15] = b'Z';
        write_decimal(&mut bytes[0..4], year);
        write_decimal(&mut bytes[4..6], month);
        write_decimal(&mut bytes[6..8], day);
        write_decimal(&mut bytes[9..11], hour);
        write_decimal(&mut bytes[11..13], minute);
        write_decimal(&mut bytes[13..15], second);
        Self { bytes }
    }
}

/// Creates a Signature Version 4 `Authorization` header value.
///
/// The caller must include `x-amz-date`, `x-amz-content-sha256`, and, for
/// temporary credentials, `x-amz-security-token` in [`SignRequest::headers`]
/// before signing.
///
/// # Errors
///
/// Returns [`TierBufError::Io`] with an invalid-input source when a request
/// component cannot be represented canonically.
pub fn sign(request: &SignRequest<'_>, credentials: &Credentials) -> Result<String> {
    validate_sign_request(request)?;

    let canonical_uri = canonical_uri(request.path);
    let canonical_query = canonical_query(request.query);
    let (canonical_headers, signed_headers) = canonical_headers(request.host, request.headers)?;
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers,
        request.payload_sha256_hex
    );

    let scope = format!(
        "{}/{}/{}/{}",
        request.timestamp.date(),
        request.region,
        request.service,
        TERMINATOR
    );
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        ALGORITHM,
        request.timestamp.amz_date(),
        scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let secret = format!("AWS4{}", credentials.secret_access_key);
    let date_key = hmac_sha256(secret.as_bytes(), request.timestamp.date().as_bytes())?;
    let region_key = hmac_sha256(&date_key, request.region.as_bytes())?;
    let service_key = hmac_sha256(&region_key, request.service.as_bytes())?;
    let signing_key = hmac_sha256(&service_key, TERMINATOR.as_bytes())?;
    let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes())?);

    Ok(format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        ALGORITHM, credentials.access_key_id, scope, signed_headers, signature
    ))
}

/// Returns the lowercase hexadecimal SHA-256 digest of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}

/// URI-encodes bytes according to the Signature Version 4 rules.
///
/// RFC 3986 unreserved characters are preserved. `/` is also preserved when
/// `encode_slash` is `false`.
#[must_use]
pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut encoded = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        if is_unreserved(byte) || (!encode_slash && byte == b'/') {
            encoded.push(char::from(byte));
        } else {
            push_percent_encoded(&mut encoded, byte);
        }
    }
    encoded
}

/// Parses an IMDS-style ISO-8601 timestamp into a system timestamp.
///
/// This remains crate-internal because it exists to share the civil-date
/// conversion with the credential provider rather than expand the public API.
pub(super) fn system_time_from_iso8601(value: &str) -> Result<SystemTime> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(invalid_input(
            "UTC timestamp must use YYYY-MM-DDTHH:MM:SSZ format",
        ));
    }

    let year = parse_decimal(&bytes[0..4])?;
    let month = parse_decimal(&bytes[5..7])?;
    let day = parse_decimal(&bytes[8..10])?;
    let hour = parse_decimal(&bytes[11..13])?;
    let minute = parse_decimal(&bytes[14..16])?;
    let second = parse_decimal(&bytes[17..19])?;
    validate_utc_components(year, month, day, hour, minute, second)?;

    let days = days_from_civil(i64::from(year), month, day);
    let seconds = days * SECONDS_PER_DAY
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + i64::from(second);
    if seconds >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds.unsigned_abs()))
            .ok_or_else(|| invalid_input("UTC timestamp is outside the SystemTime range"))
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
            .ok_or_else(|| invalid_input("UTC timestamp is outside the SystemTime range"))
    }
}

fn validate_sign_request(request: &SignRequest<'_>) -> Result<()> {
    if request.method.is_empty() {
        return Err(invalid_input("SigV4 HTTP method must not be empty"));
    }
    if request.host.is_empty() {
        return Err(invalid_input("SigV4 host must not be empty"));
    }
    if !request.path.starts_with('/') {
        return Err(invalid_input("SigV4 request path must begin with '/'"));
    }
    if request.region.is_empty() {
        return Err(invalid_input("SigV4 region must not be empty"));
    }
    if request.service.is_empty() {
        return Err(invalid_input("SigV4 service must not be empty"));
    }
    Ok(())
}

fn canonical_uri(path: &str) -> String {
    let mut canonical = String::with_capacity(path.len());
    for (index, segment) in path.split('/').enumerate() {
        if index != 0 {
            canonical.push('/');
        }
        encode_path_segment(segment, &mut canonical);
    }
    canonical
}

fn encode_path_segment(segment: &str, encoded: &mut String) {
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if is_unreserved(byte) {
            encoded.push(char::from(byte));
            index += 1;
        } else if byte == b'%'
            && index + 2 < bytes.len()
            && bytes[index + 1].is_ascii_hexdigit()
            && bytes[index + 2].is_ascii_hexdigit()
        {
            encoded.push('%');
            encoded.push(char::from(bytes[index + 1].to_ascii_uppercase()));
            encoded.push(char::from(bytes[index + 2].to_ascii_uppercase()));
            index += 3;
        } else {
            push_percent_encoded(encoded, byte);
            index += 1;
        }
    }
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut encoded: Vec<_> = query
        .iter()
        .map(|(name, value)| (uri_encode(name, true), uri_encode(value, true)))
        .collect();
    encoded.sort_unstable();
    encoded
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(host: &str, headers: &[Header]) -> Result<(String, String)> {
    let mut by_name = BTreeMap::<String, Vec<String>>::new();
    by_name.insert("host".to_owned(), vec![normalize_header_value(host)]);

    for (name, value) in headers {
        if !valid_header_name(name) {
            return Err(invalid_input("SigV4 header name is invalid"));
        }
        let canonical_name = name.to_ascii_lowercase();
        if canonical_name == "host" {
            return Err(invalid_input(
                "SigV4 headers must not contain host; use SignRequest::host",
            ));
        }
        by_name
            .entry(canonical_name)
            .or_default()
            .push(normalize_header_value(value));
    }

    let mut canonical = String::new();
    for (name, values) in &by_name {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
    }
    let signed = by_name.keys().cloned().collect::<Vec<_>>().join(";");
    Ok((canonical, signed))
}

fn valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn normalize_header_value(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|_| invalid_input("SigV4 HMAC key is invalid"))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn push_percent_encoded(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    output.push('%');
    output.push(char::from(HEX[usize::from(byte >> 4)]));
    output.push(char::from(HEX[usize::from(byte & 0x0f)]));
}

const fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(error) => {
            let duration = error.duration();
            let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
            if duration.subsec_nanos() == 0 {
                seconds.saturating_neg()
            } else {
                seconds.saturating_neg().saturating_sub(1)
            }
        }
    }
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let month_prime = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn parse_decimal(bytes: &[u8]) -> Result<u32> {
    let mut value = 0_u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return Err(invalid_input("UTC timestamp contains a non-digit field"));
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Ok(value)
}

fn validate_utc_components(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Result<()> {
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(invalid_input(
            "UTC timestamp contains an invalid date or time",
        ));
    }
    Ok(())
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn write_decimal(target: &mut [u8], mut value: u32) {
    for digit in target.iter_mut().rev() {
        *digit = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

fn ascii_str(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap_or_default()
}

fn invalid_input(message: &'static str) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn amz_timestamp_formats_known_epochs() {
        let cases = [
            (0, "19700101T000000Z"),
            (951_782_400, "20000229T000000Z"),
            (1_609_459_200, "20210101T000000Z"),
            (1_709_164_800, "20240229T000000Z"),
        ];

        for (seconds, expected) in cases {
            let timestamp =
                AmzTimestamp::from_system_time(UNIX_EPOCH + Duration::from_secs(seconds));
            assert_eq!(timestamp.amz_date(), expected);
            assert_eq!(timestamp.date(), &expected[..8]);
        }

        let before_epoch = AmzTimestamp::from_system_time(UNIX_EPOCH - Duration::from_secs(1));
        assert_eq!(before_epoch.amz_date(), "19691231T235959Z");
    }

    #[test]
    fn uri_encode_matches_sigv4_rules() {
        assert_eq!(uri_encode("a b", true), "a%20b");
        assert_eq!(uri_encode("/photos/~me", false), "/photos/~me");
        assert_eq!(uri_encode("/photos/~me", true), "%2Fphotos%2F~me");
        assert_eq!(uri_encode("한글", true), "%ED%95%9C%EA%B8%80");
    }

    #[test]
    fn aws_documented_iam_example_signature() {
        let timestamp = valid_amz_timestamp("20150830T123600Z");
        let headers = vec![
            (
                "content-type".to_owned(),
                "application/x-www-form-urlencoded; charset=utf-8".to_owned(),
            ),
            ("x-amz-date".to_owned(), timestamp.amz_date().to_owned()),
        ];
        let query = vec![
            ("Version".to_owned(), "2010-05-08".to_owned()),
            ("Action".to_owned(), "ListUsers".to_owned()),
        ];
        let request = SignRequest {
            method: "GET",
            host: "iam.amazonaws.com",
            path: "/",
            query: &query,
            headers: &headers,
            payload_sha256_hex: EMPTY_SHA256,
            region: "us-east-1",
            service: "iam",
            timestamp,
        };
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: None,
        };

        let authorization = valid_signature(&request, &credentials);
        assert_eq!(
            authorization,
            concat!(
                "AWS4-HMAC-SHA256 ",
                "Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, ",
                "SignedHeaders=content-type;host;x-amz-date, ",
                "Signature=5d672d79c15b13162d9279b0855cfba",
                "6789a8edb4c82c400e06b5924a6f2b5d7"
            )
        );
    }

    #[test]
    fn session_token_changes_signed_headers() {
        let timestamp = valid_amz_timestamp("20260731T031500Z");
        let headers = vec![
            ("x-amz-date".to_owned(), timestamp.amz_date().to_owned()),
            (
                "x-amz-security-token".to_owned(),
                "temporary-token".to_owned(),
            ),
        ];
        let request = SignRequest {
            method: "GET",
            host: "example-bucket.s3.us-east-1.amazonaws.com",
            path: "/page",
            query: &[],
            headers: &headers,
            payload_sha256_hex: EMPTY_SHA256,
            region: "us-east-1",
            service: "s3",
            timestamp,
        };
        let credentials = Credentials {
            access_key_id: "temporary-access-key".to_owned(),
            secret_access_key: "temporary-secret-key".to_owned(),
            session_token: Some("temporary-token".to_owned()),
        };

        let authorization = valid_signature(&request, &credentials);
        assert!(authorization.contains("SignedHeaders=host;x-amz-date;x-amz-security-token"));
    }

    #[test]
    fn debug_redacts_secret() {
        let credentials = Credentials {
            access_key_id: "visible-access-key".to_owned(),
            secret_access_key: "do-not-print-secret".to_owned(),
            session_token: Some("do-not-print-token".to_owned()),
        };

        let debug = format!("{credentials:?}");
        assert!(debug.contains("visible-access-key"));
        assert!(debug.contains(REDACTED));
        assert!(!debug.contains("do-not-print-secret"));
        assert!(!debug.contains("do-not-print-token"));
    }

    #[test]
    fn timestamps_reject_invalid_calendar_values() {
        assert_invalid_input(AmzTimestamp::from_amz_date_str("20230229T000000Z"));
        assert_invalid_input(AmzTimestamp::from_amz_date_str("20240101 000000Z"));
        assert_invalid_input(system_time_from_iso8601("2024-13-01T00:00:00Z"));
    }

    #[test]
    fn iso8601_parser_reuses_civil_date_conversion() {
        let parsed = match system_time_from_iso8601("2024-02-29T00:00:00Z") {
            Ok(time) => time,
            Err(error) => panic!("expected valid leap-day timestamp: {error}"),
        };
        assert_eq!(parsed, UNIX_EPOCH + Duration::from_secs(1_709_164_800));
    }

    #[test]
    fn canonical_path_preserves_existing_percent_encoding() {
        assert_eq!(
            canonical_uri("/folder/already%20encoded/raw space"),
            "/folder/already%20encoded/raw%20space"
        );
    }

    fn valid_amz_timestamp(value: &str) -> AmzTimestamp {
        match AmzTimestamp::from_amz_date_str(value) {
            Ok(timestamp) => timestamp,
            Err(error) => panic!("expected valid SigV4 timestamp: {error}"),
        }
    }

    fn valid_signature(request: &SignRequest<'_>, credentials: &Credentials) -> String {
        match sign(request, credentials) {
            Ok(authorization) => authorization,
            Err(error) => panic!("expected request to sign: {error}"),
        }
    }

    fn assert_invalid_input<T>(result: Result<T>) {
        match result {
            Err(TierBufError::Io(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            }
            Err(error) => panic!("expected invalid-input I/O error, got {error}"),
            Ok(_) => panic!("expected invalid-input error"),
        }
    }
}
