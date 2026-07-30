//! AWS credential resolution and refresh for the blocking S3 client.

use std::env;
use std::io;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use super::sigv4::{Credentials, system_time_from_iso8601, uri_encode};
use crate::{Result, TierBufError};

const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";
const IMDS_TOKEN_PATH: &str = "/latest/api/token";
const IMDS_ROLE_PATH: &str = "/latest/meta-data/iam/security-credentials/";
const IMDS_TOKEN_TTL_SECONDS: &str = "21600";
const IMDS_TIMEOUT: Duration = Duration::from_secs(1);
const REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);

/// How the S3 tier obtains AWS credentials.
#[derive(Clone, Debug, Default)]
pub enum CredentialSource {
    /// Read `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and the optional
    /// `AWS_SESSION_TOKEN` from the process environment.
    Environment,
    /// Use explicit, fixed credentials. This is primarily useful for tests and
    /// S3-compatible services such as MinIO.
    Static(Credentials),
    /// Resolve an EC2 instance role through IMDSv2 at `endpoint`.
    Imds {
        /// Base URL of the IMDSv2 service.
        endpoint: String,
    },
    /// Try the environment first, then EC2 IMDSv2 at the standard endpoint.
    #[default]
    Auto,
}

#[derive(Clone)]
struct CachedCreds {
    creds: Credentials,
    expires_at: Option<SystemTime>,
}

impl CachedCreds {
    fn needs_refresh(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| {
            expires_at
                .duration_since(now)
                .map_or(true, |remaining| remaining <= REFRESH_WINDOW)
        })
    }

    fn is_valid(&self, now: SystemTime) -> bool {
        self.expires_at.is_none_or(|expires_at| expires_at > now)
    }
}

/// Thread-safe, caching AWS credential resolver.
///
/// Temporary credentials are refreshed when they are within five minutes of
/// expiration. Concurrent callers share one cache and serialize refreshes so
/// an expiring instance role does not cause an IMDS request stampede.
pub struct CredentialProvider {
    source: CredentialSource,
    agent: ureq::Agent,
    cached: Mutex<Option<CachedCreds>>,
}

impl CredentialProvider {
    /// Creates a provider for `source`.
    #[must_use]
    pub fn new(source: CredentialSource) -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(IMDS_TIMEOUT))
            .build();
        Self {
            source,
            agent: config.into(),
            cached: Mutex::new(None),
        }
    }

    /// Returns usable credentials, resolving or refreshing them when needed.
    ///
    /// If a refresh fails while the previous temporary credentials have not
    /// yet expired, the previous credentials are returned. Once that cache has
    /// expired, the resolution error is returned instead.
    pub fn credentials(&self) -> Result<Credentials> {
        let now = SystemTime::now();
        let mut cached = match self.cached.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(current) = cached.as_ref()
            && !current.needs_refresh(now)
        {
            return Ok(current.creds.clone());
        }

        match self.resolve() {
            Ok(resolved) => {
                let credentials = resolved.creds.clone();
                *cached = Some(resolved);
                Ok(credentials)
            }
            Err(error) => {
                if let Some(current) = cached.as_ref()
                    && current.is_valid(SystemTime::now())
                {
                    return Ok(current.creds.clone());
                }
                Err(error)
            }
        }
    }

    fn resolve(&self) -> Result<CachedCreds> {
        match &self.source {
            CredentialSource::Environment => credentials_from_environment(),
            CredentialSource::Static(credentials) => Ok(CachedCreds {
                creds: credentials.clone(),
                expires_at: None,
            }),
            CredentialSource::Imds { endpoint } => self.credentials_from_imds(endpoint),
            CredentialSource::Auto => self.resolve_auto(DEFAULT_IMDS_ENDPOINT),
        }
    }

    fn resolve_auto(&self, imds_endpoint: &str) -> Result<CachedCreds> {
        match optional_credentials_from_environment()? {
            Some(credentials) => Ok(credentials),
            None => self.credentials_from_imds(imds_endpoint),
        }
    }

    fn credentials_from_imds(&self, endpoint: &str) -> Result<CachedCreds> {
        let endpoint = normalized_endpoint(endpoint)?;
        let token_url = format!("{endpoint}{IMDS_TOKEN_PATH}");
        let mut token_response = self
            .agent
            .put(&token_url)
            .header(
                "x-aws-ec2-metadata-token-ttl-seconds",
                IMDS_TOKEN_TTL_SECONDS,
            )
            .send_empty()
            .map_err(|error| ureq_error("IMDSv2 token request", error))?;
        ensure_success("IMDSv2 token request", token_response.status().as_u16())?;
        let token = token_response
            .body_mut()
            .read_to_string()
            .map_err(|error| ureq_error("reading IMDSv2 token response", error))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(invalid_data("IMDSv2 returned an empty metadata token"));
        }

        let role_url = format!("{endpoint}{IMDS_ROLE_PATH}");
        let mut role_response = self
            .agent
            .get(&role_url)
            .header("x-aws-ec2-metadata-token", token)
            .call()
            .map_err(|error| ureq_error("IMDSv2 role-name request", error))?;
        ensure_success("IMDSv2 role-name request", role_response.status().as_u16())?;
        let role_body = role_response
            .body_mut()
            .read_to_string()
            .map_err(|error| ureq_error("reading IMDSv2 role-name response", error))?;
        let role = role_body.lines().next().map(str::trim).unwrap_or_default();
        if role.is_empty() {
            return Err(invalid_data("IMDSv2 returned an empty instance-role name"));
        }

        let encoded_role = uri_encode(role, true);
        let credentials_url = format!("{endpoint}{IMDS_ROLE_PATH}{encoded_role}");
        let mut credentials_response = self
            .agent
            .get(&credentials_url)
            .header("x-aws-ec2-metadata-token", token)
            .call()
            .map_err(|error| ureq_error("IMDSv2 role-credentials request", error))?;
        ensure_success(
            "IMDSv2 role-credentials request",
            credentials_response.status().as_u16(),
        )?;
        let credentials_body = credentials_response
            .body_mut()
            .read_to_string()
            .map_err(|error| ureq_error("reading IMDSv2 credentials response", error))?;
        parse_imds_credentials(&credentials_body)
    }
}

fn optional_credentials_from_environment() -> Result<Option<CachedCreds>> {
    let access_key_present = env::var_os("AWS_ACCESS_KEY_ID").is_some();
    let secret_key_present = env::var_os("AWS_SECRET_ACCESS_KEY").is_some();
    if !access_key_present && !secret_key_present {
        return Ok(None);
    }
    credentials_from_environment().map(Some)
}

fn credentials_from_environment() -> Result<CachedCreds> {
    let access_key_id = required_environment_variable("AWS_ACCESS_KEY_ID")?;
    let secret_access_key = required_environment_variable("AWS_SECRET_ACCESS_KEY")?;
    let session_token = optional_environment_variable("AWS_SESSION_TOKEN")?;

    Ok(CachedCreds {
        creds: Credentials {
            access_key_id,
            secret_access_key,
            session_token,
        },
        expires_at: None,
    })
}

fn required_environment_variable(name: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) => Err(invalid_input(format!(
            "environment variable {name} must not be empty"
        ))),
        Err(env::VarError::NotPresent) => Err(TierBufError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        ))),
        Err(env::VarError::NotUnicode(_)) => Err(invalid_data(format!(
            "environment variable {name} is not valid Unicode"
        ))),
    }
}

fn optional_environment_variable(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(invalid_data(format!(
            "environment variable {name} is not valid Unicode"
        ))),
    }
}

fn normalized_endpoint(endpoint: &str) -> Result<&str> {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.is_empty() {
        return Err(invalid_input("IMDS endpoint must not be empty"));
    }
    Ok(endpoint)
}

fn parse_imds_credentials(body: &str) -> Result<CachedCreds> {
    let value: Value = serde_json::from_str(body).map_err(|error| {
        invalid_data(format!(
            "IMDSv2 returned malformed role credentials JSON: {error}"
        ))
    })?;
    let access_key_id = required_json_string(&value, "AccessKeyId")?.to_owned();
    let secret_access_key = required_json_string(&value, "SecretAccessKey")?.to_owned();
    let token = required_json_string(&value, "Token")?.to_owned();
    let expiration = required_json_string(&value, "Expiration")?;
    let expires_at = system_time_from_iso8601(expiration)?;

    Ok(CachedCreds {
        creds: Credentials {
            access_key_id,
            secret_access_key,
            session_token: Some(token),
        },
        expires_at: Some(expires_at),
    })
}

fn required_json_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    match value.get(field).and_then(Value::as_str) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(invalid_data(format!(
            "IMDSv2 role credentials are missing non-empty field {field}"
        ))),
    }
}

fn ensure_success(context: &str, status: u16) -> Result<()> {
    if (200..300).contains(&status) {
        return Ok(());
    }
    Err(status_error(context, status))
}

fn ureq_error(context: &str, error: ureq::Error) -> TierBufError {
    let kind = match &error {
        ureq::Error::StatusCode(status) => status_kind(*status),
        ureq::Error::Timeout(_) => io::ErrorKind::TimedOut,
        ureq::Error::Io(error) => error.kind(),
        ureq::Error::HostNotFound => io::ErrorKind::NotFound,
        ureq::Error::ConnectionFailed => io::ErrorKind::ConnectionRefused,
        _ => io::ErrorKind::Other,
    };
    TierBufError::Io(io::Error::new(kind, format!("{context}: {error}")))
}

fn status_error(context: &str, status: u16) -> TierBufError {
    TierBufError::Io(io::Error::new(
        status_kind(status),
        format!("{context}: HTTP status {status}"),
    ))
}

fn status_kind(status: u16) -> io::ErrorKind {
    match status {
        401 | 403 => io::ErrorKind::PermissionDenied,
        404 => io::ErrorKind::NotFound,
        408 => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    }
}

fn invalid_input(message: impl Into<String>) -> TierBufError {
    TierBufError::Io(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn invalid_data(message: impl Into<String>) -> TierBufError {
    TierBufError::Io(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::*;

    const CHILD_MODE: &str = "TIERBUF_CREDENTIAL_TEST_CHILD";
    const CHILD_ENDPOINT: &str = "TIERBUF_CREDENTIAL_TEST_ENDPOINT";
    const CHILD_TEST_NAME: &str = "tier::s3::credentials::tests::environment_source_child_process";

    #[derive(Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
    }

    struct StubResponse {
        status: u16,
        body: String,
    }

    struct StubServer {
        endpoint: String,
        requests: Arc<AtomicUsize>,
        handle: JoinHandle<()>,
    }

    impl StubServer {
        fn start<F>(expected_requests: usize, responder: F) -> Self
        where
            F: Fn(usize, &RecordedRequest) -> StubResponse + Send + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind local IMDS stub");
            listener
                .set_nonblocking(true)
                .expect("make local IMDS stub nonblocking");
            let address = listener.local_addr().expect("read local IMDS address");
            let requests = Arc::new(AtomicUsize::new(0));
            let thread_requests = Arc::clone(&requests);
            let handle = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                while thread_requests.load(Ordering::Acquire) < expected_requests {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream
                                .set_nonblocking(false)
                                .expect("make accepted IMDS connection blocking");
                            stream
                                .set_read_timeout(Some(Duration::from_secs(1)))
                                .expect("set IMDS stub read timeout");
                            let index = thread_requests.fetch_add(1, Ordering::AcqRel);
                            let request =
                                read_request(&mut stream).expect("read IMDS stub request");
                            let response = responder(index, &request);
                            write_response(&mut stream, response)
                                .expect("write IMDS stub response");
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "timed out waiting for {expected_requests} IMDS requests"
                            );
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("IMDS stub accept failed: {error}"),
                    }
                }
            });
            Self {
                endpoint: format!("http://{address}"),
                requests,
                handle,
            }
        }

        fn finish(self) {
            self.handle.join().expect("join local IMDS stub");
        }
    }

    fn read_request(stream: &mut TcpStream) -> io::Result<RecordedRequest> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if bytes.len() > 64 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stub request headers are too large",
                ));
            }
        }

        let request = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut lines = request.split("\r\n");
        let request_line = lines
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_owned();
        let path = request_parts.next().unwrap_or_default().to_owned();
        let mut headers = HashMap::new();
        for line in lines.take_while(|line| !line.is_empty()) {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
            }
        }
        Ok(RecordedRequest {
            method,
            path,
            headers,
        })
    }

    fn write_response(stream: &mut TcpStream, response: StubResponse) -> io::Result<()> {
        let reason = match response.status {
            200 => "OK",
            401 => "Unauthorized",
            500 => "Internal Server Error",
            _ => "Test Response",
        };
        write!(
            stream,
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{}",
            response.status,
            reason,
            response.body.len(),
            response.body
        )?;
        stream.flush()
    }

    fn credentials_json(access_key_id: &str, expiration: &str) -> String {
        format!(
            r#"{{"AccessKeyId":"{access_key_id}","SecretAccessKey":"secret","Token":"token-value","Expiration":"{expiration}"}}"#
        )
    }

    fn assert_io_kind(error: TierBufError, expected: io::ErrorKind) {
        match error {
            TierBufError::Io(error) => assert_eq!(error.kind(), expected),
            other => panic!("expected I/O error, got {other:?}"),
        }
    }

    fn run_environment_child(mode: &str, endpoint: Option<&str>) {
        let executable = std::env::current_exe().expect("locate test executable");
        let mut command = Command::new(executable);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .env(CHILD_MODE, mode)
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN");
        if !matches!(mode, "missing-secret" | "auto-missing-secret") {
            command
                .env("AWS_ACCESS_KEY_ID", "env-access")
                .env("AWS_SECRET_ACCESS_KEY", "env-secret")
                .env("AWS_SESSION_TOKEN", "env-token");
        } else {
            command.env("AWS_ACCESS_KEY_ID", "env-access");
        }
        if let Some(endpoint) = endpoint {
            command.env(CHILD_ENDPOINT, endpoint);
        }
        let output = command.output().expect("run isolated environment test");
        assert!(
            output.status.success(),
            "environment child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("credential-environment-child-complete"),
            "environment child test did not run"
        );
    }

    #[test]
    fn static_source_returns_configured_credentials() {
        let configured = Credentials {
            access_key_id: "access".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: Some("token".to_owned()),
        };
        let provider = CredentialProvider::new(CredentialSource::Static(configured.clone()));

        let resolved = provider.credentials().expect("resolve static credentials");

        assert_eq!(resolved.access_key_id, configured.access_key_id);
        assert_eq!(resolved.secret_access_key, configured.secret_access_key);
        assert_eq!(resolved.session_token, configured.session_token);
    }

    #[test]
    fn environment_source_reads_and_requires_both_keys() {
        run_environment_child("success", None);
        run_environment_child("missing-secret", None);
        run_environment_child("auto-missing-secret", None);
    }

    #[test]
    fn environment_source_child_process() {
        let Ok(mode) = std::env::var(CHILD_MODE) else {
            return;
        };
        match mode.as_str() {
            "success" => {
                let provider = CredentialProvider::new(CredentialSource::Environment);
                let credentials = provider
                    .credentials()
                    .expect("resolve child environment credentials");
                assert_eq!(credentials.access_key_id, "env-access");
                assert_eq!(credentials.secret_access_key, "env-secret");
                assert_eq!(credentials.session_token.as_deref(), Some("env-token"));
            }
            "missing-secret" => {
                let provider = CredentialProvider::new(CredentialSource::Environment);
                assert_io_kind(
                    provider
                        .credentials()
                        .expect_err("partial environment must fail"),
                    io::ErrorKind::NotFound,
                );
            }
            "auto" => {
                let endpoint =
                    std::env::var(CHILD_ENDPOINT).expect("child auto endpoint is configured");
                let provider = CredentialProvider::new(CredentialSource::Auto);
                let credentials = provider
                    .resolve_auto(&endpoint)
                    .expect("environment must satisfy automatic resolution");
                assert_eq!(credentials.creds.access_key_id, "env-access");
            }
            "auto-missing-secret" => {
                let provider = CredentialProvider::new(CredentialSource::Auto);
                let error = match provider.resolve_auto("http://127.0.0.1:9") {
                    Ok(_) => panic!("partial automatic environment must fail"),
                    Err(error) => error,
                };
                assert_io_kind(error, io::ErrorKind::NotFound);
            }
            other => panic!("unknown environment child mode {other}"),
        }
        println!("credential-environment-child-complete");
    }

    #[test]
    fn imds_flow_against_local_stub() {
        let server = StubServer::start(3, |index, request| match index {
            0 => {
                assert_eq!(request.method, "PUT");
                assert_eq!(request.path, IMDS_TOKEN_PATH);
                assert_eq!(
                    request
                        .headers
                        .get("x-aws-ec2-metadata-token-ttl-seconds")
                        .map(String::as_str),
                    Some(IMDS_TOKEN_TTL_SECONDS)
                );
                StubResponse {
                    status: 200,
                    body: "stub-token".to_owned(),
                }
            }
            1 => {
                assert_eq!(request.method, "GET");
                assert_eq!(request.path, IMDS_ROLE_PATH);
                assert_eq!(
                    request
                        .headers
                        .get("x-aws-ec2-metadata-token")
                        .map(String::as_str),
                    Some("stub-token")
                );
                StubResponse {
                    status: 200,
                    body: "test-role\n".to_owned(),
                }
            }
            2 => {
                assert_eq!(request.method, "GET");
                assert_eq!(request.path, format!("{IMDS_ROLE_PATH}test-role"));
                assert_eq!(
                    request
                        .headers
                        .get("x-aws-ec2-metadata-token")
                        .map(String::as_str),
                    Some("stub-token")
                );
                StubResponse {
                    status: 200,
                    body: credentials_json("imds-access", "2099-07-31T12:00:00Z"),
                }
            }
            _ => unreachable!(),
        });
        let provider = CredentialProvider::new(CredentialSource::Imds {
            endpoint: server.endpoint.clone(),
        });

        let result = provider.credentials();
        server.finish();
        let credentials = result.expect("resolve credentials from IMDS stub");
        assert_eq!(credentials.access_key_id, "imds-access");
        assert_eq!(credentials.secret_access_key, "secret");
        assert_eq!(credentials.session_token.as_deref(), Some("token-value"));

        let unauthorized_server = StubServer::start(2, |index, request| match index {
            0 => StubResponse {
                status: 200,
                body: "stub-token".to_owned(),
            },
            1 => {
                assert_eq!(
                    request
                        .headers
                        .get("x-aws-ec2-metadata-token")
                        .map(String::as_str),
                    Some("stub-token")
                );
                StubResponse {
                    status: 401,
                    body: String::new(),
                }
            }
            _ => unreachable!(),
        });
        let provider = CredentialProvider::new(CredentialSource::Imds {
            endpoint: unauthorized_server.endpoint.clone(),
        });
        let result = provider.credentials();
        unauthorized_server.finish();
        assert_io_kind(
            result.expect_err("unauthorized IMDS response must fail"),
            io::ErrorKind::PermissionDenied,
        );
    }

    #[test]
    fn expiring_credentials_are_refreshed() {
        let server = StubServer::start(6, |index, _request| match index % 3 {
            0 => StubResponse {
                status: 200,
                body: "refresh-token".to_owned(),
            },
            1 => StubResponse {
                status: 200,
                body: "refresh-role".to_owned(),
            },
            2 => StubResponse {
                status: 200,
                body: credentials_json(
                    if index < 3 {
                        "first-access"
                    } else {
                        "second-access"
                    },
                    "2020-01-01T00:00:00Z",
                ),
            },
            _ => unreachable!(),
        });
        let provider = CredentialProvider::new(CredentialSource::Imds {
            endpoint: server.endpoint.clone(),
        });
        let request_count = Arc::clone(&server.requests);

        let first = provider.credentials().expect("first IMDS resolution");
        let second = provider.credentials().expect("refresh expired credentials");
        server.finish();

        assert_eq!(first.access_key_id, "first-access");
        assert_eq!(second.access_key_id, "second-access");
        assert_eq!(request_count.load(Ordering::Acquire), 6);
    }

    #[test]
    fn refresh_failure_uses_still_valid_cache() {
        let server = StubServer::start(1, |_index, _request| StubResponse {
            status: 500,
            body: String::new(),
        });
        let provider = CredentialProvider::new(CredentialSource::Imds {
            endpoint: server.endpoint.clone(),
        });
        {
            let mut cache = provider.cached.lock().expect("lock test credential cache");
            *cache = Some(CachedCreds {
                creds: Credentials {
                    access_key_id: "cached-access".to_owned(),
                    secret_access_key: "cached-secret".to_owned(),
                    session_token: Some("cached-token".to_owned()),
                },
                expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
            });
        }

        let credentials = provider
            .credentials()
            .expect("fall back to still-valid cached credentials");
        server.finish();

        assert_eq!(credentials.access_key_id, "cached-access");
    }

    #[test]
    fn auto_prefers_environment() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused IMDS stub");
        listener
            .set_nonblocking(true)
            .expect("make unused IMDS stub nonblocking");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("read unused IMDS address")
        );

        run_environment_child("auto", Some(&endpoint));

        let error = listener
            .accept()
            .expect_err("automatic resolution must not call IMDS");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn credential_provider_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CredentialProvider>();
    }
}
