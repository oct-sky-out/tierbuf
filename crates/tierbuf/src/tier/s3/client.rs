//! Minimal blocking S3 object client.

use std::collections::HashMap;
use std::io;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::{Result, TierBufError};

use super::credentials::{CredentialProvider, CredentialSource};
use super::sigv4::{AmzTimestamp, Header, SignRequest, sha256_hex, sign, uri_encode};

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
const MAX_IDLE_CONNECTIONS: usize = 256;
const RETRY_BASE_MILLIS: u64 = 50;
const RETRY_CAP_MILLIS: u64 = 2_000;
const CREATE_BUCKET_XML_PREFIX: &str =
    r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#;
const CREATE_BUCKET_XML_SUFFIX: &str = "</CreateBucketConfiguration>";

/// Minimal blocking object-storage operations used by the S3 tier.
///
/// Keys are bucket-relative and must not start with `/`.
pub trait ObjectApi: Send + Sync + 'static {
    /// Fetches the complete value stored at `key`.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] when the object is missing or the request
    /// cannot be completed.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Stores `body` as the complete value at `key`.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] when the request cannot be completed.
    fn put(&self, key: &str, body: &[u8]) -> Result<()>;

    /// Deletes `key`; deleting a missing key succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] when the request cannot be completed.
    fn delete(&self, key: &str) -> Result<()>;
}

/// Connection settings for a real S3-compatible endpoint.
#[derive(Clone, Debug)]
pub struct S3ClientConfig {
    /// Bucket containing tier objects.
    pub bucket: String,
    /// AWS signing region.
    pub region: String,
    /// Optional custom endpoint; custom endpoints always use path-style URLs.
    pub endpoint: Option<String>,
    /// Credential source used to sign each request.
    pub credentials: CredentialSource,
    /// Maximum time allowed to establish a connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed for one complete HTTP request.
    pub request_timeout: Duration,
    /// Maximum retries after the initial request.
    pub max_retries: u32,
}

impl Default for S3ClientConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            credentials: CredentialSource::Auto,
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(30),
            max_retries: 4,
        }
    }
}

/// Blocking S3-compatible implementation of [`ObjectApi`].
pub struct S3Client {
    config: S3ClientConfig,
    agent: ureq::Agent,
    credentials: CredentialProvider,
}

#[derive(Debug)]
struct RequestTarget {
    url: String,
    host: String,
    path: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Get,
    Put,
    Delete,
    CreateBucket,
}

impl Operation {
    const fn method(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put | Self::CreateBucket => "PUT",
            Self::Delete => "DELETE",
        }
    }
}

impl S3Client {
    /// Creates a client and its shared HTTP connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::InvalidConfig`] when a required string is
    /// empty, a timeout is zero, or a custom endpoint is not an absolute
    /// HTTP(S) URI.
    pub fn new(config: S3ClientConfig) -> Result<Self> {
        validate_config(&config)?;
        let agent_config = ureq::Agent::config_builder()
            .timeout_connect(Some(config.connect_timeout))
            .timeout_global(Some(config.request_timeout))
            .http_status_as_error(false)
            // ureq otherwise retains only three idle connections per host.
            // Match BufConfig's maximum prefetch worker count so a completed
            // high-concurrency GET wave can reuse its sockets instead of
            // churning through the host's ephemeral port range.
            .max_idle_connections(MAX_IDLE_CONNECTIONS)
            .max_idle_connections_per_host(MAX_IDLE_CONNECTIONS)
            .build();
        let credentials = CredentialProvider::new(config.credentials.clone());
        Ok(Self {
            config,
            agent: ureq::Agent::new_with_config(agent_config),
            credentials,
        })
    }

    /// Creates the configured bucket for integration-test setup.
    ///
    /// A successful response and S3's `BucketAlreadyOwnedByYou` conflict are
    /// both treated as success.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] when signing or the HTTP request fails.
    pub fn create_bucket(&self) -> Result<()> {
        let body = create_bucket_body(&self.config.region, self.config.endpoint.is_some());
        self.execute(Operation::CreateBucket, "", &body).map(|_| ())
    }

    fn execute(&self, operation: Operation, key: &str, body: &[u8]) -> Result<Vec<u8>> {
        if operation != Operation::CreateBucket {
            validate_key(key)?;
        }
        let target = self.request_target(key, operation == Operation::CreateBucket)?;

        for attempt in 0..=self.config.max_retries {
            match self.send_once(operation, key, body, &target) {
                Ok(response) => {
                    let status = response.status;
                    if (200..300).contains(&status)
                        || (operation == Operation::Delete && status == 404)
                        || (operation == Operation::CreateBucket
                            && status == 409
                            && String::from_utf8_lossy(&response.body)
                                .contains("BucketAlreadyOwnedByYou"))
                    {
                        return Ok(response.body);
                    }
                    if is_retryable_status(status) && attempt < self.config.max_retries {
                        self.sleep_before_retry(attempt);
                        continue;
                    }
                    return Err(status_error(operation.method(), key, status));
                }
                Err(error) => {
                    if is_retryable_transport_error(&error) && attempt < self.config.max_retries {
                        self.sleep_before_retry(attempt);
                        continue;
                    }
                    return Err(transport_error(operation.method(), key, error));
                }
            }
        }

        Err(io::Error::other(format!(
            "s3 {} {}: retry loop ended unexpectedly",
            operation.method(),
            display_key(key)
        ))
        .into())
    }

    fn send_once(
        &self,
        operation: Operation,
        key: &str,
        body: &[u8],
        target: &RequestTarget,
    ) -> std::result::Result<HttpResponse, ureq::Error> {
        let credentials = self
            .credentials
            .credentials()
            .map_err(tierbuf_error_to_ureq)?;
        let timestamp = AmzTimestamp::from_system_time(SystemTime::now());
        let payload_hash = if body.is_empty() {
            EMPTY_SHA256.to_owned()
        } else {
            sha256_hex(body)
        };
        let mut headers: Vec<Header> = vec![
            ("x-amz-date".to_owned(), timestamp.amz_date().to_owned()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
        ];
        if let Some(token) = credentials.session_token.as_ref() {
            headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        match operation {
            Operation::Put => headers.push((
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            )),
            Operation::CreateBucket if !body.is_empty() => {
                headers.push(("content-type".to_owned(), "application/xml".to_owned()));
            }
            Operation::Get | Operation::Delete | Operation::CreateBucket => {}
        }
        let authorization = sign(
            &SignRequest {
                method: operation.method(),
                host: &target.host,
                path: &target.path,
                query: &[],
                headers: &headers,
                payload_sha256_hex: &payload_hash,
                region: &self.config.region,
                service: "s3",
                timestamp,
            },
            &credentials,
        )
        .map_err(tierbuf_error_to_ureq)?;
        headers.push(("authorization".to_owned(), authorization));

        let response = match operation {
            Operation::Get => {
                let mut request = self.agent.get(&target.url);
                for (name, value) in &headers {
                    request = request.header(name, value);
                }
                request.call()?
            }
            Operation::Delete => {
                let mut request = self.agent.delete(&target.url);
                for (name, value) in &headers {
                    request = request.header(name, value);
                }
                request.call()?
            }
            Operation::Put | Operation::CreateBucket => {
                let mut request = self.agent.put(&target.url);
                for (name, value) in &headers {
                    request = request.header(name, value);
                }
                request.send(body)?
            }
        };

        let mut response = response;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_vec()?;
        let _ = key;
        Ok(HttpResponse { status, body })
    }

    fn request_target(&self, key: &str, bucket_only: bool) -> Result<RequestTarget> {
        if let Some(endpoint) = self.config.endpoint.as_ref() {
            custom_request_target(endpoint, &self.config.bucket, key, bucket_only)
        } else {
            let host = format!(
                "{}.s3.{}.amazonaws.com",
                self.config.bucket, self.config.region
            );
            let path = if bucket_only {
                "/".to_owned()
            } else {
                format!("/{key}")
            };
            let encoded_path = uri_encode(&path, false);
            Ok(RequestTarget {
                url: format!("https://{host}{encoded_path}"),
                host,
                path: encoded_path,
            })
        }
    }

    fn sleep_before_retry(&self, attempt: u32) {
        let shift = attempt.min(16);
        let ceiling = RETRY_BASE_MILLIS
            .saturating_mul(1_u64 << shift)
            .min(RETRY_CAP_MILLIS);
        let address = std::ptr::from_ref(self).addr() as u64;
        let mut state = address
            ^ u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ 0xa076_1d64_78bd_642f;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let delay = state % ceiling.saturating_add(1);
        thread::sleep(Duration::from_millis(delay));
    }
}

impl ObjectApi for S3Client {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.execute(Operation::Get, key, &[])
    }

    fn put(&self, key: &str, body: &[u8]) -> Result<()> {
        self.execute(Operation::Put, key, body).map(|_| ())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.execute(Operation::Delete, key, &[]).map(|_| ())
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn custom_request_target(
    endpoint: &str,
    bucket: &str,
    key: &str,
    bucket_only: bool,
) -> Result<RequestTarget> {
    let uri: ureq::http::Uri = endpoint
        .parse()
        .map_err(|_| invalid_config("S3 endpoint must be an absolute HTTP(S) URI"))?;
    let scheme = uri
        .scheme_str()
        .filter(|scheme| matches!(*scheme, "http" | "https"))
        .ok_or_else(|| invalid_config("S3 endpoint must use http or https"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| invalid_config("S3 endpoint must include a host"))?
        .as_str();
    if uri.query().is_some() {
        return Err(invalid_config("S3 endpoint must not include a query"));
    }
    let endpoint_path = uri.path().trim_end_matches('/');
    let path = if bucket_only {
        format!("{endpoint_path}/{bucket}")
    } else {
        format!("{endpoint_path}/{bucket}/{key}")
    };
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    let encoded_path = uri_encode(&path, false);
    Ok(RequestTarget {
        url: format!("{scheme}://{authority}{encoded_path}"),
        host: authority.to_owned(),
        path: encoded_path,
    })
}

fn validate_config(config: &S3ClientConfig) -> Result<()> {
    if config.bucket.trim().is_empty() {
        return Err(invalid_config("S3 bucket must not be empty"));
    }
    if config.region.trim().is_empty() {
        return Err(invalid_config("S3 region must not be empty"));
    }
    if config.connect_timeout.is_zero() {
        return Err(invalid_config(
            "S3 connect timeout must be greater than zero",
        ));
    }
    if config.request_timeout.is_zero() {
        return Err(invalid_config(
            "S3 request timeout must be greater than zero",
        ));
    }
    if let Some(endpoint) = config.endpoint.as_ref() {
        let _ = custom_request_target(endpoint, &config.bucket, "", true)?;
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "S3 object key must be non-empty and bucket-relative",
        )
        .into());
    }
    Ok(())
}

fn create_bucket_body(region: &str, custom_endpoint: bool) -> Vec<u8> {
    if region == "us-east-1" || custom_endpoint {
        return Vec::new();
    }
    format!(
        "{CREATE_BUCKET_XML_PREFIX}<LocationConstraint>{region}</LocationConstraint>{CREATE_BUCKET_XML_SUFFIX}"
    )
    .into_bytes()
}

const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_transport_error(error: &ureq::Error) -> bool {
    match error {
        ureq::Error::Timeout(_) | ureq::Error::ConnectionFailed | ureq::Error::HostNotFound => true,
        ureq::Error::Io(source) => matches!(
            source.kind(),
            io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::Interrupted
                | io::ErrorKind::NotConnected
                | io::ErrorKind::TimedOut
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::WouldBlock
        ),
        _ => false,
    }
}

fn status_error(method: &str, key: &str, status: u16) -> TierBufError {
    let kind = match status {
        404 if method == "GET" => io::ErrorKind::NotFound,
        408 => io::ErrorKind::TimedOut,
        403 => io::ErrorKind::PermissionDenied,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(
        kind,
        format!("s3 {method} {}: status {status}", display_key(key)),
    )
    .into()
}

fn transport_error(method: &str, key: &str, error: ureq::Error) -> TierBufError {
    let kind = match &error {
        ureq::Error::Timeout(_) => io::ErrorKind::TimedOut,
        ureq::Error::Io(source) => source.kind(),
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("s3 {method} {}: {error}", display_key(key))).into()
}

fn tierbuf_error_to_ureq(error: TierBufError) -> ureq::Error {
    match error {
        TierBufError::Io(source) => ureq::Error::Io(source),
        other => ureq::Error::Other(Box::new(other)),
    }
}

fn display_key(key: &str) -> &str {
    if key.is_empty() { "<bucket>" } else { key }
}

fn invalid_config(message: &'static str) -> TierBufError {
    TierBufError::InvalidConfig(message.to_owned())
}

/// Snapshot of deterministic in-memory object API activity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryObjectApiStats {
    /// Number of `get` calls.
    pub get_requests: u64,
    /// Number of `put` calls.
    pub put_requests: u64,
    /// Number of `delete` calls.
    pub delete_requests: u64,
}

/// Deterministic in-memory [`ObjectApi`] used by tests and local embedding.
#[derive(Debug, Default)]
pub struct MemoryObjectApi {
    state: Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    objects: HashMap<String, Vec<u8>>,
    stats: MemoryObjectApiStats,
    fail_next_get: bool,
    fail_next_put: bool,
    fail_next_delete: bool,
}

impl MemoryObjectApi {
    /// Creates an empty in-memory object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes the next `get` call fail.
    pub fn fail_next_get(&self) {
        self.lock_state().fail_next_get = true;
    }

    /// Makes the next `put` call fail.
    pub fn fail_next_put(&self) {
        self.lock_state().fail_next_put = true;
    }

    /// Makes the next `delete` call fail.
    pub fn fail_next_delete(&self) {
        self.lock_state().fail_next_delete = true;
    }

    /// Returns current request counters.
    #[must_use]
    pub fn snapshot(&self) -> MemoryObjectApiStats {
        self.lock_state().stats
    }

    /// Returns whether `key` currently exists.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.lock_state().objects.contains_key(key)
    }

    /// Returns the number of currently stored objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock_state().objects.len()
    }

    /// Returns whether the object store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock_state().objects.is_empty()
    }

    fn lock_state(&self) -> MutexGuard<'_, MemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ObjectApi for MemoryObjectApi {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        validate_key(key)?;
        let mut state = self.lock_state();
        state.stats.get_requests = state.stats.get_requests.saturating_add(1);
        if std::mem::take(&mut state.fail_next_get) {
            return Err(io::Error::other("injected in-memory get failure").into());
        }
        state.objects.get(key).cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "in-memory object not found").into()
        })
    }

    fn put(&self, key: &str, body: &[u8]) -> Result<()> {
        validate_key(key)?;
        let mut state = self.lock_state();
        state.stats.put_requests = state.stats.put_requests.saturating_add(1);
        if std::mem::take(&mut state.fail_next_put) {
            return Err(io::Error::other("injected in-memory put failure").into());
        }
        state.objects.insert(key.to_owned(), body.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        let mut state = self.lock_state();
        state.stats.delete_requests = state.stats.delete_requests.saturating_add(1);
        if std::mem::take(&mut state.fail_next_delete) {
            return Err(io::Error::other("injected in-memory delete failure").into());
        }
        state.objects.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::{
        MAX_IDLE_CONNECTIONS, MemoryObjectApi, ObjectApi, Operation, S3Client, S3ClientConfig,
        create_bucket_body, custom_request_target, sha256_hex,
    };
    use crate::tier::s3::credentials::CredentialSource;
    use crate::tier::s3::sigv4::Credentials;

    #[derive(Debug)]
    struct RecordedRequest {
        request_line: String,
        headers: HashMap<String, String>,
        body: Vec<u8>,
    }

    struct StubServer {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl StubServer {
        fn start(responses: Vec<(u16, &'static str)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
            let address = listener.local_addr().expect("stub address");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured = Arc::clone(&requests);
            let worker = thread::spawn(move || {
                for (status, body) in responses {
                    let (mut stream, _) = listener.accept().expect("accept request");
                    let request = read_request(&mut stream);
                    captured
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(request);
                    let reason = match status {
                        200 => "OK",
                        408 => "Request Timeout",
                        403 => "Forbidden",
                        404 => "Not Found",
                        409 => "Conflict",
                        503 => "Service Unavailable",
                        _ => "Status",
                    };
                    write!(
                        stream,
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .expect("write response");
                }
            });
            Self {
                endpoint: format!("http://{address}"),
                requests,
                worker: Some(worker),
            }
        }

        fn finish(mut self) -> Vec<RecordedRequest> {
            self.worker
                .take()
                .expect("stub worker")
                .join()
                .expect("join stub");
            Arc::try_unwrap(self.requests)
                .expect("request owner")
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> RecordedRequest {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let count = stream.read(&mut buffer).expect("read request");
            assert_ne!(count, 0, "request ended before headers");
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let header_text = String::from_utf8(bytes[..header_end].to_vec()).expect("headers utf8");
        let mut lines = header_text.split("\r\n");
        let request_line = lines.next().expect("request line").to_owned();
        let mut headers = HashMap::new();
        for line in lines.filter(|line| !line.is_empty()) {
            let (name, value) = line.split_once(':').expect("header delimiter");
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() - header_end < content_length {
            let count = stream.read(&mut buffer).expect("read request body");
            assert_ne!(count, 0, "request body truncated");
            bytes.extend_from_slice(&buffer[..count]);
        }
        RecordedRequest {
            request_line,
            headers,
            body: bytes[header_end..header_end + content_length].to_vec(),
        }
    }

    fn credentials() -> Credentials {
        Credentials {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
        }
    }

    fn config(endpoint: String, max_retries: u32) -> S3ClientConfig {
        S3ClientConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: Some(endpoint),
            credentials: CredentialSource::Static(credentials()),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            max_retries,
        }
    }

    #[test]
    fn memory_api_roundtrip_and_delete_missing_ok() {
        let api = MemoryObjectApi::new();
        api.put("prefix/key", b"value").expect("put");
        assert_eq!(api.get("prefix/key").expect("get"), b"value");
        api.delete("prefix/key").expect("delete");
        api.delete("prefix/key").expect("delete missing");
        assert!(api.get("prefix/key").is_err());
        assert_eq!(api.snapshot().delete_requests, 2);
    }

    #[test]
    fn client_pool_retains_maximum_prefetch_concurrency() {
        let client =
            S3Client::new(config("http://127.0.0.1:9000".to_owned(), 0)).expect("client config");
        assert_eq!(
            client.agent.config().max_idle_connections(),
            MAX_IDLE_CONNECTIONS
        );
        assert_eq!(
            client.agent.config().max_idle_connections_per_host(),
            MAX_IDLE_CONNECTIONS
        );
    }

    #[test]
    fn client_sends_signed_headers() {
        let stub = StubServer::start(vec![(200, "")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 0)).expect("client");
        let body = b"signed body";
        client.put("prefix/key", body).expect("put");
        let requests = stub.finish();
        let request = &requests[0];

        assert!(request.request_line.starts_with("PUT /bucket/prefix/key "));
        assert!(
            request.headers["authorization"]
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
        );
        assert_eq!(request.headers["x-amz-content-sha256"], sha256_hex(body));
        assert_eq!(request.body, body);
    }

    #[test]
    fn retry_on_503_then_success() {
        let stub = StubServer::start(vec![(503, ""), (503, ""), (200, "value")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 2)).expect("client");
        assert_eq!(client.get("key").expect("retried get"), b"value");
        assert_eq!(stub.finish().len(), 3);

        let stub = StubServer::start(vec![(503, ""), (503, "")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 1)).expect("client");
        assert!(client.get("key").is_err());
        assert_eq!(stub.finish().len(), 2);
    }

    #[test]
    fn request_timeout_status_is_retried_and_reported_as_timed_out() {
        let stub = StubServer::start(vec![(408, ""), (200, "value")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 1)).expect("client");
        assert_eq!(client.get("key").expect("retried get"), b"value");
        assert_eq!(stub.finish().len(), 2);

        let stub = StubServer::start(vec![(408, "")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 0)).expect("client");
        let error = client.get("key").expect_err("408 must fail");
        assert_eq!(
            match error {
                crate::TierBufError::Io(source) => source.kind(),
                other => panic!("unexpected error: {other}"),
            },
            io::ErrorKind::TimedOut
        );
        assert_eq!(stub.finish().len(), 1);
    }

    #[test]
    fn forbidden_is_not_retried() {
        let stub = StubServer::start(vec![(403, "")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 4)).expect("client");
        let error = client.get("key").expect_err("403 must fail");
        assert_eq!(
            match error {
                crate::TierBufError::Io(source) => source.kind(),
                other => panic!("unexpected error: {other}"),
            },
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(stub.finish().len(), 1);
    }

    #[test]
    fn delete_404_is_ok() {
        let stub = StubServer::start(vec![(404, "")]);
        let client = S3Client::new(config(stub.endpoint.clone(), 4)).expect("client");
        client.delete("missing").expect("delete missing");
        assert_eq!(stub.finish().len(), 1);
    }

    #[test]
    fn path_style_url_for_custom_endpoint() {
        let target = custom_request_target("http://127.0.0.1:9000", "bucket", "a b/key", false)
            .expect("target");
        assert_eq!(target.url, "http://127.0.0.1:9000/bucket/a%20b/key");
        assert_eq!(target.host, "127.0.0.1:9000");
        assert_eq!(target.path, "/bucket/a%20b/key");
    }

    #[test]
    fn create_bucket_body_matches_aws_location_rules() {
        assert!(create_bucket_body("us-east-1", false).is_empty());
        assert!(create_bucket_body("ap-northeast-1", true).is_empty());
        assert_eq!(
            create_bucket_body("ap-northeast-1", false),
            br#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>ap-northeast-1</LocationConstraint></CreateBucketConfiguration>"#
        );
    }

    #[test]
    fn regional_create_bucket_xml_is_signed_with_its_content_type() {
        let stub = StubServer::start(vec![(200, "")]);
        let mut client_config = config(stub.endpoint.clone(), 0);
        client_config.region = "ap-northeast-1".to_owned();
        let client = S3Client::new(client_config).expect("client");
        let target = custom_request_target(&stub.endpoint, "bucket", "", true).expect("target");
        let body = create_bucket_body("ap-northeast-1", false);

        let response = client
            .send_once(Operation::CreateBucket, "", &body, &target)
            .expect("create bucket request");
        assert_eq!(response.status, 200);

        let requests = stub.finish();
        let request = &requests[0];
        assert_eq!(request.body, body);
        assert_eq!(request.headers["content-type"], "application/xml");
        assert_eq!(request.headers["x-amz-content-sha256"], sha256_hex(&body));
        assert!(
            request.headers["authorization"]
                .contains("SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date")
        );
    }

    #[test]
    fn create_bucket_keeps_custom_endpoint_body_empty() {
        let stub = StubServer::start(vec![(200, "")]);
        let mut client_config = config(stub.endpoint.clone(), 0);
        client_config.region = "ap-northeast-1".to_owned();
        let client = S3Client::new(client_config).expect("client");

        client.create_bucket().expect("create bucket");

        let requests = stub.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].body.is_empty());
        assert_eq!(
            requests[0].headers["x-amz-content-sha256"],
            super::EMPTY_SHA256
        );
        assert!(!requests[0].headers.contains_key("content-type"));
    }
}
