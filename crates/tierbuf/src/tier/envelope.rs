//! Self-describing containers for fixed-size tier pages.
//!
//! An envelope keeps the in-memory page size fixed while allowing storage
//! backends to persist a compressed, variable-length representation.

use std::io;

use crate::{PAGE_SIZE, Result, TierBufError};

const MAGIC: u32 = 0x5442_5031;
const VERSION: u8 = 1;
const HEADER_LEN: usize = 32;
const CODEC_OFFSET: usize = 5;
const FLAGS_OFFSET: usize = 6;
const UNCOMPRESSED_LEN_OFFSET: usize = 8;
const PAYLOAD_LEN_OFFSET: usize = 12;
const PAYLOAD_CRC_OFFSET: usize = 16;
const RESERVED_OFFSET: usize = 20;

/// Codec used to encode a page payload inside an envelope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PageCodec {
    /// Store the page bytes verbatim.
    #[default]
    None,
    /// LZ4 block compression (requires the `lz4` feature).
    Lz4,
}

impl PageCodec {
    const fn encoded(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
        }
    }

    fn from_encoded(encoded: u8) -> Result<Self> {
        match encoded {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(invalid_data("page envelope contains an unknown codec")),
        }
    }
}

#[derive(Clone, Copy)]
struct EnvelopeHeader {
    codec: PageCodec,
    payload_len: usize,
    payload_crc32: u32,
}

/// Encodes one [`PAGE_SIZE`] page into a self-describing envelope.
///
/// LZ4 output that is not smaller than the input is stored verbatim, and the
/// envelope records [`PageCodec::None`] as the codec actually used.
///
/// # Errors
///
/// Returns an invalid-input error if `page` is not exactly one page or if LZ4
/// is requested without enabling the `lz4` feature.
pub fn encode_page(page: &[u8], codec: PageCodec) -> Result<Vec<u8>> {
    validate_page_len(page.len(), "page envelope input must equal PAGE_SIZE")?;

    let (stored_codec, payload) = encode_payload(page, codec)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| invalid_input("page envelope payload length exceeds u32"))?;
    let uncompressed_len = u32::try_from(PAGE_SIZE)
        .map_err(|_| invalid_input("PAGE_SIZE exceeds the envelope length field"))?;
    let payload_crc32 = crc32fast::hash(&payload);
    let capacity = HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| invalid_input("page envelope length overflow"))?;

    let mut envelope = Vec::with_capacity(capacity);
    envelope.extend_from_slice(&MAGIC.to_le_bytes());
    envelope.push(VERSION);
    envelope.push(stored_codec.encoded());
    envelope.extend_from_slice(&0_u16.to_le_bytes());
    envelope.extend_from_slice(&uncompressed_len.to_le_bytes());
    envelope.extend_from_slice(&payload_len.to_le_bytes());
    envelope.extend_from_slice(&payload_crc32.to_le_bytes());
    envelope.extend_from_slice(&[0; HEADER_LEN - RESERVED_OFFSET]);
    envelope.extend_from_slice(&payload);
    Ok(envelope)
}

/// Decodes an envelope produced by [`encode_page`] into `out`.
///
/// # Errors
///
/// Returns an invalid-input error unless `out` is exactly [`PAGE_SIZE`] bytes.
/// Malformed headers, inconsistent lengths, checksum failures, and invalid
/// compressed data return invalid-data errors. Decoding an LZ4 envelope without
/// the `lz4` feature returns an invalid-input error.
pub fn decode_page(envelope: &[u8], out: &mut [u8]) -> Result<()> {
    validate_page_len(
        out.len(),
        "page envelope output buffer must equal PAGE_SIZE",
    )?;
    let header = parse_header(envelope)?;
    let payload = &envelope[HEADER_LEN..];

    if crc32fast::hash(payload) != header.payload_crc32 {
        return Err(invalid_data("page envelope payload CRC32 mismatch"));
    }

    match header.codec {
        PageCodec::None => {
            if header.payload_len != PAGE_SIZE {
                return Err(invalid_data(
                    "uncompressed page envelope payload must equal PAGE_SIZE",
                ));
            }
            out.copy_from_slice(payload);
            Ok(())
        }
        PageCodec::Lz4 => decode_lz4(payload, out),
    }
}

/// Returns the payload length recorded in an envelope header without decoding.
///
/// The header and total envelope length are validated, but the payload checksum
/// and compressed contents are not inspected.
///
/// # Errors
///
/// Returns an invalid-data error if the header or recorded length is invalid.
pub fn payload_len(envelope: &[u8]) -> Result<usize> {
    Ok(parse_header(envelope)?.payload_len)
}

fn encode_payload(page: &[u8], codec: PageCodec) -> Result<(PageCodec, Vec<u8>)> {
    match codec {
        PageCodec::None => Ok((PageCodec::None, page.to_vec())),
        PageCodec::Lz4 => encode_lz4(page),
    }
}

#[cfg(feature = "lz4")]
fn encode_lz4(page: &[u8]) -> Result<(PageCodec, Vec<u8>)> {
    let compressed = lz4_flex::block::compress(page);
    if compressed.len() < page.len() {
        Ok((PageCodec::Lz4, compressed))
    } else {
        Ok((PageCodec::None, page.to_vec()))
    }
}

#[cfg(not(feature = "lz4"))]
fn encode_lz4(_page: &[u8]) -> Result<(PageCodec, Vec<u8>)> {
    Err(lz4_feature_required())
}

#[cfg(feature = "lz4")]
fn decode_lz4(payload: &[u8], out: &mut [u8]) -> Result<()> {
    let decoded_len = lz4_flex::block::decompress_into(payload, out)
        .map_err(|error| invalid_data(format!("invalid LZ4 page payload: {error}")))?;
    if decoded_len != PAGE_SIZE {
        return Err(invalid_data(
            "LZ4 page payload did not decompress to PAGE_SIZE",
        ));
    }
    Ok(())
}

#[cfg(not(feature = "lz4"))]
fn decode_lz4(_payload: &[u8], _out: &mut [u8]) -> Result<()> {
    Err(lz4_feature_required())
}

fn parse_header(envelope: &[u8]) -> Result<EnvelopeHeader> {
    if envelope.len() < HEADER_LEN {
        return Err(invalid_data("page envelope is shorter than its header"));
    }

    let magic = read_u32(envelope, 0);
    if magic != MAGIC {
        return Err(invalid_data("page envelope magic mismatch"));
    }
    if envelope[4] != VERSION {
        return Err(invalid_data("unsupported page envelope version"));
    }

    let codec = PageCodec::from_encoded(envelope[CODEC_OFFSET])?;
    if read_u16(envelope, FLAGS_OFFSET) != 0 {
        return Err(invalid_data("page envelope flags must be zero"));
    }

    let uncompressed_len = usize::try_from(read_u32(envelope, UNCOMPRESSED_LEN_OFFSET))
        .map_err(|_| invalid_data("page envelope uncompressed length is invalid"))?;
    if uncompressed_len != PAGE_SIZE {
        return Err(invalid_data(
            "page envelope uncompressed length must equal PAGE_SIZE",
        ));
    }

    let payload_len = usize::try_from(read_u32(envelope, PAYLOAD_LEN_OFFSET))
        .map_err(|_| invalid_data("page envelope payload length is invalid"))?;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| invalid_data("page envelope length overflow"))?;
    if envelope.len() != expected_len {
        return Err(invalid_data(
            "page envelope length does not match its payload length",
        ));
    }

    if envelope[RESERVED_OFFSET..HEADER_LEN]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(invalid_data("page envelope reserved bytes must be zero"));
    }

    Ok(EnvelopeHeader {
        codec,
        payload_len,
        payload_crc32: read_u32(envelope, PAYLOAD_CRC_OFFSET),
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn validate_page_len(length: usize, message: &'static str) -> Result<()> {
    if length != PAGE_SIZE {
        return Err(invalid_input(message));
    }
    Ok(())
}

#[cfg(not(feature = "lz4"))]
fn lz4_feature_required() -> TierBufError {
    invalid_input("LZ4 page envelopes require you to enable the `lz4` feature")
}

fn invalid_input(message: impl Into<String>) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn invalid_data(message: impl Into<String>) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidData, message.into()).into()
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{PAGE_SIZE, TierBufError};

    use super::{
        HEADER_LEN, PAYLOAD_LEN_OFFSET, PageCodec, decode_page, encode_page, payload_len, read_u32,
    };

    fn pseudo_random_page() -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut page = vec![0; PAGE_SIZE];
        for chunk in page.chunks_exact_mut(size_of::<u64>()) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        page
    }

    fn error_kind(error: &TierBufError) -> io::ErrorKind {
        match error {
            TierBufError::Io(error) => error.kind(),
            other => panic!("expected I/O error, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_none_codec() {
        let page = pseudo_random_page();
        let envelope = encode_page(&page, PageCodec::None).expect("page should encode");
        let mut decoded = vec![0; PAGE_SIZE];

        decode_page(&envelope, &mut decoded).expect("page should decode");

        assert_eq!(decoded, page);
        assert_eq!(payload_len(&envelope).expect("valid header"), PAGE_SIZE);
        assert_eq!(envelope[5], PageCodec::None.encoded());
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn roundtrip_lz4_compressible() {
        let page: Vec<u8> = (0..PAGE_SIZE)
            .map(|index| b"tierbuf!"[index % b"tierbuf!".len()])
            .collect();
        let envelope = encode_page(&page, PageCodec::Lz4).expect("page should encode");
        let mut decoded = vec![0; PAGE_SIZE];

        decode_page(&envelope, &mut decoded).expect("page should decode");

        assert_eq!(decoded, page);
        assert!(payload_len(&envelope).expect("valid header") < PAGE_SIZE);
        assert_eq!(envelope[5], PageCodec::Lz4.encoded());
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_incompressible_falls_back_to_none() {
        let page = pseudo_random_page();
        let envelope = encode_page(&page, PageCodec::Lz4).expect("page should encode");

        assert_eq!(envelope[5], PageCodec::None.encoded());
        assert_eq!(payload_len(&envelope).expect("valid header"), PAGE_SIZE);
    }

    #[test]
    fn corrupted_crc_is_rejected() {
        let page = pseudo_random_page();
        let mut envelope = encode_page(&page, PageCodec::None).expect("page should encode");
        envelope[HEADER_LEN] ^= 0x80;
        let mut decoded = vec![0; PAGE_SIZE];

        let error = decode_page(&envelope, &mut decoded).expect_err("CRC must be checked");

        assert_eq!(error_kind(&error), io::ErrorKind::InvalidData);
    }

    #[test]
    fn truncated_and_bad_magic_are_rejected() {
        let page = pseudo_random_page();
        let envelope = encode_page(&page, PageCodec::None).expect("page should encode");
        let mut decoded = vec![0; PAGE_SIZE];

        let truncated = &envelope[..envelope.len() - 1];
        let error = decode_page(truncated, &mut decoded).expect_err("truncated payload must fail");
        assert_eq!(error_kind(&error), io::ErrorKind::InvalidData);

        let mut bad_magic = envelope;
        bad_magic[0] ^= 0x01;
        let error = decode_page(&bad_magic, &mut decoded).expect_err("bad magic must fail");
        assert_eq!(error_kind(&error), io::ErrorKind::InvalidData);
    }

    #[test]
    fn wrong_input_lengths_are_rejected() {
        let short_page = vec![0; 63 * 1024];
        let error = encode_page(&short_page, PageCodec::None).expect_err("short page must fail");
        assert_eq!(error_kind(&error), io::ErrorKind::InvalidInput);

        let page = vec![0; PAGE_SIZE];
        let envelope = encode_page(&page, PageCodec::None).expect("page should encode");
        let mut short_output = vec![0; PAGE_SIZE - 1];
        let error =
            decode_page(&envelope, &mut short_output).expect_err("short output buffer must fail");
        assert_eq!(error_kind(&error), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn payload_len_rejects_inconsistent_envelope_length() {
        let page = vec![0; PAGE_SIZE];
        let mut envelope = encode_page(&page, PageCodec::None).expect("page should encode");
        let recorded = read_u32(&envelope, PAYLOAD_LEN_OFFSET);
        envelope[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + size_of::<u32>()]
            .copy_from_slice(&(recorded - 1).to_le_bytes());

        let error = payload_len(&envelope).expect_err("inconsistent length must fail");

        assert_eq!(error_kind(&error), io::ErrorKind::InvalidData);
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn lz4_codec_requires_feature() {
        let page = vec![0; PAGE_SIZE];

        let error = encode_page(&page, PageCodec::Lz4).expect_err("LZ4 feature must be required");

        assert_eq!(error_kind(&error), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("enable"));
        assert!(error.to_string().contains("`lz4` feature"));
    }
}
