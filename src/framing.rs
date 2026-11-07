use crate::error::IpcError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Canonical 4-byte frame magic for MuObserve IPC streams (`"MUOB"`).
pub const MAGIC: [u8; 4] = *b"MUOB";

/// Current wire protocol version.
pub const CURRENT_VERSION: u16 = 1;

/// Maximum permitted frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Fixed byte length of [`IpcHeader`] (24 bytes).
pub const HEADER_SIZE: usize = std::mem::size_of::<IpcHeader>();

// Message Type Constants
/// Apache Fory control-plane RPC request/response frame.
pub const MSG_TYPE_FORY_CONTROL: u16 = 0x0001;
/// Edge collector WAL batch acknowledgment frame.
pub const MSG_TYPE_WAL_ACK: u16 = 0x0002;
/// Apache Arrow IPC stream containing `OtelLogEntry` records.
pub const MSG_TYPE_ARROW_OTEL_LOGS: u16 = 0x0010;
/// Apache Arrow IPC stream containing `OtelSpanEntry` records.
pub const MSG_TYPE_ARROW_OTEL_SPANS: u16 = 0x0011;
/// Apache Arrow IPC stream containing `OtelMetricEntry` records.
pub const MSG_TYPE_ARROW_OTEL_METRICS: u16 = 0x0012;
/// Apache Arrow IPC stream containing `TestCaseRunEntry` records.
pub const MSG_TYPE_ARROW_TEST_RUNS: u16 = 0x0013;
/// Apache Arrow IPC stream containing `BenchmarkRunEntry` records.
pub const MSG_TYPE_ARROW_BENCH_RUNS: u16 = 0x0014;

// Frame Flags
/// Indicates the frame payload is Zstd-compressed.
pub const FLAG_COMPRESSED: u32 = 0x0001;
/// Indicates the final frame in a logical stream.
pub const FLAG_END_OF_STREAM: u32 = 0x0002;
/// Marks high-priority telemetry (e.g., error logs, test runs, benchmarks, SLO alerts)
/// that must be preserved during Edge WAL ring-buffer backpressure eviction.
pub const FLAG_HIGH_PRIORITY: u32 = 0x0004;

/// 24-byte 8-byte-aligned zero-copy frame header for `MuObserve` IPC streams.
#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(C, align(8))]
pub struct IpcHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub msg_type: u16,
    pub payload_len: u32,
    pub flags: u32,
    pub sequence: u64,
}

impl IpcHeader {
    /// Creates a new [`IpcHeader`] initialized with [`MAGIC`] and [`CURRENT_VERSION`].
    #[must_use]
    pub const fn new(msg_type: u16, payload_len: u32, flags: u32, sequence: u64) -> Self {
        Self {
            magic: MAGIC,
            version: CURRENT_VERSION,
            msg_type,
            payload_len,
            flags,
            sequence,
        }
    }

    /// Parses a 24-byte prefix into an [`IpcHeader`] and returns the remaining byte slice.
    pub fn parse(buf: &[u8]) -> Result<(Self, &[u8]), IpcError> {
        if buf.len() < HEADER_SIZE {
            return Err(IpcError::BufferTooSmall {
                expected: HEADER_SIZE,
                actual: buf.len(),
            });
        }

        let (hdr, rest) =
            Self::read_from_prefix(buf).map_err(|e| IpcError::MalformedHeader(format!("{e:?}")))?;

        if hdr.magic != MAGIC {
            return Err(IpcError::InvalidMagic);
        }

        if hdr.version != CURRENT_VERSION {
            return Err(IpcError::UnsupportedVersion {
                expected: CURRENT_VERSION,
                actual: hdr.version,
            });
        }

        let payload_len = hdr.payload_len as usize;
        if payload_len > MAX_FRAME_SIZE {
            return Err(IpcError::FrameTooLarge {
                max: MAX_FRAME_SIZE,
                actual: payload_len,
            });
        }

        Ok((hdr, rest))
    }
}

/// Asynchronously reads a single 24-byte [`IpcHeader`] and its associated payload from `reader`,
/// enforcing [`MAGIC`], [`CURRENT_VERSION`], and [`MAX_FRAME_SIZE`].
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(IpcHeader, Vec<u8>), IpcError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header_buf).await?;

    let (header, _) = IpcHeader::parse(&header_buf)?;
    let payload_len = header.payload_len as usize;

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok((header, payload))
}

/// Asynchronously writes a 24-byte [`IpcHeader`] and its payload slice to `writer`,
/// enforcing [`MAX_FRAME_SIZE`] and header/payload length consistency.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: &IpcHeader,
    payload: &[u8],
) -> Result<(), IpcError> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge {
            max: MAX_FRAME_SIZE,
            actual: payload.len(),
        });
    }
    let expected_len = header.payload_len as usize;
    if expected_len != payload.len() {
        return Err(IpcError::MalformedHeader(format!(
            "header.payload_len ({expected_len}) does not match payload.len() ({})",
            payload.len()
        )));
    }

    writer.write_all(header.as_bytes()).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size_and_alignment() {
        assert_eq!(HEADER_SIZE, 24);
        assert_eq!(std::mem::size_of::<IpcHeader>(), 24);
        assert_eq!(std::mem::align_of::<IpcHeader>(), 8);
    }

    #[test]
    fn test_header_parse_and_unaligned_slice() {
        let hdr = IpcHeader::new(MSG_TYPE_ARROW_OTEL_LOGS, 512, FLAG_HIGH_PRIORITY, 42);
        let mut padded = vec![0xFFu8];
        padded.extend_from_slice(hdr.as_bytes());
        padded.extend_from_slice(b"trailing");

        let (parsed, rest) = IpcHeader::parse(&padded[1..]).expect("parse unaligned slice");
        assert_eq!(parsed, hdr);
        assert_eq!(rest, b"trailing");
    }

    #[test]
    fn test_header_validation_errors() {
        let short = [0u8; 10];
        assert!(matches!(
            IpcHeader::parse(&short),
            Err(IpcError::BufferTooSmall {
                expected: 24,
                actual: 10
            })
        ));

        let mut bad_magic = IpcHeader::new(MSG_TYPE_FORY_CONTROL, 0, 0, 1);
        bad_magic.magic = *b"K3CI";
        assert!(matches!(
            IpcHeader::parse(bad_magic.as_bytes()),
            Err(IpcError::InvalidMagic)
        ));

        let mut bad_ver = IpcHeader::new(MSG_TYPE_FORY_CONTROL, 0, 0, 1);
        bad_ver.version = 99;
        assert!(matches!(
            IpcHeader::parse(bad_ver.as_bytes()),
            Err(IpcError::UnsupportedVersion {
                expected: 1,
                actual: 99
            })
        ));

        let oversize = IpcHeader::new(MSG_TYPE_ARROW_BENCH_RUNS, (MAX_FRAME_SIZE as u32) + 1, 0, 7);
        assert!(matches!(
            IpcHeader::parse(oversize.as_bytes()),
            Err(IpcError::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn test_async_read_write_frame_roundtrip() {
        let payload = b"mu-observable-ipc telemetry frame payload".to_vec();
        let header = IpcHeader::new(
            MSG_TYPE_ARROW_OTEL_SPANS,
            payload.len() as u32,
            FLAG_COMPRESSED | FLAG_HIGH_PRIORITY,
            1001,
        );

        let (mut client, mut server) = tokio::io::duplex(4096);
        write_frame(&mut client, &header, &payload)
            .await
            .expect("write_frame");

        let (decoded_hdr, decoded_payload) = read_frame(&mut server).await.expect("read_frame");
        assert_eq!(decoded_hdr, header);
        assert_eq!(decoded_payload, payload);
    }

    #[tokio::test]
    async fn test_write_frame_rejects_length_mismatch() {
        let header = IpcHeader::new(MSG_TYPE_FORY_CONTROL, 10, 0, 1);
        let (mut client, _server) = tokio::io::duplex(256);
        let err = write_frame(&mut client, &header, b"short").await;
        assert!(matches!(err, Err(IpcError::MalformedHeader(_))));
    }
}
