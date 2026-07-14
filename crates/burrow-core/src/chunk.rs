//! Content-defined chunking.
//!
//! The FastCDC parameters below are part of burrow's on-disk format: changing
//! any of them changes chunk boundaries, which breaks deduplication against
//! every previously stored chunk. They are frozen forever; a future format
//! revision must version them explicitly.

use std::io::Read;

use crate::error::Result;

pub const CHUNK_MIN: usize = 512 * 1024;
pub const CHUNK_AVG: usize = 1024 * 1024;
pub const CHUNK_MAX: usize = 4 * 1024 * 1024;

/// Split a byte stream into content-defined chunks. Yields owned chunk bodies
/// in order; identical content always produces identical chunk boundaries.
pub fn chunk_stream<R: Read>(reader: R) -> impl Iterator<Item = Result<Vec<u8>>> {
    fastcdc::v2020::StreamCDC::new(reader, CHUNK_MIN, CHUNK_AVG, CHUNK_MAX).map(|item| {
        let chunk = item.map_err(|e| match e {
            fastcdc::v2020::Error::IoError(io) => crate::CoreError::Io(io),
            other => crate::CoreError::Io(std::io::Error::other(other.to_string())),
        })?;
        Ok(chunk.data)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
        // xorshift64*, deterministic across runs — test data must never change
        let mut s = seed.max(1);
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            out.extend_from_slice(&s.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn chunking_is_deterministic() {
        let data = pseudo_random(10 * 1024 * 1024, 42);
        let a: Vec<Vec<u8>> = chunk_stream(&data[..]).map(|c| c.unwrap()).collect();
        let b: Vec<Vec<u8>> = chunk_stream(&data[..]).map(|c| c.unwrap()).collect();
        assert_eq!(a, b);
        assert!(a.len() > 1, "10 MiB must split into multiple chunks");
        let total: usize = a.iter().map(Vec::len).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn chunk_sizes_respect_bounds() {
        let data = pseudo_random(20 * 1024 * 1024, 7);
        let chunks: Vec<Vec<u8>> = chunk_stream(&data[..]).map(|c| c.unwrap()).collect();
        for (i, c) in chunks.iter().enumerate() {
            assert!(c.len() <= CHUNK_MAX);
            if i + 1 < chunks.len() {
                assert!(c.len() >= CHUNK_MIN);
            }
        }
    }

    #[test]
    fn frozen_constants_golden_boundaries() {
        // Golden test: if this fails, the chunking format changed and dedup
        // against existing repositories is broken. Do not update the expected
        // values — fix the regression instead.
        let data = pseudo_random(8 * 1024 * 1024, 1234);
        let sizes: Vec<usize> = chunk_stream(&data[..]).map(|c| c.unwrap().len()).collect();
        // Recorded at format-freeze (fastcdc 4.0.1, v2020, 512K/1M/4M):
        const GOLDEN: &[usize] = &[1201795, 537172, 1563854, 1255399, 545948, 1913541, 1370899];
        assert_eq!(sizes, GOLDEN, "chunk boundaries changed — format break!");
    }
}
