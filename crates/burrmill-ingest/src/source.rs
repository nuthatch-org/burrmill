//! Chunked input. No paths and no mmap: a tab feeds `File.stream()`, a server feeds a reader.

use std::io::Read;

use bytes::Bytes;

use crate::error::{IngestError, Result};

pub trait ByteSource {
    fn next_chunk(&mut self) -> Result<Option<Bytes>>;
    fn len_hint(&self) -> Option<u64> {
        None
    }
}

/// A buffer already in hand. Copies each chunk so the trait can return owned bytes.
pub struct SliceSource<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceSource<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
}

impl ByteSource for SliceSource<'_> {
    fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let end = (self.pos + 64 * 1024).min(self.data.len());
        let chunk = Bytes::copy_from_slice(&self.data[self.pos..end]);
        self.pos = end;
        Ok(Some(chunk))
    }

    fn len_hint(&self) -> Option<u64> {
        Some(self.data.len() as u64)
    }
}

/// Native or wasm `Read`. The chunk size is the unit `max_bytes` is checked on.
pub struct ReadSource<R> {
    inner: R,
    chunk: usize,
    hint: Option<u64>,
}

impl<R: Read> ReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            chunk: 64 * 1024,
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: u64) -> Self {
        self.hint = Some(hint);
        self
    }
}

impl<R: Read> ByteSource for ReadSource<R> {
    fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let mut buf = vec![0u8; self.chunk];
        let mut filled = 0;
        while filled < buf.len() {
            match self.inner.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(IngestError::Source(e.to_string())),
            }
        }
        if filled == 0 {
            Ok(None)
        } else {
            buf.truncate(filled);
            Ok(Some(Bytes::from(buf)))
        }
    }

    fn len_hint(&self) -> Option<u64> {
        self.hint
    }
}
