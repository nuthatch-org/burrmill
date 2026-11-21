//! `object_store`'s `LocalFileSystem`, reading ranges on the calling thread.
//!
//! Upstream hands every read to `spawn_blocking` whenever a runtime is current, which inside a
//! statement it always is, and a ranged `get_range` makes two such trips. A segment is small and
//! read where it is cached, so the trip costs more than the read. Reads over [`INLINE`] still
//! take the trip, so a cold scan keeps the blocking pool's concurrency for what is large.

use std::ops::Range;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

const INLINE: u64 = 1 << 20;

#[derive(Debug)]
pub(crate) struct InlineLocal(Arc<LocalFileSystem>);

impl InlineLocal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(LocalFileSystem::new()))
    }
}

impl std::fmt::Display for InlineLocal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InlineLocal")
    }
}

fn io(path: &std::path::Path, e: std::io::Error) -> object_store::Error {
    match e.kind() {
        std::io::ErrorKind::NotFound => object_store::Error::NotFound {
            path: path.display().to_string(),
            source: e.into(),
        },
        _ => object_store::Error::Generic {
            store: "InlineLocal",
            source: format!("{}: {e}", path.display()).into(),
        },
    }
}

fn read(file: &std::fs::File, path: &std::path::Path, r: Range<u64>) -> Result<Bytes> {
    let mut buf = vec![0u8; (r.end - r.start) as usize];
    file.read_exact_at(&mut buf, r.start)
        .map_err(|e| io(path, e))?;
    Ok(buf.into())
}

/// As upstream's `convert_metadata`, so a cached footer's key matches the listing's.
fn meta(m: &std::fs::Metadata, location: Path) -> ObjectMeta {
    let mtime = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .unwrap_or_default();
    ObjectMeta {
        location,
        last_modified: chrono::DateTime::from_timestamp(
            mtime.as_secs() as i64,
            mtime.subsec_nanos(),
        )
        .unwrap_or_default(),
        size: m.len(),
        e_tag: Some(format!(
            "{:x}-{:x}-{:x}",
            m.ino(),
            mtime.as_micros(),
            m.len()
        )),
        version: None,
    }
}

#[async_trait::async_trait]
impl ObjectStore for InlineLocal {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let plain = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none();
        let (Some(range), true, false) = (options.range.clone(), plain, options.head) else {
            return self.0.get_opts(location, options).await;
        };
        let path = self.0.path_to_filesystem(location)?;
        let file = std::fs::File::open(&path).map_err(|e| io(&path, e))?;
        let m = file.metadata().map_err(|e| io(&path, e))?;
        let range = range
            .as_range(m.len())
            .map_err(|e| object_store::Error::Generic {
                store: "InlineLocal",
                source: e.to_string().into(),
            })?;
        if range.end - range.start > INLINE {
            return self.0.get_opts(location, options).await;
        }
        let bytes = read(&file, &path, range.clone())?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async { Ok(bytes) }))),
            meta: meta(&m, location.clone()),
            range,
            attributes: Default::default(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.iter().map(|r| r.end - r.start).sum::<u64>() > INLINE {
            return self.0.get_ranges(location, ranges).await;
        }
        let path = self.0.path_to_filesystem(location)?;
        let file = std::fs::File::open(&path).map_err(|e| io(&path, e))?;
        ranges
            .iter()
            .map(|r| read(&file, &path, r.clone()))
            .collect()
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}
