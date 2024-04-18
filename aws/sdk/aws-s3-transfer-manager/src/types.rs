use std::ops::RangeInclusive;

use aws_sdk_s3::operation::get_object::builders::{GetObjectInputBuilder, GetObjectFluentBuilder};
use aws_smithy_types::byte_stream::AggregatedBytes;

use crate::object_meta::ObjectResponseMeta;

pub struct DownloadRequest {
    pub(crate) inner: GetObjectInputBuilder,
}

// FIXME - should probably be TryFrom since checksums may conflict?
impl From<GetObjectFluentBuilder> for DownloadRequest {
    fn from(value: GetObjectFluentBuilder) -> Self {
        Self {
            inner: value.as_input().clone(),
        }
    }
}

// FIXME - expose common fields?
pub struct DownloadResponse {
    pub(crate) inner: Option<ObjectResponseMeta>,
}

impl DownloadResponse {
    pub(crate) fn builder() -> DownloadResponseBuilder {
        DownloadResponseBuilder::new()
    }
}

#[derive(Default)]
pub(crate) struct DownloadResponseBuilder {
    inner: Option<ObjectResponseMeta>
}

impl DownloadResponseBuilder {
    fn new() -> Self {
        DownloadResponseBuilder::default()
    }

    pub(crate) fn object_metadata(&mut self, meta: Option<ObjectResponseMeta>) {
        if meta.is_some() && self.inner.is_none() {
            self.inner = meta;
        }
    }

    pub(crate) fn build(self) -> DownloadResponse {
        DownloadResponse {
            inner: self.inner
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DownloadHandle {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) target_part_size: u64,
}


// FIXME - should probably be enum ChunkRequest { Range(..), Part(..) } or have an inner field like such
#[derive(Debug, Clone)]
pub(crate) struct ChunkRequest {
    // byte range to download
    pub(crate) range: RangeInclusive<u64>,
    pub(crate) input: GetObjectInputBuilder,
    // TODO - explore pooled buffers
    // buf: BytesMut,

    // sequence number
    pub(crate) seq: u64,
}

impl ChunkRequest {
    /// Size of this chunk request in bytes
    pub(crate) fn size(&self) -> u64 {
        self.range.end() - self.range.start() + 1
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkResponse {
    // the seq number
    pub(crate) seq: u64,
    // chunk data
    pub(crate) data: Option<AggregatedBytes>,
    pub(crate) object_meta: Option<ObjectResponseMeta>
}


impl Ord for ChunkResponse {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

impl PartialOrd for ChunkResponse {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ChunkResponse {}

impl PartialEq for ChunkResponse {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}
