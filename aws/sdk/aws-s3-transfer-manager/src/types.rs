use std::{ops::RangeInclusive};

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

impl From<GetObjectInputBuilder> for DownloadRequest {
    fn from(value: GetObjectInputBuilder) -> Self {
        Self {
            inner: value
        }
    }
}

// FIXME - expose common fields?
pub struct DownloadResponse {
    /// Object metadata
    pub object_meta: ObjectResponseMeta,
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
        // FIXME - object metadata needs corrected since chunks have Content-Length set to chunk size
        // which isn't the same as the total size. Probably should sanitize other fields like
        // range.
        let mut meta = self.inner.expect("response metadata set");
        meta.content_length = Some(meta.total_size() as i64);
        DownloadResponse {
            object_meta: meta
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DownloadHandle {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) target_part_size: u64,
}

// #[derive(Debug, Clone)]
// pub(crate) struct SharedDownloadHandle(Arc<DownloadHandle>);
//
// impl SharedDownloadHandle {
//     fn client(&self) -> &aws_sdk_s3::Client {
//         &self.0.client
//     }
//
//     fn target_part_size(&self) -> u64 {
//         self.0.target_part_size
//     }
//
//     fn input(&self) -> &GetObjectInputBuilder {
//         &self.0.input
//     }
//
//     fn set_response_meta(&self, meta: Option<ObjectResponseMeta>) {
//         let mut g = self.0.response.lock().unwrap();
//         *g = meta;
//     }
// }


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
    // object metadata
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
