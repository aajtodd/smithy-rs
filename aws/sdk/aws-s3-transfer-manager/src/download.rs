/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{cmp, mem, ops::RangeInclusive, str::FromStr};

use aws_sdk_s3::operation::get_object::{
    builders::{GetObjectFluentBuilder, GetObjectInputBuilder},
    GetObjectOutput,
};
use aws_smithy_types::{byte_stream::{AggregatedBytes, ByteStream}, body::SdkBody};
use aws_types::SdkConfig;
use bytes::Buf;
use tokio::{io::AsyncWrite, sync::mpsc};

use crate::{
    header::{ByteRange, Range},
    object_meta::ObjectResponseMeta,
    DownloadError, TransferError, MEBI_BYTE, MIN_PART_SIZE,
};

// FIXME - SEP specifies this should be a config option but I'm not seeing a compelling reason why
//         the user should have to (or want to) care about it. It also may lead to less efficient
//         code paths depending on the input if we are forced to use a specific download type.
//
// /// Specify how the SDK should perform a multipart download
// #[derive(Debug, Clone)]
// enum MultipartDownloadType {
//     /// Use `GetObject` with part number set
//     Part,
//
//     /// Use ranged `GET`
//     Range,
// }

// TODO - need to set User-Agent header value `ft/hll#s3-transfer`

#[derive(Debug, Clone)]
pub struct Builder {
    target_part_size_bytes: u64,
    checksum_validation_enabled: bool,
    concurrency: usize,
    // TODO
    // checksum_algorithm: Option<ChecksumAlgorithm>
    sdk_config: Option<SdkConfig>,
}

impl Builder {
    fn new() -> Self {
        Self {
            target_part_size_bytes: 8 * MEBI_BYTE,
            checksum_validation_enabled: true,
            concurrency: 8,
            sdk_config: None,
        }
    }

    /// Size of parts the object will be downloaded in, in bytes.
    /// The minimum part size is 5 MiB and any value give lessen than that will be rounded up.
    /// Defaults to 8 MiB.
    pub fn target_part_size(mut self, size_bytes: u64) -> Self {
        self.target_part_size_bytes = cmp::min(size_bytes, MIN_PART_SIZE);
        self
    }

    pub fn enable_checksum_validation(mut self, enabled: bool) -> Self {
        self.checksum_validation_enabled = enabled;
        self
    }

    pub fn sdk_config(mut self, config: SdkConfig) -> Self {
        self.sdk_config = Some(config);
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn build(self) -> Downloader {
        self.into()
    }
}

impl From<Builder> for Downloader {
    fn from(value: Builder) -> Self {
        let sdk_config = value
            .sdk_config
            .unwrap_or_else(|| SdkConfig::builder().build());
        let client = aws_sdk_s3::Client::new(&sdk_config);
        Self {
            target_part_size_bytes: value.target_part_size_bytes,
            checksum_validation_enabled: value.checksum_validation_enabled,
            concurrency: value.concurrency,
            client,
        }
    }
}

// TODO - builders + convenience conversions for request/response types

pub struct DownloadRequest {
    inner: GetObjectInputBuilder,
}

// FIXME - should probably be TryFrom since checksums may conflict?
impl From<GetObjectFluentBuilder> for DownloadRequest {
    fn from(value: GetObjectFluentBuilder) -> Self {
        Self {
            inner: value.as_input().clone(),
        }
    }
}

pub struct DownloadResponse {
    inner: ObjectResponseMeta,
}

// TODO - we may want to hide this type behind a higher level "TransferManager" type and construct
// the client there

#[derive(Debug, Clone)]
pub struct Downloader {
    target_part_size_bytes: u64,
    checksum_validation_enabled: bool,
    concurrency: usize,
    client: aws_sdk_s3::client::Client,
    // TODO - concurrency settings
    // TODO - object/part pool
}

impl Downloader {
    pub fn builder() -> Builder {
        Builder::new()
    }

    // TODO(design): SEP says we should abstract over request/response and give it same fields as
    // the GetObject response. This has some disadvantages though as we can't go immediately from
    // parts -> output (e.g. possibly using vectored writes). It may be better to take something
    // like `AsyncWrite` (similart to Go taking an io.Writer). Perhaps this can be a feature gate?

    // TODO(design): SEP says to return immediately and provide cancellation/resume mechanism
    //               e.g. fn download(&self, request: DownloadRequest) -> DownloadTransfer
    //               let transfer = dl.download(...)
    //               transfer.send().await;
    //
    // TODO(design): SEP says to provide progress

    pub async fn download<T: AsyncWrite>(
        &self,
        dest: T,
        request: DownloadRequest,
    ) -> Result<DownloadResponse, TransferError> {
        // if there is a part number then just send the default request
        if request.inner.get_part_number().is_some() {
            // let llr = request.get_object_request.send();
            // let _llr = request.inner.send_with(&self.client).await.map_err(|e| DownloadError::ChunkFailed { source: e.into() })?;
            todo!("single part download not implemented");
        }

        // make initial discovery about the object size, metadata, possibly first chunk
        let discovery = self.discover_obj_size(&request).await?;

        let (tx, rx) = mpsc::channel(self.concurrency);
        if let Some(chunk) = discovery.initial_chunk {
            // ensure initial chunk is written
            tx.send(chunk).await.expect("receiver open");
        }

        let mut handles = Vec::new();
        if discovery.remaining.is_empty() {
            drop(tx);
        }else {
            // TODO start seeding work

            for _ in 0..self.concurrency {
                let worker = download_worker(tx.clone());
                let handle = tokio::spawn(async move {
                    worker.await
                });

                handles.push(handle);
            }
        }

        recv_chunks(dest, &request, rx).await;

        todo!("not implemented");
    }

    async fn discover_obj_size(
        &self,
        request: &DownloadRequest,
    ) -> Result<DiscoverResult, TransferError> {
        let strategy = DiscoverObjectSizeStrategy::from_request(&request)?;
        match strategy {
            DiscoverObjectSizeStrategy::HeadObject => {
                self.discover_obj_size_with_head(&request).await
            }
            DiscoverObjectSizeStrategy::FirstPart => {
                let r = request.inner.clone().part_number(1);
                return self.discover_obj_size_with_get(r).await;
            }
            DiscoverObjectSizeStrategy::RangedGet => {
                let r = request
                    .inner
                    .clone()
                    .set_part_number(None)
                    .range(Range::bytes(ByteRange::Inclusive(
                        0,
                        self.target_part_size_bytes,
                    )));

                return self.discover_obj_size_with_get(r).await;
            }
            DiscoverObjectSizeStrategy::RangeGiven(range) => Ok(DiscoverResult {
                remaining: range,
                object_meta: None,
                initial_chunk: None,
            }),
        }
    }

    async fn discover_obj_size_with_head(
        &self,
        request: &DownloadRequest,
    ) -> Result<DiscoverResult, TransferError> {
        let meta: ObjectResponseMeta = self
            .client
            .head_object()
            .set_bucket(request.inner.get_bucket().clone())
            .set_key(request.inner.get_key().clone())
            .send()
            .await
            .map_err(|e| DownloadError::DiscoverFailed(e.into()))?
            .into();

        let remaining = 0..=meta.total_size();
        Ok(DiscoverResult {
            remaining,
            object_meta: Some(meta),
            initial_chunk: None,
        })
    }

    async fn discover_obj_size_with_get(
        &self,
        request: GetObjectInputBuilder,
    ) -> Result<DiscoverResult, TransferError> {
        let resp = request.send_with(&self.client).await;

        if resp.is_err() {
            // TODO - deal with empty file errors, see https://github.com/awslabs/aws-c-s3/blob/v0.5.7/source/s3_auto_ranged_get.c#L147-L153
        }

        let mut resp = resp.map_err(|e| DownloadError::DiscoverFailed(e.into()))?;
        let empty_stream = ByteStream::new(SdkBody::empty());
        let body = mem::replace(&mut resp.body, empty_stream);

        let data = body
            .collect()
            .await
            .map_err(|e| DownloadError::DiscoverFailed(e.into()))?;

        let meta: ObjectResponseMeta = resp.into();
        let remaining = (data.remaining() as u64 + 1) ..=meta.total_size();

        Ok(DiscoverResult {
            remaining,
            object_meta: Some(meta),

            // FIXME - make initial chunk
            initial_chunk: None,
        })
    }
}

struct DiscoverResult {
    // range of data remaining
    remaining: RangeInclusive<u64>,
    // the discovered metadata (may be none if we didn't execute a request)
    object_meta: Option<ObjectResponseMeta>,
    initial_chunk: Option<ChunkResponse>,
}

enum DiscoverObjectSizeStrategy {
    // Send a `HeadObject` request to discover the object size
    HeadObject,
    // Send `GetObject` with `part_number` = 1
    FirstPart,
    // Send `GetObject` for range [0, part_size]
    RangedGet,
    // Range given in request, we don't need to know the object size
    RangeGiven(RangeInclusive<u64>),
}

impl DiscoverObjectSizeStrategy {
    fn from_request(
        request: &DownloadRequest,
    ) -> Result<DiscoverObjectSizeStrategy, TransferError> {
        let strategy = match request.inner.get_range() {
            Some(h) => match Range::from_str(h)?.0 {
                ByteRange::Inclusive(start, end) => {
                    DiscoverObjectSizeStrategy::RangeGiven(start..=end)
                }
                // TODO: explore when given a start range what it would look like to just start
                // sending requests from [start, start+part_size].
                _ => DiscoverObjectSizeStrategy::HeadObject,
            },
            None => DiscoverObjectSizeStrategy::RangedGet,
        };

        Ok(strategy)
    }
}

// TODO - switch to enum for chunk vs part
#[derive(Debug, Clone)]
struct ChunkRequest {
    // byte range to download
    range: RangeInclusive<u64>,

    // TODO - explore pooled buffers
    // buf: BytesMut,

    // sequence number
    seq: u64,
}

#[derive(Debug, Clone)]
struct ChunkResponse {
    // the request for this chunk
    request: ChunkRequest,

    // chunk data
    data: AggregatedBytes,

    // sequence number
    seq: u64,
}

/// Process chunks off the channel and write them to dest in sequence
async fn recv_chunks<T: AsyncWrite>(
    dest: T,
    request: &DownloadRequest,
    mut chunks: mpsc::Receiver<ChunkResponse>,
) -> Result<(), TransferError> {
    while let Some(chunk) = chunks.recv().await {}

    // TODO - response type
    Ok(())
}

async fn download_worker(completed: mpsc::Sender<ChunkResponse>) {
}

/*

Need async-channel apparently?

Structural:
1. spin up worker to receive chunks
2. spin up concurrent workers that pull off a queue/channel for work and do the download for a single chunk


async fn recvChunks(writer, concurrency, channel<ChunkRequest>) -> ?? {
    // process chunks off the channel and write them
}


pub async fn download(&self, request: DownloadRequest) -> Result<DownloadResponse, TransferError>

#[cfg(all(feature = "rt-tokio", not(target_family = "wasm")))]

pub async fn download(&self, request: DownloadRequest, sink: AsyncWrite) -> Result<?, TransferError>

// when we do a ranged get on an object with a size greater than the part length:
    content_length: Some(
        5242881,
    ),
    content_range: Some(
        "bytes 0-5242880/1073741824",
    ),

// when we do a ranged get on an object with a size less than or equal part length:
   content_length: Some(
        2097152,
    ),
    content_range: Some(
        "bytes 0-2097151/2097152",
    ),
 */
