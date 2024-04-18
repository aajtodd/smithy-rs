/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{cmp, collections::BinaryHeap, ops::RangeInclusive};

use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_types::SdkConfig;
use bytes::Buf;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

use crate::{
    discovery::{discover_obj_size, DiscoverResult},
    error::{self, TransferError},
    header::Range,
    object_meta::ObjectResponseMeta,
    types::{ChunkRequest, ChunkResponse, DownloadHandle, DownloadRequest, DownloadResponse},
    MEBI_BYTE, MIN_PART_SIZE,
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

    pub async fn download<T: AsyncWrite + Unpin>(
        &self,
        dest: &mut T,
        request: DownloadRequest,
    ) -> Result<DownloadResponse, TransferError> {
        // if there is a part number then just send the default request
        if request.inner.get_part_number().is_some() {
            // let llr = request.get_object_request.send();
            todo!("single part download not implemented");
        }

        let handle = DownloadHandle {
            client: self.client.clone(),
            target_part_size: self.target_part_size_bytes,
        };

        // make initial discovery about the object size, metadata, possibly first chunk
        let mut discovery = discover_obj_size(&handle, &request).await?;

        let (comp_tx, comp_rx) = mpsc::channel(self.concurrency);

        if let Some(data) = discovery.initial_chunk_data.take() {
            // ensure initial chunk is written
            let chunk = ChunkResponse {
                seq: 0,
                data: Some(data),
                object_meta: discovery.object_meta.clone(),
            };
            comp_tx.send(Ok(chunk)).await.expect("channel empty");
        }

        if discovery.remaining.is_empty() {
            // discovery fetched all the payload
            drop(comp_tx);
        } else {
            // start assigning work
            let (work_tx, work_rx) = async_channel::bounded(self.concurrency);
            let input = request.inner.clone();
            let part_size = self.target_part_size_bytes;

            // FIXME - I think we'll need to cancel/abort some of these tasks on failures
            tokio::spawn(distribute_work(discovery, input, part_size, work_tx));

            // spin up workers
            for _ in 0..self.concurrency {
                let worker = chunk_downloader(handle.clone(), work_rx.clone(), comp_tx.clone());
                tokio::spawn(worker);
            }
        }

        // should block until all work has completed
        let resp = recv_chunks(dest, comp_rx).await;
        if resp.is_err() {
            todo!("handle cancelling tasks spawned")
        }

        resp
    }
}

/// Process chunks off the channel and write them to dest in sequence
async fn recv_chunks<T: AsyncWrite + Unpin>(
    dest: &mut T,
    mut chunks: mpsc::Receiver<Result<ChunkResponse, TransferError>>,
) -> Result<DownloadResponse, TransferError> {

    let mut response = DownloadResponse::builder();
    let mut sequencer = Sequencer::new(0);

    while let Some(chunk) = chunks.recv().await {
        match chunk {
            Ok(chunk_resp) => sequencer.push(chunk_resp),
            Err(err) => return Err(err),
        }

        // FIXME - drain as much as possible
       
        if matches!(sequencer.peek(), Some(chunk) if chunk.seq == next_seq) {
            let chunk_resp = sequencer.pop().expect("matched already");

            // initial response metadata may not have been set if we didn't do a `GetObject` for the first chunk
            response.object_metadata(chunk_resp.object_meta);
            if let Some(mut data) = chunk_resp.data {
                // TODO - vectored write
                while data.has_remaining() {
                    dest.write_buf(&mut data)
                        .await
                        .map_err(error::chunk_failed)?;
                }
            }
        }
    }

    // all chunks received, drain any remaining
    if !sequencer.is_empty() {
    }

    Ok(response.build())
}


struct Sequencer {
    // TODO - explore other collections
    responses: BinaryHeap<ChunkResponse>,
    next_seq: u64,
}

impl Sequencer {
    fn new(next_seq: u64) -> Self {
        Self {
            responses: BinaryHeap::new(),
            next_seq
        }
    }

    fn push(&mut self, chunk: ChunkResponse) {
        self.responses.push(chunk);
    }

    fn is_empty(&self) -> bool {
        self.responses.is_empty()
    }

    fn pop(&mut self) -> Option<ChunkResponse> {
        self.responses.pop()
    }

    fn peek(&self) -> Option<&ChunkResponse> {
        self.responses.peek()
    }

}

// Worker function that processes requests from the `requests` channel and
// sends the result back on the `completed` channel.
async fn chunk_downloader(
    handle: DownloadHandle,
    requests: async_channel::Receiver<ChunkRequest>,
    completed: mpsc::Sender<Result<ChunkResponse, TransferError>>,
) {
    while let Ok(request) = requests.recv().await {
        let result = download_chunk(&handle, request).await;
        if let Err(err) = completed.send(result).await {
            tracing::debug!(error = ?err, "chunk worker send failed");
            return;
        }
    }
}

async fn download_chunk(
    handle: &DownloadHandle,
    request: ChunkRequest,
) -> Result<ChunkResponse, TransferError> {
    let resp = request
        .input
        .send_with(&handle.client)
        .await
        .map_err(error::chunk_failed)?;

    let bytes = resp.body.collect().await.map_err(error::chunk_failed)?;

    let resp = ChunkResponse {
        seq: request.seq,
        data: Some(bytes),
        // FIXME - set meta
        object_meta: None,
    };

    Ok(resp)
}

async fn distribute_work(
    discovery: DiscoverResult,
    input: GetObjectInputBuilder,
    part_size: u64,
    tx: async_channel::Sender<ChunkRequest>,
) {
    let end = *discovery.remaining.end();
    let mut pos = *discovery.remaining.start();
    let mut remaining = end - pos;

    // NOTE: start at 1 in case there was an initial chunk in discovery
    let mut seq = 1;

    while remaining > 0 {
        let start = pos;
        let end_inclusive = cmp::min(pos + part_size, end);

        let chunk_req = next_chunk(start, end_inclusive, seq, input.clone());
        let chunk_size = chunk_req.size();
        tx.send(chunk_req).await.expect("channel open");

        seq += 1;
        remaining -= chunk_size;
        pos += chunk_size;
    }
}

fn next_chunk(
    start: u64,
    end_inclusive: u64,
    seq: u64,
    input: GetObjectInputBuilder,
) -> ChunkRequest {
    let range = start..=end_inclusive;
    let input = input.range(Range::bytes_inclusive(start, end_inclusive));
    ChunkRequest { seq, range, input }
}
