use std::{mem, ops::RangeInclusive, str::FromStr};

use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_smithy_types::{
    body::SdkBody,
    byte_stream::{AggregatedBytes, ByteStream},
};
use bytes::Buf;

use crate::{
    error::{DownloadError, TransferError},
    header::{ByteRange, Range},
    object_meta::ObjectResponseMeta,
    types::{DownloadHandle, DownloadRequest},
};

/// Result of initial object discovery
#[derive(Debug, Clone)]
pub(crate) struct DiscoverResult {
    // range of data remaining
    pub(crate) remaining: RangeInclusive<u64>,
    // the discovered metadata (may be none if we didn't execute a request)
    pub(crate) object_meta: Option<ObjectResponseMeta>,
    // the first chunk of data if discovery involved `GetObject` for first part/range
    pub(crate) initial_chunk_data: Option<AggregatedBytes>,
}

enum DiscoverObjectSizeStrategy {
    // Send a `HeadObject` request to discover the object size,
    // optionally constrained tp the given range
    HeadObject(Option<ByteRange>),
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
            Some(h) => {
                let byte_range = Range::from_str(h)?.0;
                match byte_range {
                    ByteRange::Inclusive(start, end) => {
                        DiscoverObjectSizeStrategy::RangeGiven(start..=end)
                    }
                    // TODO: explore when given a start range what it would look like to just start
                    // sending requests from [start, start+part_size].
                    _ => DiscoverObjectSizeStrategy::HeadObject(Some(byte_range)),
                }
            }
            None => DiscoverObjectSizeStrategy::RangedGet,
        };

        Ok(strategy)
    }
}

/// Discover the initial object size and metadata
pub(crate) async fn discover_obj_size(
    handle: &DownloadHandle,
    request: &DownloadRequest,
) -> Result<DiscoverResult, TransferError> {
    let strategy = DiscoverObjectSizeStrategy::from_request(request)?;
    match strategy {
        DiscoverObjectSizeStrategy::HeadObject(byte_range) => {
            discover_obj_size_with_head(handle, request, byte_range).await
        }
        DiscoverObjectSizeStrategy::FirstPart => {
            let r = request.inner.clone().part_number(1);
            discover_obj_size_with_get(handle, r).await
        }
        DiscoverObjectSizeStrategy::RangedGet => {
            let r = request
                .inner
                .clone()
                .set_part_number(None)
                .range(Range::bytes(ByteRange::Inclusive(
                    0,
                    handle.target_part_size,
                )));

            discover_obj_size_with_get(handle, r).await
        }
        DiscoverObjectSizeStrategy::RangeGiven(range) => Ok(DiscoverResult {
            remaining: range,
            object_meta: None,
            initial_chunk_data: None,
        }),
    }
}

async fn discover_obj_size_with_head(
    handle: &DownloadHandle,
    request: &DownloadRequest,
    byte_range: Option<ByteRange>,
) -> Result<DiscoverResult, TransferError> {
    let meta: ObjectResponseMeta = handle
        .client
        .head_object()
        .set_bucket(request.inner.get_bucket().clone())
        .set_key(request.inner.get_key().clone())
        .send()
        .await
        .map_err(|e| DownloadError::DiscoverFailed(e.into()))?
        .into();

    let remaining = match byte_range {
        Some(range) => match range {
            ByteRange::Inclusive(start, end) => start..=end,
            ByteRange::AllFrom(start) => start..=meta.total_size(),
            ByteRange::Last(n) => (meta.total_size() - n + 1)..=meta.total_size()
        }
        None =>  0..=meta.total_size(),
    };

    Ok(DiscoverResult {
        remaining,
        object_meta: Some(meta),
        initial_chunk_data: None,
    })
}

async fn discover_obj_size_with_get(
    handle: &DownloadHandle,
    request: GetObjectInputBuilder,
) -> Result<DiscoverResult, TransferError> {
    let resp = request.send_with(&handle.client).await;

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
    let remaining = (data.remaining() as u64 + 1)..=meta.total_size();

    Ok(DiscoverResult {
        remaining,
        object_meta: Some(meta),
        initial_chunk_data: Some(data),
    })
}
