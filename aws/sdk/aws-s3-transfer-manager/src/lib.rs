/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/* Automatically managed default lints */
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
/* End of automatically managed default lints */

//! AWS S3 Transfer Manager
//!
//! # Crate Features
//!
//! - `test-util`: Enables utilities for unit tests. DO NOT ENABLE IN PRODUCTION.

#![warn(
    // TODO - re-enable missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

use aws_smithy_types::byte_stream;

pub(crate) const MEBI_BYTE: u64 = 1024 * 1024;
pub(crate) const GIBI_BYTE: u64 = MEBI_BYTE * 1024;
pub(crate) const MIN_PART_SIZE: u64 = 5 * MEBI_BYTE;

#[derive(thiserror::Error, Debug)]
pub enum TransferError {
    #[error("invalid meta request: {0}")]
    InvalidMetaRequest(String),

    #[error("download failed")]
    DownloadFailed(#[from] DownloadError),
}

pub(crate) type GetObjectSdkError = ::aws_smithy_runtime_api::client::result::SdkError<
    aws_sdk_s3::operation::get_object::GetObjectError,
    ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
>;
pub(crate) type HeadObjectSdkError = ::aws_smithy_runtime_api::client::result::SdkError<
    aws_sdk_s3::operation::head_object::HeadObjectError,
    ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
>;

#[derive(thiserror::Error, Debug)]
pub enum DownloadError {
    #[error(transparent)]
    DiscoverFailed(SdkOperationError),

    #[error("download chunk failed")]
    ChunkFailed { source: GetObjectSdkError },
}

#[derive(thiserror::Error, Debug)]
pub enum SdkOperationError {
    #[error(transparent)]
    HeadObject(#[from] HeadObjectSdkError),

    #[error(transparent)]
    GetObject(#[from] GetObjectSdkError),

    #[error(transparent)]
    ReadError(#[from] byte_stream::error::Error),
}

mod header;
mod object_meta;

pub mod download;
