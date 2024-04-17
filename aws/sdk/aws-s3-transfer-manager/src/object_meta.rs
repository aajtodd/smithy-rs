use aws_sdk_s3::operation::{get_object::GetObjectOutput, head_object::HeadObjectOutput};

// Object metadata other than the body that can be set from either `GetObject` or `HeadObject`
pub(crate) struct ObjectResponseMeta {
    pub(crate) delete_marker: Option<bool>,
    pub(crate) accept_ranges: Option<String>,
    pub(crate) expiration: Option<String>,
    pub(crate) restore: Option<String>,
    pub(crate) last_modified: Option<::aws_smithy_types::DateTime>,
    pub(crate) content_length: Option<i64>,
    pub(crate) e_tag: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32_c: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) missing_meta: Option<i32>,
    pub(crate) version_id: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) content_language: Option<String>,
    pub(crate) content_range: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) expires: Option<::aws_smithy_types::DateTime>,
    pub(crate) website_redirect_location: Option<String>,
    pub(crate) server_side_encryption: Option<aws_sdk_s3::types::ServerSideEncryption>,
    pub(crate) metadata: Option<::std::collections::HashMap<String, String>>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) bucket_key_enabled: Option<bool>,
    pub(crate) storage_class: Option<aws_sdk_s3::types::StorageClass>,
    pub(crate) request_charged: Option<aws_sdk_s3::types::RequestCharged>,
    pub(crate) replication_status: Option<aws_sdk_s3::types::ReplicationStatus>,
    pub(crate) parts_count: Option<i32>,
    pub(crate) tag_count: Option<i32>,
    pub(crate) object_lock_mode: Option<aws_sdk_s3::types::ObjectLockMode>,
    pub(crate) object_lock_retain_until_date: Option<::aws_smithy_types::DateTime>,
    pub(crate) object_lock_legal_hold_status: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,
}

impl ObjectResponseMeta {
    pub(crate) fn total_size(&self) -> u64 {
        match (self.content_length, self.content_range) {
            (_, Some(range)) => {
                let total = range.splitn(2, '/').skip(1).next().expect("content range total");
                total.parse().expect("valid range total")
            }
            (Some(length), None) => length as u64,
            (None, None) => panic!("total object size cannot be calculated without either content length or content range headers")
        }
    }
}

impl From<GetObjectOutput> for ObjectResponseMeta {
    fn from(value: GetObjectOutput) -> Self {
        Self {
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges,
            expiration: value.expiration,
            restore: value.restore,
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            missing_meta: value.missing_meta,
            version_id: value.version_id,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_range: value.content_range,
            content_type: value.content_type,
            expires: value.expires,
            website_redirect_location: value.website_redirect_location,
            server_side_encryption: value.server_side_encryption,
            metadata: value.metadata,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            ssekms_key_id: value.ssekms_key_id,
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class,
            request_charged: value.request_charged,
            replication_status: value.replication_status,
            parts_count: value.parts_count,
            tag_count: value.tag_count,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
        }
    }
}

impl From<HeadObjectOutput> for ObjectResponseMeta {
    fn from(value: HeadObjectOutput) -> Self {
        Self { 
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges,
            expiration: value.expiration,
            restore: value.restore,
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            missing_meta: value.missing_meta,
            version_id: value.version_id,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_range: None,
            content_type: value.content_type,
            expires: value.expires,
            website_redirect_location: value.website_redirect_location,
            server_side_encryption: value.server_side_encryption,
            metadata: value.metadata,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            ssekms_key_id: value.ssekms_key_id,
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class,
            request_charged: value.request_charged,
            replication_status: value.replication_status,
            parts_count: value.parts_count,
            tag_count: None,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
        }
    }
}
