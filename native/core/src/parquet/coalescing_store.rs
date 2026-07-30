// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`CoalescingObjectStore`] — an [`ObjectStore`] decorator that merges adjacent byte
//! ranges in [`ObjectStore::get_ranges`] into a single fetch and slices the result back.
//!
//! Background: `object_store`'s default `get_ranges` coalesces nearby ranges (via
//! `coalesce_ranges`), but `object_store_opendal::OpendalStore` overrides `get_ranges`
//! with per-range concurrent reads and drops the coalescing (apache/opendal#7380). On the
//! HDFS path a Parquet row group's projected column chunks are physically contiguous, so
//! without coalescing they become many small reads instead of one large sequential read.
//!
//! This decorator restores the merge. It is opt-in via `COMET_HDFS_OPT=coalesce` (wired in
//! `parquet_support::create_hdfs_object_store`) so the three read-path variants can be
//! A/B/C benchmarked from a single build (see BENCHMARK-Parquet-HDFS-Optimize.md).

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    coalesce_ranges, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, OBJECT_STORE_COALESCE_DEFAULT,
};

/// Wraps an inner [`ObjectStore`] and, in [`ObjectStore::get_ranges`], merges adjacent
/// ranges whose gap is `<= coalesce_gap` into one [`ObjectStore::get_range`], then slices
/// the fetched bytes back to the originally requested ranges. Every other method is
/// forwarded verbatim to the inner store.
pub(crate) struct CoalescingObjectStore {
    inner: Arc<dyn ObjectStore>,
    /// Two adjacent ranges are merged when the gap between them is `<= coalesce_gap`.
    coalesce_gap: u64,
}

impl CoalescingObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        // Defaults to object_store's own 1 MiB coalescing gap; overridable (bytes) via env
        // so the merge threshold can be tuned without rebuilding the native library.
        let coalesce_gap = std::env::var("COMET_HDFS_COALESCE_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(OBJECT_STORE_COALESCE_DEFAULT);
        Self {
            inner,
            coalesce_gap,
        }
    }
}

impl Debug for CoalescingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoalescingObjectStore")
            .field("coalesce_gap", &self.coalesce_gap)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Display for CoalescingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CoalescingObjectStore(gap={}, {})",
            self.coalesce_gap, self.inner
        )
    }
}

#[async_trait]
impl ObjectStore for CoalescingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// Merge adjacent ranges into one fetch, then slice back to the requested ranges. This
    /// restores `object_store`'s default coalescing (which `OpendalStore` overrides away).
    /// Comet/arrow-rs issue one `get_ranges` per row group, so the merged span is bounded
    /// to a single row group's contiguous column chunks — one large sequential read.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        coalesce_ranges(
            ranges,
            |range| self.inner.get_range(location, range),
            self.coalesce_gap,
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
