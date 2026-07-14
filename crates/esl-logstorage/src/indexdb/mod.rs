//! Port of EsLogs `lib/logstorage/indexdb.go`.
//!
//! The per-partition stream index: maps stream tags to internal stream ids and
//! answers `_stream:{...}` filters. The rows are stored in the [`mergeset`]
//! module, a faithful port of upstream `lib/mergeset` whose on-disk format is
//! byte-compatible with upstream indexdb directories.
//!
//! PORT NOTE: `allow(dead_code)` because part of the public `indexdb` surface
//! (`search_stream_ids`, `search_tenants`, `append_stream_string`) is consumed
//! only by the storage-search query path (storage_search.go, Layer 4), which is
//! ported in parallel and does not wire it up yet. The stream-registration half
//! (`must_open_indexdb`, `must_register_stream`, `has_stream_id`, `debug_flush`,
//! `update_stats`, `must_create_snapshot_at`) is now consumed by partition.rs.
//! Remove this attribute once the query path lands.
#![allow(dead_code)]

mod mergeset;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use esl_common::bytesutil::ByteBufferPool;
use esl_common::encoding;
use esl_common::regexutil::PromRegex;

use crate::cache::CacheValue;
use crate::storage::Storage;
use crate::stream_filter::{AndStreamFilter, StreamFilter, StreamTagFilter};
use crate::stream_id::StreamID;
use crate::stream_tags::{self, get_stream_tags, must_unmarshal_stream_tags_inplace};
use crate::tenant_id::TenantID;
use crate::u128::U128;

use mergeset::{SearchError, Table, TableMetrics, TableSearch, must_open_table};

// (tenantID:streamID) entries have this prefix.
//
// These entries are used for detecting whether the given stream is already
// registered.
const NS_PREFIX_STREAM_ID: u8 = 0;

// (tenantID:streamID -> streamTagsCanonical) entries have this prefix.
const NS_PREFIX_STREAM_ID_TO_STREAM_TAGS: u8 = 1;

// (tenantID:name:value => streamIDs) entries have this prefix.
const NS_PREFIX_TAG_TO_STREAM_IDS: u8 = 2;

// PORT NOTE: `tagSeparatorChar` is a private const in stream_tags.rs; the same
// value is redeclared here (it is part of the on-disk tag encoding).
const TAG_SEPARATOR_CHAR: u8 = 1;

// PORT NOTE: `bbPool` in Go is `encoding.go`'s package-level
// `bytesutil.ByteBufferPool`; the port keeps a private pool with identical
// buffer-reuse behavior.
static BB_POOL: ByteBufferPool = ByteBufferPool::new();

/// IndexdbStats contains indexdb stats.
#[derive(Debug, Default, Clone)]
pub struct IndexdbStats {
    /// StreamsCreatedTotal is the number of log streams created since the
    /// indexdb initialization.
    pub streams_created_total: u64,
    pub indexdb_size_bytes: u64,
    pub indexdb_items_count: u64,
    pub indexdb_blocks_count: u64,
    pub indexdb_parts_count: u64,
    pub indexdb_pending_items: u64,
    pub indexdb_active_file_merges: u64,
    pub indexdb_active_inmemory_merges: u64,
    pub indexdb_file_merges_count: u64,
    pub indexdb_inmemory_merges_count: u64,
    pub indexdb_file_items_merged: u64,
    pub indexdb_inmemory_items_merged: u64,
}

/// indexdb is the per-partition stream index.
pub(crate) struct Indexdb {
    /// streams_created_total is the number of log streams created since the
    /// indexdb initialization.
    streams_created_total: AtomicU64,

    /// The generation of the filterStreamCache. It is updated (via the flush
    /// callback) each time a new item is added to `tb`.
    filter_stream_cache_generation: Arc<AtomicU32>,

    /// path is the path to indexdb.
    path: String,

    /// partition_name is the name of the partition for the indexdb.
    partition_name: String,

    /// tb is the storage for indexdb.
    tb: Arc<Table>,

    /// index_search_pool is a pool of indexSearch structs for the given indexdb.
    index_search_pool: Mutex<Vec<IndexSearch>>,

    /// s is the storage where indexdb belongs to.
    ///
    /// PORT NOTE: Go stores `s *Storage`. The port stores a `Weak<Storage>`
    /// (mirroring the datadb→partition back-reference) to break the
    /// Storage → partition → indexdb → Storage strong cycle. Storage always
    /// outlives its indexdbs (`must_close` closes partitions, hence indexdbs,
    /// before the caches/Storage are torn down), so `upgrade()` always succeeds
    /// while the indexdb is in use.
    s: Weak<Storage>,
}

/// Creates an indexdb at the given path (port of `mustCreateIndexdb`).
pub(crate) fn must_create_indexdb(path: &str) {
    esl_common::fs::must_mkdir_fail_if_exist(path);
    esl_common::fs::must_sync_path_and_parent_dir(path);
}

/// Opens the indexdb at the given path (port of `mustOpenIndexdb`).
pub(crate) fn must_open_indexdb(
    path: &str,
    partition_name: &str,
    s: &Arc<Storage>,
) -> Arc<Indexdb> {
    let cache_gen = Arc::new(AtomicU32::new(0));
    let gen_for_cb = Arc::clone(&cache_gen);
    // invalidateStreamFilterCache: bump the generation on every flush.
    let flush_cb: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        gen_for_cb.fetch_add(1, Ordering::SeqCst);
    });
    // Go: mergeset.MustOpenTable(path, s.flushInterval,
    // idb.invalidateStreamFilterCache, time.Second, mergeTagToStreamIDsRows,
    // &isReadOnly); the read-only flag is not ported (see mergeset/table.rs).
    let tb = must_open_table(
        path,
        s.flush_interval,
        Some(flush_cb),
        std::time::Duration::from_secs(1),
        Some(merge_tag_to_stream_ids_rows),
    );
    Arc::new(Indexdb {
        streams_created_total: AtomicU64::new(0),
        filter_stream_cache_generation: cache_gen,
        path: path.to_string(),
        partition_name: partition_name.to_string(),
        tb,
        index_search_pool: Mutex::new(Vec::new()),
        s: Arc::downgrade(s),
    })
}

/// Closes the indexdb (port of `mustCloseIndexdb`).
pub(crate) fn must_close_indexdb(idb: &Indexdb) {
    idb.tb.must_close();
}

impl Indexdb {
    pub(crate) fn debug_flush(&self) {
        self.tb.debug_flush();
    }

    pub(crate) fn must_create_snapshot_at(&self, dst_dir: &str) {
        self.tb.must_create_snapshot_at(dst_dir);
    }

    pub(crate) fn update_stats(&self, d: &mut IndexdbStats) {
        d.streams_created_total += self.streams_created_total.load(Ordering::SeqCst);

        let mut tm = TableMetrics::default();
        self.tb.update_metrics(&mut tm);

        d.indexdb_size_bytes += tm.inmemory_size_bytes + tm.file_size_bytes;
        d.indexdb_items_count += tm.inmemory_items_count + tm.file_items_count;
        d.indexdb_pending_items += tm.pending_items;
        d.indexdb_parts_count += tm.inmemory_parts_count + tm.file_parts_count;
        d.indexdb_blocks_count += tm.inmemory_blocks_count + tm.file_blocks_count;
        d.indexdb_active_file_merges = tm.active_file_merges;
        d.indexdb_active_inmemory_merges = tm.active_inmemory_merges;
        d.indexdb_file_merges_count += tm.file_merges_count;
        d.indexdb_inmemory_merges_count += tm.inmemory_merges_count;
        d.indexdb_file_items_merged += tm.file_items_merged;
        d.indexdb_inmemory_items_merged += tm.inmemory_items_merged;
    }

    /// Appends the human-readable stream string for sid to dst
    /// (port of `appendStreamString`).
    pub(crate) fn append_stream_string(&self, dst: &mut Vec<u8>, sid: &StreamID) {
        let mut bb = BB_POOL.get();
        bb.b.clear();
        self.append_stream_tags_by_stream_id(&mut bb.b, sid);
        if bb.b.is_empty() {
            // Couldn't find stream tags by sid. This may be the case when the
            // corresponding log stream was recently registered and its tags
            // aren't visible to search yet. The stream tags must become visible
            // in a few seconds.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6042
            BB_POOL.put(bb);
            return;
        }

        let mut st = get_stream_tags();
        must_unmarshal_stream_tags_inplace(&mut st, &bb.b);
        st.marshal_string(dst);
        crate::stream_tags::put_stream_tags(st);

        BB_POOL.put(bb);
    }

    fn append_stream_tags_by_stream_id(&self, dst: &mut Vec<u8>, sid: &StreamID) {
        let mut is = self.get_index_search();

        is.kb.clear();
        marshal_common_prefix(
            &mut is.kb,
            NS_PREFIX_STREAM_ID_TO_STREAM_TAGS,
            sid.tenant_id,
        );
        sid.id.marshal(&mut is.kb);

        match is.ts.first_item_with_prefix(&is.kb) {
            Ok(()) => {
                let data = &is.ts.item()[is.kb.len()..];
                dst.extend_from_slice(data);
            }
            Err(SearchError::Eof) => {}
            Err(SearchError::Other(err)) => {
                esl_common::panicf!(
                    "FATAL: unexpected error when searching for StreamTags by streamID={sid} in indexdb: {err}"
                );
            }
        }

        self.put_index_search(is);
    }

    /// Returns true if streamID exists in idb (port of `hasStreamID`).
    pub(crate) fn has_stream_id(&self, sid: &StreamID) -> bool {
        let mut is = self.get_index_search();

        is.kb.clear();
        marshal_common_prefix(&mut is.kb, NS_PREFIX_STREAM_ID, sid.tenant_id);
        sid.id.marshal(&mut is.kb);

        let result = match is.ts.first_item_with_prefix(&is.kb) {
            Ok(()) => is.kb.len() == is.ts.item().len(),
            Err(SearchError::Eof) => false,
            Err(SearchError::Other(err)) => {
                esl_common::panicf!(
                    "FATAL: unexpected error when searching for streamID={sid} in indexdb: {err}"
                );
                false
            }
        };

        self.put_index_search(is);
        result
    }

    /// Returns streamIDs for the given tenantIDs and stream filters
    /// (port of `searchStreamIDs`).
    pub(crate) fn search_stream_ids(
        &self,
        tenant_ids: &[TenantID],
        sf: &StreamFilter,
    ) -> Vec<StreamID> {
        // Try obtaining streamIDs from cache.
        if let Some(stream_ids) = self.load_stream_ids_from_cache(tenant_ids, sf) {
            // Fast path - streamIDs found in the cache.
            return stream_ids;
        }

        // Slow path - collect streamIDs from indexdb.
        let mut is = self.get_index_search();
        let mut m: HashSet<StreamID> = HashSet::new();
        for &tenant_id in tenant_ids {
            for asf in &sf.or_filters {
                is.update_stream_ids(&mut m, tenant_id, asf);
            }
        }
        self.put_index_search(is);

        // Convert the collected streamIDs from m to a sorted slice.
        let mut stream_ids: Vec<StreamID> = m.into_iter().collect();
        sort_stream_ids(&mut stream_ids);

        // Store the collected streamIDs to cache.
        self.store_stream_ids_to_cache(tenant_ids, sf, &stream_ids);

        stream_ids
    }

    /// Returns the tenantIDs registered in idb (port of `searchTenants`).
    pub(crate) fn search_tenants(&self) -> Vec<TenantID> {
        let mut is = self.get_index_search();
        let result = is.get_tenant_ids();
        self.put_index_search(is);
        result
    }

    /// Registers a stream (port of `mustRegisterStream`).
    ///
    /// PORT NOTE: Go iterates `StreamTags.tags` to build the tag entries;
    /// `StreamTags.tags` is private in the Rust module, so the tag entries are
    /// built by parsing `stream_tags_canonical` directly (its layout is
    /// `varuint(count){bytes(name) bytes(value)}`). The produced item bytes are
    /// identical (name/value are marshaled via `marshal_tag_value`, exactly as
    /// `Field::indexdb_marshal` does).
    pub(crate) fn must_register_stream(&self, stream_id: &StreamID, stream_tags_canonical: &[u8]) {
        let tenant_id = stream_id.tenant_id;
        let mut items: Vec<Vec<u8>> = Vec::new();

        // Register tenantID:streamID entry.
        let mut buf = Vec::new();
        marshal_common_prefix(&mut buf, NS_PREFIX_STREAM_ID, tenant_id);
        stream_id.id.marshal(&mut buf);
        items.push(buf);

        // Register tenantID:streamID -> streamTagsCanonical entry.
        let mut buf = Vec::new();
        marshal_common_prefix(&mut buf, NS_PREFIX_STREAM_ID_TO_STREAM_TAGS, tenant_id);
        stream_id.id.marshal(&mut buf);
        buf.extend_from_slice(stream_tags_canonical);
        items.push(buf);

        // Register tenantID:name:value -> streamIDs entries.
        let (n, n_size) = encoding::unmarshal_var_uint64(stream_tags_canonical);
        if n_size <= 0 {
            esl_common::panicf!("FATAL: cannot unmarshal StreamTags count from canonical form");
        }
        let mut src = &stream_tags_canonical[n_size as usize..];
        for _ in 0..n {
            let (name, name_len) = encoding::unmarshal_bytes(src);
            let name = name.expect("FATAL: cannot unmarshal tag name from StreamTags canonical");
            src = &src[name_len as usize..];
            let (value, value_len) = encoding::unmarshal_bytes(src);
            let value = value.expect("FATAL: cannot unmarshal tag value from StreamTags canonical");
            src = &src[value_len as usize..];

            let mut buf = Vec::new();
            marshal_common_prefix(&mut buf, NS_PREFIX_TAG_TO_STREAM_IDS, tenant_id);
            stream_tags::marshal_tag_value(&mut buf, name);
            stream_tags::marshal_tag_value(&mut buf, value);
            stream_id.id.marshal(&mut buf);
            items.push(buf);
        }

        // Add items to the storage.
        self.tb.add_items(&items);

        self.streams_created_total.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns the owning Storage (Go: `idb.s`). Panics if Storage was dropped,
    /// which cannot happen while the indexdb is alive (see the `s` field note).
    fn storage(&self) -> Arc<Storage> {
        self.s
            .upgrade()
            .expect("BUG: Storage dropped while indexdb is still alive")
    }

    fn marshal_stream_filter_cache_key(
        &self,
        s: &Storage,
        dst: &mut Vec<u8>,
        tenant_ids: &[TenantID],
        sf: &StreamFilter,
    ) {
        encoding::marshal_uint64(dst, s.partition_cache_generation.load(Ordering::SeqCst));
        encoding::marshal_uint32(
            dst,
            self.filter_stream_cache_generation.load(Ordering::SeqCst),
        );
        encoding::marshal_bytes(dst, self.partition_name.as_bytes());
        encoding::marshal_var_uint64(dst, tenant_ids.len() as u64);
        for tid in tenant_ids {
            tid.marshal(dst);
        }
        sf.marshal_for_cache_key(dst);
    }

    fn load_stream_ids_from_cache(
        &self,
        tenant_ids: &[TenantID],
        sf: &StreamFilter,
    ) -> Option<Vec<StreamID>> {
        let s = self.storage();
        let mut bb = BB_POOL.get();
        bb.b.clear();
        self.marshal_stream_filter_cache_key(&s, &mut bb.b, tenant_ids, sf);
        // PORT NOTE: Go's `s.filterStreamCache` is a plain *Cache; the port's
        // Storage keeps it in a `Mutex<Option<Cache>>` (so `must_close` can stop
        // the cleaner). The lock is held only for the O(1) lookup. The Cache is
        // always present while the indexdb is alive (partitions close first).
        let v = {
            let guard = s.filter_stream_cache.lock().unwrap();
            let cache = guard
                .as_ref()
                .expect("BUG: filterStreamCache stopped while indexdb is alive");
            cache.get(&bb.b)
        };
        BB_POOL.put(bb);

        let v = v?;
        // Cache hit - unpack streamIDs from data.
        let data: &Vec<u8> = v
            .downcast_ref::<Vec<u8>>()
            .expect("BUG: unexpected cache value type for filterStreamCache");
        let (n, n_size) = encoding::unmarshal_var_uint64(data);
        if n_size <= 0 {
            esl_common::panicf!(
                "BUG: unexpected error when unmarshaling the number of streamIDs from cache"
            );
        }
        let mut src = &data[n_size as usize..];
        let mut stream_ids: Vec<StreamID> = Vec::with_capacity(n as usize);
        for i in 0..n {
            let mut sid = StreamID::default();
            match sid.unmarshal(src) {
                Ok(tail) => src = tail,
                Err(err) => {
                    esl_common::panicf!(
                        "BUG: unexpected error when unmarshaling streamID #{i}: {err}"
                    );
                }
            }
            stream_ids.push(sid);
        }
        if !src.is_empty() {
            esl_common::panicf!("BUG: unexpected non-empty tail left with len={}", src.len());
        }
        Some(stream_ids)
    }

    fn store_stream_ids_to_cache(
        &self,
        tenant_ids: &[TenantID],
        sf: &StreamFilter,
        stream_ids: &[StreamID],
    ) {
        let s = self.storage();

        // marshal streamIDs
        let mut b: Vec<u8> = Vec::new();
        encoding::marshal_var_uint64(&mut b, stream_ids.len() as u64);
        for sid in stream_ids {
            sid.marshal(&mut b);
        }

        // Store marshaled streamIDs to cache.
        let mut bb = BB_POOL.get();
        bb.b.clear();
        self.marshal_stream_filter_cache_key(&s, &mut bb.b, tenant_ids, sf);
        let value: CacheValue = Arc::new(b);
        {
            let guard = s.filter_stream_cache.lock().unwrap();
            let cache = guard
                .as_ref()
                .expect("BUG: filterStreamCache stopped while indexdb is alive");
            cache.set(&bb.b, value);
        }
        BB_POOL.put(bb);
    }

    fn get_index_search(&self) -> IndexSearch {
        let mut is = self
            .index_search_pool
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_default();
        is.ts.init(&self.tb, false);
        is
    }

    fn put_index_search(&self, mut is: IndexSearch) {
        is.ts.must_close();
        is.kb.clear();
        self.index_search_pool.lock().unwrap().push(is);
    }
}

fn sort_stream_ids(stream_ids: &mut [StreamID]) {
    stream_ids.sort_by(|a, b| {
        if a.less(b) {
            std::cmp::Ordering::Less
        } else if b.less(a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

#[derive(Default)]
struct IndexSearch {
    ts: TableSearch,
    kb: Vec<u8>,
}

impl IndexSearch {
    fn update_stream_ids(
        &mut self,
        dst: &mut HashSet<StreamID>,
        tenant_id: TenantID,
        asf: &AndStreamFilter,
    ) {
        let mut m: Option<HashSet<U128>> = None;
        for tf in &asf.tag_filters {
            let ids = self.get_stream_ids_for_tag_filter(tenant_id, tf);
            if ids.is_empty() {
                // There is no need in checking the remaining filters, since the
                // result will be empty in any case.
                return;
            }
            match m {
                None => m = Some(ids),
                Some(ref mut cur) => cur.retain(|id| ids.contains(id)),
            }
        }

        if let Some(m) = m {
            for id in m {
                dst.insert(StreamID { tenant_id, id });
            }
        }
    }

    fn get_stream_ids_for_tag_filter(
        &mut self,
        tenant_id: TenantID,
        tf: &StreamTagFilter,
    ) -> HashSet<U128> {
        match tf.op.as_str() {
            "=" => {
                if tf.value.is_empty() {
                    // (field="")
                    self.get_stream_ids_for_empty_tag_value(tenant_id, &tf.tag_name)
                } else {
                    // (field="value")
                    self.get_stream_ids_for_non_empty_tag_value(tenant_id, &tf.tag_name, &tf.value)
                }
            }
            "!=" => {
                if tf.value.is_empty() {
                    // (field!="")
                    self.get_stream_ids_for_tag_name(tenant_id, &tf.tag_name)
                } else {
                    // (field!="value") => (all and not field="value")
                    let mut ids = self.get_stream_ids_for_tenant(tenant_id);
                    let ids_for_tag = self.get_stream_ids_for_non_empty_tag_value(
                        tenant_id,
                        &tf.tag_name,
                        &tf.value,
                    );
                    for id in &ids_for_tag {
                        ids.remove(id);
                    }
                    ids
                }
            }
            "=~" => {
                let re = tf.regexp.as_ref().unwrap();
                if re.match_string("") {
                    // (field=~"|re") => (field="" or field=~"re")
                    let mut ids = self.get_stream_ids_for_empty_tag_value(tenant_id, &tf.tag_name);
                    let ids_for_re =
                        self.get_stream_ids_for_tag_regexp(tenant_id, &tf.tag_name, re);
                    for id in ids_for_re {
                        ids.insert(id);
                    }
                    ids
                } else {
                    self.get_stream_ids_for_tag_regexp(tenant_id, &tf.tag_name, re)
                }
            }
            "!~" => {
                let re = tf.regexp.as_ref().unwrap();
                if re.match_string("") {
                    // (field!~"|re") => (field!="" and not field=~"re")
                    let mut ids = self.get_stream_ids_for_tag_name(tenant_id, &tf.tag_name);
                    if ids.is_empty() {
                        return ids;
                    }
                    let ids_for_re =
                        self.get_stream_ids_for_tag_regexp(tenant_id, &tf.tag_name, re);
                    for id in &ids_for_re {
                        ids.remove(id);
                    }
                    ids
                } else {
                    // (field!~"re") => (all and not field=~"re")
                    let mut ids = self.get_stream_ids_for_tenant(tenant_id);
                    let ids_for_re =
                        self.get_stream_ids_for_tag_regexp(tenant_id, &tf.tag_name, re);
                    for id in &ids_for_re {
                        ids.remove(id);
                    }
                    ids
                }
            }
            other => {
                esl_common::panicf!("BUG: unexpected operation in stream tag filter: {other:?}");
                HashSet::new()
            }
        }
    }

    fn get_stream_ids_for_non_empty_tag_value(
        &mut self,
        tenant_id: TenantID,
        tag_name: &str,
        tag_value: &[u8],
    ) -> HashSet<U128> {
        let mut ids = HashSet::new();
        let mut sp = TagToStreamIDsRowParser::default();

        self.kb.clear();
        marshal_common_prefix(&mut self.kb, NS_PREFIX_TAG_TO_STREAM_IDS, tenant_id);
        stream_tags::marshal_tag_value(&mut self.kb, tag_name.as_bytes());
        stream_tags::marshal_tag_value(&mut self.kb, tag_value);
        let prefix_len = self.kb.len();
        self.ts.seek(&self.kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(&self.kb[..prefix_len]) {
                break;
            }
            let tail = &item[prefix_len..];
            sp.update_stream_ids(&mut ids, tail);
        }
        if let Some(err) = self.ts.error() {
            esl_common::panicf!("FATAL: unexpected error: {err}");
        }

        ids
    }

    fn get_stream_ids_for_empty_tag_value(
        &mut self,
        tenant_id: TenantID,
        tag_name: &str,
    ) -> HashSet<U128> {
        let mut ids = self.get_stream_ids_for_tenant(tenant_id);
        let ids_for_tag = self.get_stream_ids_for_tag_name(tenant_id, tag_name);
        for id in &ids_for_tag {
            ids.remove(id);
        }
        ids
    }

    fn get_stream_ids_for_tenant(&mut self, tenant_id: TenantID) -> HashSet<U128> {
        let mut ids = HashSet::new();
        self.kb.clear();
        marshal_common_prefix(&mut self.kb, NS_PREFIX_STREAM_ID, tenant_id);
        let prefix_len = self.kb.len();
        self.ts.seek(&self.kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(&self.kb[..prefix_len]) {
                break;
            }
            let mut id = U128::default();
            match id.unmarshal(&item[prefix_len..]) {
                Ok(tail) => {
                    if !tail.is_empty() {
                        esl_common::panicf!(
                            "FATAL: unexpected non-empty tail left after unmarshaling streamID from (tenantID:streamID); tail len={}",
                            tail.len()
                        );
                    }
                }
                Err(err) => {
                    esl_common::panicf!(
                        "FATAL: cannot unmarshal streamID from (tenantID:streamID) entry: {err}"
                    );
                }
            }
            ids.insert(id);
        }
        if let Some(err) = self.ts.error() {
            esl_common::panicf!("FATAL: unexpected error: {err}");
        }

        ids
    }

    fn get_stream_ids_for_tag_name(
        &mut self,
        tenant_id: TenantID,
        tag_name: &str,
    ) -> HashSet<U128> {
        let mut ids = HashSet::new();
        let mut sp = TagToStreamIDsRowParser::default();

        self.kb.clear();
        marshal_common_prefix(&mut self.kb, NS_PREFIX_TAG_TO_STREAM_IDS, tenant_id);
        stream_tags::marshal_tag_value(&mut self.kb, tag_name.as_bytes());
        let prefix_len = self.kb.len();
        self.ts.seek(&self.kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(&self.kb[..prefix_len]) {
                break;
            }
            let tail = &item[prefix_len..];
            let Some(n) = tail.iter().position(|&c| c == TAG_SEPARATOR_CHAR) else {
                esl_common::panicf!("FATAL: cannot find the end of tag value");
                continue;
            };
            let tail = &tail[n + 1..];
            sp.update_stream_ids(&mut ids, tail);
        }
        if let Some(err) = self.ts.error() {
            esl_common::panicf!("FATAL: unexpected error: {err}");
        }

        ids
    }

    fn get_stream_ids_for_tag_regexp(
        &mut self,
        tenant_id: TenantID,
        tag_name: &str,
        re: &PromRegex,
    ) -> HashSet<U128> {
        let mut ids = HashSet::new();
        let mut sp = TagToStreamIDsRowParser::default();
        let mut tag_value: Vec<u8> = Vec::new();
        let mut prev_matching_tag_value: Vec<u8> = Vec::new();
        let mut has_prev_match = false;

        self.kb.clear();
        marshal_common_prefix(&mut self.kb, NS_PREFIX_TAG_TO_STREAM_IDS, tenant_id);
        stream_tags::marshal_tag_value(&mut self.kb, tag_name.as_bytes());
        let prefix_len = self.kb.len();
        self.ts.seek(&self.kb);
        while self.ts.next_item() {
            let item = self.ts.item();
            if !item.starts_with(&self.kb[..prefix_len]) {
                break;
            }
            let tail = &item[prefix_len..];
            tag_value.clear();
            let tail = match stream_tags::unmarshal_tag_value(&mut tag_value, tail) {
                Ok(tail) => tail,
                Err(err) => {
                    esl_common::panicf!("FATAL: cannot unmarshal tag value: {err}");
                    continue;
                }
            };
            if !has_prev_match || tag_value != prev_matching_tag_value {
                if !re.match_bytes(&tag_value) {
                    continue;
                }
                prev_matching_tag_value.clear();
                prev_matching_tag_value.extend_from_slice(&tag_value);
                has_prev_match = true;
            }
            sp.update_stream_ids(&mut ids, tail);
        }
        if let Some(err) = self.ts.error() {
            esl_common::panicf!("FATAL: unexpected error: {err}");
        }

        ids
    }

    fn get_tenant_ids(&mut self) -> Vec<TenantID> {
        let mut tenant_ids: Vec<TenantID> = Vec::new();
        let mut tenant_id = TenantID::default();

        self.kb.clear();
        marshal_common_prefix(&mut self.kb, NS_PREFIX_STREAM_ID, tenant_id);
        self.ts.seek(&self.kb);

        while self.ts.next_item() {
            let item = self.ts.item().to_vec();
            let (_, prefix) = match unmarshal_common_prefix(&mut tenant_id, &item) {
                Ok(v) => v,
                Err(err) => {
                    esl_common::panicf!("FATAL: cannot unmarshal tenantID: {err}");
                    return tenant_ids;
                }
            };
            if prefix != NS_PREFIX_STREAM_ID {
                // Reached the end of entries with the needed prefix.
                break;
            }
            tenant_ids.push(tenant_id);
            // Seek for the next (accountID, projectID).
            tenant_id.project_id = tenant_id.project_id.wrapping_add(1);
            if tenant_id.project_id == 0 {
                tenant_id.account_id = tenant_id.account_id.wrapping_add(1);
                if tenant_id.account_id == 0 {
                    // Reached the end (accountID, projectID) space.
                    break;
                }
            }

            self.kb.clear();
            marshal_common_prefix(&mut self.kb, NS_PREFIX_STREAM_ID, tenant_id);
            self.ts.seek(&self.kb);
        }

        if let Some(err) = self.ts.error() {
            esl_common::panicf!("FATAL: error when searching for tenant ids: {err}");
        }

        tenant_ids
    }
}

/// maxStreamIDsPerRow limits the number of streamIDs in a
/// tenantID:name:value -> streamIDs row.
const MAX_STREAM_IDS_PER_ROW: usize = 32;

/// PrepareBlockCallback merging adjacent tenantID:name:value -> streamIDs rows
/// (port of `mergeTagToStreamIDsRows`).
fn merge_tag_to_stream_ids_rows(
    data: Vec<u8>,
    items: Vec<mergeset::Item>,
) -> (Vec<u8>, Vec<mergeset::Item>) {
    // Perform quick checks whether items contain rows starting from
    // nsPrefixTagToStreamIDs based on the fact that items are sorted.
    if items.len() <= 2 {
        // The first and the last row must remain unchanged.
        return (data, items);
    }
    let first_item = items[0].bytes(&data);
    if !first_item.is_empty() && first_item[0] > NS_PREFIX_TAG_TO_STREAM_IDS {
        return (data, items);
    }
    let last_item = items[items.len() - 1].bytes(&data);
    if !last_item.is_empty() && last_item[0] < NS_PREFIX_TAG_TO_STREAM_IDS {
        return (data, items);
    }

    // items contain at least one row starting from nsPrefixTagToStreamIDs.
    // Merge rows with common tag.
    let mut tsm = TagToStreamIDsRowsMerger::default();
    let mut dst_data: Vec<u8> = Vec::new();
    let mut dst_items: Vec<mergeset::Item> = Vec::new();
    let items_len = items.len();
    for (i, it) in items.iter().enumerate() {
        let item = it.bytes(&data);
        if item.is_empty() || item[0] != NS_PREFIX_TAG_TO_STREAM_IDS || i == 0 || i == items_len - 1
        {
            // Write rows not starting with nsPrefixTagToStreamIDs as-is.
            // Additionally write the first and the last row as-is in order to
            // preserve sort order for adjacent blocks.
            tsm.flush_pending_stream_ids(&mut dst_data, &mut dst_items);
            let start = dst_data.len() as u32;
            dst_data.extend_from_slice(item);
            dst_items.push(mergeset::Item {
                start,
                end: dst_data.len() as u32,
            });
            continue;
        }
        if let Err(err) = tsm.sp.init(item) {
            esl_common::panicf!("FATAL: cannot parse row during merge: {err}");
        }
        if tsm.sp.stream_ids_len() >= MAX_STREAM_IDS_PER_ROW {
            tsm.flush_pending_stream_ids(&mut dst_data, &mut dst_items);
            let start = dst_data.len() as u32;
            dst_data.extend_from_slice(item);
            dst_items.push(mergeset::Item {
                start,
                end: dst_data.len() as u32,
            });
            continue;
        }
        if !tsm.sp.equal_prefix(&tsm.sp_prev) {
            tsm.flush_pending_stream_ids(&mut dst_data, &mut dst_items);
        }
        tsm.sp.parse_stream_ids();
        tsm.pending_stream_ids.extend_from_slice(&tsm.sp.stream_ids);
        std::mem::swap(&mut tsm.sp, &mut tsm.sp_prev);
        if tsm.pending_stream_ids.len() >= MAX_STREAM_IDS_PER_ROW {
            tsm.flush_pending_stream_ids(&mut dst_data, &mut dst_items);
        }
    }
    if !tsm.pending_stream_ids.is_empty() {
        esl_common::panicf!(
            "BUG: tsm.pending_stream_ids must be empty at this point; got {} items",
            tsm.pending_stream_ids.len()
        );
    }
    if !check_items_sorted(&dst_data, &dst_items) {
        // Items could become unsorted if initial items contain duplicate
        // streamIDs. Leave the original items unmerged, so they can be merged
        // next time.
        //
        // PORT NOTE: Go reverts to internal dataCopy/itemsCopy; the port keeps
        // the untouched `data`/`items` originals for the same effect.
        if !check_items_sorted(&data, &items) {
            esl_common::panicf!("BUG: the original items weren't sorted");
        }
        return (data, items);
    }
    (dst_data, dst_items)
}

#[derive(Default)]
struct TagToStreamIDsRowsMerger {
    pending_stream_ids: Vec<U128>,
    sp: TagToStreamIDsRowParser,
    sp_prev: TagToStreamIDsRowParser,
}

impl TagToStreamIDsRowsMerger {
    fn flush_pending_stream_ids(
        &mut self,
        dst_data: &mut Vec<u8>,
        dst_items: &mut Vec<mergeset::Item>,
    ) {
        if self.pending_stream_ids.is_empty() {
            // Nothing to flush.
            return;
        }
        // Sort and dedup (port of sort.Sort + removeDuplicateStreamIDs).
        // U128 derives Ord as (hi, lo), matching U128::less.
        self.pending_stream_ids.sort();
        self.pending_stream_ids.dedup();

        // Marshal pendingStreamIDs.
        let start = dst_data.len() as u32;
        self.sp_prev.marshal_prefix(dst_data);
        for id in &self.pending_stream_ids {
            id.marshal(dst_data);
        }
        dst_items.push(mergeset::Item {
            start,
            end: dst_data.len() as u32,
        });
        self.pending_stream_ids.clear();
    }
}

/// tagToStreamIDsRowParser parses tenantID:name:value -> streamIDs rows.
#[derive(Default)]
struct TagToStreamIDsRowParser {
    /// TenantID contains TenantID of the parsed row.
    tenant_id: TenantID,

    /// StreamIDs contains parsed StreamIDs after parse_stream_ids call.
    stream_ids: Vec<U128>,

    /// stream_ids_parsed is set to true after parse_stream_ids call.
    stream_ids_parsed: bool,

    /// Tag contains parsed tag after init call.
    tag: crate::rows::Field,

    /// tail contains the remaining unparsed streamIDs.
    tail: Vec<u8>,
}

impl TagToStreamIDsRowParser {
    fn reset(&mut self) {
        self.tenant_id.reset();
        self.stream_ids.clear();
        self.stream_ids_parsed = false;
        self.tag.reset();
        self.tail.clear();
    }

    /// Initializes sp from b (port of `Init`).
    fn init(&mut self, b: &[u8]) -> Result<(), String> {
        let (tail, ns_prefix) = unmarshal_common_prefix(&mut self.tenant_id, b)
            .map_err(|err| format!("invalid tenantID:name:value -> streamIDs row {b:X?}: {err}"))?;
        if ns_prefix != NS_PREFIX_TAG_TO_STREAM_IDS {
            return Err(format!(
                "invalid prefix for tenantID:name:value -> streamIDs row {b:X?}; got {ns_prefix}; want {NS_PREFIX_TAG_TO_STREAM_IDS}"
            ));
        }
        let tail = self.tag.indexdb_unmarshal(tail).map_err(|err| {
            format!("cannot unmarshal tag from tenantID:name:value -> streamIDs row {b:X?}: {err}")
        })?;
        // Copy the tail (Go points into b; the port owns it).
        let tail = tail.to_vec();
        self.init_only_tail(&tail).map_err(|err| {
            format!(
                "cannot initialize tail from tenantID:name:value -> streamIDs row {b:X?}: {err}"
            )
        })?;
        Ok(())
    }

    /// Marshals the row prefix without tail to dst (port of `MarshalPrefix`).
    fn marshal_prefix(&self, dst: &mut Vec<u8>) {
        marshal_common_prefix(dst, NS_PREFIX_TAG_TO_STREAM_IDS, self.tenant_id);
        self.tag.indexdb_marshal(dst);
    }

    /// Initializes sp.tail from tail (port of `InitOnlyTail`).
    fn init_only_tail(&mut self, tail: &[u8]) -> Result<(), String> {
        if tail.is_empty() {
            return Err("missing streamID in the tenantID:name:value -> streamIDs row".to_string());
        }
        if !tail.len().is_multiple_of(16) {
            return Err(format!(
                "invalid tail length in the tenantID:name:value -> streamIDs row; got {} bytes; must be multiple of 16 bytes",
                tail.len()
            ));
        }
        self.tail.clear();
        self.tail.extend_from_slice(tail);
        self.stream_ids_parsed = false;
        Ok(())
    }

    /// Returns true if prefixes for sp and x are equal (port of `EqualPrefix`).
    fn equal_prefix(&self, x: &TagToStreamIDsRowParser) -> bool {
        if !self.tenant_id.equal(&x.tenant_id) {
            return false;
        }
        self.tag == x.tag
    }

    /// Returns the number of StreamIDs in the tail (port of `StreamIDsLen`).
    fn stream_ids_len(&self) -> usize {
        self.tail.len() / 16
    }

    /// Parses StreamIDs from tail into stream_ids (port of `ParseStreamIDs`).
    fn parse_stream_ids(&mut self) {
        if self.stream_ids_parsed {
            return;
        }
        let n = self.tail.len() / 16;
        self.stream_ids.clear();
        self.stream_ids.reserve(n);
        let mut tail: &[u8] = &self.tail;
        for _ in 0..n {
            let mut id = U128::default();
            match id.unmarshal(tail) {
                Ok(t) => tail = t,
                Err(err) => {
                    esl_common::panicf!("FATAL: cannot unmarshal streamID: {err}");
                }
            }
            self.stream_ids.push(id);
        }
        self.stream_ids_parsed = true;
    }

    /// Parses the streamIDs in tail into the ids set (port of `UpdateStreamIDs`).
    fn update_stream_ids(&mut self, ids: &mut HashSet<U128>, tail: &[u8]) {
        self.reset();
        if let Err(err) = self.init_only_tail(tail) {
            esl_common::panicf!("FATAL: cannot parse '(date, tag) -> streamIDs' row: {err}");
        }
        self.parse_stream_ids();
        for id in &self.stream_ids {
            ids.insert(*id);
        }
    }
}

/// commonPrefixLen is the length of the common prefix for indexdb rows:
/// 1 byte for the ns* prefix + 8 bytes for tenantID.
const COMMON_PREFIX_LEN: usize = 1 + 8;

fn marshal_common_prefix(dst: &mut Vec<u8>, ns_prefix: u8, tenant_id: TenantID) {
    dst.push(ns_prefix);
    tenant_id.marshal(dst);
}

fn unmarshal_common_prefix<'a>(
    dst_tenant_id: &mut TenantID,
    src: &'a [u8],
) -> Result<(&'a [u8], u8), String> {
    if src.len() < COMMON_PREFIX_LEN {
        return Err(format!(
            "cannot unmarshal common prefix from {} bytes; need at least {COMMON_PREFIX_LEN} bytes; data={src:X?}",
            src.len()
        ));
    }
    let prefix = src[0];
    let tail = dst_tenant_id
        .unmarshal(&src[1..])
        .map_err(|err| format!("cannot unmarshal tenantID: {err}"))?;
    Ok((tail, prefix))
}

fn check_items_sorted(data: &[u8], items: &[mergeset::Item]) -> bool {
    if items.is_empty() {
        return true;
    }
    let mut prev = items[0].bytes(data);
    for it in &items[1..] {
        let curr = it.bytes(data);
        if prev > curr {
            return false;
        }
        prev = curr;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash128::hash128;
    use crate::storage::StorageConfig;
    use crate::stream_filter::must_new_test_stream_filter;
    use std::collections::BTreeMap;

    // PORT NOTE: Go's indexdb tests fabricate a minimal *Storage literal. The
    // port now uses the real `crate::storage::Storage` (the placeholder was
    // removed when partition↔indexdb↔storage was wired), so the test opens a
    // real Storage in a throwaway temp dir. It supplies exactly the fields
    // indexdb reads (`flush_interval`, `partition_cache_generation`,
    // `filter_stream_cache`). The caller must `must_close()` it when done.
    fn new_test_storage() -> Arc<Storage> {
        let dir = std::env::temp_dir().join(format!(
            "esl-indexdb-test-storage-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        esl_common::fs::must_remove_dir(&dir);
        let cfg = StorageConfig {
            flush_interval: 1_000_000_000, // time.Second
            ..Default::default()
        };
        Storage::must_open_storage(&dir, &cfg)
    }

    fn test_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "esl-indexdb-test-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        esl_common::fs::must_remove_dir(&dir);
        dir.to_str().unwrap().to_string()
    }

    fn stream_id_for_tags(
        tenant_id: TenantID,
        tags: &BTreeMap<&str, String>,
    ) -> (StreamID, Vec<u8>) {
        let mut st = get_stream_tags();
        for (k, v) in tags {
            st.add(k, v);
        }
        let mut canonical = Vec::new();
        st.marshal_canonical(&mut canonical);
        crate::stream_tags::put_stream_tags(st);
        let id = hash128(&canonical);
        let sid = StreamID { tenant_id, id };
        (sid, canonical)
    }

    #[test]
    fn test_storage_search_stream_ids() {
        let path = test_dir("search");
        let partition_name = "foobar";
        let s = new_test_storage();
        must_create_indexdb(&path);
        let idb = must_open_indexdb(&path, partition_name, &s);

        let tenant_id = TenantID {
            account_id: 123,
            project_id: 567,
        };

        let get_sid = |tags: &BTreeMap<&str, String>| stream_id_for_tags(tenant_id, tags);

        // Create indexdb entries.
        const JOBS_COUNT: usize = 7;
        const INSTANCES_COUNT: usize = 5;
        for i in 0..JOBS_COUNT {
            for j in 0..INSTANCES_COUNT {
                let mut tags = BTreeMap::new();
                tags.insert("job", format!("job-{i}"));
                tags.insert("instance", format!("instance-{j}"));
                let (sid, canonical) = get_sid(&tags);
                idb.must_register_stream(&sid, &canonical);
            }
        }
        idb.debug_flush();

        let idb_ref = &idb;
        let f = |filter_stream: &str, mut expected: Vec<StreamID>| {
            let sf = must_new_test_stream_filter(filter_stream);
            sort_stream_ids(&mut expected);
            for i in 0..3 {
                let stream_ids = idb_ref.search_stream_ids(&[tenant_id], &sf);
                assert_eq!(
                    stream_ids, expected,
                    "unexpected streamIDs on iteration {i} for {filter_stream}"
                );
            }
        };

        let sid_for = |job: &str, instance: &str| {
            let mut tags = BTreeMap::new();
            tags.insert("job", job.to_string());
            tags.insert("instance", instance.to_string());
            get_sid(&tags).0
        };

        // missing-tenant-id
        {
            let other_tenant = TenantID {
                account_id: 1,
                project_id: 2,
            };
            let sf = must_new_test_stream_filter(r#"{job="job-0",instance="instance-0"}"#);
            for i in 0..3 {
                let stream_ids = idb.search_stream_ids(&[other_tenant], &sf);
                assert!(
                    stream_ids.is_empty(),
                    "unexpected non-empty streamIDs on iteration {i}: {}",
                    stream_ids.len()
                );
            }
        }

        // missing-job
        f(r#"{job="non-existing-job",instance="instance-0"}"#, vec![]);
        // missing-job-re
        f(
            r#"{job=~"non-existing-job|",instance="instance-0"}"#,
            vec![],
        );
        // missing-job-negative-re
        f(r#"{job!~"job.+",instance="instance-0"}"#, vec![]);
        // empty-job
        f(r#"{job="",instance="instance-0"}"#, vec![]);
        // missing-instance
        f(r#"{job="job-0",instance="non-existing-instance"}"#, vec![]);
        // missing-instance-re
        f(
            r#"{job="job-0",instance=~"non-existing-instance|"}"#,
            vec![],
        );
        // missing-instance-negative-re
        f(r#"{job="job-0",instance!~"instance.+"}"#, vec![]);
        // empty-instance
        f(r#"{job="job-0",instance=""}"#, vec![]);
        // non-existing-tag
        f(
            r#"{job="job-0",instance="instance-0",non_existing_tag="foobar"}"#,
            vec![],
        );
        // non-existing-non-empty-tag
        f(
            r#"{job="job-0",instance="instance-0",non_existing_tag!=""}"#,
            vec![],
        );
        // non-existing-tag-re
        f(
            r#"{job="job-0",instance="instance-0",non_existing_tag=~"foo.+"}"#,
            vec![],
        );
        // non-existing-non-empty-tag-re
        f(
            r#"{job="job-0",instance="instance-0",non_existing_tag!~""}"#,
            vec![],
        );

        // match-job-instance
        f(
            r#"{job="job-0",instance="instance-0"}"#,
            vec![sid_for("job-0", "instance-0")],
        );

        // match-non-existing-tag
        f(
            r#"{job="job-0",instance="instance-0",non_existing_tag=~"foo|"}"#,
            vec![sid_for("job-0", "instance-0")],
        );

        // match-job
        {
            let mut expected = Vec::new();
            for i in 0..INSTANCES_COUNT {
                expected.push(sid_for("job-0", &format!("instance-{i}")));
            }
            f(r#"{job="job-0"}"#, expected);
        }

        // match-instance
        {
            let mut expected = Vec::new();
            for i in 0..JOBS_COUNT {
                expected.push(sid_for(&format!("job-{i}"), "instance-1"));
            }
            f(r#"{instance="instance-1"}"#, expected);
        }

        // match-re
        {
            let mut expected = Vec::new();
            for &instance_id in &[3, 1] {
                for &job_id in &[0, 2] {
                    expected.push(sid_for(
                        &format!("job-{job_id}"),
                        &format!("instance-{instance_id}"),
                    ));
                }
            }
            f(r#"{job=~"job-(0|2)",instance=~"instance-[13]"}"#, expected);
        }

        // match-re-empty-match
        {
            let mut expected = Vec::new();
            for &instance_id in &[3, 1] {
                for &job_id in &[0, 2] {
                    expected.push(sid_for(
                        &format!("job-{job_id}"),
                        &format!("instance-{instance_id}"),
                    ));
                }
            }
            f(r#"{job=~"job-(0|2)|",instance=~"instance-[13]"}"#, expected);
        }

        // match-negative-re
        {
            let instance_ids: Vec<usize> =
                (0..INSTANCES_COUNT).filter(|&i| i != 0 && i != 1).collect();
            let job_ids: Vec<usize> = (0..JOBS_COUNT).filter(|&i| i > 2).collect();
            let mut expected = Vec::new();
            for &instance_id in &instance_ids {
                for &job_id in &job_ids {
                    expected.push(sid_for(
                        &format!("job-{job_id}"),
                        &format!("instance-{instance_id}"),
                    ));
                }
            }
            f(r#"{job!~"job-[0-2]",instance!~"instance-(0|1)"}"#, expected);
        }

        // match-negative-re-empty-match
        {
            let instance_ids: Vec<usize> =
                (0..INSTANCES_COUNT).filter(|&i| i != 0 && i != 1).collect();
            let job_ids: Vec<usize> = (0..JOBS_COUNT).filter(|&i| i > 2).collect();
            let mut expected = Vec::new();
            for &instance_id in &instance_ids {
                for &job_id in &job_ids {
                    expected.push(sid_for(
                        &format!("job-{job_id}"),
                        &format!("instance-{instance_id}"),
                    ));
                }
            }
            f(
                r#"{job!~"job-[0-2]",instance!~"instance-(0|1)|"}"#,
                expected,
            );
        }

        // match-negative-job
        {
            let instance_ids = [2usize];
            let job_ids: Vec<usize> = (0..JOBS_COUNT).filter(|&i| i != 1).collect();
            let mut expected = Vec::new();
            for &instance_id in &instance_ids {
                for &job_id in &job_ids {
                    expected.push(sid_for(
                        &format!("job-{job_id}"),
                        &format!("instance-{instance_id}"),
                    ));
                }
            }
            f(r#"{instance="instance-2",job!="job-1"}"#, expected);
        }

        must_close_indexdb(&idb);
        esl_common::fs::must_remove_dir(&path);
        let storage_path = s.path.clone();
        s.must_close();
        esl_common::fs::must_remove_dir(&storage_path);
    }

    #[test]
    fn test_get_tenants_ids() {
        let path = test_dir("tenants");
        let partition_name = "foobar";
        let s = new_test_storage();
        must_create_indexdb(&path);
        let idb = must_open_indexdb(&path, partition_name, &s);

        let tenant_ids = vec![
            TenantID {
                account_id: 0,
                project_id: 0,
            },
            TenantID {
                account_id: 0,
                project_id: 1,
            },
            TenantID {
                account_id: 1,
                project_id: 0,
            },
            TenantID {
                account_id: 1,
                project_id: 1,
            },
            TenantID {
                account_id: 123,
                project_id: 567,
            },
        ];

        const JOBS_COUNT: usize = 7;
        const INSTANCES_COUNT: usize = 5;
        for i in 0..JOBS_COUNT {
            for j in 0..INSTANCES_COUNT {
                let mut tags = BTreeMap::new();
                tags.insert("job", format!("job-{i}"));
                tags.insert("instance", format!("instance-{j}"));
                // Same canonical/id across tenants; register once per tenant.
                let mut st = get_stream_tags();
                for (k, v) in &tags {
                    st.add(k, v);
                }
                let mut canonical = Vec::new();
                st.marshal_canonical(&mut canonical);
                crate::stream_tags::put_stream_tags(st);
                let id = hash128(&canonical);
                for &tenant_id in &tenant_ids {
                    let sid = StreamID { tenant_id, id };
                    idb.must_register_stream(&sid, &canonical);
                }
            }
        }
        idb.debug_flush();

        let result = idb.search_tenants();
        assert_eq!(result, tenant_ids, "unexpected tenantIDs");

        must_close_indexdb(&idb);
        esl_common::fs::must_remove_dir(&path);
        let storage_path = s.path.clone();
        s.must_close();
        esl_common::fs::must_remove_dir(&storage_path);
    }

    // -----------------------------------------------------------------------
    // Cross-compatibility with the Go reference binary.

    const GO_BINARY: &str = "/home/test/refs/bin/victoria-logs-go";

    fn http_request(port: u16, method: &str, path: &str, body: &str) -> String {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))
            .unwrap_or_else(|err| panic!("cannot connect to 127.0.0.1:{port}: {err}"));
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        resp
    }

    fn wait_for_http(port: u16, deadline: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        false
    }

    /// Cross-compat proof, Go→Rust direction: generates a data dir with the
    /// Go reference binary (ingesting a few streams over HTTP), stops it
    /// cleanly, then opens the produced `indexdb` directory with the Rust
    /// mergeset port and verifies stream searches return the Go-written
    /// streams.
    ///
    /// Ignored by default: requires the Go reference binary at
    /// /home/test/refs/bin/victoria-logs-go. Run with:
    /// `cargo test -p esl-logstorage --lib go_indexdb_cross_compat -- --ignored`
    #[test]
    #[ignore = "requires the Go reference binary (see the doc comment)"]
    fn test_go_indexdb_cross_compat() {
        if !std::path::Path::new(GO_BINARY).exists() {
            eprintln!("skipping: {GO_BINARY} is missing");
            return;
        }

        let data_dir = test_dir("go-cross-compat");
        let port: u16 = 9497;

        // Start the Go reference binary on a temp -storageDataPath.
        let mut child = std::process::Command::new(GO_BINARY)
            .arg(format!("-storageDataPath={data_dir}"))
            .arg(format!("-httpListenAddr=:{port}"))
            .arg("-retentionPeriod=10y")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("cannot start the Go reference binary");
        assert!(wait_for_http(port, std::time::Duration::from_secs(30)));

        // Ingest a few streams.
        const JOBS_COUNT: usize = 3;
        const INSTANCES_COUNT: usize = 2;
        let mut body = String::new();
        for i in 0..JOBS_COUNT {
            for j in 0..INSTANCES_COUNT {
                body.push_str(&format!(
                    "{{\"_msg\":\"test row {i} {j}\",\"_time\":\"2026-07-07T00:00:0{j}Z\",\"job\":\"go-job-{i}\",\"instance\":\"go-inst-{j}\"}}\n"
                ));
            }
        }
        let resp = http_request(
            port,
            "POST",
            "/insert/jsonline?_stream_fields=job,instance",
            &body,
        );
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "unexpected ingest response: {resp}"
        );
        let _ = http_request(port, "GET", "/internal/force_flush", "");

        // Stop the Go binary cleanly (graceful shutdown persists all parts).
        // No libc dependency in this crate; use kill(1) for the SIGINT.
        let kill_status = std::process::Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("cannot run kill");
        assert!(kill_status.success(), "kill -INT failed");
        let status = child.wait().expect("cannot wait for the Go binary");
        assert!(status.success(), "the Go binary exited with {status}");

        // Locate the per-day partition indexdb written by the Go binary.
        let partitions_dir = std::path::Path::new(&data_dir).join("partitions");
        let partition_dir = std::fs::read_dir(&partitions_dir)
            .expect("cannot read partitions dir")
            .next()
            .expect("no partitions created by the Go binary")
            .unwrap()
            .path();
        let partition_name = partition_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let indexdb_path = partition_dir.join("indexdb");
        assert!(
            indexdb_path.join("parts.json").exists(),
            "missing parts.json in the Go indexdb dir"
        );

        // Open the Go-written indexdb with the Rust port and search it.
        let s = new_test_storage();
        let idb = must_open_indexdb(indexdb_path.to_str().unwrap(), &partition_name, &s);

        let tenant_id = TenantID {
            account_id: 0,
            project_id: 0,
        };

        // searchTenants must return the (0:0) tenant registered by Go.
        let tenants = idb.search_tenants();
        assert_eq!(tenants, vec![tenant_id], "unexpected tenants");

        // Every ingested stream must be found via its stream filter, and its
        // streamID must match hash128(streamTagsCanonical) like upstream.
        for i in 0..JOBS_COUNT {
            for j in 0..INSTANCES_COUNT {
                let sf = must_new_test_stream_filter(&format!(
                    "{{job=\"go-job-{i}\",instance=\"go-inst-{j}\"}}"
                ));
                let stream_ids = idb.search_stream_ids(&[tenant_id], &sf);
                assert_eq!(
                    stream_ids.len(),
                    1,
                    "expected exactly one Go-written stream for job {i} instance {j}"
                );
                let mut tags = BTreeMap::new();
                tags.insert("job", format!("go-job-{i}"));
                tags.insert("instance", format!("go-inst-{j}"));
                let (expected_sid, _) = stream_id_for_tags(tenant_id, &tags);
                assert_eq!(stream_ids[0], expected_sid, "unexpected streamID");
                assert!(idb.has_stream_id(&expected_sid), "hasStreamID must be true");
            }
        }

        // Per-job filters must match INSTANCES_COUNT streams.
        for i in 0..JOBS_COUNT {
            let sf = must_new_test_stream_filter(&format!("{{job=\"go-job-{i}\"}}"));
            let stream_ids = idb.search_stream_ids(&[tenant_id], &sf);
            assert_eq!(
                stream_ids.len(),
                INSTANCES_COUNT,
                "unexpected streams for job {i}"
            );
        }

        // Register one more stream with the Rust port and verify it becomes
        // searchable next to the Go-written ones (Rust parts are written into
        // the same dir on close; the reverse Go-opens-this-dir direction is
        // verified live via the server binaries).
        let mut tags = BTreeMap::new();
        tags.insert("job", "rust-job".to_string());
        tags.insert("instance", "rust-inst".to_string());
        let (rust_sid, canonical) = stream_id_for_tags(tenant_id, &tags);
        idb.must_register_stream(&rust_sid, &canonical);
        idb.debug_flush();
        let sf = must_new_test_stream_filter("{job=\"rust-job\"}");
        let stream_ids = idb.search_stream_ids(&[tenant_id], &sf);
        assert_eq!(stream_ids, vec![rust_sid], "unexpected rust-job streamIDs");

        must_close_indexdb(&idb);
        drop(idb);
        let storage_path = s.path.clone();
        s.must_close();
        esl_common::fs::must_remove_dir(&storage_path);
        esl_common::fs::must_remove_dir(&data_dir);
    }
}
