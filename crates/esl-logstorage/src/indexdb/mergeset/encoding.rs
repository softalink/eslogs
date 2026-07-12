//! Port of `lib/mergeset/encoding.go`: the inmemoryBlock item encoding.
//!
//! This is the byte-format-critical core of mergeset: the items/lens block
//! encodings produced here (plain and zstd) must match upstream bit-for-bit
//! semantics so that EsLogs can open indexdb parts written by upstream
//! VictoriaLogs and vice versa.

use esl_common::encoding;

/// Item represents a single item for storing in a mergeset
/// (port of `mergeset.Item`).
#[derive(Clone, Copy, Default)]
pub(crate) struct Item {
    /// Start is start offset for the item in data.
    pub start: u32,

    /// End is end offset for the item in data.
    pub end: u32,
}

impl Item {
    /// Returns bytes representation of it obtained from data
    /// (port of `Item.Bytes`).
    pub fn bytes<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        &data[self.start as usize..self.end as usize]
    }
}

/// maxInmemoryBlockSize is the maximum inmemoryBlock.data size.
///
/// It must fit CPU cache size, i.e. 64KB for the current CPUs.
pub(crate) const MAX_INMEMORY_BLOCK_SIZE: usize = 64 * 1024;

/// inmemoryBlock holds a block of items in memory.
#[derive(Default)]
pub(crate) struct InmemoryBlock {
    /// commonPrefix contains common prefix for all the items stored in the block.
    pub common_prefix: Vec<u8>,

    /// data contains source data for items.
    pub data: Vec<u8>,

    /// items contains items stored in the block.
    /// Every item contains the prefix specified at commonPrefix.
    pub items: Vec<Item>,
}

/// storageBlock represents a block of data on the storage
/// (port of `mergeset.storageBlock`).
#[derive(Default)]
pub(crate) struct StorageBlock {
    pub items_data: Vec<u8>,
    pub lens_data: Vec<u8>,
}

impl StorageBlock {
    pub fn reset(&mut self) {
        self.items_data.clear();
        self.lens_data.clear();
    }
}

/// marshalType is the type used for block compression.
pub(crate) type MarshalType = u8;

pub(crate) const MARSHAL_TYPE_PLAIN: MarshalType = 0;
pub(crate) const MARSHAL_TYPE_ZSTD: MarshalType = 1;

pub(crate) fn check_marshal_type(mt: MarshalType) -> Result<(), String> {
    if mt > 1 {
        return Err(format!("marshalType must be in the range [0..1]; got {mt}"));
    }
    Ok(())
}

/// Returns the length of the common prefix of a and b
/// (port of `commonPrefixLen`).
pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

impl InmemoryBlock {
    pub fn copy_from(&mut self, src: &InmemoryBlock) {
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(&src.common_prefix);
        self.data.clear();
        self.data.extend_from_slice(&src.data);
        self.items.clear();
        self.items.extend_from_slice(&src.items);
    }

    /// Sorts the items and updates the common prefix
    /// (port of `inmemoryBlock.SortItems`).
    pub fn sort_items(&mut self) {
        if !self.is_sorted() {
            self.update_common_prefix_unsorted();
            let data = &self.data;
            self.items
                .sort_unstable_by(|a, b| a.bytes(data).cmp(b.bytes(data)));
        } else {
            self.update_common_prefix_sorted();
        }
    }

    /// Returns the approximate size of ib in bytes
    /// (port of `inmemoryBlock.SizeBytes`).
    pub fn size_bytes(&self) -> usize {
        size_of::<InmemoryBlock>()
            + self.common_prefix.capacity()
            + self.data.capacity()
            + self.items.capacity() * size_of::<Item>()
    }

    pub fn reset(&mut self) {
        self.common_prefix.clear();
        self.data.clear();
        self.items.clear();
    }

    fn update_common_prefix_sorted(&mut self) {
        if self.items.len() <= 1 {
            // There is no sense in duplicating a single item or zero items into
            // commonPrefix, since this only can increase blockHeader size
            // without any benefits.
            self.common_prefix.clear();
            return;
        }

        let data = &self.data;
        let first = self.items[0].bytes(data);
        let last = self.items[self.items.len() - 1].bytes(data);
        let cp_len = common_prefix_len(first, last);
        let cp = first[..cp_len].to_vec();
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(&cp);
    }

    fn update_common_prefix_unsorted(&mut self) {
        self.common_prefix.clear();
        if self.items.is_empty() {
            return;
        }
        let data = &self.data;
        let mut cp = self.items[0].bytes(data);
        for it in &self.items[1..] {
            let item = it.bytes(data);
            if item.starts_with(cp) {
                continue;
            }
            let cp_len = common_prefix_len(cp, item);
            if cp_len == 0 {
                return;
            }
            cp = &cp[..cp_len];
        }
        let cp = cp.to_vec();
        self.common_prefix.extend_from_slice(&cp);
    }

    /// Adds x to the end of ib.
    ///
    /// false is returned if x isn't added to ib due to block size constraints
    /// (port of `inmemoryBlock.Add`).
    pub fn add(&mut self, x: &[u8]) -> bool {
        if x.len() + self.data.len() > MAX_INMEMORY_BLOCK_SIZE {
            return false;
        }
        if self.data.capacity() == 0 {
            // Pre-allocate data and items in order to reduce memory allocations
            self.data.reserve(MAX_INMEMORY_BLOCK_SIZE);
            self.items.reserve(512);
        }
        let start = self.data.len() as u32;
        self.data.extend_from_slice(x);
        self.items.push(Item {
            start,
            end: self.data.len() as u32,
        });
        true
    }

    pub fn is_sorted(&self) -> bool {
        let data = &self.data;
        self.items
            .windows(2)
            .all(|w| w[0].bytes(data) <= w[1].bytes(data))
    }

    /// Marshals unsorted items from ib to sb
    /// (port of `inmemoryBlock.MarshalUnsortedData`).
    ///
    /// It also:
    /// - appends the first item to first_item_dst,
    /// - appends the common prefix for all the items to common_prefix_dst,
    /// - returns the number of items encoded including the first item,
    /// - returns the marshal type used for the encoding.
    pub fn marshal_unsorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        self.sort_items();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    /// Marshals sorted items from ib to sb
    /// (port of `inmemoryBlock.MarshalSortedData`).
    pub fn marshal_sorted_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        // PORT NOTE: Go checks sortedness only when running under `go test`
        // (isInTest); the port uses debug_assertions for the same effect.
        if cfg!(debug_assertions) && !self.is_sorted() {
            esl_common::panicf!(
                "BUG: {} items must be sorted; items:\n{}",
                self.items.len(),
                self.debug_items_string()
            );
        }
        self.update_common_prefix_sorted();
        self.marshal_data(sb, first_item_dst, common_prefix_dst, compress_level)
    }

    pub fn debug_items_string(&self) -> String {
        use std::fmt::Write;
        let mut sb = String::new();
        let mut prev_item: &[u8] = b"";
        let data = &self.data;
        for (i, it) in self.items.iter().enumerate() {
            let item = it.bytes(data);
            if item < prev_item {
                let _ = writeln!(
                    sb,
                    "!!! the next item is smaller than the previous item !!!"
                );
            }
            let _ = writeln!(sb, "{i:05} {item:X?}");
            prev_item = item;
        }
        sb
    }

    /// Preconditions:
    /// - ib.items must be sorted.
    /// - update_common_prefix* must be called.
    fn marshal_data(
        &mut self,
        sb: &mut StorageBlock,
        first_item_dst: &mut Vec<u8>,
        common_prefix_dst: &mut Vec<u8>,
        compress_level: i32,
    ) -> (u32, MarshalType) {
        if self.items.is_empty() {
            esl_common::panicf!(
                "BUG: inmemoryBlock.marshalData must be called on non-empty blocks only"
            );
        }
        // PORT NOTE: Go additionally checks len(items) < 1<<32; items.len() is
        // bounded by MAX_INMEMORY_BLOCK_SIZE (64KB of data), so the check is
        // unreachable here.

        let data = &self.data;
        let first_item = self.items[0].bytes(data);
        first_item_dst.extend_from_slice(first_item);
        common_prefix_dst.extend_from_slice(&self.common_prefix);

        if data.len() - self.common_prefix.len() * self.items.len() < 64 || self.items.len() < 2 {
            // Use plain encoding form small block, since it is cheaper.
            self.marshal_data_plain(sb);
            return (self.items.len() as u32, MARSHAL_TYPE_PLAIN);
        }

        let mut b_items: Vec<u8> = Vec::new();
        let mut b_lens: Vec<u8> = Vec::new();

        // Marshal items data.
        let mut xs: Vec<u64> = vec![0; self.items.len() - 1];

        let cp_len = self.common_prefix.len();
        let mut prev_item = &first_item[cp_len..];
        let mut prev_prefix_len = 0u64;
        for (i, it) in self.items[1..].iter().enumerate() {
            let item = &data[it.start as usize + cp_len..it.end as usize];
            let prefix_len = common_prefix_len(prev_item, item) as u64;
            b_items.extend_from_slice(&item[prefix_len as usize..]);
            let x_len = prefix_len ^ prev_prefix_len;
            prev_item = item;
            prev_prefix_len = prefix_len;

            xs[i] = x_len;
        }
        encoding::marshal_var_uint64s(&mut b_lens, &xs);
        sb.items_data.clear();
        encoding::compress_zstd_level(&mut sb.items_data, &b_items, compress_level);

        // Marshal lens data.
        let mut prev_item_len = (first_item.len() - cp_len) as u64;
        for (i, it) in self.items[1..].iter().enumerate() {
            let item_len = ((it.end - it.start) as usize - cp_len) as u64;
            let x_len = item_len ^ prev_item_len;
            prev_item_len = item_len;

            xs[i] = x_len;
        }
        encoding::marshal_var_uint64s(&mut b_lens, &xs);
        sb.lens_data.clear();
        encoding::compress_zstd_level(&mut sb.lens_data, &b_lens, compress_level);

        if sb.items_data.len() as f64
            > 0.9 * (data.len() - self.common_prefix.len() * self.items.len()) as f64
        {
            // Bad compression rate. It is cheaper to use plain encoding.
            self.marshal_data_plain(sb);
            return (self.items.len() as u32, MARSHAL_TYPE_PLAIN);
        }

        // Good compression rate.
        (self.items.len() as u32, MARSHAL_TYPE_ZSTD)
    }

    /// Port of `inmemoryBlock.unmarshalSingleItem`.
    pub fn unmarshal_single_item(
        &mut self,
        common_prefix: &[u8],
        first_item: &[u8],
        mt: MarshalType,
    ) {
        if mt != MARSHAL_TYPE_PLAIN {
            esl_common::panicf!("BUG: single item block must be always encoded with TypePlain");
        }
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(common_prefix);
        self.data.clear();
        self.data.extend_from_slice(first_item);
        self.items.clear();
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
    }

    /// Decodes items_count items from sb and first_item and stores them to ib
    /// (port of `inmemoryBlock.UnmarshalData`).
    pub fn unmarshal_data(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        common_prefix: &[u8],
        items_count: u32,
        mt: MarshalType,
    ) -> Result<(), String> {
        self.reset();

        if items_count == 0 {
            esl_common::panicf!("BUG: cannot unmarshal zero items");
        }

        self.common_prefix.extend_from_slice(common_prefix);

        match mt {
            MARSHAL_TYPE_PLAIN => {
                self.unmarshal_data_plain(sb, first_item, items_count)
                    .map_err(|err| format!("cannot unmarshal plain data: {err}"))?;
                if !self.is_sorted() {
                    return Err(format!(
                        "plain data block contains unsorted items; items:\n{}",
                        self.debug_items_string()
                    ));
                }
                return Ok(());
            }
            MARSHAL_TYPE_ZSTD => {
                // it is handled below.
            }
            _ => return Err(format!("unknown marshalType={mt}")),
        }

        // Unmarshal mt = marshalTypeZSTD

        // Unmarshal lens data.
        let mut bb: Vec<u8> = Vec::new();
        encoding::decompress_zstd(&mut bb, &sb.lens_data)
            .map_err(|err| format!("cannot decompress lensData: {err}"))?;

        let mut lens_buf: Vec<u64> = vec![0; 2 * items_count as usize];
        let (prefix_lens, lens) = lens_buf.split_at_mut(items_count as usize);

        let mut xs: Vec<u64> = vec![0; items_count as usize - 1];

        // Unmarshal prefixLens
        let tail = encoding::unmarshal_var_uint64s(&mut xs, &bb)
            .map_err(|err| format!("cannot unmarshal prefixLens from lensData: {err}"))?;
        prefix_lens[0] = 0;
        for i in 0..xs.len() {
            prefix_lens[i + 1] = xs[i] ^ prefix_lens[i];
        }

        // Unmarshal lens
        let tail = encoding::unmarshal_var_uint64s(&mut xs, tail)
            .map_err(|err| format!("cannot unmarshal lens from lensData: {err}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected tail left unmarshaling {items_count} lens; tail size={}; contents={tail:X?}",
                tail.len()
            ));
        }
        lens[0] = (first_item.len() - common_prefix.len()) as u64;
        let mut data_len = common_prefix.len() * items_count as usize;
        data_len += lens[0] as usize;
        for i in 0..xs.len() {
            let item_len = xs[i] ^ lens[i];
            lens[i + 1] = item_len;
            data_len += item_len as usize;
        }

        // Unmarshal items data.
        bb.clear();
        encoding::decompress_zstd(&mut bb, &sb.items_data)
            .map_err(|err| format!("cannot decompress lensData: {err}"))?;
        // Resize ib.data to dataLen, since the data isn't going to be resized
        // after unmarshaling. This may save memory for caching the unmarshaled
        // block.
        self.data.reserve(data_len);
        self.data.extend_from_slice(first_item);
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
        // prev_item is tracked as a range into self.data, since self.data is
        // appended to in the loop (Go keeps a slice alias into data instead).
        let mut prev_item_start = common_prefix.len();
        let mut prev_item_end = self.data.len();
        let mut b: &[u8] = &bb;
        for i in 1..items_count as usize {
            let item_len = lens[i];
            let prefix_len = prefix_lens[i];
            if prefix_len > item_len {
                return Err(format!("prefixLen={prefix_len} exceeds itemLen={item_len}"));
            }
            let suffix_len = item_len - prefix_len;
            if (b.len() as u64) < suffix_len {
                return Err(format!(
                    "not enough data for decoding item from itemsData; want {suffix_len} bytes; remained {} bytes",
                    b.len()
                ));
            }
            if prefix_len as usize > prev_item_end - prev_item_start {
                return Err(format!(
                    "prefixLen cannot exceed {}; got {prefix_len}",
                    prev_item_end - prev_item_start
                ));
            }
            let data_start = self.data.len();
            self.data.extend_from_slice(common_prefix);
            self.data
                .extend_from_within(prev_item_start..prev_item_start + prefix_len as usize);
            self.data.extend_from_slice(&b[..suffix_len as usize]);
            self.items.push(Item {
                start: data_start as u32,
                end: self.data.len() as u32,
            });
            b = &b[suffix_len as usize..];
            prev_item_end = self.data.len();
            prev_item_start = prev_item_end - item_len as usize;
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after itemsData with len {}: {b:X?}",
                b.len()
            ));
        }
        if self.data.len() != data_len {
            return Err(format!(
                "unexpected data len; got {}; want {data_len}",
                self.data.len()
            ));
        }
        if !self.is_sorted() {
            return Err(format!(
                "decoded data block contains unsorted items; items:\n{}",
                self.debug_items_string()
            ));
        }
        Ok(())
    }

    /// Port of `inmemoryBlock.marshalDataPlain`.
    fn marshal_data_plain(&self, sb: &mut StorageBlock) {
        let data = &self.data;

        // Marshal items data.
        // There is no need in marshaling the first item, since it is returned
        // to the caller in marshalData.
        let cp_len = self.common_prefix.len();
        sb.items_data.clear();
        for it in &self.items[1..] {
            sb.items_data
                .extend_from_slice(&data[it.start as usize + cp_len..it.end as usize]);
        }

        // Marshal length data.
        sb.lens_data.clear();
        for it in &self.items[1..] {
            encoding::marshal_uint64(
                &mut sb.lens_data,
                ((it.end - it.start) as usize - cp_len) as u64,
            );
        }
    }

    /// Port of `inmemoryBlock.unmarshalDataPlain`.
    fn unmarshal_data_plain(
        &mut self,
        sb: &StorageBlock,
        first_item: &[u8],
        items_count: u32,
    ) -> Result<(), String> {
        let common_prefix_len = self.common_prefix.len();

        // Unmarshal lens data.
        let mut lens: Vec<u64> = vec![0; items_count as usize];
        lens[0] = (first_item.len() - common_prefix_len) as u64;
        let mut b: &[u8] = &sb.lens_data;
        for len_dst in lens.iter_mut().skip(1) {
            if b.len() < 8 {
                return Err(format!(
                    "too short tail for decoding len from lensData; got {} bytes; want at least {} bytes",
                    b.len(),
                    8
                ));
            }
            *len_dst = encoding::unmarshal_uint64(b);
            b = &b[8..];
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after lensData with len {}: {b:X?}",
                b.len()
            ));
        }

        // Unmarshal items data.
        let data_len =
            first_item.len() + sb.items_data.len() + common_prefix_len * (items_count as usize - 1);
        self.data.clear();
        self.data.reserve(data_len);
        self.items.clear();
        self.data.extend_from_slice(first_item);
        self.items.push(Item {
            start: 0,
            end: self.data.len() as u32,
        });
        let mut b: &[u8] = &sb.items_data;
        let common_prefix = self.common_prefix.clone();
        for &item_len in lens.iter().skip(1) {
            if (b.len() as u64) < item_len {
                return Err(format!(
                    "not enough data for decoding item from itemsData; want {item_len} bytes; remained {} bytes",
                    b.len()
                ));
            }
            let data_start = self.data.len();
            self.data.extend_from_slice(&common_prefix);
            self.data.extend_from_slice(&b[..item_len as usize]);
            self.items.push(Item {
                start: data_start as u32,
                end: self.data.len() as u32,
            });
            b = &b[item_len as usize..];
        }
        if !b.is_empty() {
            return Err(format!(
                "unexpected tail left after itemsData with len {}: {b:X?}",
                b.len()
            ));
        }
        if self.data.len() != data_len {
            return Err(format!(
                "unexpected data len; got {}; want {data_len}",
                self.data.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Deterministic RNG substitute for Go's seeded math/rand
    /// (same rationale as the datadb.rs test module).
    pub(crate) struct TestRng {
        state: u64,
    }

    impl TestRng {
        pub fn new(seed: u64) -> TestRng {
            TestRng {
                state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
            }
        }

        pub fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        pub fn intn(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }

        /// Substitute for Go's `testing/quick` random []byte generator:
        /// a random-length byte string with random contents.
        pub fn random_bytes(&mut self) -> Vec<u8> {
            let n = self.intn(50);
            (0..n).map(|_| (self.next_u64() & 0xFF) as u8).collect()
        }
    }

    #[test]
    fn test_common_prefix_len() {
        let f = |a: &str, b: &str, expected: usize| {
            let prefix_len = common_prefix_len(a.as_bytes(), b.as_bytes());
            assert_eq!(
                prefix_len, expected,
                "unexpected prefix len; got {prefix_len}; want {expected}"
            );
        };
        f("", "", 0);
        f("a", "", 0);
        f("", "a", 0);
        f("a", "a", 1);
        f("abc", "xy", 0);
        f("abc", "abd", 2);
        f("01234567", "01234567", 8);
        f("01234567", "012345678", 8);
        f("012345679", "012345678", 8);
        f("01234569", "012345678", 7);
        f("01234569", "01234568", 7);
    }

    #[test]
    fn test_inmemory_block_add() {
        let mut r = TestRng::new(1);
        let mut ib = InmemoryBlock::default();

        for i in 0..30 {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            // Fill ib.
            for _ in 0..(i * 100 + 1) {
                let s = r.random_bytes();
                if !ib.add(&s) {
                    // ib is full.
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            // Verify all the items are added.
            assert_eq!(ib.items.len(), items.len());
            assert_eq!(ib.data.len(), total_len);
            for (j, it) in ib.items.iter().enumerate() {
                assert_eq!(
                    it.bytes(&ib.data),
                    &items[j][..],
                    "unexpected item at index {j} out of {}, loop {i}",
                    items.len()
                );
            }
        }
    }

    #[test]
    fn test_inmemory_block_sort() {
        let mut r = TestRng::new(1);
        let mut ib = InmemoryBlock::default();

        for i in 0..100 {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            // Fill ib.
            for _ in 0..r.intn(1500) {
                let s = r.random_bytes();
                if !ib.add(&s) {
                    // ib is full.
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            // Sort ib.
            ib.sort_items();
            items.sort();

            // Verify items are sorted.
            assert_eq!(ib.items.len(), items.len());
            assert_eq!(ib.data.len(), total_len);
            for (j, it) in ib.items.iter().enumerate() {
                assert_eq!(
                    it.bytes(&ib.data),
                    &items[j][..],
                    "unexpected item at index {j} out of {}, loop {i}",
                    items.len()
                );
            }
        }
    }

    #[test]
    fn test_inmemory_block_marshal_unmarshal() {
        let mut r = TestRng::new(1);
        let mut ib = InmemoryBlock::default();
        let mut ib2 = InmemoryBlock::default();
        let mut sb = StorageBlock::default();

        for i in (0..1000).step_by(10) {
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut total_len = 0;
            ib.reset();

            // Fill ib.
            let items_count = 2 * (r.intn(i + 1) + 1);
            for _ in 0..items_count / 2 {
                let s = r.random_bytes();
                let mut prefixed = b"prefix ".to_vec();
                prefixed.extend_from_slice(&s);
                if !ib.add(&prefixed) {
                    // ib is full.
                    break;
                }
                total_len += prefixed.len();
                items.push(prefixed);

                let s = r.random_bytes();
                if !ib.add(&s) {
                    // ib is full
                    break;
                }
                total_len += s.len();
                items.push(s);
            }

            // Marshal ib.
            items.sort();
            let mut first_item = Vec::new();
            let mut common_prefix = Vec::new();
            let (items_len, mt) =
                ib.marshal_unsorted_data(&mut sb, &mut first_item, &mut common_prefix, 0);
            assert_eq!(items_len as usize, ib.items.len());
            let first_item_expected = ib.items[0].bytes(&ib.data);
            assert_eq!(&first_item[..], first_item_expected);
            check_marshal_type(mt).unwrap();

            // Unmarshal ib.
            ib2.unmarshal_data(&sb, &first_item, &common_prefix, items_len, mt)
                .unwrap_or_else(|err| {
                    panic!(
                        "cannot unmarshal data for firstItem={first_item:X?}, commonPrefix={common_prefix:X?}, itemsLen={items_len}, mt={mt}: {err}"
                    )
                });

            // Verify all the items are sorted and unmarshaled.
            assert_eq!(ib2.items.len(), items.len());
            assert_eq!(ib2.data.len(), total_len);
            for (j, it) in ib2.items.iter().enumerate() {
                assert_eq!(
                    it.bytes(&ib2.data),
                    &items[j][..],
                    "unexpected item at index {j} out of {}, loop {i}",
                    items.len()
                );
            }
        }
    }
}
