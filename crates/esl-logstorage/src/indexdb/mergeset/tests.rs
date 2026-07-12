//! Ports of the upstream `lib/mergeset` tests that span multiple submodules:
//! merge_test.go, part_search_test.go, block_stream_reader_test.go,
//! table_test.go, table_search_test.go.
//!
//! (encoding_test.go is ported inside encoding.rs.)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::block_stream_reader::BlockStreamReader;
use super::block_stream_writer::BlockStreamWriter;
use super::encoding::tests::TestRng;
use super::encoding::{InmemoryBlock, MAX_INMEMORY_BLOCK_SIZE};
use super::inmemory_part::InmemoryPart;
use super::merge::{MergeError, merge_block_streams};
use super::part::new_part_from_inmemory_part;
use super::part_search::PartSearch;
use super::table::{TableMetrics, must_open_table_ex};
use super::table_search::TableSearch;
use super::{Item, SearchError, must_open_table};

fn test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "esl-mergeset-test-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    esl_common::fs::must_remove_dir(&dir);
    dir
}

/// Port of `newTestInmemoryBlockStreamReaders`.
fn new_test_inmemory_block_stream_readers(
    r: &mut TestRng,
    blocks_count: usize,
    max_items_per_block: usize,
) -> (Vec<BlockStreamReader<'static>>, Vec<Vec<u8>>) {
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut bsrs = Vec::new();
    for _ in 0..blocks_count {
        let mut ib = InmemoryBlock::default();
        let items_per_block = r.intn(max_items_per_block) + 1;
        for _ in 0..items_per_block {
            let item = r.random_bytes();
            if !ib.add(&item) {
                break;
            }
            items.push(item);
        }
        let mut ip = InmemoryPart::default();
        ip.init(&mut ib);
        let ip = Box::leak(Box::new(ip));
        let mut bsr = BlockStreamReader::default();
        bsr.must_init_from_inmemory_part(ip);
        bsrs.push(bsr);
    }
    items.sort();
    (bsrs, items)
}

fn new_test_block_stream_reader(ip: &InmemoryPart) -> BlockStreamReader<'_> {
    let mut bsr = BlockStreamReader::default();
    bsr.must_init_from_inmemory_part(ip);
    bsr
}

// ---------------------------------------------------------------------------
// merge_test.go

/// Port of `testCheckItems`.
fn test_check_items(dst_ip: &InmemoryPart, items: &[Vec<u8>]) -> Result<(), String> {
    if dst_ip.ph.items_count as usize != items.len() {
        return Err(format!(
            "unexpected number of items in the part; got {}; want {}",
            dst_ip.ph.items_count,
            items.len()
        ));
    }
    if dst_ip.ph.first_item != items[0] {
        return Err(format!(
            "unexpected first item; got {:X?}; want {:X?}",
            dst_ip.ph.first_item, items[0]
        ));
    }
    if dst_ip.ph.last_item != items[items.len() - 1] {
        return Err(format!(
            "unexpected last item; got {:X?}; want {:X?}",
            dst_ip.ph.last_item,
            items[items.len() - 1]
        ));
    }

    let mut dst_items: Vec<Vec<u8>> = Vec::new();
    let mut dst_bsr = new_test_block_stream_reader(dst_ip);
    while dst_bsr.next() {
        let bh = &dst_bsr.bhs[dst_bsr.bh_idx - 1];
        if bh.items_count as usize != dst_bsr.block.items.len() {
            return Err(format!(
                "unexpected number of items in the block; got {}; want {}",
                dst_bsr.block.items.len(),
                bh.items_count
            ));
        }
        if bh.items_count == 0 {
            return Err("unexpected empty block".to_string());
        }
        let item = dst_bsr.block.items[0].bytes(&dst_bsr.block.data);
        if bh.first_item != item {
            return Err(format!(
                "unexpected blockHeader.firstItem; got {:X?}; want {item:X?}",
                bh.first_item
            ));
        }
        for it in &dst_bsr.block.items {
            dst_items.push(it.bytes(&dst_bsr.block.data).to_vec());
        }
    }
    if let Some(err) = dst_bsr.error() {
        return Err(format!("unexpected error in dstBsr: {err}"));
    }
    if items != dst_items {
        return Err("unequal items".to_string());
    }
    Ok(())
}

/// Port of `testMergeBlockStreamsSerial`.
fn test_merge_block_streams_serial(
    r: &mut TestRng,
    blocks_to_merge: usize,
    max_items_per_block: usize,
) -> Result<(), String> {
    // Prepare blocks to merge.
    let (mut bsrs, items) =
        new_test_inmemory_block_stream_readers(r, blocks_to_merge, max_items_per_block);

    // Merge blocks.
    let items_merged = AtomicU64::new(0);
    let mut dst_ip = InmemoryPart::default();
    {
        let mut bsw = BlockStreamWriter::default();
        bsw.must_init_from_inmemory_part(&mut dst_ip, -4);
        let mut ph = super::part_header::PartHeader::default();
        merge_block_streams(&mut ph, &mut bsw, &mut bsrs, None, None, &items_merged)
            .map_err(|err| format!("cannot merge block streams: {err}"))?;
        dst_ip.ph = ph;
    }
    let n = items_merged.load(Ordering::SeqCst);
    if n as usize != items.len() {
        return Err(format!(
            "unexpected itemsMerged; got {n}; want {}",
            items.len()
        ));
    }

    // Verify the resulting part (dstIP) contains all the items
    // in the correct order.
    test_check_items(&dst_ip, &items).map_err(|err| format!("error checking items: {err}"))
}

/// Port of `TestMergeBlockStreams`.
#[test]
fn test_merge_block_streams() {
    for blocks_to_merge in [1usize, 2, 3, 4, 5, 10, 20] {
        for max_items_per_block in [1usize, 2, 10, 100, 1000, 10000] {
            let mut r = TestRng::new(1);
            if let Err(err) =
                test_merge_block_streams_serial(&mut r, blocks_to_merge, max_items_per_block)
            {
                panic!(
                    "unexpected error in serial test (blocks={blocks_to_merge}, maxItems={max_items_per_block}): {err}"
                );
            }

            const CONCURRENCY: usize = 3;
            std::thread::scope(|s| {
                for i in 0..CONCURRENCY {
                    s.spawn(move || {
                        let mut r = TestRng::new(i as u64);
                        if let Err(err) = test_merge_block_streams_serial(
                            &mut r,
                            blocks_to_merge,
                            max_items_per_block,
                        ) {
                            panic!("unexpected error in concurrent test: {err}");
                        }
                    });
                }
            });
        }
    }
}

/// Port of `TestMultilevelMerge`.
#[test]
fn test_multilevel_merge() {
    let mut r = TestRng::new(1);

    // Prepare blocks to merge.
    let (mut bsrs, items) = new_test_inmemory_block_stream_readers(&mut r, 10, 4000);
    let items_merged = AtomicU64::new(0);

    // First level merge
    let mut dst_ip1 = InmemoryPart::default();
    let (bsrs1, bsrs2) = bsrs.split_at_mut(5);
    {
        let mut bsw1 = BlockStreamWriter::default();
        bsw1.must_init_from_inmemory_part(&mut dst_ip1, -5);
        let mut ph = super::part_header::PartHeader::default();
        merge_block_streams(&mut ph, &mut bsw1, bsrs1, None, None, &items_merged)
            .unwrap_or_else(|err| panic!("cannot merge first level part 1: {err}"));
        dst_ip1.ph = ph;
    }

    let mut dst_ip2 = InmemoryPart::default();
    {
        let mut bsw2 = BlockStreamWriter::default();
        bsw2.must_init_from_inmemory_part(&mut dst_ip2, -5);
        let mut ph = super::part_header::PartHeader::default();
        merge_block_streams(&mut ph, &mut bsw2, bsrs2, None, None, &items_merged)
            .unwrap_or_else(|err| panic!("cannot merge first level part 2: {err}"));
        dst_ip2.ph = ph;
    }

    let n = items_merged.load(Ordering::SeqCst);
    assert_eq!(n as usize, items.len(), "unexpected itemsMerged");

    // Second level merge (aka final merge)
    items_merged.store(0, Ordering::SeqCst);
    let mut dst_ip = InmemoryPart::default();
    {
        let mut bsrs_top = vec![
            new_test_block_stream_reader(&dst_ip1),
            new_test_block_stream_reader(&dst_ip2),
        ];
        let mut bsw = BlockStreamWriter::default();
        bsw.must_init_from_inmemory_part(&mut dst_ip, 1);
        let mut ph = super::part_header::PartHeader::default();
        merge_block_streams(&mut ph, &mut bsw, &mut bsrs_top, None, None, &items_merged)
            .unwrap_or_else(|err| panic!("cannot merge second level: {err}"));
        dst_ip.ph = ph;
    }
    let n = items_merged.load(Ordering::SeqCst);
    assert_eq!(
        n as usize,
        items.len(),
        "unexpected itemsMerged after final merge"
    );

    // Verify the resulting part (dstIP) contains all the items
    // in the correct order.
    test_check_items(&dst_ip, &items).unwrap_or_else(|err| panic!("error checking items: {err}"));
}

/// Port of `TestMergeForciblyStop`.
#[test]
fn test_merge_forcibly_stop() {
    let mut r = TestRng::new(1);
    let (mut bsrs, _) = new_test_inmemory_block_stream_readers(&mut r, 20, 4000);
    let mut dst_ip = InmemoryPart::default();
    let mut bsw = BlockStreamWriter::default();
    bsw.must_init_from_inmemory_part(&mut dst_ip, 1);
    let stop = AtomicBool::new(true);
    let items_merged = AtomicU64::new(0);
    let mut ph = super::part_header::PartHeader::default();
    match merge_block_streams(
        &mut ph,
        &mut bsw,
        &mut bsrs,
        None,
        Some(&stop),
        &items_merged,
    ) {
        Err(MergeError::ForciblyStopped) => {}
        other => panic!("unexpected result during merge: {other:?}; want forcibly stopped"),
    }
    assert_eq!(
        items_merged.load(Ordering::SeqCst),
        0,
        "unexpected itemsMerged"
    );
}

// ---------------------------------------------------------------------------
// block_stream_reader_test.go

/// Port of `TestBlockStreamReaderReadFromInmemoryPart`.
#[test]
fn test_block_stream_reader_read_from_inmemory_part() {
    let mut r = TestRng::new(1);
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut ib = InmemoryBlock::default();
    for _ in 0..100 {
        let item = r.random_bytes();
        if !ib.add(&item) {
            break;
        }
        items.push(item);
    }
    items.sort();
    let mut ip = InmemoryPart::default();
    ip.init(&mut ib);

    // Make sure items may be read concurrently from the same inmemoryPart.
    let ip = &ip;
    let items = &items;
    std::thread::scope(|s| {
        for _ in 0..5 {
            s.spawn(move || {
                test_block_stream_reader_read(ip, items)
                    .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            });
        }
    });
}

fn test_block_stream_reader_read(ip: &InmemoryPart, items: &[Vec<u8>]) -> Result<(), String> {
    let mut bsr = new_test_block_stream_reader(ip);
    let mut i = 0;
    while bsr.next() {
        for it in &bsr.block.items {
            let item = it.bytes(&bsr.block.data);
            if item != items[i] {
                return Err(format!(
                    "unexpected item[{i}]; got {item:X?}; want {:X?}",
                    items[i]
                ));
            }
            i += 1;
        }
    }
    if let Some(err) = bsr.error() {
        return Err(err);
    }
    if i != items.len() {
        return Err(format!(
            "unexpected number of items read; got {i}; want {}",
            items.len()
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// part_search_test.go

/// Port of `newTestPart`.
fn new_test_part(
    r: &mut TestRng,
    blocks_count: usize,
    max_items_per_block: usize,
) -> Result<(Arc<super::table::PartWrapper>, Vec<Vec<u8>>), String> {
    let (mut bsrs, items) =
        new_test_inmemory_block_stream_readers(r, blocks_count, max_items_per_block);

    let items_merged = AtomicU64::new(0);
    let mut ip = InmemoryPart::default();
    {
        let mut bsw = BlockStreamWriter::default();
        bsw.must_init_from_inmemory_part(&mut ip, -3);
        let mut ph = super::part_header::PartHeader::default();
        merge_block_streams(&mut ph, &mut bsw, &mut bsrs, None, None, &items_merged)
            .map_err(|err| format!("cannot merge blocks: {err}"))?;
        ip.ph = ph;
    }
    let n = items_merged.load(Ordering::SeqCst);
    if n as usize != items.len() {
        return Err(format!(
            "unexpected itemsMerged; got {n}; want {}",
            items.len()
        ));
    }
    let ip = Arc::new(ip);
    let p = new_part_from_inmemory_part(&ip);
    let pw = super::table::PartWrapper::new_for_test(p, Some(ip));
    Ok((pw, items))
}

/// Port of `testPartSearchSerial`.
fn test_part_search_serial(
    r: &mut TestRng,
    p: &Arc<super::table::PartWrapper>,
    items: &[Vec<u8>],
) -> Result<(), String> {
    let mut ps = PartSearch::default();

    ps.init(Arc::clone(p), true);

    // Search for the item smaller than the items[0]
    let mut k: Vec<u8> = items[0].clone();
    if !k.is_empty() {
        k.pop();
    }
    ps.seek(&k);
    for (i, item) in items.iter().enumerate() {
        if !ps.next_item() {
            return Err(format!("missing item at position {i}"));
        }
        if ps.item() != item.as_slice() {
            return Err(format!(
                "unexpected item found at position {i}; got {:X?}; want {item:X?}",
                ps.item()
            ));
        }
    }
    if ps.next_item() {
        return Err(format!(
            "unexpected item found past the end of all the items: {:X?}",
            ps.item()
        ));
    }
    if let Some(err) = ps.error() {
        return Err(format!("unexpected error: {err}"));
    }

    // Search for the item bigger than the items[len(items)-1]
    let mut k: Vec<u8> = items[items.len() - 1].clone();
    k.extend_from_slice(b"tail");
    ps.seek(&k);
    if ps.next_item() {
        return Err(format!(
            "unexpected item found: {:X?}; want nothing",
            ps.item()
        ));
    }
    if let Some(err) = ps.error() {
        return Err(format!(
            "unexpected error when searching past the last item: {err}"
        ));
    }

    // Search for inner items
    for loop_idx in 0..100 {
        let idx = r.intn(items.len());
        let k = items[idx].clone();
        ps.seek(&k);
        let n = items.partition_point(|item| item.as_slice() < k.as_slice());
        for i in n..items.len() {
            if !ps.next_item() {
                return Err(format!(
                    "missing item at position {i} for idx {n} on the loop {loop_idx}"
                ));
            }
            if ps.item() != items[i].as_slice() {
                return Err(format!(
                    "unexpected item found at position {i} for idx {n} out of {} items; loop {loop_idx}",
                    items.len()
                ));
            }
        }
        if ps.next_item() {
            return Err(format!(
                "unexpected item found past the end of all the items for idx {n} out of {} items; loop {loop_idx}",
                items.len()
            ));
        }
        if let Some(err) = ps.error() {
            return Err(format!("unexpected error on loop {loop_idx}: {err}"));
        }
    }

    // Search for sorted items
    for (i, item) in items.iter().enumerate() {
        ps.seek(item);
        if !ps.next_item() {
            return Err(format!("cannot find items[{i}]={item:X?}"));
        }
        if ps.item() != item.as_slice() {
            return Err(format!("unexpected item found at position {i}"));
        }
        if let Some(err) = ps.error() {
            return Err(format!(
                "unexpected error when searching for items[{i}]: {err}"
            ));
        }
    }

    // Search for reversely sorted items
    for i in 0..items.len() {
        let item = &items[items.len() - i - 1];
        ps.seek(item);
        if !ps.next_item() {
            return Err(format!("cannot find items[{i}]={item:X?}"));
        }
        if ps.item() != item.as_slice() {
            return Err(format!("unexpected item found at position {i}"));
        }
        if let Some(err) = ps.error() {
            return Err(format!(
                "unexpected error when searching for items[{i}]: {err}"
            ));
        }
    }

    Ok(())
}

/// Port of `TestPartSearch` (serial + concurrent).
#[test]
fn test_part_search() {
    let mut r = TestRng::new(1);
    let (p, items) = new_test_part(&mut r, 10, 4000).expect("cannot create test part");

    test_part_search_serial(&mut r, &p, &items)
        .unwrap_or_else(|err| panic!("error in serial part search test: {err}"));

    let p = &p;
    let items = &items;
    std::thread::scope(|s| {
        for i in 0..5 {
            s.spawn(move || {
                let mut r = TestRng::new(i as u64);
                test_part_search_serial(&mut r, p, items)
                    .unwrap_or_else(|err| panic!("error in concurrent part search test: {err}"));
            });
        }
    });
}

// ---------------------------------------------------------------------------
// table_test.go

/// Port of `TestTableOpenClose`.
#[test]
fn test_table_open_close() {
    let path = test_dir("TableOpenClose");
    let path_str = path.to_str().unwrap();

    // Create a new table
    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);

    // Close it
    tb.must_close();
    drop(tb);

    // Re-open created table multiple times.
    for _ in 0..4 {
        let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
        tb.must_close();
    }

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `TestTableAddItemsTooLongItem`.
#[test]
fn test_table_add_items_too_long_item() {
    let path = test_dir("TableAddItemsTooLongItem");
    let path_str = path.to_str().unwrap();

    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
    tb.add_items(&[vec![0u8; MAX_INMEMORY_BLOCK_SIZE + 1]]);
    tb.must_close();
    esl_common::fs::must_remove_dir(&path);
}

fn test_add_items_serial(r: &mut TestRng, tb: &Arc<super::table::Table>, items_count: usize) {
    for _ in 0..items_count {
        let item = r.random_bytes();
        tb.add_items(&[item]);
    }
}

/// Port of `testReopenTable`.
fn test_reopen_table(path: &str, items_count: usize) {
    for _ in 0..10 {
        let tb = must_open_table(path, Duration::ZERO, None, Duration::ZERO, None);
        let mut m = TableMetrics::default();
        tb.update_metrics(&mut m);
        assert_eq!(
            m.total_items_count(),
            items_count as u64,
            "unexpected itemsCount after re-opening"
        );
        tb.must_close();
    }
}

/// Port of `TestTableAddItemsSerial`.
#[test]
fn test_table_add_items_serial() {
    let mut r = TestRng::new(1);
    let path = test_dir("TableAddItemsSerial");
    let path_str = path.to_str().unwrap();

    let flushes = Arc::new(AtomicU64::new(0));
    let flushes2 = Arc::clone(&flushes);
    let flush_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        flushes2.fetch_add(1, Ordering::SeqCst);
    });
    let tb = must_open_table(
        path_str,
        Duration::ZERO,
        Some(flush_callback),
        Duration::ZERO,
        None,
    );

    const ITEMS_COUNT: usize = 10_000;
    test_add_items_serial(&mut r, &tb, ITEMS_COUNT);

    // Verify items count after pending items flush.
    tb.debug_flush();
    assert_ne!(flushes.load(Ordering::SeqCst), 0, "unexpected zero flushes");

    let mut m = TableMetrics::default();
    tb.update_metrics(&mut m);
    assert_eq!(
        m.total_items_count(),
        ITEMS_COUNT as u64,
        "unexpected itemsCount"
    );

    tb.must_close();
    drop(tb);

    // Re-open the table and make sure itemsCount remains the same.
    test_reopen_table(path_str, ITEMS_COUNT);

    // Add more items in order to verify merge between inmemory parts and
    // file-based parts.
    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
    const MORE_ITEMS_COUNT: usize = ITEMS_COUNT * 3;
    test_add_items_serial(&mut r, &tb, MORE_ITEMS_COUNT);
    tb.must_close();
    drop(tb);

    // Re-open the table and verify itemsCount again.
    test_reopen_table(path_str, ITEMS_COUNT + MORE_ITEMS_COUNT);

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `TestTableCreateSnapshotAt`.
#[test]
fn test_table_create_snapshot_at() {
    let path = test_dir("TableCreateSnapshotAt");
    let path_str = path.to_str().unwrap();

    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);

    // Write a lot of items into the table, so background merges would start.
    const ITEMS_COUNT: usize = 300_000;
    for i in 0..ITEMS_COUNT {
        let item = format!("item {i}").into_bytes();
        tb.add_items(&[item]);
    }

    // Close and open the table in order to flush all the data to disk before
    // creating snapshots.
    tb.must_close();
    drop(tb);
    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);

    // Create multiple snapshots.
    let snapshot1 = format!("{path_str}-test-snapshot1");
    tb.must_create_snapshot_at(&snapshot1);

    let snapshot2 = format!("{path_str}-test-snapshot2");
    tb.must_create_snapshot_at(&snapshot2);

    // Verify snapshots contain all the data.
    let tb1 = must_open_table(&snapshot1, Duration::ZERO, None, Duration::ZERO, None);
    let tb2 = must_open_table(&snapshot2, Duration::ZERO, None, Duration::ZERO, None);

    let mut ts = TableSearch::default();
    let mut ts1 = TableSearch::default();
    let mut ts2 = TableSearch::default();
    ts.init(&tb, false);
    ts1.init(&tb1, false);
    ts2.init(&tb2, false);
    for i in 0..ITEMS_COUNT {
        let key = format!("item {i}").into_bytes();
        if let Err(SearchError::Eof | SearchError::Other(_)) = ts.first_item_with_prefix(&key) {
            panic!("cannot find item[{i}]={key:?} in the original table");
        }
        assert_eq!(
            ts.item(),
            &key[..],
            "unexpected item found in the original table"
        );
        if let Err(SearchError::Eof | SearchError::Other(_)) = ts1.first_item_with_prefix(&key) {
            panic!("cannot find item[{i}]={key:?} in snapshot1");
        }
        assert_eq!(ts1.item(), &key[..], "unexpected item found in snapshot1");
        if let Err(SearchError::Eof | SearchError::Other(_)) = ts2.first_item_with_prefix(&key) {
            panic!("cannot find item[{i}]={key:?} in snapshot2");
        }
        assert_eq!(ts2.item(), &key[..], "unexpected item found in snapshot2");
    }
    ts.must_close();
    ts1.must_close();
    ts2.must_close();

    // Close and remove tables.
    tb2.must_close();
    tb1.must_close();
    tb.must_close();

    esl_common::fs::must_remove_dir(&snapshot2);
    esl_common::fs::must_remove_dir(&snapshot1);
    esl_common::fs::must_remove_dir(&path);
}

/// Port of `TestTableAddItemsConcurrentStress`.
#[test]
fn test_table_add_items_concurrent_stress() {
    let path = test_dir("TableAddItemsConcurrentStress");
    let path_str = path.to_str().unwrap();

    const RAW_ITEMS_SHARDS_PER_TABLE: usize = 10;
    const MAX_BLOCKS_PER_SHARD: usize = 3;

    let flushes = Arc::new(AtomicU64::new(0));
    let flushes2 = Arc::clone(&flushes);
    let flush_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        flushes2.fetch_add(1, Ordering::SeqCst);
    });
    fn prepare_block(data: Vec<u8>, items: Vec<Item>) -> (Vec<u8>, Vec<Item>) {
        (data, items)
    }

    let blocks_needed = RAW_ITEMS_SHARDS_PER_TABLE * MAX_BLOCKS_PER_SHARD * 10;
    let tb = must_open_table_ex(
        path_str,
        Duration::ZERO,
        Some(flush_callback),
        Duration::ZERO,
        Some(prepare_block),
        RAW_ITEMS_SHARDS_PER_TABLE,
        MAX_BLOCKS_PER_SHARD,
    );

    let mut items_batch: Vec<Vec<u8>> = Vec::new();
    for j in 0..blocks_needed {
        items_batch.push(vec![j as u8; MAX_INMEMORY_BLOCK_SIZE - 10]);
    }
    tb.add_items(&items_batch);

    // Verify items count after pending items flush.
    tb.debug_flush();
    assert_ne!(flushes.load(Ordering::SeqCst), 0, "unexpected zero flushes");

    let mut m = TableMetrics::default();
    tb.update_metrics(&mut m);
    assert_eq!(
        m.total_items_count(),
        blocks_needed as u64,
        "unexpected itemsCount"
    );

    tb.must_close();
    drop(tb);

    // Re-open the table and make sure itemsCount remains the same.
    test_reopen_table(path_str, blocks_needed);

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `TestTableAddItemsConcurrent`.
#[test]
fn test_table_add_items_concurrent() {
    let path = test_dir("TableAddItemsConcurrent");
    let path_str = path.to_str().unwrap();

    let flushes = Arc::new(AtomicU64::new(0));
    let flushes2 = Arc::clone(&flushes);
    let flush_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        flushes2.fetch_add(1, Ordering::SeqCst);
    });
    fn prepare_block(data: Vec<u8>, items: Vec<Item>) -> (Vec<u8>, Vec<Item>) {
        (data, items)
    }
    let tb = must_open_table(
        path_str,
        Duration::ZERO,
        Some(flush_callback),
        Duration::ZERO,
        Some(prepare_block),
    );

    const ITEMS_COUNT: usize = 10_000;
    test_add_items_concurrent(&tb, ITEMS_COUNT);

    // Verify items count after pending items flush.
    tb.debug_flush();
    assert_ne!(flushes.load(Ordering::SeqCst), 0, "unexpected zero flushes");

    let mut m = TableMetrics::default();
    tb.update_metrics(&mut m);
    assert_eq!(
        m.total_items_count(),
        ITEMS_COUNT as u64,
        "unexpected itemsCount"
    );

    tb.must_close();
    drop(tb);

    // Re-open the table and make sure itemsCount remains the same.
    test_reopen_table(path_str, ITEMS_COUNT);

    // Add more items in order to verify merge between inmemory parts and
    // file-based parts.
    let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
    const MORE_ITEMS_COUNT: usize = ITEMS_COUNT * 3;
    test_add_items_concurrent(&tb, MORE_ITEMS_COUNT);
    tb.must_close();
    drop(tb);

    // Re-open the table and verify itemsCount again.
    test_reopen_table(path_str, ITEMS_COUNT + MORE_ITEMS_COUNT);

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `testAddItemsConcurrent`.
fn test_add_items_concurrent(tb: &Arc<super::table::Table>, items_count: usize) {
    const GOROUTINES_COUNT: usize = 6;
    let next = AtomicU64::new(0);
    let next = &next;
    std::thread::scope(|s| {
        for n in 0..GOROUTINES_COUNT {
            s.spawn(move || {
                let mut r = TestRng::new(n as u64);
                loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= items_count as u64 {
                        return;
                    }
                    let item = r.random_bytes();
                    tb.add_items(&[item]);
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------
// table_search_test.go

/// Port of `newTestTable`.
fn new_test_table(
    r: &mut TestRng,
    path: &str,
    items_count: usize,
) -> (Arc<super::table::Table>, Vec<Vec<u8>>) {
    let flushes = Arc::new(AtomicU64::new(0));
    let flushes2 = Arc::clone(&flushes);
    let flush_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        flushes2.fetch_add(1, Ordering::SeqCst);
    });
    let tb = must_open_table(
        path,
        Duration::ZERO,
        Some(flush_callback),
        Duration::ZERO,
        None,
    );
    let mut items = Vec::with_capacity(items_count);
    for i in 0..items_count {
        let item = format!("{}:{i}", r.intn(1_000_000_000)).into_bytes();
        tb.add_items(std::slice::from_ref(&item));
        items.push(item);
    }
    tb.debug_flush();
    if items_count > 0 {
        assert_ne!(
            flushes.load(Ordering::SeqCst),
            0,
            "unexpected zero flushes for itemsCount={items_count}"
        );
    }

    items.sort();
    (tb, items)
}

/// Port of `testTableSearchSerial`.
fn test_table_search_serial(
    tb: &Arc<super::table::Table>,
    items: &[Vec<u8>],
) -> Result<(), String> {
    let mut ts = TableSearch::default();
    ts.init(tb, false);
    let keys: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"123".to_vec(),
        b"9".to_vec(),
        b"892".to_vec(),
        b"2384329".to_vec(),
        b"fdsjflfdf".to_vec(),
        items[0].clone(),
        items[items.len() - 1].clone(),
        items[items.len() / 2].clone(),
    ];
    for key in keys {
        let mut n = items.partition_point(|item| item.as_slice() < key.as_slice());
        ts.seek(&key);
        while n < items.len() {
            let item = &items[n];
            if !ts.next_item() {
                return Err(format!(
                    "missing item {item:X?} at position {n} when searching for {key:X?}"
                ));
            }
            if ts.item() != item.as_slice() {
                return Err(format!(
                    "unexpected item found at position {n} when searching for {key:X?}; got {:X?}; want {item:X?}",
                    ts.item()
                ));
            }
            n += 1;
        }
        if ts.next_item() {
            return Err(format!(
                "superfluous item found at position {n} when searching for {key:X?}: {:X?}",
                ts.item()
            ));
        }
        if let Some(err) = ts.error() {
            return Err(format!(
                "unexpected error when searching for {key:X?}: {err}"
            ));
        }
    }
    ts.must_close();
    Ok(())
}

/// Port of `TestTableSearchSerial`.
#[test]
fn test_table_search_serial_and_reopen() {
    let path = test_dir("TableSearchSerial");
    let path_str = path.to_str().unwrap();

    const ITEMS_COUNT: usize = 100_000;

    let items = {
        let mut r = TestRng::new(1);
        let (tb, items) = new_test_table(&mut r, path_str, ITEMS_COUNT);
        test_table_search_serial(&tb, &items)
            .unwrap_or_else(|err| panic!("unexpected error: {err}"));
        tb.must_close();
        items
    };

    {
        // Re-open the table and verify the search works.
        let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
        test_table_search_serial(&tb, &items)
            .unwrap_or_else(|err| panic!("unexpected error: {err}"));
        tb.must_close();
    }

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `TestTableSearchConcurrent`.
#[test]
fn test_table_search_concurrent() {
    let path = test_dir("TableSearchConcurrent");
    let path_str = path.to_str().unwrap();

    const ITEMS_COUNT: usize = 100_000;
    let items = {
        let mut r = TestRng::new(2);
        let (tb, items) = new_test_table(&mut r, path_str, ITEMS_COUNT);
        run_table_search_concurrent(&tb, &items);
        tb.must_close();
        items
    };

    // Re-open the table and verify the search works.
    {
        let tb = must_open_table(path_str, Duration::ZERO, None, Duration::ZERO, None);
        run_table_search_concurrent(&tb, &items);
        tb.must_close();
    }

    esl_common::fs::must_remove_dir(&path);
}

/// Port of `testTableSearchConcurrent`.
fn run_table_search_concurrent(tb: &Arc<super::table::Table>, items: &[Vec<u8>]) {
    const GOROUTINES: usize = 5;
    std::thread::scope(|s| {
        for _ in 0..GOROUTINES {
            s.spawn(move || {
                test_table_search_serial(tb, items)
                    .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            });
        }
    });
}
