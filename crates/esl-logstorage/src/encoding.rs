//! Port of EsLogs `lib/logstorage/encoding.go`.
//!
//! This is part of the on-disk block format: every marshaled byte produced
//! here must be identical to the bytes produced by the Go implementation,
//! with one exception - zstd-compressed payloads (see the PORT NOTE at
//! [`marshal_bytes_block`]).
//!
//! PORT NOTE: Go represents the block strings as `[]string`; since column
//! values may contain arbitrary (non-UTF-8) encoded bytes, the port uses
//! `&[u8]`/`Vec<u8>` byte strings instead.

use std::sync::Mutex;

use esl_common::bytesutil::ByteBufferPool;
use esl_common::{encoding, slicesutil};

/// Marshals a and appends the result to dst.
///
/// The marshaled strings block can be unmarshaled with [`StringsBlockUnmarshaler`].
pub fn marshal_strings_block<S: AsRef<[u8]>>(dst: &mut Vec<u8>, a: &[S]) {
    // Encode string lengths
    let mut u64s = encoding::get_uint64s(a.len());
    let mut total_len = 0;
    for (i, s) in a.iter().enumerate() {
        let s = s.as_ref();
        u64s.a[i] = s.len() as u64;
        total_len += s.len();
    }
    marshal_uint64_block(dst, &u64s.a);
    encoding::put_uint64s(u64s);

    // Encode strings
    if are_const_values(a) {
        // Special case for const values
        marshal_bytes_block(dst, a[0].as_ref());
    } else {
        // Regular case for non-const values
        let mut bb = BB_POOL.get();

        // Pre-allocate the needed memory in order to reduce the number of
        // reallocations in the loop below.
        bb.b.clear();
        bb.b.reserve(total_len);

        for s in a {
            bb.b.extend_from_slice(s.as_ref());
        }
        marshal_bytes_block(dst, &bb.b);

        BB_POOL.put(bb);
    }
}

/// StringsBlockUnmarshaler is used for unmarshaling the block returned from
/// [`marshal_strings_block`].
///
/// Use [`get_strings_block_unmarshaler`] for obtaining the unmarshaler from
/// the pool in order to save memory allocations.
///
/// PORT NOTE: Go returns unsafe string views into `sbu.data`, valid until
/// `reset()`; the port returns owned byte strings instead, while `data` is
/// kept as the reusable decompression staging buffer.
#[derive(Default)]
pub struct StringsBlockUnmarshaler {
    /// data contains the data for the unmarshaled values.
    data: Vec<u8>,
}

impl StringsBlockUnmarshaler {
    /// Resets the unmarshaler.
    pub fn reset(&mut self) {
        self.data.clear();
    }

    /// Copies s and returns the copy.
    ///
    /// PORT NOTE: Go copies s into `sbu.data` and returns an unsafe view tied
    /// to the unmarshaler lifetime; the port returns an owned String.
    /// Go's `appendFields` helper is not ported yet, since it needs `Field`
    /// from rows.go whose module is still a stub; add it with the rows port.
    pub fn copy_string(&mut self, s: &str) -> String {
        s.to_string()
    }

    /// Unmarshals items_count strings from src and appends them to dst.
    pub fn unmarshal(
        &mut self,
        dst: &mut Vec<Vec<u8>>,
        src: &[u8],
        items_count: u64,
    ) -> Result<(), String> {
        let mut u64s = encoding::get_uint64s(0);
        let res = self.unmarshal_internal(dst, src, items_count, &mut u64s.a);
        encoding::put_uint64s(u64s);
        res
    }

    fn unmarshal_internal(
        &mut self,
        dst: &mut Vec<Vec<u8>>,
        src: &[u8],
        items_count: u64,
        a_lens: &mut Vec<u64>,
    ) -> Result<(), String> {
        // Decode string lengths
        a_lens.clear();
        let src = unmarshal_uint64_block(a_lens, src, items_count)
            .map_err(|err| format!("cannot unmarshal string lengths: {err}"))?;

        // Read bytes block into self.data
        let data_len = self.data.len();
        let tail = unmarshal_bytes_block(&mut self.data, src)
            .map_err(|err| format!("cannot unmarshal bytes block with strings: {err}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected non-empty tail after reading bytes block with strings; len(tail)={}",
                tail.len()
            ));
        }

        // Decode strings from self.data into dst
        let mut data: &[u8] = &self.data[data_len..];

        if a_lens.len() >= 2 && are_const_uint64s(a_lens) && data.len() as u64 == a_lens[0] {
            // Special case - decode a constant string
            for _ in 0..a_lens.len() {
                dst.push(data.to_vec());
            }
            return Ok(());
        }

        for &s_len in a_lens.iter() {
            if (data.len() as u64) < s_len {
                return Err(format!(
                    "cannot unmarshal a string with the length {s_len} bytes from {} bytes",
                    data.len()
                ));
            }
            let s_len = s_len as usize;
            dst.push(data[..s_len].to_vec());
            data = &data[s_len..];
        }

        Ok(())
    }
}

fn are_const_uint64s(a: &[u64]) -> bool {
    if a.is_empty() {
        return false;
    }
    let v = a[0];
    a[1..].iter().all(|&x| x == v)
}

/// Returns true when all the values in a are equal.
///
/// PORT NOTE: Go defines areConstValues in pipe_stats.go; it is inlined here
/// (private) since marshal_strings_block depends on it while pipe_stats is
/// far from being ported.
fn are_const_values<S: AsRef<[u8]>>(values: &[S]) -> bool {
    if values.is_empty() {
        return false;
    }
    let v = values[0].as_ref();
    values[1..].iter().all(|x| x.as_ref() == v)
}

/// Appends marshaled a to dst.
pub fn marshal_uint64_block(dst: &mut Vec<u8>, a: &[u64]) {
    let mut bb = BB_POOL.get();
    bb.b.clear();
    marshal_uint64_items(&mut bb.b, a);
    marshal_bytes_block(dst, &bb.b);
    BB_POOL.put(bb);
}

/// Appends items_count uint64 items unmarshaled from src to dst and returns
/// the remaining tail from src.
pub fn unmarshal_uint64_block<'a>(
    dst: &mut Vec<u64>,
    src: &'a [u8],
    items_count: u64,
) -> Result<&'a [u8], String> {
    let mut bb = BB_POOL.get();
    bb.b.clear();

    // Unmarshal the underlying bytes block
    let res = match unmarshal_bytes_block(&mut bb.b, src) {
        Ok(src) => {
            // Unmarshal the items from bb.
            match unmarshal_uint64_items(dst, &bb.b, items_count) {
                Ok(()) => Ok(src),
                Err(err) => Err(format!(
                    "cannot unmarshal {items_count} uint64 items from bytes block of length {} bytes: {err}",
                    bb.b.len()
                )),
            }
        }
        Err(err) => Err(format!("cannot unmarshal bytes block: {err}")),
    };
    BB_POOL.put(bb);
    res
}

const UINT_BLOCK_TYPE8: u8 = 0;
const UINT_BLOCK_TYPE16: u8 = 1;
const UINT_BLOCK_TYPE32: u8 = 2;
const UINT_BLOCK_TYPE64: u8 = 3;

const UINT_BLOCK_TYPE_CONST8: u8 = 4;
const UINT_BLOCK_TYPE_CONST16: u8 = 5;
const UINT_BLOCK_TYPE_CONST32: u8 = 6;
const UINT_BLOCK_TYPE_CONST64: u8 = 7;

/// Appends the marshaled a items to dst.
pub fn marshal_uint64_items(dst: &mut Vec<u8>, a: &[u64]) {
    // Do not marshal a.len(), since it is expected that unmarshaler knows it.

    let n_max = a.iter().copied().max().unwrap_or(0);
    let are_consts = a.len() >= 2 && are_const_uint64s(a);
    if n_max < (1 << 8) {
        if are_consts {
            dst.push(UINT_BLOCK_TYPE_CONST8);
            dst.push(a[0] as u8);
        } else {
            dst.push(UINT_BLOCK_TYPE8);
            for &n in a {
                dst.push(n as u8);
            }
        }
    } else if n_max < (1 << 16) {
        if are_consts {
            dst.push(UINT_BLOCK_TYPE_CONST16);
            encoding::marshal_uint16(dst, a[0] as u16);
        } else {
            dst.push(UINT_BLOCK_TYPE16);
            for &n in a {
                encoding::marshal_uint16(dst, n as u16);
            }
        }
    } else if n_max < (1 << 32) {
        if are_consts {
            dst.push(UINT_BLOCK_TYPE_CONST32);
            encoding::marshal_uint32(dst, a[0] as u32);
        } else {
            dst.push(UINT_BLOCK_TYPE32);
            for &n in a {
                encoding::marshal_uint32(dst, n as u32);
            }
        }
    } else if are_consts {
        dst.push(UINT_BLOCK_TYPE_CONST64);
        encoding::marshal_uint64(dst, a[0]);
    } else {
        dst.push(UINT_BLOCK_TYPE64);
        for &n in a {
            encoding::marshal_uint64(dst, n);
        }
    }
}

/// Appends items_count uint64 items unmarshaled from src to dst.
///
/// PORT NOTE: in Go, the uint16/uint32/uint64/const branches index `dst` from
/// the start of the whole destination slice rather than from the appended
/// part (only the uint8 branch uses `dstA`); all callers pass an empty dst,
/// making both equivalent. The port replicates the Go indexing exactly.
pub fn unmarshal_uint64_items(
    dst: &mut Vec<u64>,
    src: &[u8],
    items_count: u64,
) -> Result<(), String> {
    // Unmarshal block type
    if src.is_empty() {
        return Err("cannot unmarshal uint64 block type from empty src".to_string());
    }
    let block_type = src[0];
    let src = &src[1..];

    let dst_len = dst.len() as u64 + items_count;
    if dst_len > i64::MAX as u64 {
        return Err(format!(
            "too long destination buffer: len={dst_len}; must not exceed {}",
            i64::MAX
        ));
    }
    let prev_len = dst.len();
    slicesutil::set_length(dst, dst_len as usize);
    let items_count = items_count as usize;

    match block_type {
        UINT_BLOCK_TYPE8 => {
            // A block with items smaller than 1<<8 bytes
            if src.len() != items_count {
                return Err(format!(
                    "unexpected block length for {items_count} uint8 items; got {} bytes; want {items_count} bytes",
                    src.len()
                ));
            }
            for (dst_v, &b) in dst[prev_len..].iter_mut().zip(src) {
                *dst_v = b as u64;
            }
        }
        UINT_BLOCK_TYPE16 => {
            // A block with items smaller than 1<<16 bytes
            if src.len() != 2 * items_count {
                return Err(format!(
                    "unexpected block length for {items_count} uint16 items; got {} bytes; want {} bytes",
                    src.len(),
                    2 * items_count
                ));
            }
            for (i, dst_v) in dst.iter_mut().take(items_count).enumerate() {
                let idx = 2 * i;
                *dst_v = encoding::unmarshal_uint16(&src[idx..idx + 2]) as u64;
            }
        }
        UINT_BLOCK_TYPE32 => {
            // A block with items smaller than 1<<32 bytes
            if src.len() != 4 * items_count {
                return Err(format!(
                    "unexpected block length for {items_count} uint32 items; got {} bytes; want {} bytes",
                    src.len(),
                    4 * items_count
                ));
            }
            for (i, dst_v) in dst.iter_mut().take(items_count).enumerate() {
                let idx = 4 * i;
                *dst_v = encoding::unmarshal_uint32(&src[idx..idx + 4]) as u64;
            }
        }
        UINT_BLOCK_TYPE64 => {
            // A block with items smaller than 1<<64 bytes
            if src.len() != 8 * items_count {
                return Err(format!(
                    "unexpected block length for {items_count} uint64 items; got {} bytes; want {} bytes",
                    src.len(),
                    8 * items_count
                ));
            }
            for (i, dst_v) in dst.iter_mut().take(items_count).enumerate() {
                let idx = 8 * i;
                *dst_v = encoding::unmarshal_uint64(&src[idx..idx + 8]);
            }
        }
        UINT_BLOCK_TYPE_CONST8 => {
            if src.len() != 1 {
                return Err(format!(
                    "unexpected block length for const uint8 item; got {} bytes; want 1 byte",
                    src.len()
                ));
            }
            let v = src[0] as u64;
            for dst_v in dst.iter_mut().take(items_count) {
                *dst_v = v;
            }
        }
        UINT_BLOCK_TYPE_CONST16 => {
            if src.len() != 2 {
                return Err(format!(
                    "unexpected block length for const uint16 item; got {} bytes; want 2 bytes",
                    src.len()
                ));
            }
            let v = encoding::unmarshal_uint16(src) as u64;
            for dst_v in dst.iter_mut().take(items_count) {
                *dst_v = v;
            }
        }
        UINT_BLOCK_TYPE_CONST32 => {
            if src.len() != 4 {
                return Err(format!(
                    "unexpected block length for const uint32 item; got {} bytes; want 4 bytes",
                    src.len()
                ));
            }
            let v = encoding::unmarshal_uint32(src) as u64;
            for dst_v in dst.iter_mut().take(items_count) {
                *dst_v = v;
            }
        }
        UINT_BLOCK_TYPE_CONST64 => {
            if src.len() != 8 {
                return Err(format!(
                    "unexpected block length for const uint64 item; got {} bytes; want 8 bytes",
                    src.len()
                ));
            }
            let v = encoding::unmarshal_uint64(src);
            for dst_v in dst.iter_mut().take(items_count) {
                *dst_v = v;
            }
        }
        _ => return Err(format!("unexpected uint64 block type: {block_type}")),
    }
    Ok(())
}

const MARSHAL_BYTES_TYPE_PLAIN: u8 = 0;
const MARSHAL_BYTES_TYPE_ZSTD: u8 = 1;

/// Appends the marshaled src bytes block to dst.
///
/// PORT NOTE: the block *format* is byte-identical to Go, but the
/// zstd-compressed payload bytes may differ from Go's (gozstd vs the Rust
/// zstd bindings); any conforming zstd stream is valid here and both sides
/// can decode each other's output.
pub fn marshal_bytes_block(dst: &mut Vec<u8>, src: &[u8]) {
    if src.len() < 128 {
        // Marshal the block in plain without compression
        dst.push(MARSHAL_BYTES_TYPE_PLAIN);
        dst.push(src.len() as u8);
        dst.extend_from_slice(src);
        return;
    }

    // Compress the block
    dst.push(MARSHAL_BYTES_TYPE_ZSTD);
    let compress_level = get_compress_level(src.len());
    let mut bb = BB_POOL.get();
    bb.b.clear();
    encoding::compress_zstd_level(&mut bb.b, src, compress_level);
    encoding::marshal_var_uint64(dst, bb.b.len() as u64);
    dst.extend_from_slice(&bb.b);
    BB_POOL.put(bb);
}

fn get_compress_level(data_len: usize) -> i32 {
    if data_len <= 512 {
        return 1;
    }
    if data_len <= 4 * 1024 {
        return 2;
    }
    3
}

/// Appends the bytes block unmarshaled from src to dst and returns the
/// remaining tail from src.
pub fn unmarshal_bytes_block<'a>(dst: &mut Vec<u8>, src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.is_empty() {
        return Err("cannot unmarshal block type from empty src".to_string());
    }
    let block_type = src[0];
    let src = &src[1..];
    match block_type {
        MARSHAL_BYTES_TYPE_PLAIN => {
            // Plain block

            // Read block length
            if src.is_empty() {
                return Err("cannot unmarshal plain block size from empty src".to_string());
            }
            let block_len = src[0] as usize;
            let src = &src[1..];
            if src.len() < block_len {
                // PORT NOTE: Go formats the available length with the (erroneous)
                // `%b` verb, printing it in binary; replicated for identical
                // error messages.
                return Err(format!(
                    "cannot read plain block with the size {block_len} bytes from {:b} bytes",
                    src.len()
                ));
            }

            // Copy the block to dst
            dst.extend_from_slice(&src[..block_len]);
            Ok(&src[block_len..])
        }
        MARSHAL_BYTES_TYPE_ZSTD => {
            // Compressed block

            // Read block length
            let (block_len, n_size) = encoding::unmarshal_var_uint64(src);
            if n_size <= 0 {
                return Err("cannot unmarshal compressed block size".to_string());
            }
            let src = &src[n_size as usize..];
            if (src.len() as u64) < block_len {
                return Err(format!(
                    "cannot read compressed block with the size {block_len} bytes from {} bytes",
                    src.len()
                ));
            }
            let block_len = block_len as usize;
            let compressed_block = &src[..block_len];
            let src = &src[block_len..];

            // Decompress the block
            let mut bb = BB_POOL.get();
            bb.b.clear();
            if let Err(err) = encoding::decompress_zstd(&mut bb.b, compressed_block) {
                BB_POOL.put(bb);
                return Err(format!("cannot decompress block: {err}"));
            }

            // Copy the decompressed block to dst.
            dst.extend_from_slice(&bb.b);
            BB_POOL.put(bb);
            Ok(src)
        }
        _ => Err(format!(
            "unexpected block type: {block_type}; supported types: 0, 1"
        )),
    }
}

static BB_POOL: ByteBufferPool = ByteBufferPool::new();

/// Returns a StringsBlockUnmarshaler from the pool.
///
/// Return back the StringsBlockUnmarshaler to the pool by calling
/// [`put_strings_block_unmarshaler`].
pub fn get_strings_block_unmarshaler() -> StringsBlockUnmarshaler {
    SBU_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns back sbu to the pool.
pub fn put_strings_block_unmarshaler(mut sbu: StringsBlockUnmarshaler) {
    sbu.reset();
    SBU_POOL.lock().unwrap().push(sbu);
}

// PORT NOTE: Go uses `sync.Pool` with `*stringsBlockUnmarshaler`; the port
// uses a `Mutex<Vec<..>>` pool handing unmarshalers out by value, preserving
// the buffer reuse pattern.
static SBU_POOL: Mutex<Vec<StringsBlockUnmarshaler>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;

    fn is_almost_equal(a: usize, b: usize) -> bool {
        let fa = a as f64;
        let fb = b as f64;
        (fa - fb).abs() <= (fa + fb).abs() * 0.17
    }

    #[test]
    fn test_marshal_unmarshal_strings_block() {
        fn f(logs: &str, block_len_expected: usize) {
            let a: Vec<&str> = if logs.is_empty() {
                Vec::new()
            } else {
                logs.split('\n').collect()
            };
            let mut data = Vec::new();
            marshal_strings_block(&mut data, &a);
            assert!(
                is_almost_equal(data.len(), block_len_expected),
                "unexpected block length; got {}; want {block_len_expected}; block={data:?}",
                data.len()
            );
            let mut sbu = get_strings_block_unmarshaler();
            let mut values = Vec::new();
            sbu.unmarshal(&mut values, &data, a.len() as u64)
                .expect("cannot unmarshal strings block");
            let values_str: Vec<&str> = values
                .iter()
                .map(|v| std::str::from_utf8(v).unwrap())
                .collect();
            assert_eq!(
                values_str, a,
                "unexpected strings after unmarshaling;\ngot\n{values_str:?}\nwant\n{a:?}"
            );
            put_strings_block_unmarshaler(sbu);
        }

        // an empty string
        f("", 5);

        // a single string
        f("foo", 9);

        // Strings with identical length
        f("foo\nbar\nbaz", 18);

        // Long strings
        f(
            r#"
Apr 28 13:39:06 localhost systemd[1]: Started Network Manager Script Dispatcher Service.
Apr 28 13:39:06 localhost nm-dispatcher: req:1 'connectivity-change': new request (2 scripts)
Apr 28 13:39:06 localhost nm-dispatcher: req:1 'connectivity-change': start running ordered scripts...
Apr 28 13:40:05 localhost kernel: [35544.823503] wlp4s0: AP c8:ea:f8:00:6a:31 changed bandwidth, new config is 2437 MHz, width 1 (2437/0 MHz)
Apr 28 13:40:15 localhost kernel: [35554.295612] wlp4s0: AP c8:ea:f8:00:6a:31 changed bandwidth, new config is 2437 MHz, width 2 (2447/0 MHz)
Apr 28 13:43:37 localhost NetworkManager[1516]: <info>  [1651142617.3668] manager: NetworkManager state is now CONNECTED_GLOBAL
Apr 28 13:43:37 localhost dbus-daemon[1475]: [system] Activating via systemd: service name='org.freedesktop.nm_dispatcher' unit='dbus-org.freedesktop.nm-dispatcher.service' requested by ':1.13' (uid=0 pid=1516 comm="/usr/sbin/NetworkManager --no-daemon " label="unconfined")
Apr 28 13:43:37 localhost systemd[1]: Starting Network Manager Script Dispatcher Service...
Apr 28 13:43:37 localhost whoopsie[2812]: [13:43:37] The default IPv4 route is: /org/freedesktop/NetworkManager/ActiveConnection/10
Apr 28 13:43:37 localhost whoopsie[2812]: [13:43:37] Not a paid data plan: /org/freedesktop/NetworkManager/ActiveConnection/10
Apr 28 13:43:37 localhost whoopsie[2812]: [13:43:37] Found usable connection: /org/freedesktop/NetworkManager/ActiveConnection/10
Apr 28 13:43:37 localhost dbus-daemon[1475]: [system] Successfully activated service 'org.freedesktop.nm_dispatcher'
Apr 28 13:43:37 localhost systemd[1]: Started Network Manager Script Dispatcher Service.
Apr 28 13:43:37 localhost nm-dispatcher: req:1 'connectivity-change': new request (2 scripts)
Apr 28 13:43:37 localhost nm-dispatcher: req:1 'connectivity-change': start running ordered scripts...
Apr 28 13:43:38 localhost whoopsie[2812]: [13:43:38] online
Apr 28 13:45:01 localhost CRON[12181]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)
Apr 28 13:48:01 localhost kernel: [36020.497806] CPU0: Core temperature above threshold, cpu clock throttled (total events = 22034)
Apr 28 13:48:01 localhost kernel: [36020.497807] CPU2: Core temperature above threshold, cpu clock throttled (total events = 22034)
Apr 28 13:48:01 localhost kernel: [36020.497809] CPU1: Package temperature above threshold, cpu clock throttled (total events = 27400)
Apr 28 13:48:01 localhost kernel: [36020.497810] CPU3: Package temperature above threshold, cpu clock throttled (total events = 27400)
Apr 28 13:48:01 localhost kernel: [36020.497810] CPU2: Package temperature above threshold, cpu clock throttled (total events = 27400)
Apr 28 13:48:01 localhost kernel: [36020.497812] CPU0: Package temperature above threshold, cpu clock throttled (total events = 27400)
Apr 28 13:48:01 localhost kernel: [36020.499855] CPU2: Core temperature/speed normal
Apr 28 13:48:01 localhost kernel: [36020.499855] CPU0: Core temperature/speed normal
Apr 28 13:48:01 localhost kernel: [36020.499856] CPU1: Package temperature/speed normal
Apr 28 13:48:01 localhost kernel: [36020.499857] CPU3: Package temperature/speed normal
Apr 28 13:48:01 localhost kernel: [36020.499858] CPU0: Package temperature/speed normal
Apr 28 13:48:01 localhost kernel: [36020.499859] CPU2: Package temperature/speed normal
"#,
            951,
        );

        // const strings
        f("foo\nfoo", 10);
        f("foo\nfoo\nfoo", 9);

        // Generate a string longer than 1<<16 bytes
        let mut s = "foo".to_string();
        while s.len() < (1 << 16) {
            let s2 = s.clone();
            s.push_str(&s2);
        }
        s.push('\n');
        let mut lines = s.clone();
        f(&lines, 36);
        lines.push_str(&s);
        f(&lines, 52);

        // Generate more than 256 strings
        let mut lines = String::new();
        for i in 0..1000 {
            lines.push_str(&format!("line {i}\n"));
        }
        f(&lines, 766);
    }

    #[test]
    fn test_marshal_unmarshal_uint64_block() {
        fn f(a: &[u64]) {
            let mut data = Vec::new();
            marshal_uint64_block(&mut data, a);

            let mut result = Vec::new();
            let tail = unmarshal_uint64_block(&mut result, &data, a.len() as u64)
                .expect("unexpected error");
            assert!(
                tail.is_empty(),
                "unexpected non-nil tail with len={}: {tail:X?}",
                tail.len()
            );
            assert_eq!(a, result, "unexpected result\ngot\n{result:?}\nwant\n{a:?}");
        }

        // empty block
        f(&[]);

        // uint8
        f(&[1]);
        f(&[1, 1, 1]);
        f(&[1, 2, 3]);

        // uint16
        f(&[1234]);
        f(&[1234, 1234, 1234]);
        f(&[1234, 34, 234]);

        // uint32
        f(&[1234 << 16]);
        f(&[1234 << 16, 1234 << 16, 1234 << 16]);
        f(&[1234 << 16, (1234 << 16) + 1, (1234 << 16) + 2]);

        // uint64
        f(&[1234 << 32]);
        f(&[1234 << 32, 1234 << 32, 1234 << 32]);
        f(&[1234 << 32, (1234 << 32) + 1, (1234 << 32) + 2]);
    }
}
