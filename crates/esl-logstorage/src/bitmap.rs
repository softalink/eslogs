//! Port of EsLogs `lib/logstorage/bitmap.go`.

use std::sync::Mutex;

use esl_common::panicf;
use esl_common::slicesutil;

/// Returns a bitmap with the given bits length from the pool.
///
/// PORT NOTE: Go uses `sync.Pool` and returns `*bitmap`; the port uses a
/// `Mutex<Vec<Bitmap>>` pool and hands bitmaps out by value.
pub fn get_bitmap(bits_len: usize) -> Bitmap {
    let mut bm = BITMAP_POOL.lock().unwrap().pop().unwrap_or_default();
    bm.resize_no_init(bits_len);
    bm
}

/// Returns bm to the pool.
pub fn put_bitmap(mut bm: Bitmap) {
    bm.reset();
    BITMAP_POOL.lock().unwrap().push(bm);
}

static BITMAP_POOL: Mutex<Vec<Bitmap>> = Mutex::new(Vec::new());

/// Bitmap holds `bits_len` bits packed into `u64` words.
#[derive(Debug, Default)]
pub struct Bitmap {
    /// The underlying words with bits.
    pub a: Vec<u64>,
    /// The number of bits in the bitmap.
    pub bits_len: usize,
}

impl Bitmap {
    /// Resets the bitmap to an empty state.
    pub fn reset(&mut self) {
        self.reset_bits();
        self.a.clear();

        self.bits_len = 0;
    }

    /// Copies src into self.
    pub fn copy_from(&mut self, src: &Bitmap) {
        self.reset();

        self.a.extend_from_slice(&src.a);
        self.bits_len = src.bits_len;
    }

    /// Initializes the bitmap with the given bits length; all the bits are cleared.
    pub fn init(&mut self, bits_len: usize) {
        self.reset();
        self.resize_no_init(bits_len);
    }

    /// Resizes the bitmap to the given bits length.
    ///
    /// PORT NOTE: Go's `resizeNoInit` uses `slicesutil.SetLength`, which may
    /// expose scratch memory; the Rust `set_length` zero-fills grown words
    /// instead. Pool discipline (`put_bitmap` resets bits before pooling)
    /// makes the observable behavior identical.
    pub fn resize_no_init(&mut self, bits_len: usize) {
        let words_len = bits_len.div_ceil(64);
        slicesutil::set_length(&mut self.a, words_len);
        self.bits_len = bits_len;
    }

    /// Clears all the bits.
    pub fn reset_bits(&mut self) {
        self.a.fill(0);
    }

    /// Sets all the bits within `bits_len`.
    pub fn set_bits(&mut self) {
        let a = &mut self.a;
        for word in a.iter_mut() {
            *word = !0u64;
        }
        let tail_bits = self.bits_len % 64;
        if tail_bits > 0 && !a.is_empty() {
            // Zero bits outside bits_len at the last word
            let last = a.len() - 1;
            a[last] &= (1u64 << tail_bits) - 1;
        }
    }

    /// Returns true if no bits are set.
    pub fn is_zero(&self) -> bool {
        for &word in &self.a {
            if word != 0 {
                return false;
            }
        }
        true
    }

    /// Returns true if all the bits within `bits_len` are set.
    pub fn are_all_bits_set(&self) -> bool {
        let a = &self.a;
        for (i, &word) in a.iter().enumerate() {
            if word != u64::MAX {
                if i + 1 < a.len() {
                    return false;
                }
                let tail_bits = self.bits_len % 64;
                if tail_bits == 0 || word != (1u64 << tail_bits) - 1 {
                    return false;
                }
            }
        }
        true
    }

    /// Clears the bits in self, which are set in x.
    pub fn and_not(&mut self, x: &Bitmap) {
        if self.bits_len != x.bits_len {
            panicf!(
                "BUG: cannot merge bitmaps with distinct lengths; {} vs {}",
                self.bits_len,
                x.bits_len
            );
        }
        if x.is_zero() {
            return;
        }
        for (w, &b) in self.a.iter_mut().zip(x.a.iter()) {
            *w &= !b;
        }
    }

    /// Sets the bit at the given index.
    pub fn set_bit(&mut self, i: usize) {
        let word_idx = i / 64;
        let word_offset = i % 64;
        self.a[word_idx] |= 1u64 << word_offset;
    }

    /// Returns true if the bit at the given index is set.
    pub fn is_set_bit(&self, i: usize) -> bool {
        let word_idx = i / 64;
        let word_offset = i % 64;
        let word = self.a[word_idx];
        (word & (1u64 << word_offset)) != 0
    }

    /// Calls f for each set bit and clears that bit if f returns false.
    pub fn for_each_set_bit(&mut self, mut f: impl FnMut(usize) -> bool) {
        let bits_len = self.bits_len;
        for (i, word_ref) in self.a.iter_mut().enumerate() {
            let word = *word_ref;
            if word == 0 {
                continue;
            }
            let mut word_new = word;
            for j in 0..64 {
                let mask = 1u64 << j;
                if (word & mask) == 0 {
                    continue;
                }
                let idx = i * 64 + j;
                if idx >= bits_len {
                    return;
                }
                if !f(idx) {
                    word_new &= !mask;
                }
            }
            if word != word_new {
                *word_ref = word_new;
            }
        }
    }

    /// Calls f for each set bit.
    pub fn for_each_set_bit_readonly(&self, mut f: impl FnMut(usize)) {
        if self.are_all_bits_set() {
            let n = self.bits_len;
            for i in 0..n {
                f(i);
            }
            return;
        }

        let bits_len = self.bits_len;
        for (i, &word) in self.a.iter().enumerate() {
            if word == 0 {
                continue;
            }
            for j in 0..64 {
                let mask = 1u64 << j;
                if (word & mask) == 0 {
                    continue;
                }
                let idx = i * 64 + j;
                if idx >= bits_len {
                    return;
                }
                f(idx);
            }
        }
    }

    /// Returns the number of set bits.
    pub fn ones_count(&self) -> usize {
        let mut n = 0usize;
        for &word in &self.a {
            n += word.count_ones() as usize;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap() {
        for i in 0..100usize {
            let bits_len = i;

            let mut bm = get_bitmap(i);
            assert_eq!(
                bm.bits_len, i,
                "unexpected bits length: {}; want {}",
                bm.bits_len, i
            );

            assert!(
                bm.is_zero(),
                "all the bits must be zero for bitmap with {i} bits"
            );
            if i == 0 {
                assert!(
                    bm.are_all_bits_set(),
                    "areAllBitsSet() must return true for bitmap with 0 bits"
                );
            }
            if i > 0 {
                assert!(
                    !bm.are_all_bits_set(),
                    "areAllBitsSet() must return false on new bitmap with {i} bits; {bm:?}"
                );
            }
            let n = bm.ones_count();
            assert_eq!(n, 0, "unexpected number of set bits; got {n}; want 0");

            bm.set_bits();
            let n = bm.ones_count();
            assert_eq!(n, i, "unexpected number of set bits; got {n}; want {i}");

            // Make sure that all the bits are set.
            let mut next_idx = 0usize;
            bm.for_each_set_bit_readonly(|idx| {
                assert!(idx < i, "index must be smaller than {i}");
                assert_eq!(idx, next_idx, "unexpected idx; got {idx}; want {next_idx}");
                next_idx += 1;
            });
            assert_eq!(
                next_idx, bits_len,
                "unexpected number of bits set; got {next_idx}; want {bits_len}"
            );

            assert!(
                bm.are_all_bits_set(),
                "all bits must be set for bitmap with {i} bits"
            );

            // Clear a part of bits
            bm.for_each_set_bit(|idx| idx % 2 != 0);

            if i <= 1 {
                assert!(
                    bm.is_zero(),
                    "bm.is_zero() must return true for bitmap with {i} bits"
                );
            }
            if i > 1 {
                assert!(
                    !bm.is_zero(),
                    "bm.is_zero() must return false, since some bits are set for bitmap with {i} bits"
                );
            }
            if i == 0 {
                assert!(
                    bm.are_all_bits_set(),
                    "areAllBitsSet() must return true for bitmap with 0 bits"
                );
            }
            if i > 0 {
                assert!(
                    !bm.are_all_bits_set(),
                    "some bits mustn't be set for bitmap with {i} bits"
                );
            }

            let mut next_idx = 1usize;
            bm.for_each_set_bit_readonly(|idx| {
                assert_eq!(idx, next_idx, "unexpected idx; got {idx}; want {next_idx}");
                next_idx += 2;
            });
            assert!(
                next_idx >= bits_len,
                "unexpected number of bits visited; got {next_idx}; want {bits_len}"
            );

            // Clear all the bits
            bm.for_each_set_bit(|_| false);

            assert!(
                bm.is_zero(),
                "all the bits must be reset for bitmap with {i} bits"
            );
            if i == 0 {
                assert!(
                    bm.are_all_bits_set(),
                    "allAllBitsSet() must return true for bitmap with 0 bits"
                );
            }
            if i > 0 {
                assert!(
                    !bm.are_all_bits_set(),
                    "areAllBitsSet() must return false for bitmap with {i} bits"
                );
            }
            let n = bm.ones_count();
            assert_eq!(n, 0, "unexpected number of set bits; got {n}; want 0");

            let mut bits_count = 0usize;
            bm.for_each_set_bit_readonly(|_| {
                bits_count += 1;
            });
            assert_eq!(
                bits_count, 0,
                "unexpected non-zero number of set bits remained: {bits_count}"
            );

            // Set bits via set_bit() call
            for i in 0..bits_len {
                let n = bm.ones_count();
                assert_eq!(n, i, "unexpected number of ones set; got {n}; want {i}");
                assert!(!bm.is_set_bit(i), "the bit {i} mustn't be set");
                bm.set_bit(i);
                assert!(bm.is_set_bit(i), "the bit {i} must be set");
                let n = bm.ones_count();
                assert_eq!(
                    n,
                    i + 1,
                    "unexpected number of ones set; got {n}; want {}",
                    i + 1
                );
            }

            put_bitmap(bm);
        }
    }

    // PORT NOTE: the Go test suite doesn't cover andNot() and copyFrom();
    // this extra test pins their behavior.
    #[test]
    fn test_bitmap_and_not_copy_from() {
        let mut bm = get_bitmap(130);
        bm.set_bits();

        let mut x = get_bitmap(130);
        for i in (0..130).step_by(2) {
            x.set_bit(i);
        }

        let mut copy = Bitmap::default();
        copy.copy_from(&bm);
        assert_eq!(copy.bits_len, 130);
        assert_eq!(copy.ones_count(), 130);

        bm.and_not(&x);
        assert_eq!(bm.ones_count(), 65);
        for i in 0..130 {
            assert_eq!(bm.is_set_bit(i), i % 2 == 1, "unexpected bit {i}");
        }

        // andNot with a zero bitmap must leave bm unchanged.
        let zero = get_bitmap(130);
        bm.and_not(&zero);
        assert_eq!(bm.ones_count(), 65);

        put_bitmap(zero);
        put_bitmap(x);
        put_bitmap(bm);
    }
}
