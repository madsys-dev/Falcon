use bitvec::prelude::*;

#[derive(Default, Debug)]
pub struct PersistBitmap<'a> {
    // 0: free, 1: allocated
    bitmap: &'a mut BitSlice<Lsb0, u8>,
    /// The minimum `i` where `bitmap[i] == false`.
    min_free_bit: usize,
}

impl<'a> PersistBitmap<'a> {
    /// Create an empty bitmap over a slice.
    pub fn new_from_slice(slice: &'a mut [u8]) -> Self {
        slice.fill(0);
        PersistBitmap {
            bitmap: BitSlice::<Lsb0, u8>::from_slice_mut(slice).unwrap(),
            min_free_bit: 0,
        }
    }

    /// Construct a bitmap over a slice.
    pub fn from_slice(slice: &'a mut [u8]) -> Self {
        let bitmap = BitSlice::<Lsb0, u8>::from_slice_mut(slice).unwrap();
        PersistBitmap {
            min_free_bit: bitmap.leading_ones(),
            bitmap,
        }
    }

    /// Whether `id` is allocated.
    pub fn exists(&self, id: u64) -> bool {
        self.bitmap[id as usize]
    }

    /// Allocate a number.
    pub fn alloc(&mut self) -> Option<u64> {
        if self.min_free_bit == self.bitmap.len() {
            return None;
        }
        let allocated = self.min_free_bit;
        self.bitmap.set(allocated, true);
        self.update_min_from(allocated + 1);
        // self.min_free_bit += 1;
        Some(allocated as u64)
    }

    /// Free a number.
    pub fn free(&mut self, idx: u64) {
        assert!(
            self.bitmap[idx as usize],
            "the number you want to free is not allocated"
        );
        self.bitmap.set(idx as usize, false);
        self.min_free_bit = self.min_free_bit.min(idx as usize);
    }

    /// Allocate consequent multiple numbers.
    pub fn alloc_multi(&mut self, len: usize) -> Option<u64> {
        let mut start = self.min_free_bit;
        while start + len <= self.bitmap.len() {
            if self.bitmap[start..start + len].not_any() {
                self.bitmap[start..start + len].set_all(true);
                if start == self.min_free_bit {
                    self.update_min_from(self.min_free_bit);
                }
                return Some(start as u64);
            }
            start += self.bitmap[start..].leading_zeros();
            start += self.bitmap[start..].leading_ones();
        }
        None
    }

    fn update_min_from(&mut self, start: usize) {
        self.min_free_bit = start + self.bitmap[start..].leading_ones();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap() {
        let mut buf = [0xffu8; 2];
        let mut p = PersistBitmap::new_from_slice(&mut buf);

        for i in 0..16 {
            assert_eq!(p.alloc(), Some(i));
        }
        assert_eq!(p.alloc(), None);
        p.free(7);
        assert_eq!(p.alloc(), Some(7));
        p.free(11);

        // drop and reload
        let mut q = PersistBitmap::from_slice(&mut buf);
        assert_eq!(q.alloc(), Some(11));
    }

    #[test]
    fn test_alloc_multi() {
        let mut buf = [0xffu8; 2];
        let mut p = PersistBitmap::new_from_slice(&mut buf);

        assert_eq!(p.alloc_multi(3), Some(0));
        assert_eq!(p.alloc_multi(9), Some(3));
        assert_eq!(p.alloc_multi(8), None);
        for i in 2..10 {
            p.free(i);
        }
        assert_eq!(p.alloc_multi(8), Some(2));
    }
}
