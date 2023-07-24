use crate::config::Address;
use crate::config::U64_OFFSET;
use crate::utils::file;
use crate::utils::io;
use std::marker::PhantomData;
use std::mem::size_of;

#[derive(Default, Copy, Clone, Debug)]
pub struct PersistArray<T: Copy> {
    address: Address,
    phantom: PhantomData<T>,
    pub length_offset: u64,
}

impl<T: Copy> PersistArray<T> {
    pub fn new(address: Address) -> Self {
        let array = PersistArray {
            address: address,
            phantom: PhantomData,
            length_offset: U64_OFFSET,
        };
        array.set_len(0);
        array
    }
    pub fn new_without_length(address: Address) -> Self {
        PersistArray {
            address: address,
            phantom: PhantomData,
            length_offset: 0,
        }
    }

    pub fn reload(address: Address) -> Self {
        PersistArray {
            address: address,
            phantom: PhantomData,
            length_offset: U64_OFFSET,
        }
    }
    pub fn reload_without_length(address: Address) -> Self {
        PersistArray {
            address: address,
            phantom: PhantomData,
            length_offset: 0,
        }
    }

    pub fn _address(&self) -> Address {
        self.address
    }
    pub fn _data_address(&self) -> Address {
        self.address + self.length_offset
    }

    pub fn len(&self) -> u64 {
        assert!(self.length_offset > 0);
        let iter = self.address;
        unsafe { io::read(iter) }
    }
    pub fn clwb(&self) {
        let len = self.len() + self.length_offset;
        self.clwb_len(len);
    }
    pub fn clwb_len(&self, len: u64) {
        // file::sfence();
        let mut iter = self.address;
        let count = len / 64 + (len % 64 != 0) as u64;
        // println!("{}", count);
        for _ in 0..count {
            unsafe {
                io::clwb(iter as *const u8);
            }
            iter += 64;
        }
        //
    }
    pub fn set_len(&self, len: u64) {
        assert!(self.length_offset > 0);
        let iter = self.address;
        unsafe { io::write(iter, len) };
    }

    pub fn get(&self, index: u64) -> Option<T> {
        let offset = size_of::<T>() as u64;
        let len = self.len();
        if index >= len {
            return None;
        }
        let iter = self.address + self.length_offset + index * offset;
        Some(unsafe { io::read(iter) })
    }
    pub fn range(&self, start: u64, end: u64) -> Option<&[T]> {
        let offset = size_of::<T>() as u64;
        if self.length_offset > 0 && end > self.len() {
            return None;
        }
        let iter = self.address + self.length_offset + start * offset;
        let len = (end - start) as usize;
        Some(unsafe { io::read_slice(iter, len) })
    }
    pub fn set(&self, index: u64, value: T) {
        let offset = size_of::<T>() as u64;
        let len = self.len();
        assert!(index < len);
        let iter = self.address + self.length_offset + index * offset;

        unsafe { io::write(iter, value) };
    }

    pub fn copy_from_slice(&self, start: u64, data: &[T]) {
        // assert!(start + data.len() as u64 <= self.len());

        let offset = size_of::<T>() as u64;
        let iter = self.address + self.length_offset + start * offset;
        // println!("----- {:X}, {}", iter, data.len());
        unsafe {
            io::write_slice(iter, data);
        }
    }

    pub unsafe fn push(&self, data: T) -> u64 {
        let offset = size_of::<T>() as u64;
        let len = self.len();
        let iter = self.address + self.length_offset + len * offset;
        io::write(iter, data);
        // file::clwb(iter as *const u8);
        self.set_len(len + 1);
        len
    }
    pub fn put_slice(&self, data: &[T]) {
        let offset = size_of::<T>() as u64;
        let mut len = self.len();
        let iter = self.address + self.length_offset + len * offset;
        unsafe {
            io::write_slice(iter, data);
        }

        len += data.len() as u64;
        self.set_len(len);
    }

    pub fn to_vec(&self) -> Vec<T> {
        unsafe {
            let u = io::read_slice(self.address + self.length_offset, self.len() as usize);
            u.iter().copied().collect()
        }
    }
    pub fn to_vec_len(&self, len: usize) -> Vec<T> {
        unsafe {
            let u = io::read_slice(self.address, len);
            u.iter().copied().collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NVM_ADDR;
    #[test]
    fn test_list() {
        let _ = std::fs::remove_file("test_persist");
        file::mmap("test_persist", 1024 * 1024).unwrap();
        let p = PersistArray::<u64>::new(NVM_ADDR);

        unsafe {
            p.push(10);
            p.push(20);
        }
        assert_eq!(p.len(), 2);
    }
}
