use crate::config::Address;
use crate::utils::io;
use crate::utils::persist::persist_array::PersistArray;
use std::fmt::Display;
use std::ops::Range;
#[derive(Copy, Clone, Debug, Default)]
/// | len (u64) | metadata (data_offset) |
pub struct PersistStruct {
    data: PersistArray<u8>,
    data_offset: u64,
    new: bool,
}

impl PersistStruct {
    pub fn new(address: Address, data_offset: u64) -> Self {
        let mut s = PersistStruct {
            data: PersistArray::new(address),
            data_offset,
            new: true,
        };
        s.set_len(data_offset);
        s
    }
    pub fn new_without_length(address: Address, data_offset: u64) -> Self {
        PersistStruct {
            data: PersistArray::new_without_length(address),
            data_offset,
            new: true,
        }
    }
    pub fn reload(address: Address, data_offset: u64) -> Self {
        PersistStruct {
            data: PersistArray::reload(address),
            data_offset,
            new: false,
        }
    }
    pub fn reload_without_length(address: Address, data_offset: u64) -> Self {
        PersistStruct {
            data: PersistArray::reload_without_length(address),
            data_offset,
            new: false,
        }
    }
    pub fn clwb(&self) {
        self.data.clwb();
    }
    pub fn clwb_len(&self, len: u64) {
        self.data.clwb_len(len);
    }
    pub fn _address(&self) -> Address {
        self.data._address()
    }
    pub fn _new(&self) -> bool {
        self.new
    }
    pub fn _data_address(&self) -> Address {
        self.data._address() + self.data_offset + self.data.length_offset
    }

    pub fn get_meta_data<T: Copy + Display>(&self, parameter: Range<u64>) -> T {
        unsafe {
            let value = io::read(self._address() + parameter.start + self.data.length_offset);
            value
        }
    }
    pub fn set_meta_data<T: Display>(&self, parameter: Range<u64>, value: T) {
        unsafe {
            io::write(
                self._address() + parameter.start + self.data.length_offset,
                value,
            );
        };
    }

    pub fn len(&self) -> u64 {
        self.data.len()
    }

    pub fn data_len(&self) -> u64 {
        self.data.len() - self.data_offset
    }

    pub fn set_len(&mut self, len: u64) {
        self.data.set_len(len);
    }
    pub fn save(&mut self, bytes: &[u8]) {
        assert_eq!(self.new, true);
        self.data.put_slice(bytes);
        self.new = false;
    }
    pub fn data(&self) -> &[u8] {
        self.data.range(self.data_offset, self.len()).unwrap()
    }
    pub fn range(&self, start: u64, end: u64) -> &[u8] {
        self.data.range(start, end).unwrap()
    }
    pub fn put_slice(&self, bytes: &[u8]) {
        self.data.put_slice(bytes);
    }
    pub fn to_vec(&self) -> Vec<u8> {
        self.data.to_vec()
    }
    pub fn to_vec_len(&self, len: usize) -> Vec<u8> {
        self.data.to_vec_len(len)
    }
    pub fn copy_from_slice(&self, start: u64, data: &[u8]) {
        self.data.copy_from_slice(start, data);
    }
}
