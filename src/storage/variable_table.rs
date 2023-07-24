use crate::config::U64_OFFSET;
use crate::utils::file;
use crate::{Error, Result};
use std::ptr;
const HEADER_LENGTH: u64 = 16;
use crate::config::Address;
type DataAddress = u64;
type FreeSize = u64;
type Id = u8;

#[derive(Clone, Copy, Debug)]
pub struct VariableHeader {
    pub data_address: DataAddress,
    pub free_size: FreeSize,
}
impl VariableHeader {
    pub fn new(data_address: DataAddress, free_size: FreeSize) -> Self {
        VariableHeader {
            data_address,
            free_size,
        }
    }
}
/// ```ignore
/// free_size = 0 means occupied
/// -------------------------------------------------------------
/// | header_count(u64) | start_address (u64) | free_size (u64) |
/// -------------------------------------------------------------
/// | start_address(u64) |    free_size (u64)    | ...header... |
/// -------------------------------------------------------------
/// |  persist_struct(id, data)  |     ...persist_struct...     |
/// -------------------------------------------------------------
/// ```
#[derive(Debug)]
pub struct VariableTable {
    address: Address,
}

impl VariableTable {
    pub fn new(address: Address, size: u64) -> Self {
        unsafe {
            ptr::write_bytes(address as *mut u8, 0, size as usize);
        }
        let free_size = size - 3 * U64_OFFSET;
        let start_address = address + 3 * U64_OFFSET;
        unsafe {
            *(address as *mut u64) = 1;
            *((address + U64_OFFSET) as *mut u64) = start_address;
            *((address + HEADER_LENGTH) as *mut u64) = free_size;
        }
        VariableTable { address }
    }

    pub fn reload(address: Address) -> Self {
        VariableTable { address }
    }
    pub fn header_length(&self) -> u64 {
        unsafe { *(self.address as *const u64) }
    }

    pub fn add_header(&mut self, start_address: Address, free_size: u64) -> Id {
        let offset = self.header_length();
        let header_address = self.address + U64_OFFSET + offset * HEADER_LENGTH;
        unsafe {
            *(self.address as *mut u64) = offset + 1;
        }

        self.set_header(header_address, start_address, free_size);
        offset as Id
    }
    pub fn get_header(&self, header_address: Address) -> VariableHeader {
        unsafe {
            VariableHeader::new(
                *(header_address as *const u64),
                *((header_address + U64_OFFSET) as *const u64),
            )
        }
    }
    pub fn get_all_headers(&self) -> Vec<VariableHeader> {
        let length = self.header_length();
        let mut iter = self.address + U64_OFFSET;
        let mut headers = Vec::new();
        for _ in 0..length {
            headers.push(self.get_header(iter));
            iter += HEADER_LENGTH;
        }
        headers
    }
    pub fn set_header(&mut self, header_address: Address, start_address: Address, free_size: u64) {
        unsafe {
            *(header_address as *mut u64) = start_address;
            *((header_address + U64_OFFSET) as *mut u64) = free_size;
        }
    }
    pub fn set_header_by_id(&mut self, id: u8, start_address: Address, free_size: u64) {
        self.set_header(
            self.address + U64_OFFSET + id as u64 * HEADER_LENGTH,
            start_address,
            free_size,
        );
    }
    pub fn allocate(&mut self, data_len: u64) -> Result<(Id, DataAddress)> {
        let size = self.header_length();
        let mut iter = self.address + U64_OFFSET;
        for _ in 0..size {
            let header = self.get_header(iter);
            if header.free_size >= data_len + HEADER_LENGTH {
                let data_address = header.data_address + header.free_size - data_len;
                self.set_header(
                    iter,
                    header.data_address + HEADER_LENGTH,
                    header.free_size - data_len - HEADER_LENGTH,
                );
                let id = self.add_header(data_address, data_len);
                return Ok((id, data_address));
            }
            iter += HEADER_LENGTH;
        }
        Err(Error::NoSpace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NVM_ADDR;
    use crate::storage::nvm_file::NVMTableStorage;

    #[test]
    fn test_variable() {
        NVMTableStorage::init_test_database();
        let mut d = VariableTable::new(NVM_ADDR, 100);
        let header = d.get_header(NVM_ADDR + U64_OFFSET);
        assert_eq!(header.data_address, NVM_ADDR + 24);
        assert_eq!(header.free_size, 100 - 24);
        assert_eq!(d.allocate(10).unwrap().1, NVM_ADDR + 90);
        assert_eq!(d.allocate(10).unwrap().1, NVM_ADDR + 80);
        assert_eq!(d.header_length(), 3);
    }
}
