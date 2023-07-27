use crate::config::NVM_ADDR;
use libc;
use memmap::MmapMut;
use std::arch::asm;
use std::os::unix::prelude::AsRawFd;
use std::{fs::OpenOptions, io, path::Path};

/// Open or create a file, then mmap it to the address space.
pub fn mmap_lib(file_path: impl AsRef<Path>, file_size: u64) -> io::Result<MmapMut> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;

    debug!("create or open file");

    f.set_len(file_size)?;
    let map = unsafe { MmapMut::map_mut(&f) }?;
    // let map = unsafe { MmapOptions::new().map_mut(&f)?};
    Ok(map)
}

pub fn mmap(file_path: impl AsRef<Path>, file_size: u64) -> io::Result<*mut u8> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;

    debug!("create or open file");

    f.set_len(file_size)?;
    unsafe {
        let data = libc::mmap(
            /* addr: */ NVM_ADDR as *mut libc::c_void,
            /* len: */ file_size as usize,
            /* prot: */ libc::PROT_READ | libc::PROT_WRITE,
            // Then make the mapping *public* so it is written back to the file
            /* flags: */
            libc::MAP_SHARED,
            /* fd: */ f.as_raw_fd(),
            /* offset: */ 0,
        ) as *mut u8;
        Ok(data)
    }
}

#[inline]
pub fn sfence() {
    use std::sync::atomic::{fence, Ordering};
    // fence(Ordering::Release);

    // fence(Ordering::SeqCst);
    unsafe {
        asm!("sfence");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmap_lib() {
        let mut mmap = mmap_lib("/tmp/test.txt", 16).unwrap();
        let src = "hello";
        mmap[..src.len()].copy_from_slice(src.as_bytes());
        assert_eq!(&mmap[..src.len()], src.as_bytes());
    }
}
