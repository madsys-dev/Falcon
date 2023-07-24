use numeric_enum_macro::numeric_enum;

numeric_enum! {
    #[repr(u8)]
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub enum TxStatus {
        Nop = 0,
        Initial = 1,
        InProgress = 2,
        Committing = 3,
        Committed = 4,
        Aborting = 5,
        Aborted = 6,
    }
}

pub mod access;
pub mod clog;
pub mod snapshot;
pub mod transaction;
pub mod transaction_buffer;

pub const THREAD_ID_BITS: u64 = 8;
pub const THREAD_ID_MASK: u64 = (1u64 << THREAD_ID_BITS) - 1;

pub fn get_thread_id(tid: u64) -> u64 {
    #[cfg(not(feature = "cc_cfg_multiclock"))]
    return tid;

    #[cfg(feature = "cc_cfg_multiclock")]
    return tid & THREAD_ID_MASK;
}
pub fn get_timestamp(tid: u64) -> u64 {
    return tid >> THREAD_ID_BITS;
}
pub fn get_tid(thread_id: u64, timestamp: u64) -> u64 {
    return timestamp << THREAD_ID_BITS | thread_id;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn enum_cast_example() {
        let s = TxStatus::Initial;
        let n: u8 = s.into();
        let s1: TxStatus = n.try_into().unwrap();
        assert_eq!(s, s1);
    }
}
