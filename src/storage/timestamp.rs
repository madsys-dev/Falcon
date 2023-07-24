use crate::{config::U64_OFFSET, range};
use std::{fmt, ops::Range};

pub const TS_TID: Range<u64> = range!(0, U64_OFFSET);
#[cfg(feature = "read_ts")]
pub const TS_READ_TS: Range<u64> = range!(TS_TID.end, U64_OFFSET);
// #[cfg(any(feature = "read_ts"))]
// pub const TS_BEGIN_TS: Range<u64> = range!(TS_READ_TS.end, U64_OFFSET);
// #[cfg(any(feature = "read_ts"))]
// pub const TS_END_TS: Range<u64> = range!(TS_BEGIN_TS.end, U64_OFFSET);

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeStamp {
    pub tid: u64,
    #[cfg(feature = "read_ts")]
    pub read_ts: u64,
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.tid)
    }
}

impl Default for TimeStamp {
    fn default() -> Self {
        TimeStamp {
            tid: 0,
            #[cfg(feature = "read_ts")]
            read_ts: 0,
        }
    }
}
