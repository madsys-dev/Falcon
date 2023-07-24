use crate::config::U64_OFFSET;
use crate::range;
use crate::storage::timestamp::TimeStamp;
use std::mem::size_of;
use std::ops::Range;

pub const TID: Range<u64> = range!(0, size_of::<TimeStamp>() as u64);
pub const NEXT_DELTA_ADDRESS: Range<u64> = range!(TID.end, U64_OFFSET);
pub const TUPLE_ADDRESS: Range<u64> = range!(NEXT_DELTA_ADDRESS.end, U64_OFFSET);

pub const DELTA_TABLE_ID: Range<u64> = range!(TUPLE_ADDRESS.end, size_of::<u32>() as u64);
pub const DELTA_COLUMN_OFFSET: Range<u64> = range!(DELTA_TABLE_ID.end, size_of::<u32>() as u64);
pub const DELTA_DATA_OFFSET: u64 = DELTA_COLUMN_OFFSET.end;

// delta
// |----------------------------------------------------|
// |         len(u64)         |        tid (u64)        |
// |----------------------------------------------------|
// |              next_delta_address(u64)               |
// |----------------------------------------------------|
// |  delta_column_offset (usize)  |  delta_data([u8])  |
// |----------------------------------------------------|
