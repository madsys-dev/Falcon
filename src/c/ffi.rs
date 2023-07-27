use libc::c_char;
use libc::c_int;
use libc::c_uint;
use libc::c_void;

// #[link(name = "btree")]
// extern {
//     pub fn btree_create() -> *mut c_void;
//     pub fn btree_insert(btree: *mut c_void, key: u64, value: u64) -> c_int;
//     pub fn btree_find(btree: *mut c_void, key: u64) -> u64;
//     pub fn btree_remove(btree: *mut c_void, key: u64) -> u64;
//     pub fn btree_init_for_thread(thread_id: c_int);

// }

#[link(name = "dash")]
extern "C" {
    pub fn plus(a: c_int, b: c_int) -> c_int;
    pub fn init(file: *const c_char);
    pub fn dash_create() -> *mut c_void;
    pub fn dash_insert(dash: *mut c_void, key: u64, value: u64) -> c_int;
    pub fn dash_update(dash: *mut c_void, key: u64, value: u64) -> c_int;

    pub fn dash_find(dash: *mut c_void, key: u64) -> u64;
    pub fn dash_remove(dash: *mut c_void, key: u64) -> u64;

    pub fn dashstring_create() -> *mut c_void;

    pub fn dashstring_insert(
        dash: *mut c_void,
        key: *const c_char,
        lenght: c_int,
        value: u64,
    ) -> u64;
    pub fn dashstring_find(dash: *mut c_void, key: *const c_char, lenght: c_int) -> u64;
    pub fn dashstring_update(
        dash: *mut c_void,
        key: *const c_char,
        lenght: c_int,
        value: u64,
    ) -> u64;
    pub fn dashstring_remove(dash: *mut c_void, key: *const c_char, lenght: c_int) -> u64;

}
