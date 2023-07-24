#[macro_export]
macro_rules! range {
    ($start:expr, $len:expr) => {
        $start..($start + $len)
    };
}
