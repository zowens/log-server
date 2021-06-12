#[macro_export]
macro_rules! rare {
    ($e:expr) => {
        ::std::intrinsics::unlikely($e)
    };
}
