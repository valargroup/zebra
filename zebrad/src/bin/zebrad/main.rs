//! Main entry point for Zebrad

use zebrad::application::{boot, APPLICATION};

#[cfg(all(feature = "jemalloc-profiling", not(target_env = "msvc")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Process entry point for `zebrad`
fn main() {
    // Enable backtraces by default for zebrad, but allow users to override it.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
        // Disable library backtraces (i.e. eyre) to avoid performance hit for
        // non-panic errors, but allow users to override it.
        if std::env::var_os("RUST_LIB_BACKTRACE").is_none() {
            std::env::set_var("RUST_LIB_BACKTRACE", "0");
        }
    }
    boot(&APPLICATION);
}
