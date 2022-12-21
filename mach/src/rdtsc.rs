#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
use core::arch::x86_64::_rdtsc;

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
pub fn rdtsc() -> u64 {
    unsafe { _rdtsc() }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "sse2")))]
pub fn rdtsc() -> u64 {
    panic!("can't use counter without TSC support");
}
