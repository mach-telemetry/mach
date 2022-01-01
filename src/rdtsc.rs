use std::time::Duration;
use std::convert::TryInto;
use num::NumCast;

// Taken from: journalctl --boot | grep 'kernel: tsc:' -i | cut -d' ' -f5-
pub const TSC_HZ: f64 = 2693.672 * 1_000_000.;

macro_rules! rdtsc {
    () => {
    unsafe {
            let hi: u32;
            let lo: u32;
            #[allow(deprecated)]
            llvm_asm!("
                lfence;
                rdtscp;
                mov %edx, $0;
                mov %eax, $1;
                lfence;
                "
                : "=r"(hi), "=r"(lo)
                :
                : "rax", "rbx", "rcx", "rdx", "memory" : "volatile"
            );
            (hi as u64) << 32 | lo as u64
    }
    }
}

pub fn cycles_to_seconds(cycles: u64) -> f64 {
    let cycles: f64 = <f64 as NumCast>::from(cycles).unwrap();
    cycles / TSC_HZ
}
