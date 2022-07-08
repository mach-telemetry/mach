use num::NumCast;

// Taken from: journalctl --boot | grep 'kernel: tsc:' -i | cut -d' ' -f5-
//pub const TSC_HZ: f64 = 2693.672 * 1_000_000.;
//lazy_static! {
//    pub static ref TSC_HZ: f64 = {
//        let tsc_info = CpuId::new().get_tsc_info().unwrap();
//        let hz = tsc_info.tsc_frequency().unwrap() as f64 * 1_000_000.;
//        println!("hz: {}", hz);
//        hz
//    };
//}

#[allow(dead_code)]
pub const TSC_HZ: &f64 = &(2693.672 * 1_000_000.0f64);

#[allow(dead_code)]
pub const TSC_HZ_MILLIS: &f64 = &(2693.672 * 1_000.0f64);

#[allow(dead_code)]
pub const TSC_HZ_MICROS: &f64 = &2693.672f64;

#[macro_export]
macro_rules! rdtsc {
    () => {
    unsafe {
            let hi: u32;
            let lo: u32;
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

#[allow(dead_code)]
pub fn cycles_to_seconds(cycles: u64) -> f64 {
    let cycles: f64 = <f64 as NumCast>::from(cycles).unwrap();
    cycles / *TSC_HZ
}
