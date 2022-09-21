use std::thread;
use std::time::Duration;

fn main() {
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(10));
        println!("ALLOCATING");
        let x = vec![53u8; 1_000_000].into_boxed_slice();
        thread::sleep(Duration::from_secs(300));
        println!("x len {}", x.len());
    });
    loop {
        let stat = procinfo::pid::stat_self().unwrap();
        //let result = unsafe { libc::getrusage(who, &mut rusage) };
        println!("vsize: {}", stat.vsize);
        println!("threads: {}", stat.num_threads);
        //println!("MRSS: {}", rusage.ru_maxrss);
        //println!("CPU: {:?}", rusage.ru_utime);
        thread::sleep(Duration::from_secs(1));
    }
}

