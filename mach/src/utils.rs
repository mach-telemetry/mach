use std::time::SystemTime;

pub fn now_in_micros() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap()
}
