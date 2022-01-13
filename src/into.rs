
use std::convert::Into;

impl Into<[u8; 8]> for f64 {
    fn into(self) -> T {
        self.to_be_bytes()
    }
}

