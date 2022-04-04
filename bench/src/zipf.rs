use num::NumCast;
use rand::{prelude::*, rngs::ThreadRng};

pub struct Zipfian {
    items: f64,
    alpha: f64,
    zeta_n: f64,
    eta: f64,
    theta: f64,
    rng: ThreadRng,
}

impl Zipfian {
    pub fn new(items: u64, theta: f64) -> Self {
        Self::new_with_rng(items, theta, rand::thread_rng())
    }
    pub fn new_with_rng(items: u64, theta: f64, rng: ThreadRng) -> Self {
        let zeta_n = zeta(items, theta);
        let zeta2theta = zeta(2, theta);
        let items_float: f64 = NumCast::from(items).unwrap();
        Self {
            items: items_float,
            theta,
            alpha: 1. / (1. - theta),
            zeta_n,
            eta: (1. - (2. / items_float).powf(1. - theta)) / (1. - zeta2theta / zeta_n),
            rng,
        }
    }

    pub fn _items(&self) -> u64 {
        NumCast::from(self.items).unwrap()
    }

    pub fn next_item(&mut self) -> u64 {
        let u = self.rng.gen_range(0.0..1.0);
        let uz = u * self.zeta_n;
        if uz < 1.0 {
            0
        } else if uz < 1.0 + (0.5_f64).powf(self.theta) {
            1
        } else {
            let res = self.items * (self.eta * u - self.eta + 1.).powf(self.alpha);
            NumCast::from(res).unwrap()
        }
    }

    pub fn _reset(&mut self, items: u64) {
        let zeta2theta = zeta(2, self.theta);
        let items_float: f64 = NumCast::from(items).unwrap();

        self.items = items_float;
        self.zeta_n = zeta(items, self.theta);
        self.eta =
            (1. - (2. / items_float).powf(1. - self.theta)) / (1. - zeta2theta / self.zeta_n);
    }
}

fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 0..n {
        let denom: f64 = NumCast::from(i + 1).unwrap();
        sum += 1.0_f64 / denom.powf(theta);
    }
    sum
}
