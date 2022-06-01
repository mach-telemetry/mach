use std::collections::HashMap;
use std::hash::Hash;
use redis::ToRedisArgs;

pub struct IdTable<I: Hash> {
    cache: HashMap<I, usize>,
    redis: redis::Connection,
}

impl<I: Hash + Copy + AsRef<[u8]>> IdTable<I> {
    pub fn new() -> Self {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut redis = client.get_connection().unwrap();
        let res: () = redis::cmd("SETNX")
            .arg("counter")
            .arg(0usize)
            .query(&mut redis).unwrap();
        Self {
            cache: HashMap::new(),
            redis,
        }
    }

    pub fn get_ids(&mut self, keys: &[I]) -> Vec<usize> {
        // Try to get all items
        let mut pipe = redis::pipe();
        for k in keys {
            pipe.cmd("GET").arg(k.as_ref());
        }
        let result: Vec<Option<usize>> = pipe.query(&mut self.redis).unwrap();

        // Find items that need ids
        let need_new_ids: Vec<I> = result
            .iter()
            .zip(keys.iter().copied())
            .filter(|x| { x.0.is_none() })
            .map(|x| { x.1 })
            .collect();

        let l = need_new_ids.len();

        // If there are none, unwrap all the ids
        if l == 0 {
            result.iter().map(|x| x.unwrap()).collect()
        }

        // Otherwise
        else {
            // atomic increment the counter by the number we need (allocate ids)
            let counter: usize = redis::cmd("INCRBY")
                .arg("counter")
                .arg(l)
                .query(&mut self.redis).unwrap();

            // figure out where the first allocated counter is
            let counter_start = counter - l;

            // set these as the ids
            let mut pipe = redis::pipe();
            for (k, id) in need_new_ids.iter().zip(counter_start..counter) {
                pipe.cmd("SET").arg(k.as_ref()).arg(id);
            }
            let _: () = pipe.query(&mut self.redis).unwrap();

            // And return the combined results
            let mut c = counter_start;
            result.iter().map(|x| {
                match *x {
                    Some(id) => id,
                    None => {
                        let c2 = c;
                        c += 1;
                        c2
                    }
                }
            }).collect()
        }
    }
}

