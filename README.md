### Setup

* install rust
* run the following

```
cd this/directory
rustup override set nightly
cargo test
```

### Data

unpack data.tar.gz to a path. update test data path
Note: still need to clean-up how the tests interact with the data so ignore this for now

### TODOs
- [x] Persist file store
- [ ] Persist keys
- [ ] XOR Compression
- [x] Row-based compression
- [ ] Load files from directory
