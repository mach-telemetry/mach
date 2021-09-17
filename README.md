### Setup

* install rust
* run the following

```
cd this/directory
rustup override set nightly
tar -xzf data.tar.gz
cargo test
```

### Data

`tar -xzf data.tar.gz` extracts test univariate and multivariate data into `mach-private/data`.
This directory is ignored in `.gitignore`

### TODOs
- [x] Persist file store
- [x] Persist keys
- [ ] XOR Compression
- [x] Row-based compression
- [x] Load files from directory
- [x] Load key metadata from tsdb directory
- [ ] Persist time series options
- [ ] Reload time series options into Db
