### Data
`tar -xzf data.tar.gz` extracts test univariate and multivariate data into `mach-private/data`.
This directory is ignored in `.gitignore`

### MOAR data

`wget https://www.dropbox.com/s/ya7mwmvq17v6hqh/data.tar.gz`


### Test with features,

```
cargo test --lib --features kafka-backend,file-backend
```


