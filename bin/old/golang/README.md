
Run the tsdb first:

```
cd ..
cargo run --release --bin tsdb
```

Run this application to stream simple data into tsdb

```
go run client.go
```

If `mach-proto` is updated:

```
go get -u github.com/fsolleza/mach-proto/golang
```

