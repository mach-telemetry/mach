### Need to pull proto submodule

```
git submodule update --init --recursive
```


### Run Server

make sure you are running the tsdb server

```
cargo run --release --bin tsdb
```

### Run example clients

**Rust:** `cargo run --release --bin writer-client`

**Go:** `cd golang && go run client.go server_grpc.pb.go server.pb.go`

