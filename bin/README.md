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


### Build docker

Assumes you're in this directory. Change Dockerfile to point to correct kafka bootstraps. If you're running on minikube for testing, set minikube as the image repo

```
eval $(minikube docker-env)
cd ../ # cd to root project dir
docker build . -t mach/tsdb -f bin/Dockerfile  # use root proj dir as context
kubectl apply -f mach.yaml
```



