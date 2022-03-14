### Protobuf compile

If `server_grpc.pb.go` and `server.pb.go` need to be recompiled

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1
export PATH="$PATH:$(go env GOPATH)/bin"
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative --proto_path=../proto server.proto
```

