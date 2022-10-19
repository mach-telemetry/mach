package main

import (
	"context"
	//"flag"
	"log"
	"time"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/fsolleza/mach-proto/golang"
)

func generate_request() pb.PushRequest {
	request_samples := []*pb.Samples{}
	counter := uint64(0)
	for _, t := range []string{"a", "b", "c"} {
		samples := pb.Samples {
			Tags: map[string]string { t:t },
			Samples: []*pb.Sample{},
		}

		for i := 0; i < 3; i++ {
			timestamp := uint64(time.Now().UnixMicro())
			var values []*pb.Value
			values = append(values, &pb.Value { ValueType: &pb.Value_F64 { F64: 123.456 }})
			values = append(values, &pb.Value { ValueType: &pb.Value_Str { Str: "foobar string"}})
			sample := pb.Sample { Id: counter, Timestamp: timestamp, Values: values }
			samples.Samples = append(samples.Samples, &sample)
			counter = counter + 1
		}

		request_samples = append(request_samples, &samples)
	}
	request := pb.PushRequest { Samples: request_samples }
	return request
}


func main() {
	conn, err := grpc.Dial("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	tsdb := pb.NewTsdbServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := tsdb.Echo(ctx, &pb.EchoRequest{ Msg: "message" })
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println("Echo success: ", result.Msg)

	stream, err := tsdb.PushStream(context.Background())

	// Stream samples into tsdb
	go func() {
		for {
			request := generate_request()
			if err := stream.Send(&request); err != nil {
				panic("ERROR")
			}
		}
	}()

	result_count := uint64(0)

	// Consume response
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				panic("ERROR")
			}
			atomic.AddUint64(&result_count, uint64(len(resp.Responses)))
		}
	}()

	// Monitor counts
	done := make(chan bool) // force wait
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("Received", atomic.LoadUint64(&result_count), "/s")
		}
		close(done)
	}()
	<-done
}
