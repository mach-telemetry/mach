package main

import (
	"context"
	//"flag"
	"log"
	"time"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	//pb "./machserver"
)

func main() {
	conn, err := grpc.Dial("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	tsdb := NewTsdbServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := tsdb.Echo(ctx, &EchoRequest{ Msg: "message" })
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println("Echo success: ", result.Msg)

	// Register source
	tags := make(map[string]string)
	tags["foo"] = "bar"
	var types []AddSeriesRequest_ValueType
	types = append(types, AddSeriesRequest_F64)
	types = append(types, AddSeriesRequest_Bytes)
	add_series_request := AddSeriesRequest { Types: types, Tags: tags }
	add_series_result, err := tsdb.AddSeries(ctx, &add_series_request)
	if err != nil {
		log.Fatalf("could not register: %v", err)
	}
	fmt.Println("Address and SeriesId", add_series_result.WriterAddress, add_series_result.SeriesId)
	series_id := add_series_result.SeriesId
	_ = series_id
	writer_address := add_series_result.WriterAddress

	// Connect to writer
	writer_conn, err := grpc.Dial(writer_address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer writer_conn.Close()
	writer := NewWriterServiceClient(writer_conn)

	writer_echo_result, err := writer.Echo(ctx, &EchoRequest{ Msg: "message" })
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println("Writer Echo success: ", writer_echo_result.Msg)

	// Get writer series reference
	series_ref_request := GetSeriesReferenceRequest { SeriesId: series_id}
	series_ref_response, err := writer.GetSeriesReference(ctx, &series_ref_request)
	if err != nil {
		log.Fatalf("could not get_ref: %v", err)
	}
	series_ref := series_ref_response.SeriesReference

	// Prepare sample
	var timestamp uint64 = 12345
	var values []*Value
	values = append(values, &Value { PbType: &Value_F64 { F64: 123.456 }})
	values = append(values, &Value { PbType: &Value_Str { Str: "foobar string"}})
	sample := Sample { Timestamp: timestamp, Values: values }


	// Prepare samples
	samples := make(map[uint64]*Sample)
	samples[series_ref] = &sample

	// Write samples
	push_request := PushRequest { Samples: samples }
	push_response, err := writer.Push(ctx, &push_request)
	if err != nil {
		log.Fatalf("could not push: %v", err)
	}
	fmt.Println("Results push", push_response.Results)
}
