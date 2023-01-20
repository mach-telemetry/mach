// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/bytes-service.proto")?;
    //tonic_build::configure()
    //    .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
    //    .compile(
    //        &[
    //            "opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
    //            "opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
    //            "opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
    //        ],
    //        &["opentelemetry-proto/"],
    //    )
    //    .unwrap();
    Ok(())
}
