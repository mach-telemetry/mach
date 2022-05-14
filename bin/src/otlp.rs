pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
                }
            }
            pub mod metrics {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
                }
            }
            pub mod trace {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.collector.trace.v1");
                }
            }
        }
        pub mod logs {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.logs.v1");
            }
        }
        pub mod metrics {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.metrics.v1");
            }
        }
        pub mod trace {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.trace.v1");
            }
        }
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.common.v1");
            }
        }
        pub mod resource {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.resource.v1");
            }
        }
    }
}
use opentelemetry::proto as otlp;

pub use otlp::*;

use otlp::{
    common::v1::{KeyValue, any_value::Value, ArrayValue, AnyValue, KeyValueList},
    logs::v1::ResourceLogs,
    metrics::v1::{metric::Data, ResourceMetrics},
    trace::v1::ResourceSpans,
};
use std::hash::{Hash, Hasher};

impl Hash for AnyValue {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        match self.value.as_ref().unwrap() {
            Value::StringValue(x) => x.hash(hasher),
            Value::IntValue(x) => x.hash(hasher),
            Value::BoolValue(x) => x.hash(hasher),
            Value::DoubleValue(x) => x.to_be_bytes().hash(hasher),
            Value::BytesValue(x) => x.hash(hasher),
            Value::ArrayValue(x) => x.hash(hasher),
            Value::KvlistValue(x) => x.hash(hasher),
        };
    }
}

impl Hash for KeyValueList {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.values.hash(hasher)
    }
}

impl Hash for ArrayValue {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.values.hash(hasher)
    }
}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.key.hash(hasher);
        self.value.as_ref().unwrap().hash(hasher);
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum OtlpData {
    Logs(Vec<ResourceLogs>),
    Metrics(Vec<ResourceMetrics>),
    Spans(Vec<ResourceSpans>),
}


impl OtlpData {
    pub fn add_attribute(&mut self, kv: KeyValue) {
        match self {
            OtlpData::Metrics(resource_metrics) => {

                // Iterate over each resource
                for resource in resource_metrics.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_metrics.iter_mut() {

                        // Iterate over each metric
                        for metric in scope.metrics.iter_mut() {

                            // There are several types of metrics
                            match metric.data.as_mut().unwrap() {

                                Data::Gauge(x) => {
                                    for point in &mut x.data_points {
                                        point.attributes.push(kv.clone());
                                    }
                                },

                                Data::Sum(x) => {
                                    for point in &mut x.data_points {
                                        point.attributes.push(kv.clone());
                                    }
                                },

                                Data::Histogram(x) => {
                                    for point in &mut x.data_points {
                                        point.attributes.push(kv.clone());
                                    }
                                },

                                Data::ExponentialHistogram(x) => {
                                    for point in &mut x.data_points {
                                        point.attributes.push(kv.clone());
                                    }
                                },

                                Data::Summary(x) => {
                                    for point in &mut x.data_points {
                                        point.attributes.push(kv.clone());
                                    }
                                },
                            } // match brace
                        } // metric loop
                    } // scope loop
                } // resource loop
            }, // match Metric

            OtlpData::Logs(resource_logs) => {

                // Iterate over each resource
                for resource in resource_logs.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_logs.iter_mut() {

                        // Iterate over each log
                        for log in &mut scope.log_records.iter_mut() {
                            log.attributes.push(kv.clone());
                        } // log loop
                    } // scope loop
                } // resource loop
            }, // match Logs

            OtlpData::Spans(resource_spans) => {

                // Iterate over each resource
                for resource in resource_spans.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_spans.iter_mut() {

                        // Iterate over each span
                        for span in scope.spans.iter_mut() {
                            span.attributes.push(kv.clone());
                        } // span loop
                    } // scope loop
                } // resource loop
            }, // match Span

        } // match item
    }


    pub fn update_timestamp(&mut self, timestamp: u64) {
        match self {
            OtlpData::Metrics(resource_metrics) => {

                // Iterate over each resource
                for resource in resource_metrics.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_metrics.iter_mut() {

                        // Iterate over each metric
                        for metric in scope.metrics.iter_mut() {

                            // There are several types of metrics
                            match metric.data.as_mut().unwrap() {

                                Data::Gauge(x) => {
                                    for point in &mut x.data_points {
                                        point.time_unix_nano = timestamp;
                                    }
                                },

                                Data::Sum(x) => {
                                    for point in &mut x.data_points {
                                        point.time_unix_nano = timestamp;
                                    }
                                },

                                Data::Histogram(x) => {
                                    for point in &mut x.data_points {
                                        point.time_unix_nano = timestamp;
                                    }
                                },

                                Data::ExponentialHistogram(x) => {
                                    for point in &mut x.data_points {
                                        point.time_unix_nano = timestamp;
                                    }
                                },

                                Data::Summary(x) => {
                                    for point in &mut x.data_points {
                                        point.time_unix_nano = timestamp;
                                    }
                                },
                            } // match brace
                        } // metric loop
                    } // scope loop
                } // resource loop
            }, // match Metric

            OtlpData::Logs(resource_logs) => {

                // Iterate over each resource
                for resource in resource_logs.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_logs.iter_mut() {

                        // Iterate over each log
                        for log in &mut scope.log_records.iter_mut() {
                            log.time_unix_nano = timestamp;
                        } // log loop
                    } // scope loop
                } // resource loop
            }, // match Logs

            OtlpData::Spans(resource_spans) => {

                // Iterate over each resource
                for resource in resource_spans.iter_mut() {

                    // Iterate over each scope
                    for scope in resource.scope_spans.iter_mut() {

                        // Iterate over each span
                        for span in scope.spans.iter_mut() {
                            let start = span.start_time_unix_nano;
                            let diff = timestamp - start;
                            span.start_time_unix_nano += diff;
                            span.end_time_unix_nano += diff;
                            for event in &mut span.events {
                                event.time_unix_nano += diff;
                            }
                        } // span loop
                    } // scope loop
                } // resource loop
            }, // match Span

        } // match item
    }
}
