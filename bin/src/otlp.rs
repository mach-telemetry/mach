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
    metrics::v1::{number_data_point, metric::Data, ResourceMetrics},
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
    pub fn sample_count(&self) -> usize {
        let mut count = 0;
        match self {
            OtlpData::Metrics(resource_metrics) => {
                for resource in resource_metrics.iter() {
                    for scope in resource.scope_metrics.iter() {
                        for metric in scope.metrics.iter() {
                            match metric.data.as_ref().unwrap() {
                                Data::Gauge(x) => {
                                    for point in &x.data_points {
                                        count += 1;
                                    }
                                },
                                Data::Sum(x) => {
                                    for point in &x.data_points {
                                        count += 1;
                                    }
                                },

                                Data::Histogram(x) => {
                                    for point in &x.data_points {
                                        count += 1;
                                    }
                                },

                                Data::ExponentialHistogram(x) => {
                                    for point in &x.data_points {
                                        count += 1;
                                    }
                                },

                                Data::Summary(x) => {
                                    for point in &x.data_points {
                                        count += 1;
                                    }
                                },
                            } // match brace
                        } // metric loop
                    } // scope loop
                } // resource loop
            }, // match Metric

            OtlpData::Logs(resource_logs) => {
                for resource in resource_logs.iter() {
                    for scope in resource.scope_logs.iter() {
                        for log in &mut scope.log_records.iter() {
                            count += 1;
                        } // log loop
                    } // scope loop
                } // resource loop
            }, // match Logs

            OtlpData::Spans(resource_spans) => {
                for resource in resource_spans.iter() {
                    for scope in resource.scope_spans.iter() {
                        for span in scope.spans.iter() {
                            count += 1;
                        } // span loop
                    } // scope loop
                } // resource loop
            }, // match Span

        } // match item
        count
    }

    pub fn modify_name<F: Fn(&str) -> String>(&mut self, func: F) {
        match self {
            OtlpData::Metrics(resource_metrics) => {
                for resource in resource_metrics.iter_mut() {
                    for scope in resource.scope_metrics.iter_mut() {
                        for metric in scope.metrics.iter_mut() {
                            metric.name = func(metric.name.as_str());
                        } // metric loop
                    } // scope loop
                } // resource loop
            }, // match Metric

            OtlpData::Logs(resource_logs) => {
                unimplemented!();
            }, // match Logs

            OtlpData::Spans(resource_spans) => {
                for resource in resource_spans.iter_mut() {
                    for scope in resource.scope_spans.iter_mut() {
                        for span in scope.spans.iter_mut() {
                            span.name = func(span.name.as_str());
                        } // span loop
                    } // scope loop
                } // resource loop
            }, // match Span
        } // match item
    }

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

use mach::{
    id::SeriesId,
    sample::Type,
};
use std::collections::hash_map::DefaultHasher;

impl ResourceSpans {
    pub fn get_samples(&self) -> Vec<(SeriesId, u64, Vec<Type>)> {
        let mut spans = Vec::new();
        let resource_attribs = &self.resource.as_ref().unwrap().attributes;

        // Iterate over each scope
        for scope in &self.scope_spans {
            let scope_name = &scope.scope.as_ref().unwrap().name;

            // Iterate over each span
            for span in &scope.spans {
                let mut hasher = DefaultHasher::new();
                resource_attribs.hash(&mut hasher);
                scope_name.hash(&mut hasher);
                span.name.hash(&mut hasher);
                let series_id = SeriesId(hasher.finish());

                let mut v = Vec::with_capacity(1);
                v.push(Type::Bytes(bincode::serialize(&span).unwrap()));

                //let mut v = Vec::with_capacity(7);
                //v.push(Type::Bytes(span.trace_id));
                //v.push(Type::Bytes(span.span_id));
                //v.push(Type::Bytes(span.parent_span_id));
                //v.push(Type::Bytes(bincode::serialize(&span.attributes).unwrap()));
                //v.push(Type::Bytes(bincode::serialize(&span.links).unwrap()));
                //v.push(Type::Bytes(bincode::serialize(&span.events).unwrap()));
                //v.push(Type::U64(span.end_time_unix_nano));

                spans.push((series_id, span.start_time_unix_nano, v));
            }
        }
        spans
    }
}

impl ResourceMetrics {
    pub fn get_samples(&self) -> Vec<(SeriesId, u64, Vec<Type>)> {
        let mut items = Vec::new();

        let resource_attribs = &self.resource.as_ref().unwrap().attributes;

        // Iterate over each scope
        for scope in &self.scope_metrics {
            let scope_name = &scope.scope.as_ref().unwrap().name;

            // Iterate over each metric
            for metric in &scope.metrics {
                let metric_name = &metric.name;

                // There are several types of metrics
                match metric.data.as_ref().unwrap() {
                    Data::Gauge(x) => {
                        unimplemented!();
                    },

                    Data::Sum(x) => {
                        for point in &x.data_points {
                            let point_attribs = &point.attributes;
                            let mut hasher = DefaultHasher::new();
                            resource_attribs.hash(&mut hasher);
                            scope_name.hash(&mut hasher);
                            metric_name.hash(&mut hasher);
                            point_attribs.hash(&mut hasher);
                            let id = SeriesId(hasher.finish());

                            // Push sample
                            let timestamp = point.time_unix_nano;
                            let value = match point.value.as_ref().unwrap() {
                                number_data_point::Value::AsDouble(x) => *x,
                                number_data_point::Value::AsInt(x) => {
                                    let x: i32 = (*x).try_into().unwrap();
                                    x.into()
                                }
                            };
                            let value = vec![Type::F64(value)];
                            items.push((id, timestamp, value));
                        }
                    },

                    Data::Histogram(x) => {
                        for point in &x.data_points {
                            let point_attribs = &point.attributes;
                            let mut hasher = DefaultHasher::new();
                            resource_attribs.hash(&mut hasher);
                            scope_name.hash(&mut hasher);
                            metric_name.hash(&mut hasher);
                            point_attribs.hash(&mut hasher);
                            let id = SeriesId(hasher.finish());

                            // Push sample
                            let timestamp = point.time_unix_nano;
                            let value: Vec<Type> = point.bucket_counts.iter().map(|x| {
                                let x: i32 = (*x).try_into().unwrap();
                                Type::F64(x.into())
                            }).collect();
                            items.push((id, timestamp, value));
                        }
                    },

                    Data::ExponentialHistogram(x) => {
                        unimplemented!();
                    },

                    Data::Summary(x) => {
                        unimplemented!();
                    },
                } // match brace
            } // metric loop
        } // scope loop
        items
    }
}
