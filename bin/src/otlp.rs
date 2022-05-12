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

use otlp::{
    //logs::v1::ResourceLogs,
    //metrics::v1::{ResourceMetrics, metric::Data},
    //trace::v1::ResourceSpans,
    //collector::{
    //    logs::v1::{
    //        logs_service_server::{LogsService, LogsServiceServer},
    //        ExportLogsServiceRequest, ExportLogsServiceResponse,
    //    },
    //    metrics::v1::{
    //        metrics_service_server::{MetricsService, MetricsServiceServer},
    //        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    //    },
    //    trace::v1::{
    //        trace_service_server::{TraceService, TraceServiceServer},
    //        ExportTraceServiceRequest, ExportTraceServiceResponse,
    //    },
    //},
    common::v1::{KeyValue, any_value::Value, ArrayValue, AnyValue, KeyValueList},
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

pub use otlp::*;
