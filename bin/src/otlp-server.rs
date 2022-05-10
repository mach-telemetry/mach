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
pub use opentelemetry::proto as otlp;

use otlp::collector::logs::v1::{
    logs_service_server::{LogsService, LogsServiceServer},
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use otlp::collector::metrics::v1::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use otlp::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};

use tonic::{transport::Server, Request, Response, Status};

pub struct TraceServer {}

#[tonic::async_trait]
impl TraceService for TraceServer {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        println!("Got traces");
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

pub struct LogsServer {}

#[tonic::async_trait]
impl LogsService for LogsServer {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        println!("Got logs");
        Ok(Response::new(ExportLogsServiceResponse {}))
    }
}

pub struct MetricsServer {}

#[tonic::async_trait]
impl MetricsService for MetricsServer {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        println!("Got metrics");
        Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

#[tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:4317".parse().unwrap();
    Server::builder()
        .add_service(TraceServiceServer::new(TraceServer{}))
        .add_service(LogsServiceServer::new(LogsServer{}))
        .add_service(MetricsServiceServer::new(MetricsServer{}))
        .serve(addr)
        .await?;

    Ok(())
}
