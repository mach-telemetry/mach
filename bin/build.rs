fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("mach-proto/proto/server.proto")?;
    tonic_build::configure()
        .compile(
            &[
                "opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
                "opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
            ],
            &["opentelemetry-proto/"],
        )
        .unwrap();
    Ok(())
}
