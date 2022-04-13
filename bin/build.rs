fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("mach-proto/proto/server.proto")?;
    Ok(())
}
