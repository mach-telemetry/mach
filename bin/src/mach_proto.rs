mod mach_proto {
    tonic::include_proto!("mach_proto"); // The string specified here must match the proto package name
}

pub use mach_proto::{
    ExportMessage,
    ExportResponse,
    mach_service_server::{MachService, MachServiceServer},
    mach_service_client::MachServiceClient
};


