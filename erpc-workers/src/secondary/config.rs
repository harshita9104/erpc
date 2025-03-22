use anyhow::{self, Context};
use std::env;

#[derive(Debug, Clone)]
pub struct SecondaryWorkerConfig {
    /// No of parallel circuits build attempts to make
    /// If ```None``` then secondary worker will have to request primary worker
    pub no_of_parallel_circuit_builds: Option<u32>,

    /// The URL of the primary worker gRPC server
    pub primary_worker_server_url: String,
}

impl SecondaryWorkerConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let no_of_parallel_circuit_builds = match env::var("NO_OF_PARALLEL_CIRCUIT_BUILDS") {
            Ok(v) => Some(v.parse()?),
            Err(_) => None,
        };

        let primary_worker_server_url =
            env::var("PRIMARY_WORKER_GRPC_SERVER_URL").context("PRIMARY_WORKER_GRPC_SERVER_URL not found as environment variable")?;

        Ok(Self {
            no_of_parallel_circuit_builds,
            primary_worker_server_url,
        })
    }
}
