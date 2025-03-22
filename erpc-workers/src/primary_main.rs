//! Some heavy features to add in the future
//!
//! - Add an gRPC interface to stop including a certian relay in the scan(in a certian
//! direction i.e towards it or away from it) during the runtime
//! - Abilityt to pause and resume
//! - Ability to load current graph from the database and start working
//! - Ability to choose only specific relays(i.e through the IP address)
//! - Ability to stop the gRPC server during the Runtime
//! - Ability to start the gRPC server during the Runtime
#![allow(clippy::map_entry)]
#![allow(dead_code)]
#![warn(unused_imports)]
#![warn(noop_method_call)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::semicolon_if_nothing_returned)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![allow(clippy::mutable_key_type)]

pub mod error;
mod primary;
mod shared;

use anyhow::Context;
use arti_client::{config::TorClientConfigBuilder, TorClient};
use clap::Parser;
use erpc_scanner::relay::NetDirProvider;
use log::info;
use primary::{args::Args, config::PrimaryWorkerConfig, worker::PrimaryWorker};
use std::process;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Loading the arguments and environment variables
    let args = Args::parse();

    // Setup validation and logging
    if let Err(e) = args.setup() {
        use std::io::{self, Write};
        let _ = writeln!(io::stderr(), "{}", e);
        process::exit(1);
    }

    dotenv::from_path(args.env.as_str()).context("The env file was not found")?;

    info!("Loading the configs from {} and environment variables from {}", args.env, args.env);

    let primary_worker_config = Arc::new(PrimaryWorkerConfig::load_from_env_and_config(&args)?);
    info!("Current configs set to {primary_worker_config:#?}");

    info!("Spawning a bootstrapped arti_client::TorClient");
    let mut tor_client_config_builder = TorClientConfigBuilder::default();
    tor_client_config_builder.override_net_params().insert(
        String::from("cbtmintimeout"),
        primary_worker_config.primary.min_circuit_build_timeout,
    );
    let tor_client_config = tor_client_config_builder.build()?;
    let tor_client = TorClient::create_bootstrapped(tor_client_config).await?;
    info!("Spawned a bootstrapped arti_client::TorClient");

    let circmgr = tor_client.circmgr().clone();

    let dirmgr = tor_client.dirmgr().clone();
    let netdir_provider = NetDirProvider::from_dirmgr(dirmgr).await?;

    // Create primary worker based on configuration
    let primary_worker = PrimaryWorker::new(primary_worker_config, netdir_provider.clone(), circmgr).await?;

    primary_worker.start().await;
    Ok(())
}
