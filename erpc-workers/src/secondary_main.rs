#![warn(unused_imports)]
#![warn(noop_method_call)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::semicolon_if_nothing_returned)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]

mod secondary;
mod shared;

use anyhow::Context;
use arti_client::{TorClient, TorClientConfig};
use dotenv::dotenv;
use env_logger::{Builder, Target};
use erpc_scanner::relay::NetDirProvider;
use log::{info, warn};
use secondary::{config::SecondaryWorkerConfig, worker::SecondaryWorker};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().context(".env file not found")?;
    let mut builder = Builder::from_default_env();
    builder.target(Target::Stdout).init();

    info!("Loading the configs from .env file");
    let secondary_worker_config = SecondaryWorkerConfig::from_env()?;
    info!("Current configs set to {secondary_worker_config:#?}");

    // We spin a TorClient and distribute it's parts i.e DirMgr and CircMgr,
    // similar to how we do it in PrimaryWorker
    info!("Spawning a bootstrapped arti_client::TorClient");
    //let mut tor_client_config_builder = TorClientConfigBuilder::default();
    // TODO: Add build timeout
    //    tor_client_config_builder
    //        .override_net_params()
    //        .insert(String::from("cbtmintimeout"), primary_worker_config.primary.min_circuit_build_timeout);
    //let tor_client_config = tor_client_config_builder.build()?;
    let tor_client_config = TorClientConfig::default();
    let tor_client = TorClient::create_bootstrapped(tor_client_config).await?;
    info!("Spawned a bootstrapped arti_client::TorClient");

    let circmgr = tor_client.circmgr().clone();
    let dirmgr = tor_client.dirmgr().clone();
    let netdir_provider = NetDirProvider::from_dirmgr(dirmgr).await?;

    let mut secondary_worker = SecondaryWorker::new(secondary_worker_config, netdir_provider, circmgr);

    secondary_worker.start().await?;

    Ok(())
}
