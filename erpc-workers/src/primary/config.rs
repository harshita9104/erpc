#![allow(dead_code)]

use anyhow::Context;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

use super::args::Args;

/// Configurations to control the behaviour of the [PrimaryWorker]
#[derive(Debug, Deserialize)]
pub struct PrimaryWorkerConfig {
    /// The attributes to control the primary worker behaviour
    pub primary: PrimaryConfig,

    /// The attributes to control the secondary worker behaviour within [PrimaryWorker]
    pub secondary: SecondaryConfig,

    /// The attributres and credentials to communicate with Neo4j Database
    #[serde(skip_deserializing)]
    pub neo4j_config: Option<Neo4jConfig>,

    /// The attributres and credentials to spin gRPC server for secondary Workers
    #[serde(skip_deserializing)]
    pub secondary_workers_grpc_server_config: Option<SecondaryWorkerGRPCServerConfig>,

    /// The attributres to control the sqlite3 database
    /// TODO: This is not used as of right now, make use of it
    #[serde(skip_deserializing)]
    pub sqlite3_config: Option<Sqlite3Config>,

    /// The valid arguments passed to the application
    #[serde(skip_deserializing)]
    pub args: Option<Args>,
}

impl PrimaryWorkerConfig {
    /// Load the configurations from  ".env" file and provided
    /// ```Config.toml``` file
    pub fn load_from_env_and_config(args: &Args) -> anyhow::Result<Self> {
        let config_file_path = args.config.as_str();
        let mut config_file = File::open(config_file_path)?;
        let mut config_file_contents = String::new();
        config_file.read_to_string(&mut config_file_contents)?;

        let mut config: Self = toml::from_str(config_file_contents.as_str())?;

        // Check for neo4j config
        config.neo4j_config = if config.primary.neo4j_allowed {
            let uri = env::var("NEO4J_DB_ADDR").context("NEO4J_DB_ADDR not found as environment variable")?;
            let username = env::var("NEO4J_DB_USERNAME").context("NEO4J_DB_USERNAME not found as environment variable")?;
            let password = env::var("NEO4J_DB_PASSWORD").context("NEO4J_DB_PASSWORD not found as environment variable")?;
            Some(Neo4jConfig { uri, username, password })
        } else {
            None
        };

        // Check for sqlite3 config
        config.sqlite3_config = if config.primary.sqlite_allowed {
            let path = env::var("SQLITE3_FILE_PATH").context("SQLITE3_FILE_PATH not found as environment variable")?;
            Some(Sqlite3Config { path })
        } else {
            None
        };

        // Check for secondary worker grpc server config
        config.secondary_workers_grpc_server_config = if config.primary.secondary_allowed {
            let socket_addr = {
                env::var("SECONDARY_WORKER_GRPC_SERVER_ADDR")
                    .context("SECONDARY_WORKER_GRPC_SERVER_ADDR not found as environment variable")?
                    .parse()
                    .context("The SocketAddr set at SECONDARY_WORKER_GRPC_SERVER_ADDR is not valid")?
            };
            Some(SecondaryWorkerGRPCServerConfig { socket_addr })
        } else {
            None
        };

        config.args = Some(args.clone());

        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub struct PrimaryConfig {
    /// No of parallel circuti creation attempts [PrimaryWorker] can make
    pub no_of_parallel_circuits: u32,

    /// No of secondary workers allowed to connect
    pub secondary_allowed: bool,

    /// If PrimaryWorker Scanner is allowed to create circuits
    pub primary_worker_scanner_allowed: bool,

    /// Max allowed no of secondary workers
    /// TODO :Make use of this
    pub max_allowed_secondary: u32,

    /// If circuit with Relay on the same subnet should be attempted
    pub circuit_with_relay_on_same_subnet: bool,

    /// If circuit with Relay on the same family should be attempted
    pub circuit_with_relay_of_same_family: bool,

    /// The size of each [IncompleteWork] batch
    pub work_batch_size: u32,

    /// The time interval in which the arrival of fresh NetDir should be checked
    pub fresh_netdir_check_interval: u64,

    /// Choose either to use neo4j database or not
    pub neo4j_allowed: bool,

    /// Choose either to use sqlite databse or not
    ///
    /// It might be suitable when you want to run this app instantly without
    /// setting up neo4j and caring about the features it provides
    pub sqlite_allowed: bool,

    /// The minimum circuit build timeout that we should aim for
    /// in millisecond
    pub min_circuit_build_timeout: i32,

    /// Whether to use onionperf hosts to check for circuits or not
    pub use_onionperf: bool,

    /// The onionperf hosts to be used (to download OnionPerf analysis files or other logs)
    pub onionperf_hosts: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SecondaryConfig {
    /// No of paralle circuit creation attempts that primary worker can make
    pub no_of_parallel_circuits: u32,
}

/// The configurations and credentials to communicate with the Neo4j database
#[derive(Debug, Clone, Default)]
pub struct Neo4jConfig {
    /// The URI of the database
    pub uri: String,

    /// The username for the database
    pub username: String,

    /// The password for the database
    pub password: String,
}

/// The Configuration to control the gRPC server exposed for secondary Workers
#[derive(Debug, Clone)]
pub struct SecondaryWorkerGRPCServerConfig {
    /// The SocketAddress at which the gRPC server is running
    pub socket_addr: SocketAddr,
}

/// The config to sqlite3 database
///
/// # Example
///
/// ```ignore
///
/// let path = "x/y/z.db"
/// let sqlite3_config = Sqlite3Config { path };
///
/// ```
#[derive(Debug, Clone)]
pub struct Sqlite3Config {
    /// The sqlite3 database file path
    pub path: String,
}
