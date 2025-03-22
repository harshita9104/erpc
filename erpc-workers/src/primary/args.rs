use anyhow;
use clap::Parser;
use std::path;

/// The Primary Worker to scan for partitions using two hop circuits
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Args {
    /// The path to the config file for the Primary Worker
    #[arg(short, long, default_value = "config/primary/Config.toml")]
    pub config: String,

    /// The path to the environment file for the Primary Worker
    #[arg(short, long, default_value = "config/primary/.env")]
    pub env: String,

    /// The path to log config of log4rs
    #[arg(short, long, default_value = "config/primary/log_config.yml")]
    pub log_config: String,

    /// The path to the sqlite3 database to resume the worker from previous state
    #[arg(short, long)]
    pub resume: Option<String>,
}

impl Args {
    // Setup method that handles both validation and logging initialization
    pub fn setup(&self) -> anyhow::Result<()> {
        self.validate_arguments()?;
        self.init_logging()?;
        Ok(())
    }

    // Validate arguments
    pub fn validate_arguments(&self) -> anyhow::Result<()> {
        // Check for the file existence
        for (file, description) in [
            (&self.log_config, "Log Config"),
            (&self.config, "Config"),
            (&self.env, "Environment"),
            //(&self.resume,"Path to the sqlite3 database to resume the worker"),
        ] {
            if !path::Path::new(file).exists() {
                return Err(anyhow::anyhow!("Error: {} file '{}' is missing", description, file));
            }
        }
        Ok(())
    }

    // Initialize logging
    pub fn init_logging(&self) -> anyhow::Result<()> {
        log4rs::init_file(&self.log_config, Default::default()).map_err(|e| anyhow::anyhow!("Error initializing log config: {}", e))
    }
}
