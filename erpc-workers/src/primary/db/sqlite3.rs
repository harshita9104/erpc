use super::super::config::Sqlite3Config;
use deadqueue::unlimited::Queue;
use erpc_scanner::work::{CompletedWork, CompletedWorkStatus};
use r2d2::Pool;
use r2d2_sqlite::rusqlite::params;
use r2d2_sqlite::SqliteConnectionManager;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::sleep;

const TABLE_NAME_FOR_STORING_RELAYS: &str = "relays";
const TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS: &str = "success_circuits";
const TABLE_NAME_FOR_STORING_FAILED_CIRCUITS: &str = "failed_circuits";
const TABLE_NAME_FOR_ONIONPERF_HOST_METADATA_STATE: &str = "onionperf";

const RETRY_INTERVAL_ON_ERROR: u64 = 200;
const MAX_INSERTION_LIMIT: u64 = 400;

pub struct Sqlite3DbClient {
    /// Connection pool to the sqlite3 database
    sqlite3_connection_pool: Arc<Pool<SqliteConnectionManager>>,

    /// A completed work queue
    completed_work_queue: Arc<Queue<CompletedWork>>,
}

impl Sqlite3DbClient {
    /// Creates a new [Sqlite3DbClient] from the given [Sqlite3Config]
    pub fn new(sqlite3_config: &Sqlite3Config) -> anyhow::Result<Self> {
        let sqlite3_connection_manager = SqliteConnectionManager::file(sqlite3_config.path.as_str());
        let sqlite3_connection_pool = Arc::new(Pool::new(sqlite3_connection_manager)?);
        let connection = sqlite3_connection_pool.get()?;
        let completed_work_queue = Arc::new(Queue::new());

        let create_relays_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                fingerprint TEXT PRIMARY KEY
            );",
            TABLE_NAME_FOR_STORING_RELAYS
        );
        connection.execute(&create_relays_table_query, params![])?;

        let create_circuits_success_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                firsthop TEXT,
                secondhop TEXT,
                message TEXT,
                timestamp INTEGER
            );",
            TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS
        );
        connection.execute(&create_circuits_success_table_query, params![])?;

        let create_circuits_failure_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                firsthop TEXT,
                secondhop TEXT,
                message TEXT,
                timestamp INTEGER
            );",
            TABLE_NAME_FOR_STORING_FAILED_CIRCUITS
        );
        connection.execute(&create_circuits_failure_table_query, params![])?;

        let create_onionperf_anaysis_file_host_date = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                onionperf_host_name TEXT,
                utc_date TEXT
            );",
            TABLE_NAME_FOR_ONIONPERF_HOST_METADATA_STATE
        );

        connection.execute(&create_onionperf_anaysis_file_host_date, params![])?;

        let sqlite3dbclient = Self {
            sqlite3_connection_pool,
            completed_work_queue,
        };

        sqlite3dbclient.start_getting_completed_works();

        Ok(sqlite3dbclient)
    }

    /// Starts receiving [CompletedWork] from the queue, it will continuously try to add
    /// required item into the sqlite3 database until it gets inserted
    pub fn start_getting_completed_works(&self) {
        let completed_work_queue = self.completed_work_queue.clone();
        let sqlite3_connection_pool = self.sqlite3_connection_pool.clone();

        tokio::task::spawn(async move {
            let mut completed_works: Vec<CompletedWork> = vec![];

            let push_completed_works = |sqlite3_connection_pool: Arc<Pool<SqliteConnectionManager>>,
                                        _completed_works: Vec<CompletedWork>| async move {
                if let Ok(mut connection) = sqlite3_connection_pool.get() {
                    if let Ok(transaction) = connection.transaction() {
                        let (success_completed_works, failed_completed_works, relays) = {
                            let mut success_completed_works: Vec<CompletedWork> = Vec::new();
                            let mut failed_completed_works: Vec<CompletedWork> = Vec::new();
                            let mut relays: HashSet<String> = HashSet::new();
                            for completed_work in &_completed_works {
                                relays.insert(completed_work.source_relay.clone());
                                relays.insert(completed_work.destination_relay.clone());
                                if completed_work.status == CompletedWorkStatus::Success {
                                    success_completed_works.push(completed_work.clone());
                                } else {
                                    failed_completed_works.push(completed_work.clone());
                                }
                            }

                            (success_completed_works, failed_completed_works, relays)
                        };

                        let add_circuit_success_query = format!(
                            "INSERT INTO {} (firsthop, secondhop, message, timestamp) VALUES (?1, ?2, ?3, ?4)",
                            TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS
                        );

                        let add_circuit_failed_query = format!(
                            "INSERT INTO {} (firsthop, secondhop, message, timestamp) VALUES (?1, ?2, ?3, ?4)",
                            TABLE_NAME_FOR_STORING_FAILED_CIRCUITS
                        );

                        let add_relays_query = format!("INSERT INTO {} (fingerprint) VALUES (?1)", TABLE_NAME_FOR_STORING_RELAYS);

                        let mut stmterror = false;

                        if let Ok(mut stmt) = transaction.prepare(&add_relays_query) {
                            for relay in &relays {
                                if stmt.execute(params![relay]).is_err() {
                                    stmterror = true;
                                };
                            }
                        }

                        if let Ok(mut stmt) = transaction.prepare(&add_circuit_success_query) {
                            for success_completed_work in success_completed_works {
                                let firsthop = &success_completed_work.source_relay;
                                let secondhop = &success_completed_work.destination_relay;
                                let timestamp = &success_completed_work.timestamp;

                                let message = {
                                    match success_completed_work.status {
                                        CompletedWorkStatus::Success => String::from("Success"),
                                        CompletedWorkStatus::Failure(ref message) => message.clone(),
                                    }
                                };
                                if stmt.execute(params![firsthop, secondhop, message, timestamp]).is_err() {
                                    stmterror = true;
                                };
                            }
                        }

                        if let Ok(mut stmt) = transaction.prepare(&add_circuit_failed_query) {
                            for failed_completed_work in failed_completed_works {
                                let firsthop = &failed_completed_work.source_relay;
                                let secondhop = &failed_completed_work.destination_relay;
                                let timestamp = &failed_completed_work.timestamp;

                                let (_, message) = {
                                    match failed_completed_work.status {
                                        CompletedWorkStatus::Success => (1, String::from("Success")),
                                        CompletedWorkStatus::Failure(ref message) => (0, message.clone()),
                                    }
                                };
                                if stmt.execute(params![firsthop, secondhop, message, timestamp,]).is_err() {
                                    stmterror = true;
                                };
                            }
                        }
                        transaction.commit().is_ok() && !stmterror
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            let mut count = 0;
            loop {
                match tokio::time::timeout(Duration::from_millis(100), completed_work_queue.pop()).await {
                    Ok(completed_work) => {
                        if count < MAX_INSERTION_LIMIT {
                            completed_works.push(completed_work);
                            count += 1;
                        } else {
                            'error_recovery: loop {
                                if push_completed_works(sqlite3_connection_pool.clone(), completed_works.clone()).await {
                                    completed_works.clear();
                                    count = 0;
                                    break 'error_recovery;
                                }
                                sleep(Duration::from_millis(RETRY_INTERVAL_ON_ERROR)).await;
                            }
                        }
                    }
                    Err(_) => {
                        // On timeout we push what we already have
                        if !completed_works.is_empty() {
                            'error_recovery: loop {
                                if push_completed_works(sqlite3_connection_pool.clone(), completed_works.clone()).await {
                                    completed_works.clear();
                                    count = 0;
                                    break 'error_recovery;
                                }

                                sleep(Duration::from_millis(RETRY_INTERVAL_ON_ERROR)).await;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Add the result of circuit creation attempt into the sqlite3 database i.e [CompletedWork]
    pub fn add_completed_work(&self, completed_work: CompletedWork) {
        self.completed_work_queue.push(completed_work);
    }

    /// Add data regarding which date's onionperf_analysis file has been already used in the
    /// database
    pub fn add_onionperf_analysisfile_time_data(&self, host_name: String, date: String) {
        let query = format!(
            "INSERT INTO {} (onionperf_host_name, utc_date) VALUES (?1, ?2)",
            TABLE_NAME_FOR_ONIONPERF_HOST_METADATA_STATE
        );

        let sqlite3_connection_pool = self.sqlite3_connection_pool.clone();

        tokio::task::spawn(async move {
            'error_recovery: loop {
                if let Ok(connection) = sqlite3_connection_pool.get() {
                    if connection.execute(&query, params![host_name, date]).is_ok() {
                        break 'error_recovery;
                    };
                    sleep(Duration::from_millis(RETRY_INTERVAL_ON_ERROR)).await;
                }
            }
        });
    }
}

/// Sqlite3 db client to get the CompletedWork out of the database
pub struct Sqlite3DbResumeClient {
    /// Connection pool to the sqlite3 database
    sqlite3_connection_pool: Arc<Pool<SqliteConnectionManager>>,
}

impl Sqlite3DbResumeClient {
    //    /// Creates a new [Sqlite3DbClient] from the given [Sqlite3Config]
    pub fn new(sqlite3_config: &Sqlite3Config) -> anyhow::Result<Self> {
        let sqlite3_connection_manager = SqliteConnectionManager::file(sqlite3_config.path.as_str());
        let sqlite3_connection_pool = Arc::new(Pool::new(sqlite3_connection_manager)?);

        let sqlite3dbclient = Self { sqlite3_connection_pool };

        Ok(sqlite3dbclient)
    }

    /// Gives all the circuits that were created from the database in the resume flag
    pub async fn get_all_completed_works(&self) -> anyhow::Result<Vec<CompletedWork>> {
        let connection = self.sqlite3_connection_pool.get()?;
        let query = format!("SELECT guard_relay, exit_relay, timestamp, status, message FROM {TABLE_NAME_FOR_STORING_FAILED_CIRCUITS}");
        let mut stmt = connection.prepare(&query)?;
        let completed_works = stmt.query_map([], |row| {
            let source_relay: String = row.get(0)?;
            let destination_relay: String = row.get(1)?;
            let timestamp: u64 = row.get(2)?;
            let status: u64 = row.get(3)?;
            let message: String = row.get(4)?;

            let status = if status == 1 {
                CompletedWorkStatus::Failure(message)
            } else {
                CompletedWorkStatus::Success
            };

            Ok(CompletedWork {
                source_relay,
                destination_relay,
                timestamp,
                status,
            })
        })?;

        let mut _completed_works = Vec::new();
        for completed_work in completed_works {
            _completed_works.push(completed_work?);
        }

        Ok(_completed_works)
    }

    /// Gives all the hosts and the dates of the onionperf analysis file that has been already used
    /// in the database
    pub async fn get_all_checked_onionperf_analysis_file_date(&self) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let connection = self.sqlite3_connection_pool.get()?;

        let query = format!("SELECT onionperf_host_name, utc_date FROM {TABLE_NAME_FOR_ONIONPERF_HOST_METADATA_STATE}");
        let mut stmt = connection.prepare(&query)?;
        let checked_hosts = stmt.query_map([], |row| {
            let onionperf_host_name: String = row.get(0)?;
            let utc_date: String = row.get(1)?;

            Ok((onionperf_host_name, utc_date))
        })?;

        let mut _checked_hosts: HashMap<String, Vec<String>> = HashMap::new();
        for checked_host in checked_hosts {
            let (onionperf_host_name, utc_date) = checked_host?;
            if let Some(dates) = _checked_hosts.get_mut(&onionperf_host_name) {
                dates.push(utc_date);
            } else {
                _checked_hosts.insert(onionperf_host_name, vec![utc_date]);
            }
        }

        Ok(_checked_hosts)
    }
}
