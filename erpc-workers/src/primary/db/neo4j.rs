use super::super::{config::Neo4jConfig, relay::Node};
use deadqueue::unlimited::Queue;
use erpc_scanner::work::{CompletedWork, CompletedWorkStatus};
use log::error;
use neo4rs::{query, Graph};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const RETRY_INTERVAL_ON_ERROR: u64 = 200;
const MAX_INSERTION_LIMIT: u64 = 400;

pub struct Neo4jDbClient {
    /// The Neo4j connection
    graph: Arc<neo4rs::Graph>,

    /// A completed work queue
    completed_work_queue: Arc<Queue<CompletedWork>>,
}

impl Neo4jDbClient {
    /// Creates a new [Neo4jDbClient] from the given [Neo4jConfig]
    pub async fn new(neo4j_config: &Neo4jConfig) -> anyhow::Result<Self> {
        let uri = neo4j_config.uri.clone();
        let username = neo4j_config.username.clone();
        let password = neo4j_config.password.clone();

        let graph = Arc::new(neo4rs::Graph::new(uri, username, password).await?);
        let completed_work_queue = Arc::new(Queue::new());

        let neo4jdbclient = Self {
            graph,
            completed_work_queue,
        };

        neo4jdbclient.start_getting_completed_works();

        Ok(neo4jdbclient)
    }

    async fn push_completed_works(neo4j_connection: Arc<Graph>, completed_works: Vec<CompletedWork>) -> bool {
        if let Ok(transaction) = neo4j_connection.start_txn().await {
            for completed_work in completed_works {
                let guard_relay = &completed_work.source_relay;
                let exit_relay = &completed_work.destination_relay;
                let timestamp = completed_work.timestamp;
                let (status, message) = {
                    match completed_work.status {
                        CompletedWorkStatus::Success => (1, String::from("Success")),
                        CompletedWorkStatus::Failure(ref message) => (0, message.clone()),
                    }
                };
                let raw_query = format!(
                    "MERGE (guard_relay:Relay {{fingerprint: $guard_relay_fingerprint}})
                           MERGE (exit_relay:Relay {{fingerprint: $exit_relay_fingerprint}})
                           MERGE (guard_relay)-[rel:{}]->(exit_relay)
                           SET rel.message = $message, rel.timestamp = $timestamp ",
                    if status == 1 { "CIRCUIT_SUCCESS" } else { "CIRCUIT_FAILURE" }
                );

                let query = query(&raw_query)
                    .param("guard_relay_fingerprint", guard_relay.as_str())
                    .param("exit_relay_fingerprint", exit_relay.as_str())
                    .param("message", message)
                    .param("timestamp", timestamp.to_string());

                if transaction.execute(query).await.is_err() {
                    return false;
                }
            }
            transaction.commit().await.is_ok()
        } else {
            false
        }
    }

    fn start_getting_completed_works(&self) {
        let completed_work_queue = self.completed_work_queue.clone();
        let neo4j_connection = self.graph.clone();

        tokio::task::spawn(async move {
            let mut count = 0;
            let mut completed_works: Vec<CompletedWork> = Vec::new();

            loop {
                match tokio::time::timeout(Duration::from_millis(50), completed_work_queue.pop()).await {
                    Ok(completed_work) => {
                        if count < MAX_INSERTION_LIMIT {
                            completed_works.push(completed_work);
                            count += 1;
                        } else {
                            'error_recovery: loop {
                                if Self::push_completed_works(neo4j_connection.clone(), completed_works.clone()).await {
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
                                if Self::push_completed_works(neo4j_connection.clone(), completed_works.clone()).await {
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

    /// Add an edge i.e circuit creation attempt into the database
    pub async fn add_completed_work(&self, completed_work: CompletedWork) {
        self.completed_work_queue.push(completed_work);
    }

    /// Add the indexing item of the relay i.e the fingerprint of the relay
    pub async fn add_node(&self, node: Arc<Node>) {
        let query = query("MERGE (:Relay {fingerprint:  $fingerprint})").param("fingerprint", node.fingerprint());
        if let Err(err) = self.graph.run(query).await {
            error!(
                "Coudln't add the relay with fingerprint {} in the neo4j database | Error : {err:?}",
                node.fingerprint(),
            );
        }
    }
}
