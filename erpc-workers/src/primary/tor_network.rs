//! This moduale contains code necessary to
//! - Store the circuit data in a Graph Database
//! - Create the graph of the Tor Network
//! - Create ```IncompleteWork```
//!
//! The approach is that, we have a struct called [TorNetwork] that basically stores the graph of the
//! entire TorNetwork
//!
//! The Vertex of this graph represents a [Node] that basically stores the fingerprint of the Relay
// The following line avoids clippy error `mutable_key_type`
// (https://rust-lang.github.io/rust-clippy/master/index.html#/mutable_key_type)
// Check and change if needed `HashSet` for 'already_used_as_exit_hasmap` and
// `nodes_set`
#![allow(clippy::mutable_key_type)]
use super::config::Sqlite3Config;
use super::db::neo4j::Neo4jDbClient;
use super::db::sqlite3::Sqlite3DbClient;
use super::db::sqlite3::Sqlite3DbResumeClient;
use super::{
    config::PrimaryWorkerConfig,
    relay::{Node, NodeStatus},
};
use crossbeam::atomic::AtomicCell;
use erpc_scanner::work::CompletedWorkStatus;
use erpc_scanner::{
    relay::{NetDirProvider, NetDirProviderEvent, RelaysPool},
    work::{CompletedWork, IncompleteWork},
};
use humantime::format_rfc3339;
use log::{error, info};
use petgraph::graph::DiGraph;
use petgraph::graph::NodeIndex;
use std::collections::HashSet;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot, Mutex, RwLock,
};
use tor_netdir::{Relay, SubnetConfig};
use tor_netdoc::doc::netstatus::RelayWeight;

type FingerprintNodeHashmap = HashMap<String, (Arc<Node>, NodeIndex)>;

/// A Datastructure to represent the Tor Network in a graph
pub struct TorNetwork {
    /// The graph to store the entire Tor Network and all the operations we are performing
    /// within the Tor Network
    ///
    /// NOTE: It's not useful(enough) right now, but in the FUTURE,
    /// it can be used to sync the database and the graph i.e if any data is missing in the
    /// database at the end of the scan it can be used to validate the database
    graph: Arc<RwLock<DiGraph<Arc<Node>, Edge>>>,

    /// The Receiver half to use when we want to receive a new NetDir
    netdir_provider: Arc<NetDirProvider>,

    /// The sending half to use when we want to send ```IncompleteWork```
    incomplete_work_sender: UnboundedSender<IncompleteWork>,

    /// The receiving half to use when we want to receive a ```IncompleteWork``` produced
    pub incomplete_work_receiver: Arc<Mutex<UnboundedReceiver<IncompleteWork>>>,

    /// The sending half to use when we have to send a ```CompletedWork``` to be used within
    /// [TorNetwork]
    pub completed_work_sender: UnboundedSender<CompletedWork>,

    /// The receiving half to use when we have to receive a ```CompletedWork```
    completed_work_receiver: Arc<Mutex<UnboundedReceiver<CompletedWork>>>,

    /// Client to the Neo4j Graph Database
    neo4j_client: Option<Arc<Neo4jDbClient>>,

    /// A [Sqlite3DbClient], which is a wrapper around Sqlite3 connection pool and
    /// has abstractions to add circuit creation attempts
    sqlite3_client: Option<Arc<Sqlite3DbClient>>,

    /// The [PrimaryWorker] config
    primary_worker_config: Arc<PrimaryWorkerConfig>,

    /// The current status of the [TorNetwork] i.e if it's either
    /// set to "NotStarted", "Running" or "Paused"
    ///
    /// It can be mutated during the runtime between "Running" and "Paused"
    /// in order to pause or continue producing [IncompleteWork]
    ///
    ///
    /// !FEATURE TODO: Add some kind of way to pause and resume through this
    tor_network_status: Arc<AtomicCell<TorNetworkStatus>>,

    /// A HashMap to get the (NodeIndex and Arc<Node>) just using the fingerprint of the items in
    /// the current NetDir
    fingerprint_node_hashmap: Arc<RwLock<FingerprintNodeHashmap>>,

    /// Data to resume from if we are provided any
    pub resume_data: Vec<CompletedWork>,

    /// Key value pair of OnionPerfHostName and OnionPerfAnalysisFileDate that have been
    /// already injested into the database
    pub checked_onionperf_dates: HashMap<String, Vec<String>>,
}

impl TorNetwork {
    /// Create a empty [TorNetwork], that is not running
    pub async fn new(primary_worker_config: Arc<PrimaryWorkerConfig>, netdir_provider: Arc<NetDirProvider>) -> anyhow::Result<Arc<Self>> {
        let neo4j_client = match primary_worker_config.neo4j_config {
            Some(ref neo4j_config) => Some(Arc::new(Neo4jDbClient::new(neo4j_config).await?)),
            None => None,
        };

        let resume_data = {
            let mut v = vec![];
            if let Some(ref args) = primary_worker_config.args {
                if let Some(ref path) = args.resume {
                    log::info!("Getting previously created circuits from the database {path}");
                    let sqlite3_config = Sqlite3Config { path: path.clone() };
                    let sqlite3_db_resume_client = Sqlite3DbResumeClient::new(&sqlite3_config)?;

                    let completed_works = sqlite3_db_resume_client.get_all_completed_works().await?;
                    log::info!(
                        "Loaded all previous created circuits from the database {path}. Total {} circuit creation attempts were stored",
                        completed_works.len()
                    );
                    v = completed_works;
                }
            }
            v
        };

        let checked_onionperf_dates = {
            let mut checked = HashMap::new();
            if let Some(ref args) = primary_worker_config.args {
                if let Some(ref path) = args.resume {
                    log::info!("Getting previous checked onionperf anlaysis file dates from the database {path}");
                    let sqlite3_config = Sqlite3Config { path: path.clone() };
                    let sqlite3_db_resume_client = Sqlite3DbResumeClient::new(&sqlite3_config)?;

                    let checked_onionperf_analysis_file_dates =
                        sqlite3_db_resume_client.get_all_checked_onionperf_analysis_file_date().await?;
                    log::info!("Loaded all checked onionperf anlaysis file dates");
                    checked = checked_onionperf_analysis_file_dates;
                }
            }
            checked
        };

        // If we need to store in sqlite3 or not and also if we would need then check if there's
        // the resume state sqlite3 database or not, if there is then we start resuming there
        // diretly
        let sqlite3_client = match primary_worker_config.sqlite3_config {
            Some(ref sqlite3_config) => {
                if let Some(ref args) = primary_worker_config.args {
                    if let Some(ref path) = args.resume {
                        let sqlite3_config = Sqlite3Config { path: path.clone() };
                        Some(Arc::new(Sqlite3DbClient::new(&sqlite3_config)?))
                    } else {
                        Some(Arc::new(Sqlite3DbClient::new(sqlite3_config)?))
                    }
                } else {
                    Some(Arc::new(Sqlite3DbClient::new(sqlite3_config)?))
                }
            }
            None => None,
        };

        if neo4j_client.is_none() && sqlite3_client.is_none() {
            error!("You haven't set any external database to store the results, please add one and run again else running this tool will be of no use");
            panic!()
        }

        let tor_network_status = Arc::new(AtomicCell::new(TorNetworkStatus::NotStarted));
        let graph = Arc::default();
        let (incomplete_work_sender, incomplete_work_receiver) = unbounded_channel::<IncompleteWork>();
        let (completed_work_sender, completed_work_receiver) = unbounded_channel::<CompletedWork>();

        let incomplete_work_receiver = Arc::new(Mutex::new(incomplete_work_receiver));
        let completed_work_receiver = Arc::new(Mutex::new(completed_work_receiver));

        let fingerprint_node_hashmap = Arc::default();

        let tor_network = Arc::new(Self {
            graph,
            netdir_provider,
            incomplete_work_sender,
            incomplete_work_receiver,
            completed_work_sender,
            completed_work_receiver,
            sqlite3_client,
            neo4j_client,
            primary_worker_config,
            tor_network_status,
            fingerprint_node_hashmap,
            resume_data,
            checked_onionperf_dates,
        });

        Ok(tor_network)
    }

    /// Get the current [TorNetworkStatus]
    #[allow(dead_code)]
    pub async fn get_tor_network_status(&self) -> TorNetworkStatus {
        self.tor_network_status.load()
    }

    /// Set the [TorNetworkStatus] to either "Pause" or "Running"
    #[allow(dead_code)]
    pub fn set_tor_network_status(&self, tor_network_status: TorNetworkStatus) {
        self.tor_network_status.store(tor_network_status);
    }

    /// Start running the TorNetwork
    pub async fn start(&self) {
        self.set_tor_network_status(TorNetworkStatus::Running);
        info!("TorNetworkStatus set to {:?}", TorNetworkStatus::Running);

        // Subscribe to the NetDirProviderEvent receiving handle
        let mut netdir_provider_event_receiver = self.netdir_provider.get_netdirprodiver_event_receiver();

        // Create a netdir and relays_pool varaible that can be accessed by everyone under this scope
        let mut current_netdir = self.netdir_provider.current_netdir().await;
        let relays: Vec<Relay<'_>> = current_netdir.relays().collect();

        // A hashmap of relays currently in the NetDir and (tor_netdir::Relay, Arc<Node>)
        let mut relays_pool = RelaysPool::empty();

        //// A hashmap of relays currently in the graph and (Arc<Node>)
        //let mut hashmap_graph_nodes = HashMap::<String, (Arc<Node>, NodeIndex)>::new();

        info!(
            "TorNetwork started with NetDir of valid lifetime upto UTC Time : {}, and has {} Relays",
            format_rfc3339(current_netdir.lifetime().valid_until()),
            relays.len()
        );

        // Create new Nodes and RelaysPool
        for relay in &relays {
            let weight = match relay.rs().rs.weight {
                RelayWeight::Measured(measured_w) => measured_w,
                _ => 1, // Giving lowest priority to the unmeasured relay
            };
            let fingerprint = relay.rsa_id().to_string();
            let node = Node::new(fingerprint.clone(), self.incomplete_work_sender.clone(), weight);
            relays_pool.add_relay(fingerprint.clone(), relay.clone(), node.clone());
        }

        // Stores any relay that isn't in the internal graph yet! from the RelaysPool(NetDir)
        // in the internal petgraph and database(neo4j or/and sqlite3)
        self.store_nodes(&relays_pool).await;

        // For each relay, check if they are in the same subnet or same family
        // with the other relay and then add those relay in to_be_used_as_exit
        // or in_same_subnet_or_family accordingly
        self.add_nodes_to_be_used_as_exit_for_each_node_in_the_graph(&relays_pool).await;

        let mut first_attempt = true;

        loop {
            match netdir_provider_event_receiver.try_recv() {
                Ok(NetDirProviderEvent::NetDirChanged(new_netdir)) => {
                    // If it's the first attempt, then it should directly go for Err(_) in this
                    // match statement, not here, it can come here because the arti has a cache and
                    // it produces DirEvent::NewConsensus in few seconds after we have already gotten the
                    // NetDir
                    if first_attempt {
                        first_attempt = false;
                    } else {
                        // New NetDir was received, handle influx of new Relay
                        current_netdir = new_netdir;
                        relays_pool = RelaysPool::empty();

                        let relays: Vec<Relay<'_>> = current_netdir.relays().collect();
                        for relay in &relays {
                            let fingerprint = relay.rsa_id().to_string();
                            let weight = match relay.rs().rs.weight {
                                RelayWeight::Measured(measured_w) => measured_w,
                                _ => 1, // Giving lowest priority to the unmeasured relay
                            };
                            let node = Node::new(fingerprint.clone(), self.incomplete_work_sender.clone(), weight);
                            relays_pool.add_relay(fingerprint.clone(), relay.clone(), node.clone());
                        }
                        info!(
                            "Received a new NetDir of valid lifetime upto UTC Time : {}, and has {} Relays",
                            format_rfc3339(current_netdir.lifetime().valid_until()),
                            relays.len()
                        );

                        self.store_nodes(&relays_pool).await;

                        self.add_nodes_to_be_used_as_exit_for_each_node_in_the_graph(&relays_pool).await;
                    }
                }

                Err(_) => {
                    // If it arrives here on the very first attempt, we turn of the first_attempt
                    first_attempt = false;

                    // No new NetDir was received, let's continue with where we left our work
                    // A scope to drop the read guard on the graph
                    {
                        let graph = self.graph.read().await;

                        // Go through all the Nodes in the graph and start(producing IncompleteWork) those nodes if they
                        // haven't been or paused
                        for node in graph.node_weights() {
                            match node.get_status() {
                                NodeStatus::NotStarted => {
                                    let node = node.clone();
                                    tokio::task::spawn(async move {
                                        node.set_status(NodeStatus::Running);
                                        Node::start(node).await.unwrap();
                                    });
                                }
                                NodeStatus::Paused => {
                                    // TODO: Add support for resume if pause is supported
                                }
                                _ => {
                                    // Do nothing beacuse eithe the relay was stopped or it's
                                    // running
                                }
                            }
                        }
                    }

                    let (sd, mut rv) = oneshot::channel();
                    let fresh_netdir_check_interval = self.primary_worker_config.primary.fresh_netdir_check_interval;
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(fresh_netdir_check_interval)).await;
                        sd.send(())
                    });

                    // Start receiving the completed work here for the next 20 mins and then
                    // got to check if there's influx of new Relay
                    let mut completed_work_receiver = self.completed_work_receiver.lock().await;
                    while let Some(completed_work) = completed_work_receiver.recv().await {
                        // A scope to drop the read guard on the fingerprint_node_hashmap
                        let (guard_relay_node, exit_relay_node) = {
                            let fingerprint_node_hashmap = self.fingerprint_node_hashmap.read().await;
                            let guard_relay = fingerprint_node_hashmap.get(completed_work.source_relay.as_str());
                            let exit_relay = fingerprint_node_hashmap.get(completed_work.destination_relay.as_str());

                            match (guard_relay, exit_relay) {
                                (Some((guard_relay, _)), Some((exit_relay, _))) => (Some(guard_relay.clone()), Some(exit_relay.clone())),
                                _ => (None, None),
                            }
                        };

                        if let (Some(guard_relay_node), Some(exit_relay_node)) = (guard_relay_node, exit_relay_node) {
                            let _completed_work = completed_work.clone();
                            tokio::spawn(async move {
                                guard_relay_node.submit_completed_work(_completed_work.clone()).await;
                                exit_relay_node.submit_completed_work(_completed_work).await;
                            });

                            // Add in the petgraph and database
                            self.store_edge(completed_work).await;
                        }
                        // Move on after 20 mins
                        if rv.try_recv().is_ok() {
                            break;
                        }
                    }
                }
            }
        }
    }

    // Each relay goes through all the relays and checks if those relays are in the
    // same subnet as them or are in the same family as them
    async fn add_nodes_to_be_used_as_exit_for_each_node_in_the_graph(&self, relays_pool: &RelaysPool<'_, Arc<Node>>) {
        info!("Started filtering the two hop circuit combinations that a Relay should make and ignore(if a relay should make circuit with relay that's in the same subnet/family). Please wait few seconds");
        let start_time = Instant::now();
        let subnet_config = SubnetConfig::default();

        let circuit_with_relay_on_same_subnet = self.primary_worker_config.primary.circuit_with_relay_on_same_subnet;
        let circuit_with_relay_on_same_family = self.primary_worker_config.primary.circuit_with_relay_of_same_family;

        let nodes: Vec<Arc<Node>> = {
            let graph = self.graph.read().await;
            graph.node_weights().cloned().collect()
        };

        // NOTE : It should run only at the initial of program
        // TODO: Make sure it runs only once if it's allowed to run
        let mut already_used_as_exit_nodes: HashMap<String, HashSet<Arc<Node>>> = HashMap::new();
        for completed_work in &self.resume_data {
            if let Some((_, source_node)) = relays_pool.relays_hashmap.get(&completed_work.source_relay) {
                if let Some((_, destination_node)) = relays_pool.relays_hashmap.get(&completed_work.destination_relay) {
                    let node_set = already_used_as_exit_nodes.entry(source_node.fingerprint.clone()).or_default();
                    node_set.insert(destination_node.clone());
                }
            }
        }

        log::info!("The total keys were {:?}", already_used_as_exit_nodes.keys().len());
        // We'll go through the all the nodes in the graph (i.e the Relays that are in the NetDir and that are
        // not in the NetDir) that we had stored
        //
        // Only those Nodes that are in the RelaysPool will be considered here, because it has the
        // corresponding ```tor_netdir::Relay```
        for node_1 in &nodes {
            if let Some((relay_1, _)) = relays_pool.get_relay(node_1.fingerprint.as_str()) {
                // Adding relays to the ignore list because they are in the same subnet as relay_1
                if !circuit_with_relay_on_same_subnet {
                    for (relay_2, node_2) in relays_pool.relays_hashmap.values() {
                        // If we are not allowed to make circut with relay on the same subnet then we
                        // add that relay in the ignore list

                        if relay_1.in_same_subnet(relay_2, &subnet_config) {
                            node_1.add_a_node_in_same_subnet_or_family(node_2.clone()).await;
                        }
                    }
                }

                // Adding relays to the ignore list because they are in the same subnet as relay_1
                if !circuit_with_relay_on_same_family {
                    let md = relay_1.md().family().members();
                    for rsa_identity in md {
                        if let Some((_, node_2)) = relays_pool.get_relay(rsa_identity.to_string()) {
                            node_1.add_a_node_in_same_subnet_or_family(node_2.clone()).await;
                        }
                    }
                }

                // Now that we have set the ignore list(in_same_subnet_or_family) of all the
                // relays(Node). Each Node will try to add a Node that's not been already added
                // (checking the to_be_used_as_exit and already_used_as_exit) and that's not
                // in the ignore list

                let _nodes = nodes.clone();
                let node_already_used_as_exit_nodes = {
                    let mut v = vec![];
                    if let Some(_already_used_as_exit_nodes) = already_used_as_exit_nodes.get(&node_1.fingerprint) {
                        v = _already_used_as_exit_nodes.iter().cloned().collect();
                    }
                    v
                };

                let _node_1 = node_1.clone();
                tokio::spawn(async move {
                    // The to_be_used_as_exit lock gets acquired, so we don't have to worry about
                    // Node getting started before to_be_used_as_exit as is filled
                    _node_1.add_relays_in_already_used_as_exit(node_already_used_as_exit_nodes).await;
                    _node_1.add_relays_in_to_be_used_as_exit(_nodes).await;
                });
            }
        }

        let end_time = Instant::now();
        info!("Finished filtering in {:?} ", end_time.duration_since(start_time));
    }

    /// Add [Node] i.e a Relay in the graph with no edge between them, from
    /// the ```RelaysPool```
    ///
    /// It only adds those [Node] in the graph, that doesn't have the same
    /// fingerprint as other [Node] in the graph, which means all the
    /// [Node] in the graph are unique.
    ///
    /// **NOTE** :
    /// - It adds a Node in the **petgraph** and (if turned on) in the **Neo4j Database**
    /// - It doesn't add anything in the sqlite3 database because it has tables and there's no
    /// concept of nodes, so we only store the edge i.e the circuit creation attempt,
    /// which can be either **failed** or **success**
    pub async fn store_nodes(&self, relays_pool: &RelaysPool<'_, Arc<Node>>) {
        info!(
            "Attempting to add unique relays from the NetDir in the internal petgraph {} ",
            if self.primary_worker_config.primary.neo4j_allowed {
                "and Neo4j Database"
            } else {
                ""
            },
        );
        let mut count = 0;
        for (_, node) in relays_pool.relays_hashmap.values() {
            if self.store_node_checked(node.clone()).await {
                count += 1;
            }
        }
        info!(
            "Added {count} unique relays in the internal petgraph {}",
            if self.primary_worker_config.primary.neo4j_allowed {
                "and spawned tokio task to add unique relays in neo4j graph database"
            } else {
                ""
            },
        );
    }

    /// Add the result of circuit creation attempt
    ///
    /// It adds the data in the internal petgraph, graph database(if it's allowed)
    /// and the sqlite3 databasee(if it's allowed)
    pub async fn store_edge(&self, completed_work: CompletedWork) {
        // Add in the sqlite3 database(if it's allowed)
        if let Some(ref sqlite3_client) = self.sqlite3_client {
            let sqlite3_client = sqlite3_client.clone();
            let completed_work = completed_work.clone();
            tokio::spawn(async move {
                sqlite3_client.add_completed_work(completed_work);
            });
        }

        // Add in the Neo4j Database(if it's allowed)
        if let Some(ref neo4j_client) = self.neo4j_client {
            let neo4j_client = neo4j_client.clone();
            let completed_work = completed_work.clone();
            tokio::spawn(async move {
                neo4j_client.add_completed_work(completed_work).await;
            });
        }

        // Add in the petgraph
        let mut graph = self.graph.write().await;
        let fingerprint_node_hashmap = self.fingerprint_node_hashmap.read().await;

        let guard_relay = fingerprint_node_hashmap.get(&completed_work.source_relay);
        let exit_relay = fingerprint_node_hashmap.get(&completed_work.destination_relay);

        if let (Some((_, guard_relay_node_index)), Some((_, exit_relay_node_index))) = (guard_relay, exit_relay) {
            let edge = Edge {
                status: completed_work.status,
            };
            graph.add_edge(*guard_relay_node_index, *exit_relay_node_index, edge);
        };
    }

    pub fn store_onionperf_analysis_time_data(&self, host_name: String, date: String) {
        if let Some(ref sqlite3_client) = self.sqlite3_client {
            let sqlite3_client = sqlite3_client.clone();
            sqlite3_client.add_onionperf_analysisfile_time_data(host_name, date);
        }
    }

    /// It checks if the Node already exists in the graph by checking
    /// the fingerprint of the provided [Node] against fingerprint of
    /// all [Node] in the graph
    ///
    /// A [Node] is only added to a graph database and the internal petgraph
    /// it's not added in the sqlite3 database
    ///
    /// Returns true if the Node wasn't duplicate and was added into the graph
    pub async fn store_node_checked(&self, node: Arc<Node>) -> bool {
        let fingerprint = node.fingerprint();
        let mut graph = self.graph.write().await;
        let mut fingerprint_node_hashmap = self.fingerprint_node_hashmap.write().await;

        if !fingerprint_node_hashmap.contains_key(&fingerprint) {
            // Add a Node in the petgraph
            let node_index = graph.add_node(node.clone());

            // Add the (fingerprint, (node, node_index)) in the hashmap
            fingerprint_node_hashmap.insert(fingerprint, (node.clone(), node_index));

            // Add a Node in the Neo4j Database
            if let Some(ref neo4j_client) = self.neo4j_client {
                let neo4j_client = neo4j_client.clone();
                tokio::spawn(async move {
                    neo4j_client.add_node(node.clone()).await;
                });
            }
            true
        } else {
            false
        }
    }
}

/// Represents the status of the [TorNetwork] i.e
/// if it's either producing [IncompleteWork] or not and reciving [CompletedWork]
#[derive(Debug, Clone, Copy)]
pub enum TorNetworkStatus {
    /// [TorNetwork] is not started yet and it's not producing [IncompleteWork]
    NotStarted,

    /// [TorNetwork] is running and it's [IncompleteWork]
    Running,

    /// [TorNetwork] is paused and it's not producing [IncompleteWork]
    #[allow(dead_code)]
    Paused,
}

/// The Node of the Tor Network graph, which holds the information about the relay, it just holds the data to index a relay
/// from Relays Pool through it's RSA ID

/// The Edge of the Nodes in the Tor Network graph, which basically represents the type of
/// circuit a relay has with other relay
///
/// If there's no edge between two relays then it means that they were in the same family or they
/// are in the same /16 subnet family
///
/// (See : https://gitlab.torproject.org/tpo/network-health/erpc/-/issues/17)
#[derive(Debug)]
#[allow(dead_code)]
pub struct Edge {
    status: CompletedWorkStatus,
}
