use super::{
    config::PrimaryWorkerConfig,
    service::{PrimaryWorkerService, WorkerServiceServer, DESCRIPTOR_SET},
    tor_network::TorNetwork,
};
use crate::shared::Worker;
use anyhow::Context;
use crossbeam::atomic::AtomicCell;
use deadqueue::unlimited::Queue;
use erpc_metrics::runner::OnionPerfRunner;
use erpc_scanner::{
    relay::NetDirProvider,
    scanner::Scanner,
    work::{CompletedWork, IncompleteWork},
};
use futures::future::join;
use log::{debug, error, info};
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    Mutex,
};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tor_circmgr::CircMgr;
use tor_rtcompat::PreferredRuntime;

pub struct PrimaryWorker {
    /// The [Scanner] used by PrimaryWorker to use the IncompleteWork and complete them to
    /// CompletedWork
    scanner: Arc<Scanner>,

    // The [TorNetwork] that's responsbile for creating IncompleteWork(s) and processing
    // the CompletedWork(s) and saving them to the Database
    //
    // It's the core place from where all the request of circuit to be created comes in the
    // form of IncompleteWork
    tor_network: Arc<TorNetwork>,

    /// The configurations that primary worker needs to function
    config: Arc<PrimaryWorkerConfig>,

    /// Denotes whether the gRPC Server is on or not and PrimaryWorker Scanner is on or not
    distribution_mode: DistributionMode,

    /// A Queue/Buffer that holds all the incoming [IncompleteWork] from the [TorNetwork]
    incomplete_work_queue: Arc<Queue<IncompleteWork>>,

    /// The load handle of the Primary Worker Scanner
    primary_worker_scanner_load_handle: Arc<LoadHandle>,

    /// All clients to connect and get data from OnionPerf hosts
    onionperf_runners: Vec<Arc<OnionPerfRunner>>,
}

impl PrimaryWorker {
    /// Create a new [PrimaryWorker] based on given [PrimaryWorkerConfig]
    pub async fn new(
        config: Arc<PrimaryWorkerConfig>,
        netdir_provider: Arc<NetDirProvider>,
        circmgr: Arc<CircMgr<PreferredRuntime>>,
    ) -> anyhow::Result<Arc<Self>> {
        let scanner = Arc::new(Scanner::new(config.primary.no_of_parallel_circuits, netdir_provider.clone(), circmgr).await?);
        let tor_network = TorNetwork::new(config.clone(), netdir_provider).await?;
        let distribution_mode = DistributionMode::from_primary_worker_config(&config);
        let incomplete_work_queue = Arc::new(Queue::new());
        let primary_worker_scanner_load_handle = LoadHandle::new(0);

        // If the configuration says to not use onionperf then there will be empty vector
        // of OnionPerfRunner
        let onionperf_runners = if config.primary.use_onionperf {
            let mut runners = vec![];
            for host_name in &config.primary.onionperf_hosts {
                runners.push(Arc::new(OnionPerfRunner::new(host_name.as_str())));
            }
            runners
        } else {
            vec![]
        };

        let primary_worker = Arc::new(Self {
            scanner,
            tor_network,
            config,
            distribution_mode,
            incomplete_work_queue,
            primary_worker_scanner_load_handle,
            onionperf_runners,
        });

        Ok(primary_worker)
    }

    /// Run scanner on [PrimaryWorker]
    async fn run_primary_worker_scanner(self: Arc<Self>) -> Arc<Mutex<UnboundedReceiver<CompletedWork>>> {
        let incomplete_work_queue = self.incomplete_work_queue.clone();

        let (primary_worker_completed_work_sender, primary_worker_completed_work_receiver) = unbounded_channel::<CompletedWork>();
        let primary_worker_completed_work_receiver = Arc::new(Mutex::new(primary_worker_completed_work_receiver));

        let scanner = self.scanner.clone();
        let primary_worker_scanner_load_handle = self.primary_worker_scanner_load_handle.clone();
        let no_of_parallel_circuits = self.config.primary.no_of_parallel_circuits;

        // Tokio task to continuously get the ```IncompleteWork``` from the queue and fetch it to
        // the scanner
        let mut count = 1;
        tokio::spawn(async move {
            loop {
                while primary_worker_scanner_load_handle.current_load() < no_of_parallel_circuits {
                    let incomplete_work = incomplete_work_queue.pop().await;
                    debug!(
                        "({count}) Scheduled to build two hop circuit from relay {} to {}",
                        incomplete_work.source_relay, incomplete_work.destination_relay
                    );
                    scanner
                        .push_incomplete_work(incomplete_work)
                        .expect("Scanner is not receiving work");
                    primary_worker_scanner_load_handle.increase_load_by_1();
                    count += 1;
                }

                // The PrimaryWorker Scanner has reached its limit of max parallel circuit attempts,
                // after 1 second it will be tried again to reach the limit
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        // Task to continuously get the ```CompletedWork``` made from ```IncompleteWork``` fetched
        // into the scanner
        let scanner = self.scanner.clone();
        let primary_worker_scanner_load_handle = self.primary_worker_scanner_load_handle.clone();

        tokio::spawn(async move {
            while let Some(completed_work) = scanner.recv_completed_work().await {
                debug!(
                    "Completed two hop circuit build attempt from relay {} to {}",
                    completed_work.source_relay, completed_work.destination_relay
                );
                primary_worker_completed_work_sender
                    .send(completed_work)
                    .expect("The primary worker Scanner can't send CompletedWork");
                primary_worker_scanner_load_handle.decrease_load_by_1();
            }
        });
        primary_worker_completed_work_receiver
    }

    /// Starts the [TorNetwork], listening to the [IncompleteWork] that [TorNetwork] produces
    /// and putting it in the **Queue**
    fn start_tor_network(&self) {
        let tor_network = self.tor_network.clone();
        tokio::spawn(async move {
            tor_network.start().await;
        });

        let incomplete_works_queue = self.incomplete_work_queue.clone();
        let incommplete_work_receiver = self.tor_network.incomplete_work_receiver.clone();
        tokio::spawn(async move {
            let mut incomplete_work_receiver = incommplete_work_receiver.lock().await;
            while let Some(incomplete_work) = incomplete_work_receiver.recv().await {
                incomplete_works_queue.push(incomplete_work);
            }
        });
    }

    /// Start the onionperf runner
    fn start_onionperf_runners(&self) {
        // Start running all onionperf_runner
        for onionperf_runner in &self.onionperf_runners {
            let onionperf_runner = onionperf_runner.clone();
            tokio::spawn(async move {
                log::info!("Started the runner");
                onionperf_runner.start().await;
            });
        }

        // Start receiving data from onionperf_runner
        for onionperf_runner in &self.onionperf_runners {
            let tor_network = self.tor_network.clone();
            let onionperf_runner = onionperf_runner.clone();
            let checked_onionperf_dates = tor_network.checked_onionperf_dates.clone();

            tokio::spawn(async move {
                let mut onionperf_analysis_receiver_stream = onionperf_runner.onionperf_analysis_receiver_stream.lock().await;
                while let Some(onionperf_analysis) = onionperf_analysis_receiver_stream.next().await {
                    let host_name = onionperf_analysis.data.keys().next().unwrap().clone();
                    let date = onionperf_analysis.time.as_ref().unwrap();

                    if let Some(hosts) = checked_onionperf_dates.get(&host_name) {
                        if hosts.contains(date) {
                            log::debug!(
                                "Received a OnionPerfAnalysis file from {} which was already used",
                                onionperf_runner.host_name
                            );
                            continue;
                        }
                    }

                    log::debug!("Received a fresh OnionPerfAnalysis file from {}", onionperf_runner.host_name);
                    let successful_two_hops = onionperf_analysis.get_all_successful_two_hop_paths();
                    for onionperf_two_hop_circuit in successful_two_hops {
                        let completed_work = CompletedWork {
                            source_relay: onionperf_two_hop_circuit.first_hop,
                            destination_relay: onionperf_two_hop_circuit.second_hop,
                            status: erpc_scanner::work::CompletedWorkStatus::Success,
                            timestamp: onionperf_two_hop_circuit.timestamp,
                        };
                        tor_network.store_edge(completed_work).await;
                    }
                    tor_network.store_onionperf_analysis_time_data(host_name, date.clone());
                }
            });
        }
    }

    /// Start the work on the TorNetwork
    ///
    /// We must realize that [TorNetwork] at self.tor_network is responsbile for producing the
    /// [IncompleteWork] within this application and the gRPC server that has handles to consume
    /// [IncompleteWork] and produce CompletedWork is one entity responsbile for resolving works
    /// and other entity is PrimaryWorker's internal worker itself(TO BE IMPLEMENTED)
    ///
    /// Our job is to listen for those works and then distribute those work between
    /// the gRPC server that's distributing the IncompleteWork and the PrimaryWorker's internal
    /// worker that's doing the work
    ///
    /// Steps :
    ///  - Listen on the receiging half of the  channel exposed by [TorNetwork] that gives us
    ///  ```IncompleteWork``` over time
    ///  -
    pub async fn start(self: Arc<Self>) {
        self.start_tor_network();
        if self.config.primary.use_onionperf {
            self.start_onionperf_runners();
        }

        let distribution_mode = self.distribution_mode;
        info!("Running in distribution mode : {distribution_mode:?}");

        let tor_network = self.tor_network.clone();
        let tor_network_completed_work_sender_0 = tor_network.completed_work_sender.clone();
        let tor_network_completed_work_sender_1 = tor_network.completed_work_sender.clone();

        match distribution_mode {
            DistributionMode::OnlyPrimaryWorker => {
                let primary_worker_completed_work_receiver = self.clone().run_primary_worker_scanner().await;

                // Receive [CompletedWork] from PrimaryWorker Scanner and give it back to TorNetwork
                let mut primary_worker_completed_work_receiver = primary_worker_completed_work_receiver.lock().await;
                while let Some(completed_work) = primary_worker_completed_work_receiver.recv().await {
                    tor_network_completed_work_sender_0.send(completed_work).unwrap();
                }
            }

            // Only gRPC Service is used
            DistributionMode::OnlyGRPCService => {
                let grpc_completed_work_receiver = self.run_secondary_worker_grpc_service();

                // Receive [CompletedWork] from SecondaryWorker(s) and give it back to TorNetwork
                let mut grpc_completed_work_receiver = grpc_completed_work_receiver.lock().await;
                while let Some(completed_work) = grpc_completed_work_receiver.recv().await {
                    tor_network_completed_work_sender_0.send(completed_work).unwrap();
                }
            }

            // Workload distribution happens for both the grPC Service and PrimaryWorker Scanner
            DistributionMode::PrimaryWorkerAndGRPCService => {
                let grpc_completed_work_receiver = self.run_secondary_worker_grpc_service();

                let primary_worker_completed_work_receiver = self.clone().run_primary_worker_scanner().await;

                // Receive [CompletedWork] from SecondaryWorker(s) and give it back to TorNetwork
                let completed_work_task_0 = async move {
                    let mut grpc_completed_work_receiver = grpc_completed_work_receiver.lock().await;
                    while let Some(completed_work) = grpc_completed_work_receiver.recv().await {
                        tor_network_completed_work_sender_0.send(completed_work).unwrap();
                    }
                };

                // Receive [CompletedWork] from PrimaryWorker Scanner and give it back to TorNetwork
                let completed_work_task_1 = async move {
                    let mut primary_worker_completed_work_receiver = primary_worker_completed_work_receiver.lock().await;
                    while let Some(completed_work) = primary_worker_completed_work_receiver.recv().await {
                        tor_network_completed_work_sender_1.send(completed_work).unwrap();
                    }
                };

                join(completed_work_task_0, completed_work_task_1).await;
            }
        }
    }

    /// Start the Secondary Worker gRPC server if it's enabled in the PrimaryWorkerConfig.
    ///
    /// This application is meant to be an optional distributed system, which means a user can choose
    /// not to run the gRPC server and solely rely on the PrimaryWorker to do all the work.
    ///
    /// It is also possible to choose not to run this gRPC server.
    ///
    /// This function runs a tokio task that drives the gRPC server and returns a sender half of the channel.
    /// The sender half allows us to push **IncompleteWork** into the server service, which then distributes the work.
    /// After the work is done, the **CompletedWork** is returned back to us through the receiver half of the channel.
    ///
    /// Please note that there is no guarantee that the `CompletedWork` receiving channel will give us
    /// the CompletedWork(s) in the same order that we sent the IncompleteWork(s).
    ///
    /// # Arguments
    ///
    /// - `config` - The configuration for the Secondary Worker gRPC server.
    ///
    /// # Returns
    ///
    /// A tuple containing the following:
    ///
    /// - `incomplete_works_sender` - The sender half of the channel to send works that need to be distributed to secondary workers.
    /// - `completed_work_receiver` - The receiver half of the channel that receives completed works.
    ///
    pub fn run_secondary_worker_grpc_service(&self) -> Arc<Mutex<UnboundedReceiver<CompletedWork>>> {
        let config = self.config.secondary_workers_grpc_server_config.as_ref().unwrap();
        let primary_worker_service = PrimaryWorkerService::new(self.config.clone(), self.incomplete_work_queue.clone());

        // A sender half to send the works that needs to be distributed secondary workers
        let completed_work_receiver = primary_worker_service.completed_works_receiver.clone();
        let service = WorkerServiceServer::new(primary_worker_service);

        let grpc_server_socket_addr = config.socket_addr;
        tokio::task::spawn(async move {
            // Enabling gRPC reflection
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(DESCRIPTOR_SET)
                .build()
                .unwrap();

            Server::builder()
                .add_service(reflection_service)
                .add_service(service)
                .serve(grpc_server_socket_addr)
                .await
                .unwrap();
        });

        completed_work_receiver
    }
}

#[tonic::async_trait]
impl Worker for PrimaryWorker {
    /// Performs work through the PrimaryWorker's own Scanner
    /// NOTE: This method SHOULD NOT be called in parallel just because it takes &self
    async fn perform_work(&self, incomplete_works: &[IncompleteWork]) -> anyhow::Result<Vec<CompletedWork>> {
        for incomplete_work in incomplete_works {
            self.scanner
                .push_incomplete_work(incomplete_work.clone())
                .context("Can't push IncompleteWork into the Scanner")?;
        }

        let batch_size = incomplete_works.len();
        let completed_works = if batch_size > 0 {
            let mut completed_works: Vec<CompletedWork> = Vec::new();
            while let Some(completed_work) = self.scanner.recv_completed_work().await {
                completed_works.push(completed_work);

                if completed_works.len() == batch_size {
                    break;
                }
            }
            completed_works
        } else {
            vec![]
        };
        Ok(completed_works)
    }
}

/// The [IncompleteWork] distribution mode
#[derive(Debug, Clone, Copy)]
pub enum DistributionMode {
    /// Doesn't run the gRPC service and only runs everything on PrimaryWorker's Scanner
    OnlyPrimaryWorker,

    /// Doesn't run the PrimaryWorker's Scanner and only runs everything on gRPC Service
    OnlyGRPCService,

    /// Distributes work dynamically to both PrimaryWorker and gRPC Service
    PrimaryWorkerAndGRPCService,
}

impl DistributionMode {
    /// Build [DistributionMode] from the [PrimaryWorkerConfig]
    pub fn from_primary_worker_config(primary_worker_config: &PrimaryWorkerConfig) -> Self {
        let secondary_allowed = primary_worker_config.primary.secondary_allowed;
        let primary_worker_scanner_allowed = primary_worker_config.primary.primary_worker_scanner_allowed;

        if secondary_allowed && primary_worker_scanner_allowed {
            Self::PrimaryWorkerAndGRPCService
        } else if !secondary_allowed && primary_worker_scanner_allowed {
            Self::OnlyPrimaryWorker
        } else if secondary_allowed && !primary_worker_scanner_allowed {
            Self::OnlyGRPCService
        } else {
            error!("The current configuration doesn't allow any circuit build attempts");
            panic!()
        }
    }
}

/// A handle to store the current load experienced by the worker
struct LoadHandle {
    load: AtomicCell<u32>,
}

impl LoadHandle {
    /// Start with initial load value
    pub fn new(value: u32) -> Arc<Self> {
        Arc::new(Self {
            load: AtomicCell::new(value),
        })
    }

    /// Get the current load
    pub fn current_load(&self) -> u32 {
        self.load.load()
    }

    /// Increase the load by 1
    pub fn increase_load_by_1(&self) {
        let current_load = self.current_load();
        self.load.store(current_load + 1);
    }

    /// Decrease the load by 1
    pub fn decrease_load_by_1(&self) {
        let current_load = self.current_load();
        if current_load > 0 {
            self.load.store(current_load - 1);
        }
    }
}
