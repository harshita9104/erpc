pub mod worker_service {
    tonic::include_proto!("worker");
}

use self::worker_service::{CompletedWorkBatch, IncompleteWorkBatch, SecondaryCompletedWork};

use super::config::SecondaryWorkerConfig;
use crate::shared::Worker;
use anyhow::Context;
use erpc_scanner::{
    relay::NetDirProvider,
    scanner::Scanner,
    work::{CompletedWork, CompletedWorkStatus, IncompleteWork},
};
use log::{debug, error, info};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::{Code, Request};
use tor_circmgr::CircMgr;
use tor_rtcompat::PreferredRuntime;
use worker_service::worker_service_client::WorkerServiceClient;

const MAX_SUBMISSION_ATTEMPTS: u8 = 3; // Max CompletedWorkBatch submission attempts despite of failure
const SERVICE_DOWN_SLEEP_DURATION: u64 = 30; // Sleep duration until next try when primary worker service is down
const WORK_BATCH_LOCK_SLEEP_DURATION: u64 = 60; // Sleep duration when previous IncompleteWorkBatch isn't submitted and the secondary is locked
const NO_INCOMPLETE_WORK_SLEEP_DURATION: u64 = 60; // Sleep duration when there's no IncompleteWork with primary worker currently

const CBT_MIN_TIMEOUT_SET_SLEEP_DURATION: u64 = 10; // No of seconds to sleep, so that the events of NetDir doesn't make NetworkParameters
                                                    // interfere with circuit creation
/// The heart of the secondary workers
pub struct SecondaryWorker {
    /// The [Scanner] used by SecondaryWorker to receive IncompleteWork from the PrimaryWorker and
    /// work on them to convert to CompletedWork
    scanner: Option<Arc<Scanner>>,

    /// The configurations for SecondaryWorker
    config: SecondaryWorkerConfig,

    /// The NetDirProvider to get the latest NetDir
    netdir_provider: Arc<NetDirProvider>,

    /// The CircMgr that provides CircuitBuilder to build circuits
    circmgr: Arc<CircMgr<PreferredRuntime>>,
}

impl SecondaryWorker {
    /// Create a new SecondaryWorker instance
    pub fn new(config: SecondaryWorkerConfig, netdir_provider: Arc<NetDirProvider>, circmgr: Arc<CircMgr<PreferredRuntime>>) -> Self {
        let scanner = None;

        Self {
            scanner,
            config,
            netdir_provider,
            circmgr,
        }
    }

    /// Builds the scannner
    async fn build_scanner(&self, no_of_parallel_circuit_builds: u32) -> anyhow::Result<Arc<Scanner>> {
        let netdir_provider = self.netdir_provider.clone();
        let circmgr = self.circmgr.clone();

        Ok(Arc::new(
            Scanner::new(no_of_parallel_circuit_builds, netdir_provider, circmgr).await?,
        ))
    }

    // Start the secondary worker
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let primary_worker_server_addr = self.config.primary_worker_server_url.clone();
        info!("Attempting to connect to the Primary Worker gRPC Server at : {primary_worker_server_addr}");
        let mut grpc_client = WorkerServiceClient::connect(primary_worker_server_addr)
            .await
            .context("Can't connect to the primary worker gRPC server")?;
        info!("Connected to the gRPC Server");

        // If not set explicity through the env variable then we request the primary worker
        // for no of paralle circuits build to attempt
        let no_of_parallel_circuit_builds = match self.config.no_of_parallel_circuit_builds {
            Some(v) => {
                info!("Using the no of parallel circuit builds for this worker explicitly set in the env file {v}");
                v
            }
            None => {
                let req = grpc_client
                    .no_of_parallel_circuits_build_attempt(Request::new(worker_service::Empty {}))
                    .await?;
                let v = req.into_inner().value;
                info!("Using the no of parallel circuit builds for this worker set by the primary worker - {v}");
                v
            }
        };

        // TODO: Breaking change here, either make TorClient after receiving this call or make it
        // to configuration level of the secondary worker
        let cbt_min_timeout = grpc_client
            .get_cbt_min_timeout(Request::new(worker_service::Empty {}))
            .await?
            .into_inner()
            .value;

        self.scanner = Some(self.build_scanner(no_of_parallel_circuit_builds).await?);

        // Start running the secondary worker after 10 seconds
        //
        // PROBLEM : As of right now, the NetworkParameters gets updated automatically because of
        // https://gitlab.torproject.org/tpo/core/arti/-/blob/main/crates/tor-circmgr/src/lib.rs#L623
        debug!("Wait for {CBT_MIN_TIMEOUT_SET_SLEEP_DURATION} seconds for the startup in order to get no more events from NetDir");
        tokio::time::sleep(Duration::from_secs(CBT_MIN_TIMEOUT_SET_SLEEP_DURATION)).await;
        info!("Using the minimum time for circuit build timeout for this worker set by the primary worker - {cbt_min_timeout} seconds");

        let mut batch_no: u64 = 1;
        loop {
            info!("Attempting to get batch {batch_no} from Primary Worker");
            match grpc_client.get_incomplete_works(Request::new(worker_service::Empty {})).await {
                Ok(incomplete_work_batch) => {
                    let incomplete_work_batch = incomplete_work_batch.into_inner();
                    let incomplete_works = incomplete_works_from_incomplete_work_batch(&incomplete_work_batch);

                    info!("Received the batch {batch_no}, with size {}", incomplete_works.len());

                    let submission_time = Instant::now();
                    let completed_works = match self.perform_work(&incomplete_works).await {
                        Ok(completed_works) => completed_works,
                        Err(e) => {
                            error!("The Scanner is not functioning right: {e:?}");
                            panic!();
                        }
                    };

                    let completion_time = Instant::now();
                    info!(
                        "Completed batch {batch_no} in {:?}",
                        completion_time.duration_since(submission_time)
                    );

                    let completed_work_batch = completed_work_batch_from_completed_works(&completed_works);

                    let mut submission_attempt = 1;
                    while submission_attempt <= MAX_SUBMISSION_ATTEMPTS {
                        match grpc_client.submit_completed_works(completed_work_batch.clone()).await {
                            Ok(_) => {
                                info!(
                                    "Submitted the {batch_no}, of size {} in submission attempt {submission_attempt}",
                                    completed_works.len()
                                );
                                batch_no += 1;
                                break;
                            }
                            Err(e) => {
                                error!("Submission attempt {submission_attempt} failed | {e:?})");
                                submission_attempt += 1;
                            }
                        };
                    }
                }
                Err(e) => {
                    let status_code = e.code();
                    match status_code {
                        Code::Unavailable | Code::Unknown => {
                            error!("Can't connect to the service, trying again after {SERVICE_DOWN_SLEEP_DURATION} seconds | {e:?}",);
                            tokio::time::sleep(Duration::from_secs(SERVICE_DOWN_SLEEP_DURATION)).await;
                        }
                        Code::Cancelled => {
                            debug!("There's no IncompleteWork with the Primary Worker currently, will try again after {NO_INCOMPLETE_WORK_SLEEP_DURATION} seconds",);
                            tokio::time::sleep(Duration::from_secs(NO_INCOMPLETE_WORK_SLEEP_DURATION)).await;
                        }
                        _ => {
                            error!("Requested a batch of work without submitting completed verison of previous batch of works, trying agian after {WORK_BATCH_LOCK_SLEEP_DURATION} seconds | {e:?}");
                            tokio::time::sleep(Duration::from_secs(WORK_BATCH_LOCK_SLEEP_DURATION)).await;
                        }
                    }
                }
            }
        }
    }
}

fn incomplete_works_from_incomplete_work_batch(incomplete_work_batch: &IncompleteWorkBatch) -> Vec<IncompleteWork> {
    let mut incomplete_works = Vec::new();
    for secondary_incomplete_work in incomplete_work_batch.incomplete_works.iter() {
        let incomplete_work = IncompleteWork {
            source_relay: secondary_incomplete_work.source_relay.clone(),
            destination_relay: secondary_incomplete_work.destination_relay.clone(),
        };
        incomplete_works.push(incomplete_work);
    }
    incomplete_works
}

fn completed_work_batch_from_completed_works(completed_works: &Vec<CompletedWork>) -> CompletedWorkBatch {
    let mut secondary_completed_works = Vec::new();

    for completed_work in completed_works {
        let (status, message) = match completed_work.status {
            CompletedWorkStatus::Success => (1, String::from("Success")),
            CompletedWorkStatus::Failure(ref error) => (0, error.clone()),
        };

        let secondary_completed_work = SecondaryCompletedWork {
            source_relay: completed_work.source_relay.clone(),
            destination_relay: completed_work.destination_relay.clone(),
            timestamp: completed_work.timestamp,
            status,
            message,
        };

        secondary_completed_works.push(secondary_completed_work);
    }

    CompletedWorkBatch {
        completed_works: secondary_completed_works,
    }
}

#[tonic::async_trait]
impl Worker for SecondaryWorker {
    /// Performs work through the Scanner
    async fn perform_work(&self, incomplete_works: &[IncompleteWork]) -> anyhow::Result<Vec<CompletedWork>> {
        for incomplete_work in incomplete_works {
            self.scanner
                .as_ref()
                .unwrap()
                .push_incomplete_work(incomplete_work.clone())
                .context("Can't push IncompleteWork into the Scanner")?;
        }

        let batch_size = incomplete_works.len();
        let completed_works = if batch_size > 0 {
            let mut completed_works: Vec<CompletedWork> = Vec::new();
            while let Some(completed_work) = self.scanner.as_ref().unwrap().recv_completed_work().await {
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
