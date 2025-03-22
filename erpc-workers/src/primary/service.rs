//! This module contains the code for the gRPC service that's provided by the primary worker
//! in order to distributed the IncompleteWork to the secondary workers and get the CompletedWork
//! from them

use self::worker::{CbtMinTimeout, IncompleteWorkBatch, ParallelCircuitsToBuild, SecondaryCompletedWork, SecondaryIncompleteWork};
use super::config::PrimaryWorkerConfig;
use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use deadqueue::unlimited::Queue;
use erpc_scanner::work::{CompletedWork, CompletedWorkStatus, IncompleteWork};
use log::debug;
use std::{cmp, collections::HashSet, net::IpAddr, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, RwLock,
    },
    task::JoinHandle,
};
use tonic::{Code, Request, Response, Status};
pub use worker::{
    worker_service_server::{WorkerService, WorkerServiceServer},
    DESCRIPTOR_SET,
};

pub mod worker {
    pub const DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("descriptor");

    tonic::include_proto!("worker");
}

const RECV_CHANNEL_TIMEOUT_DURATION: u64 = 15;

/// The GRPC service to distribute work among the secondary workers
#[derive(Debug)]
pub struct PrimaryWorkerService {
    /// The **Concurrent Hashmap** that stores the **IpAddr** of the
    /// secondary worker as the Key and the state associated with the secondary worker
    /// as the value
    state: DashMap<IpAddr, Arc<RwLock<SecondaryWorkerState>>>, //state: Arc<Mutex>,

    /// The no of works that are in a single batch of work
    ///
    /// One should only use batch_size to create a ```IncompleteWorkBatch``` and
    /// nothing else, other stuff such as length validation of ```CompletedWorkBatch```
    /// should be done against the length of ```IncompleteWorkBatch``` that was sent
    /// because the ```batch_size``` can change during runtime
    batch_size: AtomicCell<u32>,

    /// A high priority buffer to store ```IncompleteWork```
    ///
    /// These are the ```IncompleteWork``` that certain Secondary Workers had failed to
    /// submit ```CompletedWork``` of, before the timeout.
    ///
    /// They will be next distributed again with the highest priority because
    /// until these ```IncompleteWork```s are worked on, those Relays involved in the
    /// ```IncompleteWork``` can't make circuit with other Relay, so
    /// we must complete it ASAP!
    ///
    /// **It's to be only used to generate batch of IncompleteWork**
    high_priority_incomplete_works_buffer: Arc<Mutex<Vec<IncompleteWork>>>,

    /// The queue to get [IncompleteWork] from
    incomplete_work_queue: Arc<Queue<IncompleteWork>>,

    /// The sender half of the channel that is used to send the ```CompletedWork``` received
    /// from the secondary workers back to the one that assigned it
    completed_works_sender: UnboundedSender<CompletedWork>,

    /// The receiving half of the channel that receives the ```CompletedWork```
    /// from the secondary workers
    ///
    /// **It's to be used where we wanna listen for CompletedWork given back to us by the
    /// secondary workers**
    pub completed_works_receiver: Arc<Mutex<UnboundedReceiver<CompletedWork>>>,

    /// Configuration of PrimaryWorker
    primary_worker_config: Arc<PrimaryWorkerConfig>,
}

impl PrimaryWorkerService {
    pub fn new(primary_worker_config: Arc<PrimaryWorkerConfig>, incomplete_work_queue: Arc<Queue<IncompleteWork>>) -> Self {
        let state = DashMap::new();
        let batch_size = AtomicCell::new(primary_worker_config.primary.work_batch_size);
        let high_priority_incomplete_works_buffer = Arc::default();

        let (cw_sd, cw_rv) = unbounded_channel();

        Self {
            state,
            batch_size,
            high_priority_incomplete_works_buffer,
            incomplete_work_queue,
            completed_works_sender: cw_sd,
            completed_works_receiver: Arc::new(Mutex::new(cw_rv)),
            primary_worker_config,
        }
    }

    /// Set the batch size during the runtime
    #[allow(dead_code)]
    pub fn set_batch_size(&self, size: u32) {
        self.batch_size.store(size);
    }

    /// Gives you the current batch size
    pub fn get_batch_size(&self) -> u32 {
        self.batch_size.load()
    }

    /// Builds ```CompletedWork``` from ```SecondaryCompletedWork```
    pub fn build_completed_work(secondary_cw: &SecondaryCompletedWork) -> CompletedWork {
        let source_relay = secondary_cw.source_relay.clone();
        let destination_relay = secondary_cw.destination_relay.clone();
        let timestamp = secondary_cw.timestamp;
        let message = secondary_cw.message.clone(); // Only exists if status is not "0"
        let status = {
            if secondary_cw.status != 0 {
                CompletedWorkStatus::Success
            } else {
                CompletedWorkStatus::Failure(message)
            }
        };

        CompletedWork {
            source_relay,
            destination_relay,
            status,
            timestamp,
        }
    }
}

#[tonic::async_trait]
impl WorkerService for PrimaryWorkerService {
    async fn get_incomplete_works(&self, request: Request<worker::Empty>) -> Result<Response<worker::IncompleteWorkBatch>, tonic::Status> {
        // Getting the unique identifier to the Secondary Worker i.e it's IP Address
        let remote_ip_adr = {
            let remote_adr = request.remote_addr().unwrap();
            remote_adr.ip()
        };

        debug!("{remote_ip_adr} requested for IncompleteWorkBatch");

        // Check in the Hashmap if the secondary worker was registered already, if not
        // then register the secondary worker
        let secondary_worker_state = match self.state.get(&remote_ip_adr) {
            Some(v) => (*v).clone(),
            None => {
                let new_secondary_worker_state = SecondaryWorkerState::new(self.high_priority_incomplete_works_buffer.clone());
                self.state.insert(remote_ip_adr, new_secondary_worker_state.clone());
                new_secondary_worker_state.clone()
            }
        };

        // Only proceed with giving IncompleteWorkBatch to this Secondary Worker, if the secondary isn't working
        // on a previous batch of work still
        let mut secondary_worker_state_write = secondary_worker_state.write().await;
        let resp = if secondary_worker_state_write.assigned_incomplete_works.is_none() {
            let batch_size = self.get_batch_size() as usize;
            let incomplete_works = {
                let mut _x = Vec::<IncompleteWork>::new();

                // Firstly we'll attempt to get IncompleteWork from the high_priority_incomplete_works_buffer
                {
                    let mut high_priority_incomplete_works_buffer = self.high_priority_incomplete_works_buffer.lock().await;
                    let high_priority_incomplete_works_buffer_len = high_priority_incomplete_works_buffer.len();
                    _x = high_priority_incomplete_works_buffer
                        .drain(..(cmp::min(high_priority_incomplete_works_buffer_len, batch_size)))
                        .collect();
                }

                // Secondly we'll attempt to get IncompleteWork from the incomplete_work_receiver channel until the
                // size of buffer is less than batch size
                while _x.len() < batch_size {
                    // We need a timeout here, because at the end of this relay partition test,
                    // there will be less no of IncompleteWork in the buffer, so less that they
                    // can't fulfill the batch size requirement, but the channel will still be
                    // alive, if that's the case then we'll wait upto RECV_CHANNEL_TIMEOUT_DURATION
                    // for some IncompleteWork to come then only we'll decide it
                    match timeout(Duration::from_secs(RECV_CHANNEL_TIMEOUT_DURATION), self.incomplete_work_queue.pop()).await {
                        Ok(incomplete_work) => {
                            _x.push(incomplete_work);
                        }
                        Err(_) => {
                            //timeout
                            break;
                        }
                    }
                }

                if _x.is_empty() {
                    // If we get empty IncompleteWork(S) at the very first request from buffer, it
                    // means the IncompleteWork hasn't arrived
                    let err = Status::new(Code::Cancelled, "IncompleteWork are not availaible currently");
                    return Err(err);
                } else {
                    _x
                }
            };

            debug!("{remote_ip_adr} was assigned a batch of work with size {}", incomplete_works.len());
            secondary_worker_state_write.assign_incomplete_works(incomplete_works.clone());
            drop(secondary_worker_state_write);

            let work_batch_size = self.get_batch_size() as f64;
            let max_parallel_circuits = self.primary_worker_config.primary.no_of_parallel_circuits as f64;
            let work_batch_timeout_duration: f64 = ((work_batch_size / max_parallel_circuits).ceil())
                * (self.primary_worker_config.primary.min_circuit_build_timeout as f64 / 1000_f64);

            SecondaryWorkerState::start_incomplete_works_timeout_task(secondary_worker_state.clone(), work_batch_timeout_duration as u64)
                .await;
            let incomplete_work_batch = incomplete_work_batch_from_incomplete_works(&incomplete_works);
            Ok(Response::new(incomplete_work_batch))
        } else {
            let err = Status::new(
                Code::AlreadyExists,
                "The previous batch of incomplete work assigned to you hasn't been completed and submitted yet",
            );
            Err(err)
        };

        resp
    }

    /// When secondary returns us a batch of **CompletedWork** through this interface
    ///
    /// The Steps involved:
    ///
    /// - Check if the IpAddr of the Secondary Worker was registered in the HashMap at ```self.state```,
    /// which is done by the ```get_incomplete_works()``` rpc method automatically when we request
    /// for work, it's ABSOLUTELY necessary that the Secondary Worker has called
    /// ```get_incomplete_works()``` before calling this funciton, if a server
    /// calls this function before registering itself, the we return an Error
    ///
    /// - We have stored the **batch** of IncompleteWork that we assigned to this Secondary Worker,
    /// stored at ```SecondaryWorkerState```  
    ///     - We check the batch of CompletedWork with the length of the **batch** that we
    ///     submitted, if they're the same then only we proceed to check if it contains all
    ///     information i.e **Source Relay** and **Destination Relay** that we had put in the
    ///     ```IncompleteWork``` against the ```CompletedWork```, then only we consider the
    ///     work has been completed and then we push the CompletedWork to the channel that
    ///     collects the ```CompletedWork```, btw we don't check the lengh against ```self.batch_size```
    ///     because we have allowed this feature to change the batch size during the runtime itself
    ///
    ///     - If the length doesn't match OR if some work given are missing i.e the **Source
    ///     Relay** and the **Destination Relay** fingerprint is in the batch of IncompleteWork(s)
    ///     but not in the batch of CompletedWork(s) then work that was done by the secondary worker will be
    ///     ENTIRELY discarded(we need to check the secondary worker, because it SHOULD NOT miss sending
    ///     single work that was assigned to it) and (pushed to the front of IncompleteWork buffer
    ///     OR some way we will make that work be done ASAP in the next request)(TODO)
    ///
    /// - Clean the IncompleteWorkBatch that we are currently holding in SecondaryWorkerState
    async fn submit_completed_works(&self, request: tonic::Request<worker::CompletedWorkBatch>) -> Result<Response<worker::Empty>, Status> {
        // Initially, we get the IpAddr of the Secondary Worker through the tonic::Request
        // We use IpAddr of the Secondary Worker to get identity of the secondary worker as of right now
        let remote_ip_adr = {
            // TODO : Consider the unwrap works, if this works on unit tests
            let remote_adr = request.remote_addr().unwrap();
            remote_adr.ip()
        };

        let complete_work_batch = request.into_inner().completed_works;

        // Check in the Hashmap if the worker was registered already, if not then we throw an Error
        // on this RPC call
        let secondary_worker_state = match self.state.get(&remote_ip_adr) {
            Some(v) => (*v).clone(),
            None => {
                let err = Status::new(
                    Code::Aborted,
                    "You haven't ever received a batch of works to ever to consider submitting, please assign yourself a batch of work first"
                );
                return Err(err);
            }
        };

        let mut secondary_worker_state_write = secondary_worker_state.write().await;

        // Check if we had even assigned a batch of work or not
        match secondary_worker_state_write.assigned_incomplete_works.as_ref() {
            Some(incomplete_work_batch) => {
                // Validate the CompletedWorkBatch that we got from the Secondary Worker, coz we don't
                // let's imagine we don't trust it
                //
                // Start with checking if the size of batch we sent and received are same
                // and it contains the same combination that we sent
                if incomplete_work_batch.len() == complete_work_batch.len() {
                    let mut set = HashSet::new();
                    for incomplete_work in incomplete_work_batch {
                        let source_relay_fingerprint = incomplete_work.source_relay.clone();
                        let destination_relay_fingerprint = incomplete_work.destination_relay.clone();
                        set.insert((source_relay_fingerprint, destination_relay_fingerprint));
                    }

                    //  Check if there's all the Source + Destination Relay fingerprint
                    //  configuration that we had sent
                    for completed_work in complete_work_batch.iter() {
                        let source_relay_fingerprint = completed_work.source_relay.clone();
                        let destination_relay_fingerprint = completed_work.destination_relay.clone();
                        set.insert((source_relay_fingerprint, destination_relay_fingerprint));
                    }

                    if set.len() == incomplete_work_batch.len() {
                        secondary_worker_state_write.stop_incomplete_work_batch_timeout_task();
                        secondary_worker_state_write.remove_assigned_incomplete_work_batch();

                        for secondary_completed_work in complete_work_batch.iter() {
                            let completed_work = Self::build_completed_work(secondary_completed_work);
                            self.completed_works_sender.send(completed_work).unwrap();
                        }

                        return Ok(Response::new(worker::Empty::default()));
                    } else {
                        let err = Status::new(Code::Aborted, "Some work that we assigned were absent");
                        return Err(err);
                    }
                } else {
                    let err = format!(
                        "The size of incomplete work batch we assigned was {} and the size of completed work batch we received was {}",
                        incomplete_work_batch.len(),
                        complete_work_batch.len()
                    );
                    let err = Status::new(Code::Aborted, err);
                    return Err(err);
                }
            }
            None => {
                let err = Status::new(
                    Code::Aborted,
                    "Either you submitted too slow or you weren't assigned any work. Please make a request again",
                );
                return Err(err);
            }
        };
    }

    async fn no_of_parallel_circuits_build_attempt(
        &self,
        _request: tonic::Request<worker::Empty>,
    ) -> std::result::Result<tonic::Response<worker::ParallelCircuitsToBuild>, tonic::Status> {
        Ok(Response::new(ParallelCircuitsToBuild {
            value: self.primary_worker_config.secondary.no_of_parallel_circuits,
        }))
    }

    async fn get_cbt_min_timeout(
        &self,
        _request: tonic::Request<worker::Empty>,
    ) -> Result<tonic::Response<worker::CbtMinTimeout>, tonic::Status> {
        Ok(Response::new(CbtMinTimeout {
            value: self.primary_worker_config.primary.min_circuit_build_timeout,
        }))
    }
}

/// The view of Secondary Worker from the POV of primary worker, it only carries the state of the
/// Secondary Worker
/// Worker and the work they are doing for the primary worker
#[derive(Debug)]
struct SecondaryWorkerState {
    /// The IncompleteWorkBatch that's currently assigned to the Secondary Worker to finish and whose
    /// primary worker is expecting CompletedWorkBatch
    pub assigned_incomplete_works: Option<Vec<IncompleteWork>>,

    /// The task we spawned to expire the batch of work we had assigned to the
    /// Secondary Worker
    assigned_incomplete_work_batch_timeout_task_handle: Option<JoinHandle<()>>,

    /// The ```clone``` of the high_priority_incomplete_works_buffer of [PrimaryWorkerService]
    high_priority_incomplete_works_buffer: Arc<Mutex<Vec<IncompleteWork>>>,
}

impl SecondaryWorkerState {
    pub fn new(high_priority_incomplete_works_buffer: Arc<Mutex<Vec<IncompleteWork>>>) -> Arc<RwLock<Self>> {
        let assigned_incomplete_works = None;
        let assigned_incomplete_work_batch_timeout_task_handle = None;

        Arc::new(RwLock::new(Self {
            assigned_incomplete_works,
            assigned_incomplete_work_batch_timeout_task_handle,
            high_priority_incomplete_works_buffer,
        }))
    }

    pub fn assign_incomplete_works(&mut self, batch: Vec<IncompleteWork>) {
        self.assigned_incomplete_works = Some(batch);
    }

    /// Start the task that expires the batch of IncompleteWork(s) assigned to the Secondary Worker
    ///
    /// On Expiration the [IncompleteWork] moves to the high priority queue
    pub async fn start_incomplete_works_timeout_task(self_arc: Arc<RwLock<Self>>, timeout_duration: u64) {
        let _self_arc = self_arc.clone();

        let mut _self = self_arc.write().await;
        _self.assigned_incomplete_work_batch_timeout_task_handle = Some(tokio::task::spawn(async move {
            // TODO: Figure out exact time

            // as of right now we're choosing maximum of 10 minutes to work on the batch of work
            // if that batch of work is still stuck(we see the batch id), then we remove that batch of work
            let sleep_duration = Duration::from_secs(timeout_duration);
            sleep(sleep_duration).await;

            let mut _self = _self_arc.write().await;

            // Move the timeout expired IncompleteWorkBatch to the
            // high_priority_incomplete_works_buffer
            {
                let mut high_priority_incomplete_works_buffer = _self.high_priority_incomplete_works_buffer.lock().await;
                if let Some(ref incomplete_works) = _self.assigned_incomplete_works {
                    for incomplete_work in incomplete_works {
                        high_priority_incomplete_works_buffer.push(incomplete_work.clone());
                    }
                }
            }

            _self.remove_assigned_incomplete_work_batch();
        }));
    }

    /// Drop the timeout task that was spawned
    fn stop_incomplete_work_batch_timeout_task(&mut self) {
        if let Some(ref handle) = self.assigned_incomplete_work_batch_timeout_task_handle {
            handle.abort();
        }

        self.assigned_incomplete_work_batch_timeout_task_handle = None;
    }

    fn remove_assigned_incomplete_work_batch(&mut self) {
        self.assigned_incomplete_works = None;
    }
}

fn incomplete_work_batch_from_incomplete_works(incomplete_works: &[IncompleteWork]) -> IncompleteWorkBatch {
    let mut secondary_incomplete_works = Vec::new();
    for incomplete_work in incomplete_works {
        secondary_incomplete_works.push(SecondaryIncompleteWork {
            source_relay: incomplete_work.source_relay.clone(),
            destination_relay: incomplete_work.destination_relay.clone(),
        });
    }
    IncompleteWorkBatch {
        incomplete_works: secondary_incomplete_works,
    }
}
