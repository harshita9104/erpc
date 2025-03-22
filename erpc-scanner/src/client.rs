use crate::{
    relay::{NetDirProvider, NetDirProviderEvent, RelaysPool},
    utils::CombinedUnboundedChannel,
    work::{CompletedWork, CompletedWorkStatus, IncompleteWork},
};
use crossbeam::atomic::AtomicCell;
use futures::stream::StreamExt;
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tor_chanmgr::ChannelUsage;
use tor_circmgr::{path::TorPath, CircMgr};
use tor_netdir::{NetDir, Relay};
use tor_proto::circuit::CircParameters;
use tor_rtcompat::Runtime;

pub(crate) struct Client<R: Runtime> {
    // CircMgr used to build circuits
    circmgr: Arc<CircMgr<R>>,

    // The [NetDirProvider] to get the latest NetDir
    netdir_provider: Arc<NetDirProvider>,

    // Channel for getting the IncompleteWork from the pool
    incomplete_work_channel: Arc<CombinedUnboundedChannel<IncompleteWork>>,

    /// The stream to access the result of completed work given out by this [Client]
    output_stream: Mutex<UnboundedReceiverStream<CompletedWork>>,

    /// The current no of work left in the channel
    current_load: AtomicCell<u16>,
}

impl<R: Runtime> Client<R> {
    /// Creates a new `WorkerTorClient` instance with the provided `tor_client`.
    pub(crate) async fn new(
        circmgr: Arc<CircMgr<R>>,
        netdir_provider: Arc<NetDirProvider>,
        netdir: Arc<NetDir>,
    ) -> anyhow::Result<Arc<Self>> {
        // Channel for transferring the CompletedWork from client to the pool
        let (sd, rv) = mpsc::unbounded_channel();

        let client = Arc::new(Self {
            circmgr,
            netdir_provider,
            incomplete_work_channel: Arc::new(CombinedUnboundedChannel::<IncompleteWork>::new()),
            output_stream: Mutex::new(UnboundedReceiverStream::new(rv)),
            current_load: AtomicCell::<u16>::new(0),
        });

        client.start_work(sd, netdir);
        Ok(client)
    }

    /// Pushes the [IncompleteWork] into the incomplete_work_channel
    pub(crate) fn push_incomplete_work(&self, work: &IncompleteWork) -> anyhow::Result<()> {
        self.incomplete_work_channel.sender.send(work.clone())?;

        // Increase the load counter by 1
        self.current_load.store(self.current_load.load() + 1);
        Ok(())
    }

    /// Receiving half for the CompletedWork
    pub(crate) async fn recv_completed_work(&self) -> Option<CompletedWork> {
        let mut output_stream = self.output_stream.lock().await;
        match output_stream.next().await {
            Some(completed_work) => {
                let current_load = self.current_load.load();
                if current_load > 0 {
                    self.current_load.store(self.current_load.load() - 1);
                }
                Some(completed_work)
            }
            None => None,
        }
    }

    /// Starts the work on the WorkerTorClient as a tokio task, the spawned "task",
    /// it listens for IncompleteWork in the ```self.incomplete_work_channel``` half
    /// and then performs the work and gives back the completed work to the internal
    /// channel of the WorkerTorClient through UnboundedSender<CompletedWork> ```self.output_stream```
    /// can be used to dequeue the incomplete work
    fn start_work(&self, sd: UnboundedSender<CompletedWork>, netdir: Arc<NetDir>) {
        let circmgr = self.circmgr.clone();
        let netdir_provider = self.netdir_provider.clone();

        // The channel where we will get IncompleteWork to work on
        let incomplete_work_channel = self.incomplete_work_channel.clone();

        tokio::spawn(async move {
            let mut current_netdir = netdir;
            let mut relays_pool = {
                let relays: Vec<(Relay<'_>, ())> = current_netdir.relays().map(|relay| (relay, ())).collect();
                RelaysPool::from_relays(&relays)
            };
            let mut incomplete_work_receiver_guard = incomplete_work_channel.receiver.lock().await;
            let completed_work_sender = sd;

            let mut netdir_provider_event_receiver = netdir_provider.get_netdirprodiver_event_receiver();

            // Start receiving IncompleteWork provided by the pool
            while let Some(incomplete_work) = incomplete_work_receiver_guard.recv().await {
                // If there's a new NetDirProviderEvent, this loop prevents the IncompleteWork being lost,
                // this loop will run until there's an Error on receiving and is broken after
                // hadning over the work to one of the client
                'work_loss_prevention: loop {
                    // Check if a new NetDirProviderEvent has arrived
                    match netdir_provider_event_receiver.try_recv() {
                        // If there's a new NetDir we update the NetDir and the RelaysPool
                        Ok(netdirprovider_event) => match netdirprovider_event {
                            NetDirProviderEvent::NetDirChanged(netdir) => {
                                current_netdir = netdir;
                                relays_pool = {
                                    let relays: Vec<(Relay<'_>, ())> = current_netdir.relays().map(|relay| (relay, ())).collect();
                                    RelaysPool::from_relays(&relays)
                                };
                            }
                        },
                        Err(_) => {
                            let source_relay = { relays_pool.get_relay(&incomplete_work.source_relay) };
                            let destination_relay = { relays_pool.get_relay(&incomplete_work.destination_relay) };

                            // The relays between whom we were requested for a circuit to be created in
                            // the IncompleteWork, we should check if they even exist in the current FRESH NetDir by
                            // looking at the current RelaysPool
                            //
                            // If either source or destination relay is not present, THEN (TODO)(Figure
                            // out a solution)

                            let timestamp = chrono::Utc::now().timestamp() as u64;
                            let completed_work = if let (Some(source_relay), Some(destination_relay)) = (source_relay, destination_relay) {
                                let source_relay = source_relay.clone();
                                let destination_relay = destination_relay.clone();

                                let two_hop_path = TorPath::new_multihop([source_relay.0.clone(), destination_relay.0.clone()]);
                                let circ_params = CircParameters::default();
                                let circ_usage = ChannelUsage::UselessCircuit;

                                let completed_work_status = match circmgr.builder().build(&two_hop_path, &circ_params, circ_usage).await {
                                    Ok(circ) => {
                                        // Async order to shut down the circuit
                                        circ.terminate();
                                        CompletedWorkStatus::Success
                                    }
                                    Err(err) => CompletedWorkStatus::Failure(err.to_string()),
                                };
                                CompletedWork {
                                    source_relay: incomplete_work.source_relay,
                                    destination_relay: incomplete_work.destination_relay,
                                    timestamp,
                                    status: completed_work_status,
                                }
                            } else {
                                let failure = {
                                    let source_relay_presence = if source_relay.is_none() { "not present" } else { "present" };
                                    let destination_relay_presence = if destination_relay.is_none() { "not present" } else { "present" };
                                    // TODO: Specify someway to index the NetDir, either give info
                                    // about md consensus that NetDir was built from or the lifetime of
                                    // the NetDir
                                    format!(
                                        "The source relay was {} and destination relay was {} in the NetDir",
                                        source_relay_presence, destination_relay_presence
                                    )
                                };
                                CompletedWork {
                                    source_relay: incomplete_work.source_relay,
                                    destination_relay: incomplete_work.destination_relay,
                                    timestamp,
                                    status: CompletedWorkStatus::Failure(failure),
                                }
                            };
                            completed_work_sender.send(completed_work).unwrap();
                            break 'work_loss_prevention;
                        }
                    }
                }
            }
        });
    }
}

/// The implementation of PartialEq only compares the load value of the WorkerTorClient, which
/// means two [Client]s are the same if they have the same  no of load, they are same in a
/// sense that they carry equal no of work to perform
impl<R: Runtime> PartialEq for Client<R> {
    fn eq(&self, other: &Self) -> bool {
        let current_load_left = self.current_load.load();
        let current_load_right = other.current_load.load();

        current_load_left == current_load_right
    }
}

impl<R: Runtime> Eq for Client<R> {}

// The implementation of PartialOrd also only compares the load value of the Client
#[allow(clippy::comparison_chain)]
impl<R: Runtime> PartialOrd for Client<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[allow(clippy::comparison_chain)]
impl<R: Runtime> Ord for Client<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        let current_load_left = self.current_load.load();
        let current_load_right = other.current_load.load();

        current_load_left.cmp(&current_load_right)
    }
}
