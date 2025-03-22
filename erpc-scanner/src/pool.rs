use crate::{
    client::Client,
    relay::NetDirProvider,
    utils::CombinedUnboundedChannel,
    work::{CompletedWork, IncompleteWork},
};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    Mutex,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tor_circmgr::CircMgr;
use tor_rtcompat::{PreferredRuntime, Runtime};

pub(crate) struct ClientPool<R: Runtime> {
    /// The list of Worker [Client]s among whom the [IncompleteWork] is going to be distributed
    /// properly
    worker_clients: Vec<Arc<Client<R>>>,

    /// Internal buffer of the entire pool, it's a unbounded channel that receives the
    /// [IncompleteWork] from the [Scanner] through it's Sending half and the [IncompleteWork] is
    /// then distributed among the [Client] through the receiving half
    incomplete_work_buffer: Arc<CombinedUnboundedChannel<IncompleteWork>>,

    /// Stream to receive the Completed Work from the [Client]s
    completed_work_receiver_stream: Mutex<UnboundedReceiverStream<CompletedWork>>,
}

impl ClientPool<PreferredRuntime> {
    pub(crate) async fn new(
        no_of_clients: u32,
        netdir_provider: Arc<NetDirProvider>,
        circmgr: Arc<CircMgr<PreferredRuntime>>,
    ) -> anyhow::Result<Self> {
        // The channels to push [IncompleteWork] into the [Client]s and get [CompletedWork] out of them
        let incomplete_work_buffer = Arc::new(CombinedUnboundedChannel::<IncompleteWork>::new());
        let (completed_work_sender, completed_work_receiver) = unbounded_channel::<CompletedWork>();

        let netdir = netdir_provider.current_netdir().await;

        // All the Worker [Client]
        let worker_clients = {
            let mut clients = Vec::new();
            for _ in 0..no_of_clients {
                clients.push(Client::new(circmgr.clone(), netdir_provider.clone(), netdir.clone()).await?);
            }
            clients
        };

        // The receiving stream for [CompletedWork]
        let completed_work_receiver_stream = Mutex::new(UnboundedReceiverStream::new(completed_work_receiver));

        let pool = Self {
            worker_clients,
            incomplete_work_buffer,
            completed_work_receiver_stream,
        };

        pool.start_workers(completed_work_sender);

        Ok(pool)
    }

    /// Send the work into the internal channel of the pool, which then shall be then received by
    /// the receiver running in the [self.start_work] function, where work is going to be performed
    pub(crate) fn push_incomplete_work(&self, work: IncompleteWork) -> anyhow::Result<()> {
        self.incomplete_work_buffer.sender.send(work)?;
        Ok(())
    }

    pub(crate) async fn recv_completed_work(&self) -> Option<CompletedWork> {
        let mut completed_work_receiver_stream_lock = self.completed_work_receiver_stream.lock().await;
        //completed_work_receiver_stream_lock.
        //TODO: only take the required work from the stream
        //completed_work_receiver_stream_lock.try_next
        completed_work_receiver_stream_lock.next().await
    }

    /// Starts the work(i.e circuit creation and testing) and sends it to the
    /// inner UnboundedReceiverStream through sender half, UnboundedSender<CompletedWork> in the argument
    fn start_workers(&self, completed_work_sender: UnboundedSender<CompletedWork>) {
        let mut worker_clients = self.worker_clients.clone();
        let incomplete_work_buffer = self.incomplete_work_buffer.clone();

        // Continuosly listen for completed work in all the [Client]s and push them into
        // this pool's [CompletedWork] Sender half
        for worker_client in &worker_clients {
            let __completed_work_sender = completed_work_sender.clone();
            let __worker_client = worker_client.clone();
            tokio::spawn(async move {
                while let Some(completed_work) = __worker_client.recv_completed_work().await {
                    __completed_work_sender.send(completed_work).unwrap();
                }
            });
        }

        tokio::spawn(async move {
            let mut worker_buffer_receiver_lock = incomplete_work_buffer.receiver.lock().await;
            while let Some(incomplete_work) = worker_buffer_receiver_lock.recv().await {
                // Sort the relays by increasing order of weight
                // lower weight means, less no of works were assigned and higher weight higher no
                // of works
                worker_clients.sort();

                // As the relays are sorted by lowest load to highest load, send until it is
                // successfully received by one of the lowest load worker
                for worker_client in &worker_clients {
                    if worker_client.push_incomplete_work(&incomplete_work).is_ok() {
                        break;
                    }
                }
            }
        });
    }
}
