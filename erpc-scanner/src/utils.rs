use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};
/// Combination of both the [UnboundedSender] and [UnboundedReceiver]
pub(crate) struct CombinedUnboundedChannel<T> {
    pub(crate) sender: UnboundedSender<T>,
    pub(crate) receiver: Mutex<UnboundedReceiver<T>>,
}

impl<T> CombinedUnboundedChannel<T> {
    /// Create a new [CombinedUnboundedChannel] by invoking ```unbounded_channel``` method
    pub(crate) fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        let receiver = Mutex::new(receiver);
        CombinedUnboundedChannel { sender, receiver }
    }
}
