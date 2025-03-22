use erpc_scanner::work::{CompletedWork, IncompleteWork};
use tonic::async_trait;

#[async_trait]
pub trait Worker {
    /// Given a certain amount of work, it returns after the work is completely done rather than a
    /// stream
    ///
    /// The caller of this function should be sent small amount of work so that  the calle doesn't
    /// await too long
    async fn perform_work(&self, incomplete_works: &[IncompleteWork]) -> anyhow::Result<Vec<CompletedWork>>;
}
