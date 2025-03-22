/// Errors that may occur when attempting to assign a Relay as **EXIT**
#[derive(Debug, thiserror::Error)]
pub enum AssignAsExitError {
    #[error("Exit relay is already assigned as a guard or exit relay")]
    ExitRelayHasEnoughAssigned,
    #[error("Cannot acquire mutex lock as it is currently held by another task")]
    NoLock,
}

/// Errors that can occur when starting a Node (i.e during circuit creation)
#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    #[error("No available relays to use as exit relay")]
    NoRelayToUseAsExit,
    #[error("Failed to send incomplete work: receiver dropped")]
    IncompleteWorkSenderChannelDropped,
}
