//! Items to store metadata for/of circuit creation
//!
//! In the context of ```Scanner```,
//!
//! [IncompleteWork] is the information about
//! the guard relay and the exit relay of the two hop circuit to verify
//! if those relays are **partitioned** or **not partitioned**
//!
//! [CompletedWork] is the end result of using the [IncompleteWork] to
//! perform the circuit creation attempt

/// The metadata about the two hop circuit we want to create.
///
/// It represents the two hop circuit that needs to be created where ```source_relay```
/// represents the guard relay of the two hop and ```destination_relay``` represents
/// the exit relay of the two hop circuit
#[derive(Debug, Clone)]
pub struct IncompleteWork {
    /// Metadata to identify the source relay
    pub source_relay: String,

    /// Metadata to identify the destination relay
    pub destination_relay: String,
}

impl IncompleteWork {
    /// Create ```IncompleteWork``` from a ```source_relay``` and ```destination_relay``` that
    /// can be converted to String
    pub fn from<T: ToString>(source_relay: T, destination_relay: T) -> Self {
        Self {
            source_relay: source_relay.to_string(),
            destination_relay: destination_relay.to_string(),
        }
    }
    /// Check whether the given relay fingerprint is of the source relay of IncompleteWork
    pub fn is_source_relay<T: AsRef<str>>(&self, relay_fingerprint: T) -> bool {
        relay_fingerprint.as_ref() == self.source_relay
    }

    /// Check whether the given relay fingerprint is of the destination relay of IncompleteWork
    pub fn is_destination_relay<T: AsRef<str>>(&self, relay_fingerprint: T) -> bool {
        relay_fingerprint.as_ref() == self.destination_relay
    }
}

/// The end result of two hop circuit creation attempt between
/// a guard relay and an exit relay represented by
/// ```source_relay``` and ```destination_relay``` respectively
#[derive(Debug, Clone)]
pub struct CompletedWork {
    /// Fingerprint to identify the source relay
    pub source_relay: String,

    /// Fingerprint to identify the destination relay
    pub destination_relay: String,

    /// Shows how the work was completed, was it a success or failure
    pub status: CompletedWorkStatus,

    /// The Unix epoch at which the test was performed
    pub timestamp: u64,
}

/// The status of completion of [CompletedWork]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletedWorkStatus {
    /// The circuit creation attempt successful
    Success,

    /// The circuit creation attempt failed with an error
    Failure(String),
}
