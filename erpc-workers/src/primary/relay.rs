//! We can think of each [Node] as a Relay in the Tor Network
//!
//! NOTE: The word "Relay(s)" and "Node(s)" are used interchangeably
//!
//! In order to check for Relay Partition, we need to make a two hop circuit.
//! Within this two hop circuit, we've called the Relay at the first hop to be the
//! **guard** relay and relay at the second hop to be the **exit** relay.
//!
//! Current scanning strategy:
//!
//! Let's imagine we have 100 Relays, which means 100 Relays trying to make circuit with each other.
//! Each Relay tries to make the 99 remaining relays as the exit relay by keeping itself the guard
//! relay. So at any instant in time that one relay is allowed to make a circuit with one of those
//! 99 relays, that means that the relay that got selected from those 99 relays will also have
//! metadata about which relay has tried to keep it as the exit relay.
//!
//! Storing information is two way, the one that attempts to create the [IncompleteWork] and the
//! one that was used as the exit relay to create [IncompleteWork] stores the metadata about the
//! relay that was/is selected. So that only one circuit is built if we were to check at any instant, whether it's the Relay
//! being used as the **guard** relay or **exit** relay bceause we can know at any instant if the
//! Relay is being involved as a **guard relay** or **exit relay**
//!
//! This means that AT ANY INSTANT IN TIME, the relay is being involved in ONLY one circuit, either
//! as a guard or as an EXIT relay
//!
//! So for 7000 relays, each relay attempts to make a circuit with 6999 relays in one direction and
//! each of those 6999 relays attempt to create circuit back with that one relay in other
//! direction
//!
//! So, in total a SINGLE RELAY get's involved in (n-1) two hop circuits as a guard relay and in
//! other remaining (n-1) two hop circuits as the exit relay.
//!
//! i.e (n-1) + (n-1) = 2(n - 1)
//!
//! Total no of circuits that will be created with N relays = N * (N - 1)
//!
//! Where N is the total no of relays
//!
//! TODO(Node): How to handle creating [IncompleteWork] multiple times in a Relay, I mean after what time should a relay
//! repeat it's circuit creation with that combination. We could do that by pushing the relay from already_used_as_exit
//! to as certain position in the vector to_be_used_as_exit to define the priority
//!
//! TODO(Node):
//! Make Use Of NodeStatus
//! Incase in the future if we need some ability to control the state of the
//! application
//! WHat happens if we pause?
//! Does the assigned IncompleteWork, do we discard it?
//! What happens if we resume it back again?
//!
//!
//!TODO(Node): What to do of the Relays that are in the Graph but not in the NetDir which means
//!they existed before but now they no longer do in the fresh NetDir
//! We can have a field such as in_graph_but_not_in_netdir : Mutex<Vec<Arc<Node>>>
//!

#![allow(dead_code)]
use crate::error::{AssignAsExitError, NodeStartError};
use crossbeam::atomic::AtomicCell;
use erpc_scanner::work::{CompletedWork, IncompleteWork};
use std::{cmp::Ordering, collections::HashSet, hash::Hash, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc::UnboundedSender, Notify, RwLock, RwLockWriteGuard},
    time::sleep,
};

const MAX_ALLOWED_GUARDS: usize = 1;
const MAX_ALLOWED_EXITS: usize = 1;

/// Represents the current status of a Node.
///
/// Used to track the functioning status of a Node. It provides
/// information about whether the Node has started, is currently running, paused, or stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// The Node hasn't been started yet to produce [IncompleteWork]
    NotStarted,

    /// The Node is running and is actively producing [IncompleteWork].
    Running,

    /// The Node is currently paused and not producing any [IncompleteWork].
    Paused,

    /// The Node has been stopped, indicating that it was previously started and is now
    /// permanently stopped. It cannot be resumed.
    Stopped,
}

/// Stores metadata about the Relay and the Relays that it plans
/// to/not create circuits with
///
/// It holds the core method to operate on Relay(s) and they are the fundamental
/// data type to produce [IncompleteWork]
#[derive(Debug)]
pub struct Node {
    //rsa_id : RsaIden
    /// The unique identifier of the ```Node```
    /// We usually use the stringified view of RSA_ID
    pub fingerprint: String,

    /// The current status of the Node that can be used
    /// to monitor whether the ```Node``` is currently  
    /// - NotStarted
    /// - Running
    /// - Paused
    /// - Stopped
    node_status: Arc<AtomicCell<NodeStatus>>,

    /// Relays that have involved this relays in the [IncompleteWork] currently that they
    /// have produced where this Relay is the Exit Relay and they are the Guard Relay
    ///
    /// -------**GUARD**-------------**EXIT**-------
    /// ----**<THOSE_RELAYS>**---**<THIS_RELAY>**----
    ///
    /// We can interpret this information from **POV** of this relay as :
    /// *"these relays created IncompleteWork(s) where they kept me as the
    ///  exit relay, and they made me store this information so that i don't cross my
    ///  limit i.e no of times i can be exit relay in a circuit at any Instant in time.
    ///  I can remove them from this buffer as soon i find out the CompletedWork was generated"*
    acting_as_guard: RwLock<HashSet<Arc<Node>>>,

    /// Relays that this Relay has involved as Exit Relay and produced
    /// [IncompleteWork] with, where this relay is the Guad Relay
    ///
    /// -------**GUARD**-------------**EXIT**-------
    /// ----**<THIS_RELAY>**---**<THOSE_RELAYS>**----
    ///
    /// We can interpret this information from **POV** of this relay as :
    /// *"I've created ```IncompleteWork```(s) where I used these
    /// Relays as the Exit Relay, so i call it ```acting_as_exit```
    /// because they are acting as an exit relay for me.
    /// I'm recording this information so that i don't cross my
    /// limit i.e no of times i can be guard relay at any
    /// Instant in time. After they are done, i mean the
    /// ```CompletedWork``` has been provided, i'll move them
    /// to ```already_used_as_exit```"*
    acting_as_exit: RwLock<HashSet<Arc<Node>>>,

    /// All Relays that this Relay plans to use as the Exit Relay in the circuit
    pub to_be_used_as_exit: RwLock<HashSet<Arc<Node>>>,

    /// All Relays that this Relay has already used as Exit Relay in the circuit
    already_used_as_exit: RwLock<HashSet<Arc<Node>>>,

    /// List of relays that are in the same subnet or same family and
    /// has to be avoided in creating a [IncompleteWork] i.e a circuit
    pub in_same_subnet_or_family: RwLock<HashSet<Arc<Node>>>,

    /// Notifier to notify when the acting_as_exit gets freed
    notifier_acting_as_exit: Notify,

    /// Notifier to notify when the acting_as_guard gets freed
    notifier_acting_as_guard: Notify,

    /// Notifier to notify when the NodeStatus changes
    /// TODO: To be used in future while adding feature of pause and resume
    notifier_node_status: Notify,

    /// The Sender half to send the IncompleteWork produced by this Node
    pub incomplete_work_sender: UnboundedSender<IncompleteWork>,

    /// The weight of the relay
    weight: Arc<AtomicCell<u32>>,
}

impl Node {
    /// Build a new Node from a given fingerprint and a UnboundedSender to send the IncompleteWork
    /// generated by this Node
    ///
    /// A fingerprint can be anything that can be used to uniquely identify a ```Node```
    pub fn new<T: AsRef<str>>(fingerprint: T, incomplete_work_sender: UnboundedSender<IncompleteWork>, weight: u32) -> Arc<Self> {
        let fingerprint = fingerprint.as_ref().to_string();
        let node_status = Arc::new(AtomicCell::new(NodeStatus::NotStarted));
        let acting_as_guard = RwLock::default();
        let acting_as_exit = RwLock::default();
        let to_be_used_as_exit = RwLock::default();
        let already_used_as_exit = RwLock::default();
        let in_same_subnet_or_family = RwLock::default();
        let notifier_acting_as_exit = Notify::new();
        let notifier_acting_as_guard = Notify::new();
        let notifier_node_status = Notify::new();
        let weight = Arc::new(AtomicCell::new(weight));

        Arc::new(Self {
            fingerprint,
            node_status,
            acting_as_guard,
            acting_as_exit,
            to_be_used_as_exit,
            already_used_as_exit,
            in_same_subnet_or_family,
            notifier_acting_as_exit,
            notifier_acting_as_guard,
            notifier_node_status,
            incomplete_work_sender,
            weight,
        })
    }

    /// Get the weight of the Relay
    pub fn get_weight(&self) -> u32 {
        self.weight.load()
    }

    /// Set the weight of the Relay
    pub fn set_weight(&self, weight: u32) {
        self.weight.store(weight);
    }

    /// Get the latest Node Status
    pub fn get_status(&self) -> NodeStatus {
        self.node_status.load()
    }

    /// Set the Node Status
    pub fn set_status(&self, node_status: NodeStatus) {
        self.node_status.store(node_status);
    }

    /// Check if Relay **was started**
    pub fn was_started(&self) -> bool {
        self.get_status() != NodeStatus::NotStarted
    }

    /// Check if Relay **is running**
    pub fn is_running(&self) -> bool {
        self.get_status() == NodeStatus::Running
    }

    /// FEATURE TODO: Resume the Node
    pub fn resume(&self) {
        self.set_status(NodeStatus::Running);
        // TODO: Add a guard for not resuming a Stopped and NotStarted and already running
    }

    /// FEATURE TODO : Pause the Node
    pub fn pause(&self) {
        self.set_status(NodeStatus::Paused);
        // TODO: Add a guard for not pausing a Stopped and NotStarted and already paused
    }

    /// Start producing [Incompleteork] where the relay we pass is the GUARD Relay
    pub async fn start(guard_relay: Arc<Self>) -> Result<(), NodeStartError> {
        if guard_relay.is_to_be_used_as_exit_empty().await {
            log::error!("There are no relays here to work on, this means that the filtering finished too fast too");
            return Err(NodeStartError::NoRelayToUseAsExit);
        }

        'outer: loop {
            let notify_change_in_acting_as_exit = guard_relay.notifier_acting_as_exit.notified();
            let notify_change_in_acting_as_guard = guard_relay.notifier_acting_as_guard.notified();

            let to_be_used_as_exit: Vec<Arc<Node>> = {
                let to_be_used_as_exit_read_guard = guard_relay.to_be_used_as_exit.read().await;
                let mut to_be_used_as_exit: Vec<Arc<Node>> = to_be_used_as_exit_read_guard.iter().cloned().collect();
                to_be_used_as_exit.sort();
                to_be_used_as_exit.reverse();
                to_be_used_as_exit
            };

            // Relays to remove after they have been used as an Exit Relay
            let mut remove_from_to_be_used_as_exit = Vec::new();

            // NOTE and TODO: What to do after all circuits have been tested
            // How to again start for testing?
            //
            // Should we cleanup already_used_as_exit and fill with other set of relays that
            // showed error and start working on the new set
            //
            // As of right now we'll just exit if to_be_used_as_exit is empty, which happens
            // after guard_relay have used all the relays in to_be_used_as_exit as it's EXIT
            // relay in the circuit
            if to_be_used_as_exit.is_empty() {
                // TODO
                break 'outer;
            }

            // Consider all relays we want to use as if they are about to act as an exit relay
            'to_be_used_as_exit_iteration: for exit_relay in to_be_used_as_exit.iter() {
                // Until we get lock on acting_as_exit and acting_as_guard we don't continue,
                // because not getting these locks means that some other Relay is currently
                // trying to use this relay for producing [IncompleteWork]
                let (acting_as_guard, mut acting_as_exit) = guard_relay.acting_as_exit_and_guard_lock(200).await;

                // We only attempt to create a circuit if currently guard_relay doesn't have
                // any involvement in any IncompleteWork i.e circuit, the acting_as_exit and
                // acting_as_guard tracks this involvment and if they are both lower than
                // MAX_ALLOWED_EXITS and MAX_ALLOWED_GUARDS, then only we proceed on the
                // attempt to create a circuit
                if acting_as_exit.len() < MAX_ALLOWED_EXITS && acting_as_guard.len() < MAX_ALLOWED_GUARDS {
                    if Node::try_assign(&guard_relay, exit_relay, &mut acting_as_exit).is_ok() {
                        let incomplete_work = IncompleteWork {
                            source_relay: guard_relay.fingerprint.clone(),
                            destination_relay: exit_relay.fingerprint.clone(),
                        };

                        match guard_relay.incomplete_work_sender.send(incomplete_work) {
                            Ok(_) => {
                                remove_from_to_be_used_as_exit.push(exit_relay.clone());
                                let mut already_used_as_exit = guard_relay.already_used_as_exit.write().await;
                                already_used_as_exit.insert(exit_relay.clone());
                            }
                            Err(_) => {
                                // This only happens if the receiver half is dropped and
                                // this should not happen
                                return Err(NodeStartError::IncompleteWorkSenderChannelDropped);
                            }
                        };
                    }
                } else {
                    // Drop these guards because while trying to free the buffer of
                    // acting_as_exit or acting_as_guard other methods will NEED the lock
                    drop(acting_as_exit);
                    drop(acting_as_guard);

                    // This relay has already assigned itself in enough circuits as the guard relay(those relays acting as
                    // exit in those circuits are tracked through acting_as_exit) or enough relays have assigned this relay
                    // as the exit relay in their circuit(those relays acting as guard in those
                    // circuits are tracked through acting_as_guard)
                    //
                    // This means that we're currently full in either of them i.e in capability to assign ourself as the guard
                    // or in capability to be assigned as an exit
                    //
                    // Because we're only involving a relay in ONE circuit at any moment in
                    // time, we'll always have one of the buffer empty i.e acting_as_exit or
                    // acting_as_guard at any instant; either way, we can't create circuit/get involved in
                    // circuits unless both the acting_as_exit and acting_as_guard buffer are
                    // empty
                    //
                    // So, we block by awaiting on the task until one of the buffer is free
                    //
                    // After we find out that the buffer is free through the notifier, we
                    // iterate from the very start of "to_be_used_as_exit"

                    tokio::select! {
                        () = notify_change_in_acting_as_exit => (),
                        () = notify_change_in_acting_as_guard => ()
                    }

                    break 'to_be_used_as_exit_iteration;
                }
            }

            let mut to_be_used_as_exit = guard_relay.to_be_used_as_exit.write().await;
            to_be_used_as_exit.retain(|relay| !remove_from_to_be_used_as_exit.contains(relay));

            // This sleep duration is the time, so that it gives other relays enough time for each relay
            // TODO : Make this random for each task ran
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    // This relay is the second hop
    async fn add_onionperf_two_hop_succesful_circuit(&self) {}
    /// Acquires locks for both `acting_as_exit` and `acting_as_guard` in a loop until both locks are acquired.
    ///
    /// This method attempts to acquire the lock for `acting_as_exit` and `acting_as_guard` simultaneously using the try_lock
    /// It will keep running in a loop until both locks are successfully acquired, the
    /// wait_duration_ms is the sleep duration after which the next attempt will be made
    /// This wait duration at the init mechanism is built so that other relays will have time to
    /// acquire the lock and assign this relay in their circuit
    ///
    /// # Args
    ///  `wait_duration_ms` - The duration to wait afer an unsuccessful attempt.
    ///
    /// # Returns
    ///
    /// A tuple containing the acquired `MutexGuard` for `acting_as_exit` and `acting_as_guard`.
    ///
    async fn acting_as_exit_and_guard_lock(
        &self,
        wait_duration_ms: u64,
    ) -> (RwLockWriteGuard<'_, HashSet<Arc<Node>>>, RwLockWriteGuard<'_, HashSet<Arc<Node>>>) {
        loop {
            // Attempt to acquire locks
            let exit_lock_result = self.acting_as_exit.try_write();
            let guard_lock_result = self.acting_as_guard.try_write();

            if let (Ok(acting_as_exit), Ok(acting_as_guard)) = (exit_lock_result, guard_lock_result) {
                return (acting_as_guard, acting_as_exit);
            }

            // if can't acquire the lock then sleep for wait_duration_ms
            sleep(Duration::from_millis(wait_duration_ms)).await;
        }
    }

    /// Try to assign guard_relay as the **GUARD** relay and exit_relay as the **EXIT** relay
    /// in the two hop circuit
    fn try_assign(
        guard_relay: &Arc<Self>,
        exit_relay: &Arc<Self>,
        acting_as_exit_of_guard_relay: &mut RwLockWriteGuard<'_, HashSet<Arc<Node>>>,
    ) -> Result<(), AssignAsExitError> {
        match (exit_relay.acting_as_guard.try_write(), exit_relay.acting_as_exit.try_write()) {
            (Ok(mut acting_as_guard_of_exit_relay), Ok(acting_as_exit_of_exit_relay)) => {
                // Making sure that the exit_relay we chose isn't working on any other circuit
                if (acting_as_exit_of_exit_relay.len() < 1) && (acting_as_guard_of_exit_relay.len() < 1) {
                    acting_as_guard_of_exit_relay.insert(guard_relay.clone());
                    acting_as_exit_of_guard_relay.insert(exit_relay.clone());
                    Ok(())
                } else {
                    Err(AssignAsExitError::ExitRelayHasEnoughAssigned)
                }
            }
            _ => Err(AssignAsExitError::NoLock),
        }
    }

    /// Submit the CompletedWork in which this relay was involved either by being a guard relay or
    /// exit relay
    ///
    /// NOTE: This is not the method to dump the CompletedWork, one can dump the clone of the
    /// CompletedWork in which this Relay was associated so that it can track the work it needs to
    /// do and not do
    pub async fn submit_completed_work(&self, completed_work: CompletedWork) {
        // If the involvement of this relay was as the guard relay then we must remove the relay
        // that we had stored in acting_as_exit and mvoe it to already_used_as_exit so that we
        // don't make circuit with it again until.................
        if self.fingerprint == completed_work.source_relay {
            let mut acting_as_exit = self.acting_as_exit.write().await;

            *acting_as_exit = acting_as_exit
                .iter()
                .filter(|relay| relay.fingerprint != completed_work.destination_relay)
                .cloned()
                .collect();

            // Notify for change in acting_as_exit within self
            self.notifier_acting_as_exit.notify_one();

            // Else if the involvlment of this relay was as an exit relay, then we must remove the
            // relay that we had stored in acting_as_guard
        } else if self.fingerprint == completed_work.destination_relay {
            let mut acting_as_guard = self.acting_as_guard.write().await;

            *acting_as_guard = acting_as_guard
                .iter()
                .filter(|relay| relay.fingerprint != completed_work.source_relay)
                .cloned()
                .collect();

            // Notify for change in acting_as_guard within self
            self.notifier_acting_as_guard.notify_one();
        }
    }

    // Only invoke this method is this relay was the source_relay
    // Submit a CompletedWork that was generated from OnionPerf, so that we can avoid creating that
    // two hop circuit
    //
    // We simply move relays in to_be_used_as_exit to already_used_as_exit in that case, we do
    // nothing else
    pub async fn submit_onion_perf_completed_work(&self, completed_work: CompletedWork) {
        if completed_work.source_relay == self.fingerprint {
            let mut _to_be_used_as_exit = self.to_be_used_as_exit.write().await;
            let mut _already_used_as_exit = self.already_used_as_exit.write().await;
            todo!()
        }
    }

    pub fn fingerprint(&self) -> String {
        self.fingerprint.clone()
    }

    // Gives no of relays present in to_be_used_as_exit currently
    pub async fn no_of_relays_in_to_be_used_as_exit(&self) -> usize {
        let to_be_used_as_exit = self.to_be_used_as_exit.read().await;
        to_be_used_as_exit.len()
    }

    // Check if to_be_used_as_exit contains a specified Relay
    pub async fn to_be_used_as_exit_contains(&self, relay: Arc<Node>) -> bool {
        let to_be_used_as_exit = self.to_be_used_as_exit.read().await;
        to_be_used_as_exit.contains(&relay)
    }

    // Check if already_used_as_exit contains a specified Relay
    pub async fn already_used_as_exit_contains(&self, relay: &Arc<Node>) -> bool {
        let already_used_as_exit = self.already_used_as_exit.read().await;
        already_used_as_exit.contains(relay)
    }

    pub async fn in_same_subnet_or_family_contains(&self, relay: &Arc<Node>) -> bool {
        let in_same_subnet_or_family = self.in_same_subnet_or_family.read().await;
        in_same_subnet_or_family.contains(relay)
    }

    /// Add a fresh Relay that we got from NetDir, to make circuits with
    ///
    // TODO: Understand if we really need to push it to the last or at what position,
    // do we have to filter the to_be_used_as_exit vector
    pub async fn add_a_node_in_to_be_used_as_exit(&self, node: Arc<Self>) {
        let mut to_be_used_as_exit = self.to_be_used_as_exit.write().await;

        if !self.already_used_as_exit_contains(&node).await
            && !self.in_same_subnet_or_family_contains(&node).await
            && self.fingerprint != node.fingerprint
        {
            to_be_used_as_exit.insert(node);
        }
    }

    // Add all relays in already used as exit
    pub async fn add_relays_in_already_used_as_exit<I: IntoIterator<Item = Arc<Node>>>(&self, nodes: I) {
        let mut already_used_as_exit = self.already_used_as_exit.write().await;
        for node in nodes.into_iter() {
            already_used_as_exit.insert(node);
        }
    }

    // Add all relays that this relay should use as exit relay
    pub async fn add_relays_in_to_be_used_as_exit<I: IntoIterator<Item = Arc<Node>>>(&self, nodes: I) {
        let mut to_be_used_as_exit = self.to_be_used_as_exit.write().await;
        for node in nodes.into_iter() {
            if !self.already_used_as_exit_contains(&node).await
                && !self.in_same_subnet_or_family_contains(&node).await
                && self.fingerprint != node.fingerprint
            {
                to_be_used_as_exit.insert(node);
            }
        }
    }

    /// Add a [Node] that's in the same subnet or same family as this [Node]
    pub async fn add_a_node_in_same_subnet_or_family(&self, node: Arc<Self>) {
        let mut in_same_subnet_or_family = self.in_same_subnet_or_family.write().await;
        in_same_subnet_or_family.insert(node);
    }

    /// Check if ```to_be_used_as_exit``` field of the ```Node``` is empty or not
    pub async fn is_to_be_used_as_exit_empty(&self) -> bool {
        let to_be_used_as_exit = self.to_be_used_as_exit.read().await;
        to_be_used_as_exit.len() == 0
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_weight = self.weight.load();
        let other_weight = other.weight.load();
        self_weight.cmp(&other_weight)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arti_client::{TorClient, TorClientConfig};
    use erpc_scanner::relay::NetDirProvider;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn create_all_nodes_from_relays_in_netdir() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();
        let dirmgr = tor_client.dirmgr();

        let netdirprovider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let (incomplete_work_sender, _) = unbounded_channel();

        let nodes: Vec<Arc<Node>> = netdirprovider
            .current_netdir()
            .await
            .relays()
            .map(|relay| Node::new(relay.rsa_id().to_string(), incomplete_work_sender.clone(), 0))
            .collect();

        for i in 0..nodes.len() {
            let mut relays_to_be_used_as_exit = Vec::new();
            for (j, _) in nodes.iter().enumerate() {
                if i != j {
                    relays_to_be_used_as_exit.push(nodes[j].clone());
                }
            }
            nodes[i].add_relays_in_to_be_used_as_exit(relays_to_be_used_as_exit).await;
        }

        let total_nodes = nodes.len();
        for node in &nodes {
            assert_eq!(node.no_of_relays_in_to_be_used_as_exit().await, total_nodes - 1);
            assert!(!node.to_be_used_as_exit_contains(node.clone()).await);
        }
    }

    #[tokio::test]
    async fn nodes_dont_contain_themsleves_in_to_be_used_as_exit() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();
        let dirmgr = tor_client.dirmgr();

        let netdirprovider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let (incomplete_work_sender, _) = unbounded_channel();

        let nodes: Vec<Arc<Node>> = netdirprovider
            .current_netdir()
            .await
            .relays()
            .map(|relay| Node::new(relay.rsa_id().to_string(), incomplete_work_sender.clone(), 0))
            .collect();

        for i in 0..nodes.len() {
            let mut relays_to_be_used_as_exit = Vec::new();
            for (j, _) in nodes.iter().enumerate() {
                if i != j {
                    relays_to_be_used_as_exit.push(nodes[j].clone());
                }
            }
            nodes[i].add_relays_in_to_be_used_as_exit(relays_to_be_used_as_exit).await;
        }

        for node in &nodes {
            assert!(!node.to_be_used_as_exit_contains(node.clone()).await);
        }
    }

    #[tokio::test]
    async fn nodes_contain_one_less_node_than_total_available_nodes_in_to_be_used_as_exit_intially() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();
        let dirmgr = tor_client.dirmgr();

        let netdirprovider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let (incomplete_work_sender, _) = unbounded_channel();

        let nodes: Vec<Arc<Node>> = netdirprovider
            .current_netdir()
            .await
            .relays()
            .map(|relay| Node::new(relay.rsa_id().to_string(), incomplete_work_sender.clone(), 0))
            .collect();

        for i in 0..nodes.len() {
            let mut relays_to_be_used_as_exit = Vec::new();
            for (j, _) in nodes.iter().enumerate() {
                if i != j {
                    relays_to_be_used_as_exit.push(nodes[j].clone());
                }
            }
            nodes[i].add_relays_in_to_be_used_as_exit(relays_to_be_used_as_exit).await;
        }

        let total_nodes = nodes.len();
        for node in &nodes {
            assert_eq!(node.no_of_relays_in_to_be_used_as_exit().await, total_nodes - 1);
        }
    }

    #[tokio::test]
    async fn start_work_on_3_nodes() {
        let fingerprints = ["0", "1", "2"];
        let mut all_possible_incomplete_works = vec![
            IncompleteWork {
                source_relay: String::from("0"),
                destination_relay: String::from("1"),
            },
            IncompleteWork {
                source_relay: String::from("0"),
                destination_relay: String::from("2"),
            },
            IncompleteWork {
                source_relay: String::from("1"),
                destination_relay: String::from("0"),
            },
            IncompleteWork {
                source_relay: String::from("1"),
                destination_relay: String::from("2"),
            },
            IncompleteWork {
                source_relay: String::from("2"),
                destination_relay: String::from("0"),
            },
            IncompleteWork {
                source_relay: String::from("2"),
                destination_relay: String::from("1"),
            },
        ];
        let (incomplete_work_sender, mut incomplete_work_receiver) = unbounded_channel();

        let nodes: Vec<Arc<Node>> = fingerprints
            .iter()
            .map(|fingerprint| Node::new(fingerprint, incomplete_work_sender.clone(), 0))
            .collect();

        // Assign Nodes inside of to_be_used_as_exit within each Node
        for i in 0..nodes.len() {
            let mut relays_to_be_used_as_exit = Vec::new();
            for (j, _) in nodes.iter().enumerate() {
                if i != j {
                    relays_to_be_used_as_exit.push(nodes[j].clone());
                }
            }
            nodes[i].add_relays_in_to_be_used_as_exit(relays_to_be_used_as_exit).await;
        }

        // Start task on all relays
        let _nodes_start_tasks = {
            let mut v = Vec::new();
            for node in &nodes {
                let node = node.clone();
                v.push(tokio::task::spawn(async move {
                    Node::start(node).await.unwrap();
                }));
            }
            v
        };

        let mut iteration_count = 0;
        while let Some(incomplete_work) = incomplete_work_receiver.recv().await {
            all_possible_incomplete_works.retain(|i_w| {
                !(i_w.is_source_relay(incomplete_work.source_relay.clone())
                    && i_w.is_destination_relay(incomplete_work.destination_relay.clone()))
            });

            let source_relay = incomplete_work.source_relay.clone();
            let destination_relay = incomplete_work.destination_relay.clone();

            for node in &nodes {
                if node.fingerprint == source_relay || node.fingerprint == destination_relay {
                    let completed_work = CompletedWork {
                        source_relay: source_relay.clone(),
                        destination_relay: destination_relay.clone(),
                        status: erpc_scanner::work::CompletedWorkStatus::Success,
                        timestamp: 10,
                    };

                    let _node = node.clone();
                    tokio::task::spawn(async move {
                        _node.submit_completed_work(completed_work).await;
                    });
                }
            }

            iteration_count += 1;
            if iteration_count == 6 {
                assert_eq!(all_possible_incomplete_works.len(), 0);
                break;
            }
        }
    }
}
