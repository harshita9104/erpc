//! Relay storing bodies

use futures::stream::StreamExt;
use log::debug;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    broadcast::{channel, Receiver, Sender},
    RwLock,
};
use tor_dirmgr::DirProvider;
use tor_netdir::{DirEvent, NetDir, Relay};

/// NetDirProvider acts as a wrapper around the DirProvider trait object to be used to provide the latest NetDir,
/// by latest I mean whenever the NetDir is updated internally through the DirProvider trait
/// object, the new NetDir is saved
///
/// The NetDir is then used to build a [RelaysPool]
pub struct NetDirProvider {
    /// The trait object used to get the latest NetDir
    dirmgr: Arc<dyn DirProvider>,

    /// The current NetDir in use
    netdir: Arc<RwLock<Arc<NetDir>>>,

    /// The Sender Half ot the Broadcast channel to send the changed NetDir
    /// One can use netdirevents_sender.subscribe() to get the receiving handle
    netdirevents_sender: Sender<NetDirProviderEvent>,

    /// The Receiver Half of the broadcast channel to receive the latest NetDir
    // It's used to store the reciever half temporarily so that the channel doesn't drop
    #[allow(unused, dead_code)]
    netdirevents_receiver: Receiver<NetDirProviderEvent>,
}

impl NetDirProvider {
    /// Create a new NetDirProvider from a DirProvider trait object.
    pub async fn from_dirmgr(dirmgr: Arc<dyn DirProvider>) -> anyhow::Result<Arc<Self>> {
        let netdir = dirmgr.timely_netdir()?;

        // Channel to redirect the events from DirMgr to NetDirProviderEvents(New NetDir)
        let (netdirevents_sender, netdirevents_receiver) = channel::<NetDirProviderEvent>(1);
        let _netdirevents_sender = netdirevents_sender.clone();

        let netdir_provider = Arc::new(Self {
            dirmgr,
            netdir: Arc::new(RwLock::new(netdir)),
            netdirevents_sender,
            netdirevents_receiver,
        });

        // A task spawned to auto update NetDir we hold in self.netdir by listening to the events
        // produces by DirMgr and getting a new NetDir and sending that NetDir as an event through
        // self.netdirevents_receiver
        //
        let _netdir_provider = netdir_provider.clone();
        tokio::task::spawn(async move {
            _netdir_provider.auto_update(_netdirevents_sender).await;
        });

        Ok(netdir_provider)
    }

    /// Auto update the NetDir by waiting on the events from DirMgr
    async fn auto_update(&self, netdirevents_sender: Sender<NetDirProviderEvent>) {
        while let Some(dir_event) = self.dirmgr.events().next().await {
            debug!("Received a DirEvent {dir_event:?} from DirMgr");
            if dir_event == DirEvent::NewConsensus {
                let mut current_netdir = self.netdir.write().await;

                if let Ok(new_netdir) = self.dirmgr.timely_netdir() {
                    *current_netdir = new_netdir;
                    netdirevents_sender
                        .send(NetDirProviderEvent::NetDirChanged(current_netdir.clone()))
                        .expect("The channel to send the new NetDir seems to be closed");
                }
            }
        }
    }

    /// Get the latest NetDir
    pub async fn current_netdir(&self) -> Arc<NetDir> {
        let netdir = self.netdir.read().await;
        netdir.clone()
    }

    /// Gives you the receiver handle of the channel to listen for the NetDirProviderEvent
    pub fn get_netdirprodiver_event_receiver(&self) -> Receiver<NetDirProviderEvent> {
        self.netdirevents_sender.subscribe()
    }
}

/// Event that represents the arrival of new NetDir
#[derive(Debug, Clone)]
pub enum NetDirProviderEvent {
    /// NetDir has changed and it encapsulates the latest NetDir
    NetDirChanged(Arc<NetDir>),
}

/// RelaysPool stores the hashmap of Fingerprint of the Relay and the Relay itself and provides the
/// abstraction to get the reference to the relay through the ```get_relay(fingerprint)```  method
///
/// The RelaysPool holds the hashmap of <RELAY_FINGERPRINT, tor_netdir::Relay>, so that the Tor
/// Network graph stores only data enough to index the Relay i.e the RELAY_FINGERPRINT and
/// whenever it needs some data related to a certain relay, it simply requests this pool a
/// reference to the tor_netdir::Relay through ```get_relay(RELAY_FINGERPRINT)``` method
/// where RELAY_FINGERPRINT is a string that represents the stringified RSA ID of the relay
pub struct RelaysPool<'a, T> {
    /// Maps <Fingerprint, Relay>
    pub relays_hashmap: HashMap<String, (Relay<'a>, T)>,
}

impl<'a, T: Clone> RelaysPool<'a, T> {
    /// Create empty ```RelaysPool```
    ///
    /// It basically makes the inner HashMap empty
    pub fn empty() -> Self {
        let relays_hashmap = HashMap::<String, (Relay<'a>, T)>::new();
        Self { relays_hashmap }
    }
    /// Takes the NetDir and creaates a [RelaysPool]
    ///
    /// Why not get Arc<NetDir> and build the RelaysPool directly from there?
    /// Well, it seems that when we take Arc<NetDir> as input parameter and get relays through
    /// Relay<'a>, the lifetime of the Relay is tied to this parameter itself and
    /// then we cannot send the [RelaysPool] we created out of this method
    pub fn from_relays(relays: &[(Relay<'a>, T)]) -> Self {
        let mut relays_hashmap = HashMap::<String, (Relay<'a>, T)>::new();

        for relay in relays.iter() {
            let key = relay.0.rsa_id().to_string();
            relays_hashmap.insert(key, (relay.0.clone(), relay.1.clone()));
        }

        Self { relays_hashmap }
    }

    /// Gets you the reference to the Relay as you provide the fingerprint of the relay
    pub fn get_relay<S: AsRef<str>>(&self, fingerprint: S) -> Option<&(Relay<'a>, T)> {
        self.relays_hashmap.get(fingerprint.as_ref())
    }

    /// Add a ```tor_netdir::Relay<'_>``` into the RelaysPool with a certain fingerprint as the key
    pub fn add_relay<S: ToString>(&mut self, fingerprint: S, relay: Relay<'a>, v: T) {
        self.relays_hashmap.insert(fingerprint.to_string(), (relay, v));
    }

    /// Calculate total no of Relays in the ```RelaysPool```
    pub fn total_relays(&self) -> usize {
        self.relays_hashmap.len()
    }
}
