//! The module for [Scanner], necessary to attempt circuit builds
//!
//! This module only contains one type i.e [Scanner] and it's to be used where you need to
//! test where two hop circuit of given two relays gets built or not
//!
//! **X ---(hop 1)--- RELAY1 ---(hop 2)--- RELAY2**
//!
//! The [Scanner] is built to check if two relays are partitioned or not, on a high level, you push something called [IncompleteWork] using
//! the ```push_incomplete_work```, which is basically the Fingerprint of Relay1 and Relay2, whose two hop circuits we want to build, if this
//! two hop circuit get's built then **(AS OF RIGHT NOW)** we consider that relays is not partitioned and if it fails then (as of right now)
//! we consider that relays are partitioned (Of course direction is important here, that's why ```IncompleteWork``` has a source_relay and
//! destination_relay field)
//!
//! We initiate a [Scanner], with the ```no of clients``` we want, it basically means that those no of clients will attempt to create a circuit through the
//! ```CircuitBuilder``` at once, if they are all loaded with an ```IncompleteWork``` what happens underneath is that we spawn a single
//! ```arti_client::TorClient``` and we clone it's ```CircMgr``` and ```DirMgr```, which is used to build a type called ```NetDirProvider```, that has a broadcast
//! channel to give out a fresh ```Arc<NetDir>``` to it's subscribers, the ```Client```
//!
use crate::relay::NetDirProvider;

use super::{
    pool::ClientPool,
    work::{CompletedWork, IncompleteWork},
};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tor_circmgr::CircMgr;
use tor_rtcompat::PreferredRuntime;

/// A utility to test for two hop circuits
///
/// It's mainly used for completing the [IncompleteWork] into [CompletedWork] by attempting to create a circuit,
/// it distributes the [IncompleteWork] provided to it through the ```push_incomplete_work```
/// method, among all the ```Client``` it has spawned internally. These ```Client``` basically hold clones of ```CircMgr```
/// and underneath they call the method to build circuit through the ```CircuitBuilder``` abstracted by
/// ```CircMgr```
///
/// If we were to push 1000 [IncompleteWork] into internal channel of the [Scanner], then the speed
/// of completion of those work whould depend mainly on no of clients we spawned within the
/// [Scanner], which can be controlled by passing a ```u8``` value inside of ```Scanner::new()```
pub struct Scanner {
    /// Pool of all tor [Client]s(including the parent tor client)
    pool: ClientPool<PreferredRuntime>,
}

impl Scanner {
    /// Create a [Scanner], behind the scenes it creates a single arti ```TorClient``` and
    /// it's ```DirMgr```, ```CircMgr``` is used for all the so called ```Client```
    ///
    /// We only have to provide no of ```Client``` we wanna spin in the pool, just make sure it's not 0
    pub async fn new(
        no_of_parallel_circuit_builds: u32,
        netdir_provider: Arc<NetDirProvider>,
        circmgr: Arc<CircMgr<PreferredRuntime>>,
    ) -> Result<Self> {
        let pool = if no_of_parallel_circuit_builds < 1 {
            return Err(anyhow!("You should provide atleast have one client to build"));
        } else {
            ClientPool::new(no_of_parallel_circuit_builds, netdir_provider, circmgr).await?
        };

        let scanner = Scanner { pool };
        Ok(scanner)
    }

    /// Push [IncompleteWork] into the internal buffer of the [Scanner]
    pub fn push_incomplete_work(&self, work: IncompleteWork) -> anyhow::Result<()> {
        self.pool.push_incomplete_work(work)
    }

    /// Receive the [CompletedWork] over time from the internal buffer of the [Scanner]
    pub async fn recv_completed_work(&self) -> Option<CompletedWork> {
        self.pool.recv_completed_work().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::CompletedWorkStatus;
    use arti_client::{TorClient, TorClientConfig};
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    #[should_panic]
    async fn build_scanner_with_0_clients() {
        let tor_client_config = TorClientConfig::default();
        if let Ok(tor_client) = TorClient::create_bootstrapped(tor_client_config).await {
            let dirmgr = tor_client.dirmgr().clone();
            if let Ok(netdir_provider) = NetDirProvider::from_dirmgr(dirmgr).await {
                let circmgr = tor_client.circmgr().clone();
                let no_of_clients = 0;
                let _ = Scanner::new(no_of_clients, netdir_provider, circmgr).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn build_scanner_with_high_number_of_clients() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();

        let dirmgr = tor_client.dirmgr().clone();
        let circmgr = tor_client.circmgr().clone();
        let netdir_provider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let no_of_clients = 10000;
        let _ = Scanner::new(no_of_clients, netdir_provider, circmgr).await.unwrap();
    }

    #[tokio::test]
    async fn scanner_completes_all_assigned_number_of_works() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();

        let dirmgr = tor_client.dirmgr().clone();
        let circmgr = tor_client.circmgr().clone();
        let netdir_provider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let no_of_clients = 10;
        let scanner = Scanner::new(no_of_clients, netdir_provider, circmgr).await.unwrap();

        let x_y = IncompleteWork {
            source_relay: "$f956360aa5f1e61064e2671fe231c5064d2afead".to_string(),
            destination_relay: "$ffb8295543e765fddcfe63ad63c10b307604f72c".to_string(),
        };

        let y_x = IncompleteWork {
            source_relay: "$ffb8295543e765fddcfe63ad63c10b307604f72c".to_string(),
            destination_relay: "$f956360aa5f1e61064e2671fe231c5064d2afead".to_string(),
        };

        scanner.push_incomplete_work(x_y).unwrap();
        scanner.push_incomplete_work(y_x).unwrap();

        // Even waiting for upto 10 seconds, it should give us only 2 CompletedWork
        //
        // This proves that all the work assigned(only 2) was finished and no any extra
        // CompletedWork was given to us
        let mut completed_works = Vec::new();
        let _ = time::timeout(Duration::from_secs(20), async {
            while let Some(cw) = scanner.recv_completed_work().await {
                completed_works.push(cw);
            }
        })
        .await;

        assert_eq!(2, completed_works.len());
    }

    #[tokio::test]
    async fn testing_for_circuit_which_doesnt_exist_in_netdir() {
        let tor_client_config = TorClientConfig::default();
        let tor_client = TorClient::create_bootstrapped(tor_client_config).await.unwrap();

        let dirmgr = tor_client.dirmgr().clone();
        let circmgr = tor_client.circmgr().clone();
        let netdir_provider = NetDirProvider::from_dirmgr(dirmgr.clone()).await.unwrap();

        let no_of_clients = 10;
        let scanner = Scanner::new(no_of_clients, netdir_provider, circmgr).await.unwrap();

        let x_y = IncompleteWork {
            source_relay: "XXXXX".to_string(),
            destination_relay: "YYYYY".to_string(),
        };

        scanner.push_incomplete_work(x_y.clone()).unwrap();

        // Even waiting for upto 10 seconds, it should give us only one CompletedWork
        // This proves that all the work assigned(only 1) was finished and no any extra
        // CompletedWork was given to us
        let mut completed_works = Vec::new();
        let _ = time::timeout(Duration::from_secs(10), async {
            while let Some(cw) = scanner.recv_completed_work().await {
                completed_works.push(cw);
            }
        })
        .await;

        assert_eq!(1, completed_works.len());
        assert_ne!(completed_works[0].status, CompletedWorkStatus::Success);
        assert!(x_y.is_source_relay(completed_works[0].source_relay.as_str()));
        assert!(x_y.is_destination_relay(completed_works[0].destination_relay.as_str()));
    }
}
