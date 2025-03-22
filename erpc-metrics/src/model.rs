use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Data Structure that maps to OnionPerf Analysis JSON data (Version : 3.1)
#[derive(Deserialize, Serialize)]
pub struct OnionPerfAnalysis {
    /// Measurement data by source name
    /// The onionperf host name seems to be the only key here
    pub data: HashMap<String, Data>,

    #[serde(skip_deserializing, skip_serializing)]
    pub filters: Option<Value>,

    //Document type
    #[serde(rename = "type")]
    pub _type: String,

    //Document version
    pub version: String,

    // UTC date at which his OnionPerfAnalysis file was received
    // Date is in format of **YYYY-MM-DD**
    #[serde(skip_deserializing, skip_serializing)]
    pub time: Option<String>,
}

pub struct OnionPerfTwoHopCircuit {
    /// The fingerprint of the first hop relay
    pub first_hop: String,

    /// The fingerprint of the second hop relay
    pub second_hop: String,

    /// 0 means failed 1 means success
    pub status: u8,

    // The message
    pub message: String,

    pub timestamp: u64,
}

impl OnionPerfAnalysis {
    pub fn get_all_failed_circuits(&self) {
        unimplemented!();
        //  for data in self.data.values() {
        //      for circuit in data.tor.circuits.values() {
        //          if circuit.failure_reason_local.is_none() && circuit.failure_reason_remote.is_none() {
        //              if let Some(ref relay_detail) = circuit.path {}
        //          }

        //          //if let Some(ref failure_reason_remote) = circuit.failure_reason_local {
        //          //if circuit.path.is_some() {
        //          //let len = circuit.path.as_ref().unwrap().len();
        //          //print!("{failure_reason_remote} {:#?}", len);
        //          //continue;
        //          //}
        //          //}

        //          //if let Some(ref failure_reason_local) = circuit.failure_reason_local {}
        //      }
        //  }
    }

    ///Extract out all two hop combinations from successful circuits
    pub fn get_all_successful_two_hop_paths(&self) -> Vec<OnionPerfTwoHopCircuit> {
        let mut two_hop_paths: Vec<Vec<OnionPerfTwoHopCircuit>> = Vec::default();
        for data in self.data.values() {
            for circuit in data.tor.circuits.values() {
                if circuit.failure_reason_local.is_none() && circuit.failure_reason_remote.is_none() {
                    if let Some(fil) = circuit.filtered_out {
                        if !fil {
                            if let Some(ref relays_detail) = circuit.path {
                                let two_hop_path = Self::generate_two_hops(0, &relays_detail[..], circuit.unix_ts_start as u64);
                                two_hop_paths.push(two_hop_path);
                            }
                        }
                    }
                }
            }
        }

        two_hop_paths.into_iter().flatten().collect()
    }

    /// Generate two hop circuit combinations from a path recursively
    pub fn generate_two_hops(index: usize, path: &[RelayDetail], timestamp: u64) -> Vec<OnionPerfTwoHopCircuit> {
        if index + 1 >= path.len() {
            vec![]
        } else {
            let relay_detail_1 = path.get(index).unwrap();
            let relay_detail_2 = path.get(index + 1).unwrap();

            let mut pairs = vec![];

            if let (Some(first_hop), Some(second_hop)) = (relay_detail_1.rsa_fingeprint(), relay_detail_2.rsa_fingeprint()) {
                let onionperf_two_hop_circuit = OnionPerfTwoHopCircuit {
                    first_hop,
                    second_hop,
                    status: 1,
                    message: String::from("(OnionPerf) Success"),
                    timestamp,
                };

                pairs.push(onionperf_two_hop_circuit);
            }

            //Recurse to generate pairs for the next index
            let mut sub_pairs = OnionPerfAnalysis::generate_two_hops(index + 1, path, timestamp);
            pairs.append(&mut sub_pairs);

            pairs
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Data {
    ///Public IP address of the measuring host
    measurement_ip: String,

    ///Measurement data obtained from client-side TGen logs
    tgen: Value,

    //Metadata obtained from client-side Tor controller logs
    tor: Tor,
}

#[derive(Serialize, Deserialize)]
pub struct Tor {
    circuits: HashMap<String, Circuit>,

    #[serde(skip_deserializing)]
    streams: Option<Value>,

    #[serde(skip_deserializing)]
    guards: Option<Value>,
}

///Information about Tor circuits, by circuit identifier, obtained from CIRC and CIRC_MINOR
///events, for all circuits created by the Tor client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Circuit {
    //Circuit identifier, obtained from CIRC and CIRC_MINOR events
    circuit_id: usize,

    //Elapsed seconds until receiving and logging CIRC and CIRC_MINOR events
    elapsed_seconds: Vec<Value>,

    /// Final end time of the circuit, obtained from the log time of the last CIRC CLOSED or CIRC FAILED event, given in seconds since the epoch
    unix_ts_start: f32,

    /// Initial start time of the circuit, obtained from the log time of the CIRC LAUNCHED event, given in seconds since the epoch"
    unix_ts_end: f32,

    ///Local failure reason, obtained from CIRC FAILED events"
    failure_reason_local: Option<String>,

    ///Remote failure reason, obtained from CIRC FAILED events"
    failure_reason_remote: Option<String>,

    ///Build time in seconds, computed as time elapsed between CIRC LAUNCHED and CIRC BUILT events
    buildtime_seconds: Option<f32>,

    ///Whether this circuit has been filtered out when applying filters in `onionperf filter`
    ///TODO: Figure out what onionperf filter means
    filtered_out: Option<bool>,

    ///Path information
    path: Option<Vec<RelayDetail>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RelayDetail {
    #[serde(rename = "0")]
    fingerprint: String,

    #[serde(rename = "1")]
    time_passed: f32,
}

impl RelayDetail {
    /// The fingerprint field from the file is in the form **$DD0C8EEC5CA402A9FA4478F10C31A440F71F6885~chaosDelroth**
    /// We need to only extract out upto ~ starting form $
    pub fn rsa_fingeprint(&self) -> Option<String> {
        let parts: Vec<&str> = self.fingerprint.split('~').collect();
        if parts.len() == 2 {
            Some(parts[0].to_lowercase())
        } else {
            None
        }
    }
}
