//! Downloads the onionperf analysis file from date in in format of **YYYY-MM-DD**.
//! Eg(2023-10-21)
//!
//! It generates the URL
//! https://op-de7a.onionperf.torproject.net:8443/htdocs/2023-08-05.onionperf.analysis.json.xz
//! **or**
//! https://op-de7a.onionperf.torproject.net:8443/2023-08-05.onionperf.analysis.json.xz
//!
//! Only one of the two url works

use super::model::OnionPerfAnalysis;
use super::utils::decompress_xz;
use chrono::{DateTime, Datelike, Days, Utc};
use reqwest::{StatusCode, Url};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;

// The paths to lookup for onionperfs analysis file
const ONION_PERF_ANALYAIS_PATHS: [&str; 2] = ["/htdocs/", "/"];

// The interval (hour) to recheck for existence of the file
const RECHECK_INTERVAL: u64 = 2;

/// The client to connect to a OnionPerfRunner Host and get the most recent OnionPerf Analysis file
/// at certain intervals
pub struct OnionPerfRunner {
    /// Name of the host **Eg. "op-de7a"**
    pub host_name: String,

    /// The UTC time(only day) at which the OnionPerf analysis file have been already checked and need not to be checked again
    already_checked: Arc<Mutex<Vec<DateTime<Utc>>>>,

    /// The sender half to send the OnionPerfAnalysis files
    onionperf_analysis_sender: UnboundedSender<OnionPerfAnalysis>,

    /// The receiver stream to receive the OnionPerfAnalysis files
    pub onionperf_analysis_receiver_stream: Arc<Mutex<UnboundedReceiverStream<OnionPerfAnalysis>>>,
}

impl OnionPerfRunner {
    /// Create a new client to get data from OnionPerfRunner Host
    pub fn new<T: AsRef<str>>(host_name: T) -> Self {
        let already_checked = Arc::default();
        let (sd, rv) = tokio::sync::mpsc::unbounded_channel::<OnionPerfAnalysis>();
        let onionperf_analysis_receiver_stream = Arc::new(Mutex::new(UnboundedReceiverStream::new(rv)));

        OnionPerfRunner {
            host_name: String::from(host_name.as_ref()),
            onionperf_analysis_sender: sd,
            onionperf_analysis_receiver_stream,
            already_checked,
        }
    }

    // Start downloading data from the OnionPerfRunner hosts
    pub async fn start(&self) {
        let mut current_utc_time = Utc::now();
        loop {
            let checked = {
                let already_checked = self.already_checked.lock().await;

                // We don't try to download again if we have already downloaded the ones whose "day" matches
                already_checked
                    .iter()
                    .any(|already_checked_utc_time| already_checked_utc_time.day() == current_utc_time.day())
            };

            if !checked {
                let current_utc_date = self.generate_date(current_utc_time);
                let mut tries = 0;

                // Loop for retrying the failed item due to decompresssion issue or parsing issue
                'decompression_parsing_error_try_iteration: loop {
                    match self.download_onionperf_analysis_file(current_utc_date.clone()).await {
                        Ok(onionperf_analysis_file_compressed) => match decompress_xz(&onionperf_analysis_file_compressed).await {
                            Ok(onionperf_analysis_file_decompressed) => {
                                // Parsing the OnionPerf analysis file
                                let onionperf_analysis =
                                    serde_json::from_slice::<OnionPerfAnalysis>(&onionperf_analysis_file_decompressed[..]);

                                match onionperf_analysis {
                                    Ok(mut v) => {
                                        v.time = Some(current_utc_date);
                                        let _ = self.onionperf_analysis_sender.send(v);

                                        let mut already_checked = self.already_checked.lock().await;
                                        already_checked.push(current_utc_time);
                                        break 'decompression_parsing_error_try_iteration;
                                    }
                                    Err(e) => {
                                        // If there was parsing issue we'll download again
                                        log::error!("There was error on parsing OnionPerfAnalysis JSON file at tries no {tries} {:?}", e);
                                        tries += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "There was error on decompressing the xz compressed OnionPerfAnalysis JSON file aat tries no {tries} {:?}",
                                    e
                                );
                                tries += 1;
                                // If there was decompression issue then we'll download the file
                                // with same date
                            }
                        },
                        Err(onionperfrunner_error) => {
                            match onionperfrunner_error {
                                // Reasons :
                                // - We're checking the wrong date(usually too ahead of time), for that we can reduce the day by 1 (TODO : add a guard on how far back we can go by days)
                                // - The file has (actually not been published, how do we determine that?) and we need to wait
                                // for the files by the onionperf server to publish(TODO: Imp!!!!)
                                OnionPerfRunnerError::OnionPerfAnalysisFileNotFound => {
                                    log::warn!("The OnionPerfAnalysis file was not found, it mostly due to file not being available at the link at this time 
                                                and server throwing 404 errors");

                                    current_utc_time = current_utc_time.checked_sub_days(Days::new(1)).unwrap();
                                    let checked = {
                                        let already_checked = self.already_checked.lock().await;

                                        // We don't try to download again if we have already downloaded the ones whose "day" matches
                                        already_checked
                                            .iter()
                                            .any(|already_checked_utc_time| already_checked_utc_time.day() == current_utc_time.day())
                                    };

                                    // If the date has been checked then we go back to where we were
                                    // left at before
                                    if checked {
                                        current_utc_time = current_utc_time.checked_add_days(Days::new(1)).unwrap();
                                    }
                                    break 'decompression_parsing_error_try_iteration;
                                }
                                OnionPerfRunnerError::OnionPerfAnalysisNetworkError => {
                                    log::error!("Network error has occured");
                                }
                            }
                        }
                    };

                    // We'll make max of 5 iterations
                    if tries >= 5 {
                        sleep(Duration::from_secs(RECHECK_INTERVAL * 60 * 60)).await;
                        break 'decompression_parsing_error_try_iteration;
                    }
                }
            } else {
                // Sleep for 4 hours if the latest UTC Time data was also already fetched
                sleep(Duration::from_secs(RECHECK_INTERVAL * 60 * 60)).await;

                // We'll increase the time by 1 day, because the current date has been already
                // checked
                current_utc_time = current_utc_time.checked_add_days(Days::new(1)).unwrap();
            }
        }
    }

    pub async fn stop() {
        // Make use of one shot channel to stop the running loop in start()
        unimplemented!()
    }

    /// Downloads and decompresses the onionperf analysis file that's in ".xz" form
    async fn download_onionperf_analysis_file(&self, date: String) -> Result<Vec<u8>, OnionPerfRunnerError> {
        let (url_1, url_2) = self.generate_onionperf_analysis_urls(date);

        let reqwest_client = reqwest::Client::new();

        let resp_1 = reqwest_client.get(url_1).send().await;
        let resp_2 = reqwest_client.get(url_2).send().await;

        if let Ok(resp) = resp_1 {
            if resp.status() == StatusCode::OK {
                let data = resp.bytes().await.unwrap().into();
                return Ok(data);
            }
        }

        match resp_2 {
            Ok(resp) => {
                if resp.status() == StatusCode::OK {
                    let data = resp.bytes().await.unwrap().into();
                    Ok(data)
                } else {
                    Err(OnionPerfRunnerError::OnionPerfAnalysisFileNotFound)
                }
            }
            Err(_) => {
                // TODO : LOg error
                Err(OnionPerfRunnerError::OnionPerfAnalysisNetworkError)
            }
        }
    }

    /// The given UTC time in DATE format of "YYYY-MM-DD". Eg(2023-10-21)
    fn generate_date(&self, time: DateTime<Utc>) -> String {
        let year = time.year();
        let month = time.month();
        let day = time.day();

        let date = format!("{year}-{month:02}-{day:02}");
        date
    }

    /// Generate URLs for downloading the OnionPerf analysis file according to the current UTC date
    fn generate_onionperf_analysis_urls(&self, date: String) -> (Url, Url) {
        let (url_1, url_2) = {
            let url_1 = format!(
                "https://{}.onionperf.torproject.net:8443{}{}",
                self.host_name,
                ONION_PERF_ANALYAIS_PATHS[0],
                format_args!("{}.onionperf.analysis.json.xz", date)
            );
            let url_2 = format!(
                "https://{}.onionperf.torproject.net:8443{}{}",
                self.host_name,
                ONION_PERF_ANALYAIS_PATHS[1],
                format_args!("{}.onionperf.analysis.json.xz", date)
            );

            log::debug!("Trying to download onionperf analysis either from {url_1} or {url_2}");

            (Url::parse(&url_1), Url::parse(&url_2))
        };

        (url_1.unwrap(), url_2.unwrap())
    }
}

#[derive(Debug, thiserror::Error)]
enum OnionPerfRunnerError {
    #[error("The onion perf analysis file could not found at both links")]
    OnionPerfAnalysisFileNotFound,

    #[error("Can't connect to both links")]
    OnionPerfAnalysisNetworkError,
}

#[cfg(test)]
mod tests {

    use super::OnionPerfRunner;
    use std::sync::Arc;

    #[tokio::test]
    async fn test() {
        let onionperf_runner = Arc::new(OnionPerfRunner::new("op-us7a"));
        let _onionperf_runner = onionperf_runner;

        // TODO: Add test here
        //tokio::spawn(async move {
        //    onionperf_runner.start().await;
        //});

        //let mut onionperf_analysis_receiver_stream = _onionperf_runner.onionperf_analysis_receiver_stream.lock().await;
        //while let Some(x) = onionperf_analysis_receiver_stream.next().await {
        //    let x = x.get_all_successful_two_hop_paths();
        //    println!("{:?}", x.len());
        //}
    }
}
