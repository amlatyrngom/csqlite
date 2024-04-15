use obelisk::{HandlerKit, ScalingState, ServerlessHandler};

use crate::client::KVClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const START_KEY: usize = 1_000_000_000; // Ensures same key length. Must have 10 characters.
pub const KEY_SIZE_B: usize = 100; // Keep small
pub const VAL_SIZE_B: usize = 9900; // Sum of val + key should be multiple of 1000
pub const DATA_SIZE_MB: usize = 20 * 1000; // 20GB
pub const DATA_SIZE_KB: usize = DATA_SIZE_MB * 1000;
pub const NUM_KEYS: usize = DATA_SIZE_KB / ((VAL_SIZE_B + KEY_SIZE_B) / 1000);

pub struct BenchFn {
    // dbcl: Arc<DBClient>,
    kvcl: Arc<KVClient>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum BenchReq {
    ReadOnly(Vec<usize>),
    ReadWrite(Vec<usize>, Vec<usize>),
}

#[async_trait::async_trait]
impl ServerlessHandler for BenchFn {
    /// Call the benchmark function.
    async fn handle(&self, meta: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: BenchReq = serde_json::from_str(&meta).unwrap();
        match req {
            BenchReq::ReadOnly(rkeys) => self.handle_request(rkeys, vec![]).await,
            BenchReq::ReadWrite(rkeys, wkeys) => self.handle_request(rkeys, wkeys).await,
        }
    }

    /// Do checkpointing.
    async fn checkpoint(&self, _scaling_state: &ScalingState, _terminating: bool) {}
}

impl BenchFn {
    /// Create new function.
    pub async fn new(_kit: HandlerKit) -> Self {
        let kvcl = Arc::new(KVClient::new(Some(0), Some(512)).await);
        BenchFn { kvcl }
    }

    pub fn make_key_val(key: usize, key_size_b: usize, val_size_b: usize) -> (String, String) {
        let key = key.to_string();
        let key = key.repeat(key_size_b / key.len());
        let val = key.repeat(val_size_b / key.len());
        (key, val)
    }

    async fn handle_request(&self, rkeys: Vec<usize>, wkeys: Vec<usize>) -> (String, Vec<u8>) {
        let mut durations = Vec::new();
        // Read keys.
        for key in rkeys {
            let (key, _val) = Self::make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
            let start_time = std::time::Instant::now();
            let resp = self.kvcl.txn(vec![], vec![], vec![key], false).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if resp.is_ok() {
                durations.push((duration, String::from("Read")));
            }
        }
        // Write keys.
        for key in wkeys {
            let (key, val) = Self::make_key_val(key, KEY_SIZE_B, VAL_SIZE_B);
            let start_time = std::time::Instant::now();
            let resp = self
                .kvcl
                .txn(vec![], vec![(key, Some(val))], vec![], false)
                .await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if resp.is_ok() {
                durations.push((duration, String::from("Write")));
            }
        }
        (serde_json::to_string(&durations).unwrap(), vec![])
    }
}

#[cfg(test)]
mod tests {
    use obelisk::FunctionalClient;
    use rand::prelude::Distribution;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::bench::BenchReq;

    use super::{NUM_KEYS, START_KEY};

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_test() {
        run_simple_bench_test().await;
    }

    async fn run_simple_bench_test() {
        let fc = Arc::new(FunctionalClient::new("csqlite", "dbbench", None, Some(512)).await);
        let rkeys: Vec<usize> = vec![37];
        let req = BenchReq::ReadWrite(rkeys, vec![]);
        let meta = serde_json::to_string(&req).unwrap();
        let (resp, _) = fc.invoke(&meta, &[]).await.unwrap();
        let resp: Vec<(Duration, String)> = serde_json::from_str(&resp).unwrap();
        println!("Resp: {resp:?}");
    }

    /// Write bench output.
    async fn write_bench_output(points: Vec<(u64, f64, String)>, expt_name: &str) {
        let expt_dir = "results/csqlite";
        std::fs::create_dir_all(expt_dir).unwrap();
        let mut writer = csv::WriterBuilder::new()
            .from_path(format!("{expt_dir}/{expt_name}.csv"))
            .unwrap();
        for (since, duration, operation) in points {
            writer
                .write_record(&[since.to_string(), duration.to_string(), operation])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    // #[derive(Debug)]
    // enum RequestRate {
    //     Low,
    //     Medium,
    //     High(usize),
    // }

    #[derive(Debug, Clone)]
    enum RequestPattern {
        Zipf(bool),
        Small(bool),
    }

    fn make_req(pattern: &RequestPattern, per_request_count: usize) -> (String, Vec<u8>) {
        let (keys, read_only) = match pattern {
            RequestPattern::Small(read_only) => {
                let keys: Vec<usize> = (START_KEY..(START_KEY + per_request_count)).collect();
                (keys, *read_only)
            }
            RequestPattern::Zipf(read_only) => {
                let mut rng = rand::thread_rng();
                let zipf = zipf::ZipfDistribution::new(NUM_KEYS, 0.5).unwrap();
                let keys = (0..per_request_count)
                    .map(|_| {
                        START_KEY + zipf.sample(&mut rng) - 1 // Starts from 1.
                    })
                    .collect();
                (keys, *read_only)
            }
        };
        let (rkeys, wkeys) = if read_only {
            (keys, vec![])
        } else {
            let mid = (per_request_count + 1) / 2;
            let rkeys = &keys[..mid];
            let wkeys = &keys[mid..];
            (rkeys.into(), wkeys.into())
        };
        let req = BenchReq::ReadWrite(rkeys, wkeys);
        let meta = serde_json::to_string(&req).unwrap();
        (meta, vec![])
    }

    struct RequestSender {
        pattern: RequestPattern,
        curr_avg_latency: f64,
        desired_requests_per_second: f64,
        fc: Arc<FunctionalClient>,
    }

    impl RequestSender {
        // Next 5 seconds of requests
        async fn send_request_window(&mut self) -> Vec<(Duration, String)> {
            // Window duration.
            let window_duration = 5.0;
            let num_needed_threads =
                (self.desired_requests_per_second * self.curr_avg_latency).ceil();
            let total_num_requests = window_duration * self.desired_requests_per_second;
            let requests_per_thread = (total_num_requests / num_needed_threads).ceil();
            let actual_total_num_requests = requests_per_thread * num_needed_threads;
            let num_needed_threads = num_needed_threads as u64;
            println!("NT={num_needed_threads}; RPT={requests_per_thread};");
            let mut ts = Vec::new();
            let overall_start_time = Instant::now();
            for _ in 0..num_needed_threads {
                let requests_per_thread = requests_per_thread as u64;
                let fc = self.fc.clone();
                let pattern = self.pattern.clone();
                let t = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let mut responses = Vec::new();
                    let mut curr_idx = 0;
                    while curr_idx < requests_per_thread {
                        // Find number of calls to make and update curr idx.
                        let batch_size = 10;
                        let call_count = if requests_per_thread - curr_idx < batch_size {
                            requests_per_thread - curr_idx
                        } else {
                            batch_size
                        };
                        curr_idx += call_count;
                        // Now send requests.
                        let (meta, payload) = make_req(&pattern, call_count as usize);
                        let resp = fc.invoke(&meta, &payload).await;
                        if resp.is_err() {
                            println!("Err: {resp:?}");
                            continue;
                        }
                        let (resp, _) = resp.unwrap();
                        let mut resp: Vec<(Duration, String)> =
                            serde_json::from_str(&resp).unwrap();
                        responses.append(&mut resp);
                    }
                    let end_time = std::time::Instant::now();
                    let duration = end_time.duration_since(start_time);

                    (duration, responses)
                });
                ts.push(t);
            }
            let mut sum_duration = Duration::from_millis(0);
            let mut all_responses = Vec::new();
            for t in ts {
                let (duration, mut responses) = t.await.unwrap();
                sum_duration = sum_duration.checked_add(duration).unwrap();
                all_responses.append(&mut responses);
            }
            let avg_duration = sum_duration.as_secs_f64() / (actual_total_num_requests);
            self.curr_avg_latency = 0.9 * self.curr_avg_latency + 0.1 * avg_duration;
            let mut avg_req_latency =
                all_responses.iter().fold(Duration::from_secs(0), |acc, x| {
                    acc.checked_add(x.0.clone()).unwrap()
                });
            if all_responses.len() > 0 {
                avg_req_latency = avg_req_latency.div_f64(all_responses.len() as f64);
            }
            println!(
                "AVG_LATENCY={avg_duration}; CURR_AVG_LATENCY={}; REQ_LATENCY={avg_req_latency:?}",
                self.curr_avg_latency
            );
            let overall_end_time = Instant::now();
            let overall_duration = overall_end_time.duration_since(overall_start_time);
            if overall_duration.as_secs_f64() < window_duration {
                let sleep_duration =
                    Duration::from_secs_f64(window_duration - overall_duration.as_secs_f64());
                println!("Window sleeping for: {:?}.", sleep_duration);
                tokio::time::sleep(sleep_duration).await;
            }
            all_responses
        }
    }

    async fn run_bench(resquest_sender: &mut RequestSender, name: &str, test_duration: Duration) {
        let mut results = Vec::new();
        let start_time = std::time::Instant::now();
        loop {
            // Pick an image at random.
            // TODO: Find a better way to select images.
            let curr_time = std::time::Instant::now();
            let since = curr_time.duration_since(start_time);
            if since > test_duration {
                break;
            }
            let since = since.as_millis() as u64;
            let resp = resquest_sender.send_request_window().await;
            for (duration, op_type) in resp {
                results.push((since, duration.as_secs_f64(), op_type));
            }
        }
        write_bench_output(results, &format!("{name}")).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn full_bench_test() {
        let fc = Arc::new(FunctionalClient::new("csqlite", "dbbench", None, Some(512)).await);
        let duration_mins = 5.0;
        let low_req_per_secs = 2.0;
        let medium_req_per_secs = 40.0;
        let high_req_per_secs = 400.0;
        let mut request_sender = RequestSender {
            curr_avg_latency: 0.010,
            desired_requests_per_second: 0.0,
            pattern: RequestPattern::Zipf(true),
            fc: fc.clone(),
        };
        // // Low
        // request_sender.desired_requests_per_second = low_req_per_secs;
        // run_bench(
        //     &mut request_sender,
        //     "pre_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // )
        // .await;
        // Medium
        request_sender.desired_requests_per_second = medium_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_medium",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // High
        request_sender.desired_requests_per_second = high_req_per_secs;
        run_bench(
            &mut request_sender,
            "pre_high",
            Duration::from_secs_f64(60.0 * duration_mins),
        )
        .await;
        // // Low again.
        // request_sender.desired_requests_per_second = low_req_per_secs;
        // run_bench(
        //     &mut request_sender,
        //     "post_low",
        //     Duration::from_secs_f64(60.0 * duration_mins),
        // )
        // .await;
    }
}
