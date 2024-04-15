use super::client::{KVResp, KVTxn};
use obelisk::persistence::PersistentLog;
use obelisk::{HandlerKit, ScalingState, ServerlessHandler, ServerlessStorage};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct RKVActor {
    db: Arc<RwLock<Option<rocksdb::OptimisticTransactionDB>>>,
    plog: Arc<PersistentLog>,
    locked_keys: Arc<RwLock<HashSet<String>>>,
}

pub enum RKVOp {
    Read { key: String },
    Write { key: String, val: Option<String> },
}

#[async_trait::async_trait]
impl ServerlessHandler for RKVActor {
    async fn handle(&self, _meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        // TODO: If query too large, should put in payload.
        let kvop: KVTxn = serde_json::from_slice(&payload).unwrap();
        self.handle_req(kvop).await
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        if terminating {
            self.terminate().await;
        }
    }
}

impl RKVActor {
    /// Create sqlite structure.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            serverless_storage,
        } = kit;
        let serverless_storage = serverless_storage.unwrap();
        let plog = Arc::new(
            PersistentLog::new(instance_info.clone(), serverless_storage.clone())
                .await
                .unwrap(),
        );
        let storage_dir =
            ServerlessStorage::get_storage_dir("csqlite", &instance_info.identifier, false);
        let dbfile = format!("{storage_dir}/rkvactor.db");
        let mode = std::env::var("OBK_EXECUTION_MODE").unwrap_or("local".into());
        let max_num_tries = if mode.contains("local") {
            1
        } else if mode.contains("lambda") {
            5
        } else {
            20
        };
        let max_open_files = if mode.contains("lambda") { 512 } else { 8192 };
        let mut db = None;
        let mut options = rocksdb::Options::default();
        options.set_max_open_files(max_open_files);
        options.create_if_missing(true);
        for _ in 0..max_num_tries {
            let res = rocksdb::OptimisticTransactionDB::open(&options, &dbfile);
            match res {
                Ok(res) => {
                    db = Some(res);
                    break;
                }
                Err(e) => {
                    println!("RDB Open Err: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_secs_f64(0.5)).await;
                }
            }
        }
        if db.is_none() && !mode.contains("local") {
            eprintln!("Could not open db");
            std::process::exit(1);
        }
        let db = Arc::new(RwLock::new(db));
        let locked_keys = Arc::new(RwLock::new(HashSet::new()));
        let structure = RKVActor {
            db,
            plog,
            locked_keys,
        };
        println!("Made KVActor: {}.", instance_info.identifier);
        structure
    }

    async fn terminate(&self) {
        // Drop connection.
        let mut db = self.db.write().await;
        *db = None;
    }

    /// Handle request.
    async fn handle_req(&self, kvtxn: KVTxn) -> (String, Vec<u8>) {
        let all_keys = kvtxn
            .reads
            .iter()
            .chain(kvtxn.updates.iter().map(|(k, _)| k))
            .chain(kvtxn.conditions.iter().map(|(k, _)| k))
            .cloned()
            .collect::<Vec<_>>();
        {
            let mut locked_keys = self.locked_keys.write().await;
            for key in &all_keys {
                if locked_keys.contains(key) {
                    let resp = KVResp::Err("Concurrent key access.".into());
                    let resp = serde_json::to_string(&resp).unwrap();
                    return (resp, vec![]);
                }
            }
            for key in &all_keys {
                locked_keys.insert(key.clone());
            }
        }
        let ((resp, rows), handle) = self.execute(kvtxn).await;
        if let Some(handle) = handle {
            let locked_keys = self.locked_keys.clone();
            tokio::spawn(async move {
                let r = handle.await;
                if r.is_err() {
                    eprintln!("RDB Handle Error: {r:?}");
                    std::process::exit(1);
                }
                let mut locked_keys = locked_keys.write().await;
                for key in all_keys {
                    locked_keys.remove(&key);
                }
            });
        } else {
            let mut locked_keys = self.locked_keys.write().await;
            for key in all_keys {
                locked_keys.remove(&key);
            }
        }
        (serde_json::to_string(&resp).unwrap(), rows)
    }

    /// Execute a transaction.
    pub async fn execute(
        &self,
        kvtxn: KVTxn,
    ) -> (
        (KVResp, Vec<u8>),
        Option<JoinHandle<(KVResp, Vec<Option<String>>)>>,
    ) {
        let db = self.db.clone();
        let db = db.read_owned().await;
        let read_only = kvtxn.updates.is_empty() && kvtxn.conditions.is_empty();
        let write_only = kvtxn.reads.is_empty() && kvtxn.conditions.is_empty();
        let use_log = write_only && !kvtxn.for_data_load;
        if use_log {
            let plog = self.plog.clone();
            let log_entry = bincode::serialize(&kvtxn).unwrap();
            let lsn = plog.enqueue(log_entry, Some(kvtxn.caller_mem)).await;
            plog.flush_at(Some(lsn)).await;
        }
        let handle = tokio::task::spawn_blocking(move || {
            let db: Option<_> = db.as_ref();
            if db.is_none() {
                return (KVResp::Err("KV Terminating!".into()), vec![]);
            }
            let db = db.unwrap();
            if read_only {
                Self::read_only_txn(db, &kvtxn)
            } else if !use_log {
                Self::read_write_txn(db, &kvtxn)
            } else {
                for _ in 0..10 {
                    let (resp, data) = Self::read_write_txn(db, &kvtxn);
                    match &resp {
                        KVResp::Ok => return (resp, data),
                        KVResp::Err(e) => {
                            if e.contains("False Condition") {
                                return (resp, data);
                            }
                            println!("RDB retrying txn due to unexpected error: {e:?}.");
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                    }
                }
                eprintln!("RDB Retried too many times. Exiting.");
                std::process::exit(1);
            }
        });
        let (resp, data, handle) = if !use_log {
            let (resp, data) = handle.await.unwrap();
            (resp, data, None)
        } else {
            (KVResp::Ok, vec![], Some(handle))
        };
        let data = serde_json::to_vec(&data).unwrap();
        ((resp, data), handle)
    }

    fn read_only_txn(
        db: &rocksdb::OptimisticTransactionDB,
        kvtxn: &KVTxn,
    ) -> (KVResp, Vec<Option<String>>) {
        let gets = db.multi_get(kvtxn.reads.iter().map(|k| k.as_bytes()));
        let mut results = Vec::new();
        for val in gets {
            match val {
                Err(e) => {
                    println!("Unexpected RDB Read Error: {e:?}");
                    return (KVResp::Err(format!("{e:?}")), vec![]);
                }
                Ok(val) => {
                    results.push(val.map(|v| String::from_utf8(v).unwrap()));
                }
            }
        }
        (KVResp::Ok, results)
    }

    // Write txn.
    fn read_write_txn(
        db: &rocksdb::OptimisticTransactionDB,
        kvtxn: &KVTxn,
    ) -> (KVResp, Vec<Option<String>>) {
        let mut wopt = rocksdb::WriteOptions::default();
        wopt.set_sync(true);
        let txnopt = rocksdb::OptimisticTransactionOptions::default();
        let txn = db.transaction_opt(&wopt, &txnopt);
        for (condkey, condval) in &kvtxn.conditions {
            match txn.get(condkey.as_bytes()) {
                Err(e) => {
                    eprintln!("Unexpected Error: {e:?}");
                    return (KVResp::Err(format!("{e:?}")), vec![]);
                }
                Ok(actualval) => {
                    let actualval = actualval.as_ref().map(|v| v.as_slice());
                    if actualval != condval.as_ref().map(|s| s.as_bytes()) {
                        let e = format!("False Condition on {condkey}");
                        return (KVResp::Err(e), vec![]);
                    }
                }
            }
        }
        // Do updates.
        for (key, val) in &kvtxn.updates {
            if let Some(val) = val {
                match txn.put(key.as_bytes(), val.as_bytes()) {
                    Err(e) => {
                        eprintln!("Unexpected Error: {e:?}");
                        std::process::exit(1);
                    }
                    Ok(_) => {}
                }
            } else {
                match txn.delete(key.as_bytes()) {
                    Err(e) => {
                        eprintln!("Unexpected Error: {e:?}");
                        std::process::exit(1);
                    }
                    Ok(_) => {}
                }
            }
        }
        // Do reads.
        let mut results = Vec::new();
        for key in &kvtxn.reads {
            match txn.get(key.as_bytes()) {
                Err(e) => {
                    eprintln!("Unexpected Error: {e:?}");
                    std::process::exit(1);
                }
                Ok(val) => {
                    results.push(val.map(|v| String::from_utf8(v).unwrap()));
                }
            }
        }
        // Do Commit.
        match txn.commit() {
            Err(e) => {
                eprintln!("Unexpected Error: {e:?}");
                std::process::exit(1);
            }
            Ok(_) => (KVResp::Ok, results),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obelisk::{HandlerKit, InstanceInfo, ServerlessStorage};

    use crate::{client::KVTxn, RKVActor};

    async fn make_test_actor_kit(reset: bool) -> HandlerKit {
        if reset {
            let _ = std::fs::remove_dir_all(obelisk::common::shared_storage_prefix());
        }
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        std::env::set_var("OBK_EXTERNAL_ACCESS", false.to_string());
        let instance_info = Arc::new(InstanceInfo {
            peer_id: "111-222-333".into(),
            az: Some("af-sn-1".into()),
            mem: 512,
            cpus: 256,
            public_url: Some("chezmoi.com".into()),
            private_url: Some("chezmoi.com".into()),
            service_name: None,
            handler_name: Some("kvactor".into()),
            subsystem: "functional".into(),
            namespace: "csqlite".into(),
            identifier: "kvactor0".into(),
            unique: true,
            persistent: true,
        });

        let serverless_storage = ServerlessStorage::new_from_info(instance_info.clone())
            .await
            .unwrap();

        HandlerKit {
            instance_info,
            serverless_storage, // Does not matter.
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_kv_test() {
        run_simple_kv_test().await;
    }

    async fn run_simple_kv_test() {
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        let kit = make_test_actor_kit(true).await;
        let actor = RKVActor::new(kit.clone()).await;
        let (resp, rows) = actor
            .handle_req(KVTxn {
                conditions: Vec::new(),
                updates: vec![
                    ("Amadou0".into(), Some("Ngom0".into())),
                    ("Amadou1".into(), Some("Ngom1".into())),
                ],
                reads: vec!["Amadou0".into()],
                for_data_load: false,
                caller_mem: 512,
            })
            .await;
        println!("Resp: {resp:?}");
        let rows: Vec<String> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
        let (resp, _rows) = actor
            .handle_req(KVTxn {
                conditions: vec![("Amadou0".into(), None)],
                updates: vec![
                    ("Amadou0".into(), Some("Ngom0".into())),
                    ("Amadou1".into(), Some("Ngom1".into())),
                ],
                reads: vec!["Amadou0".into()],
                for_data_load: false,
                caller_mem: 512,
            })
            .await;
        println!("Resp: {resp:?}");
        // Check locking.
        let actor1 = RKVActor::new(kit.clone()).await;
        {
            let db = actor1.db.read().await;
            assert!(db.is_none());
        }
        // Check locking again after termination.
        actor.terminate().await;
        let actor1 = RKVActor::new(kit.clone()).await;
        {
            let db = actor1.db.read().await;
            assert!(db.is_some());
        }
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    // async fn simple_bench_test() {
    //     run_simple_bench_test().await;
    // }

    // async fn run_simple_bench_test() {
    //     let kit = make_test_actor_kit(true).await;
    //     let actor = Arc::new(RKVActor::new(kit).await);
    //     let txn1 = KVTxn {
    //         conditions: vec![],
    //         updates: vec![
    //             // ("Amadou0".into(), Some("Ngom0".into())),
    //         ],
    //         reads: vec!["Amadou0".into()],
    //     };
    //     let txn2 = KVTxn {
    //         conditions: vec![],
    //         updates: vec![
    //             // ("Amadou1".into(), Some("Ngom1".into())),
    //         ],
    //         reads: vec!["Amadou0".into()],
    //     };
    //     let mut ts = Vec::new();
    //     let ops_per_thread = 400;
    //     let num_threads = 8; // Some decent number.
    //     let mut txns = Vec::new();
    //     for _ in 0..(num_threads / 2) {
    //         txns.push(txn1.clone());
    //         txns.push(txn2.clone());
    //     }
    //     for (i, txn) in txns.into_iter().enumerate() {
    //         let actor = actor.clone();
    //         ts.push(tokio::spawn(async move {
    //             let start_time = std::time::Instant::now();
    //             for _ in 0..ops_per_thread {
    //                 let _resp = actor.handle_req(txn.clone()).await;
    //             }
    //             let end_time = std::time::Instant::now();
    //             let duration = end_time.duration_since(start_time);
    //             println!("Thread {i}. Duration={duration:?}.");
    //         }));
    //     }
    //     for t in ts {
    //         let _ = t.await;
    //     }
    // }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    // async fn datagen_test() {
    //     run_datagen_test().await;
    // }

    // fn make_key_val(key: usize, key_size_kb: usize, val_size_kb: usize) -> (String, String) {
    //     let key = key.to_string();
    //     let key = key.repeat((key_size_kb * 1000) / key.len());
    //     let val = key.repeat((val_size_kb * 1000) / key.len());
    //     (key, val)
    // }

    // async fn run_datagen_test() {
    //     let reset = false;
    //     let kit = make_test_actor_kit(reset).await;
    //     let actor = Arc::new(CSqliteActor::new(kit).await);
    //     let start_key = 1_000_000_000; // Large enough to guarantee same key size.
    //     let key_size_kb: usize = 1;
    //     let val_size_kb: usize = 9;
    //     if reset {
    //         let start_time = std::time::Instant::now();
    //         let data_size_kb: usize = 20 * 1000 * 1000; // Approx ~20GB.
    //         let num_keys = data_size_kb / (val_size_kb + key_size_kb); // Approx.
    //         let batch_size: usize = 1000;
    //         let end_key = start_key + num_keys;
    //         let mut i: usize = start_key;
    //         while i < end_key {
    //             let batch_start = i;
    //             let batch_end = batch_start + batch_size;
    //             let keyvals = (batch_start..batch_end)
    //                 .map(|k| make_key_val(k, key_size_kb, val_size_kb))
    //                 .collect::<Vec<_>>();
    //             let updates = keyvals
    //                 .into_iter()
    //                 .map(|(k, v)| format!("REPLACE INTO keyvalues VALUES ('{k}', '{v}');"))
    //                 .collect::<Vec<_>>();
    //             let db_txn = DBTxn {
    //                 conditions: vec![],
    //                 updates,
    //                 query: None,
    //                 caller_mem: 512,
    //             };
    //             let (resp, _) = actor.execute(db_txn).await;
    //             assert!(matches!(resp, DBResp::Ok));
    //             i += batch_size;
    //         }
    //         let end_time = std::time::Instant::now();
    //         let duration = end_time.duration_since(start_time);
    //         println!("Gen Duration: {duration:?}");
    //     }
    //     let (key, _) = make_key_val(start_key + 37, 1, val_size_kb);
    //     let db_txn = DBTxn {
    //         conditions: vec![],
    //         updates: vec![],
    //         query: Some(format!("SELECT * FROM keyvalues WHERE key='{key}';")),
    //         caller_mem: 512,
    //     };
    //     let (resp, rows) = actor.execute(db_txn).await;
    //     println!("Resp: {resp:?}");
    //     let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
    //     println!("{rows:?}");
    // }
}
