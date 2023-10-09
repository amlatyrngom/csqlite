use super::client::{KVResp, KVTxn};
use obelisk::{
    common::debug_format, HandlerKit, ScalingState, ServerlessHandler, ServerlessStorage,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct SKVActor {
    db: Arc<RwLock<Option<sled::Db>>>,
    flush_lock: Arc<Mutex<()>>,
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    curr_txn_id: usize,
    waiting: HashSet<usize>,
}

#[async_trait::async_trait]
impl ServerlessHandler for SKVActor {
    async fn handle(&self, _meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        // TODO: If query too large, should put in payload.
        let kvtxn: KVTxn = serde_json::from_slice(&payload).unwrap();
        self.handle_req(kvtxn).await
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        if terminating {
            self.terminate().await;
        }
    }
}

impl SKVActor {
    /// Create sqlite structure.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            serverless_storage: _,
        } = kit;
        let storage_dir =
            ServerlessStorage::get_storage_dir("csqlite", &instance_info.identifier, false);
        let dbfile = format!("{storage_dir}/kvactor.db");
        let db = sled::open(&dbfile).ok();
        let db = Arc::new(RwLock::new(db));
        let inner = Inner {
            curr_txn_id: 0,
            waiting: HashSet::new(),
        };
        let structure = SKVActor {
            db,
            flush_lock: Arc::new(Mutex::new(())),
            inner: Arc::new(Mutex::new(inner)),
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
        let (resp, rows) = self.execute(kvtxn).await;
        (serde_json::to_string(&resp).unwrap(), rows)
    }

    /// Execute a transaction.
    pub async fn execute(&self, kvtxn: KVTxn) -> (KVResp, Vec<u8>) {
        let db = self.db.read().await;
        let db: Option<sled::Db> = db.clone();
        if db.is_none() {
            return (KVResp::Err("KV Terminating!".into()), vec![]);
        }
        let db = db.unwrap();
        let (resp, data) = self.read_write_txn(&db, &kvtxn).await;
        let wait_for_flush = !kvtxn.updates.is_empty() && matches!(&resp, KVResp::Ok);
        if wait_for_flush {
            // Append self to flushing queue.
            let waiting_id = {
                let mut inner = self.inner.lock().await;
                let waiting_id = inner.curr_txn_id;
                inner.curr_txn_id += 1;
                inner.waiting.insert(waiting_id);
                waiting_id
            };
            self.wait_for_flush(db, waiting_id).await;
        }
        let data = serde_json::to_vec(&data).unwrap();
        (resp, data)
    }

    // Write txn.
    async fn read_write_txn(&self, db: &sled::Db, kvtxn: &KVTxn) -> (KVResp, Vec<Option<String>>) {
        let res = db.transaction(|tx_db| {
            let mut results = Vec::new();
            // Check conditions
            for (condkey, condval) in &kvtxn.conditions {
                match tx_db.get(condkey.as_bytes()) {
                    Err(e) => sled::transaction::abort(format!("{e:?}"))?,
                    Ok(actualval) => {
                        let actualval = actualval.map(|v| String::from_utf8(v.to_vec()).unwrap());
                        if actualval != *condval {
                            let e = format!("False Condition on {condkey}");
                            sled::transaction::abort(format!("{e:?}"))?;
                        }
                    }
                }
            }
            // Do updates.
            for (key, val) in &kvtxn.updates {
                if let Some(val) = val {
                    match tx_db.insert(key.as_bytes(), val.as_bytes()) {
                        Err(e) => sled::transaction::abort(format!("{e:?}"))?,
                        Ok(_) => {}
                    }
                } else {
                    match tx_db.remove(key.as_bytes()) {
                        Err(e) => sled::transaction::abort(format!("{e:?}"))?,
                        Ok(_) => {}
                    }
                }
            }
            // Do Reads.
            for key in &kvtxn.reads {
                let val = tx_db.get(key.as_bytes());
                match val {
                    Err(e) => sled::transaction::abort(format!("{e:?}"))?,
                    Ok(val) => {
                        results.push(val);
                    }
                }
            }
            Ok(results)
        });
        match res {
            Err(e) => {
                let err = debug_format!()(e);
                (KVResp::Err(err), vec![])
            }
            Ok(res) => {
                let res = res
                    .into_iter()
                    .map(|v| v.map(|v| String::from_utf8(v.to_vec()).unwrap()))
                    .collect::<Vec<Option<String>>>();
                (KVResp::Ok, res)
            }
        }
    }

    async fn wait_for_flush(&self, db: sled::Db, at_id: usize) {
        let _l = self.flush_lock.lock().await;
        {
            let mut inner = self.inner.lock().await;
            // Check if already flushed away.
            if !inner.waiting.contains(&at_id) {
                return;
            }
            // Empty out waiting queue.
            inner.waiting = HashSet::new();
        }
        let start_time = std::time::Instant::now();
        tokio::task::spawn_blocking(move || {
            let _ = db.flush().unwrap();
        })
        .await
        .unwrap();
        // db.flush_async().await.unwrap();
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Flush Duration: {duration:?}");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obelisk::{HandlerKit, InstanceInfo, ServerlessStorage};

    use crate::{
        client::{KVResp, KVTxn},
        SKVActor,
    };

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
        let actor = SKVActor::new(kit.clone()).await;
        let (resp, rows) = actor
            .handle_req(KVTxn {
                conditions: Vec::new(),
                updates: vec![
                    ("Amadou0".into(), Some("Ngom0".into())),
                    ("Amadou1".into(), Some("Ngom1".into())),
                ],
                reads: vec!["Amadou0".into()],
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
            })
            .await;
        println!("Resp: {resp:?}");
        // Check locking.
        let actor1 = SKVActor::new(kit.clone()).await;
        {
            let db = actor1.db.read().await;
            assert!(db.is_none());
        }
        // Check locking again after termination.
        actor.terminate().await;
        let actor1 = SKVActor::new(kit.clone()).await;
        {
            let db = actor1.db.read().await;
            assert!(db.is_some());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_test() {
        run_simple_bench_test().await;
    }

    async fn run_simple_bench_test() {
        let kit = make_test_actor_kit(true).await;
        let actor = Arc::new(SKVActor::new(kit).await);
        let txn1 = KVTxn {
            conditions: vec![],
            updates: vec![("Amadou0".into(), Some("Ngom0".into()))],
            reads: vec![],
        };
        let txn2 = KVTxn {
            conditions: vec![],
            updates: vec![("Amadou1".into(), Some("Ngom1".into()))],
            reads: vec![],
        };
        let mut ts = Vec::new();
        let ops_per_thread = 10; // ~1B per month.
        let num_threads = 16; // Some decent number.
        let mut txns = Vec::new();
        for _ in 0..(num_threads / 2) {
            txns.push(txn1.clone());
            txns.push(txn2.clone());
        }
        for (i, txn) in txns.into_iter().enumerate() {
            let actor = actor.clone();
            ts.push(tokio::spawn(async move {
                let start_time = std::time::Instant::now();
                for _ in 0..ops_per_thread {
                    let _resp = actor.handle_req(txn.clone()).await;
                }
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                println!("Thread {i}. Duration={duration:?}.");
            }));
        }
        for t in ts {
            let _ = t.await;
        }
    }

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
