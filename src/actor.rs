use super::client::{DBResp, DBTxn};
use obelisk::{HandlerKit, ScalingState, ServerlessHandler};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Savepoint;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct CSqliteActor {
    pool: Pool<SqliteConnectionManager>,
    inner: Arc<Mutex<CSqliteInner>>,
    execute_lock: Arc<Mutex<()>>,
}

struct CSqliteInner {
    txn_queue: Vec<(DBTxn, usize)>,
    curr_txn_id: usize,
    txn_results: HashMap<usize, (DBResp, Vec<Vec<Value>>)>,
}

#[macro_export]
macro_rules! strong_unwrap {
    ($r:expr) => {
        match $r {
            Ok(x) => x,
            Err(e) => {
                eprintln!("{e:?}. Exiting.");
                std::process::exit(1);
            }
        }
    };
}

#[async_trait::async_trait]
impl ServerlessHandler for CSqliteActor {
    async fn handle(&self, _meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        // TODO: If query too large, should put in payload.
        let dbtxn: DBTxn = serde_json::from_slice(&payload).unwrap();
        self.handle_req(dbtxn).await
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, _terminating: bool) {}
}

impl CSqliteActor {
    /// Create sqlite structure.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info: _,
            serverless_storage,
        } = kit;
        let serverless_storage = serverless_storage.unwrap();
        let pool = serverless_storage.exclusive_pool.clone().unwrap();
        let conn = pool.get().unwrap();
        match conn.execute(
            "CREATE TABLE IF NOT EXISTS system__last_lsn (unique_row INT PRIMARY KEY, lsn BIGINT)",
            (),
        ) {
            Ok(_) => {}
            Err(x) => {
                println!("Create error: {x:?}");
                std::process::exit(1);
            }
        }
        match conn.execute(
            "CREATE TABLE IF NOT EXISTS keyvalues (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            (),
        ) {
            Ok(_) => {}
            Err(x) => {
                println!("Create error: {x:?}");
                std::process::exit(1);
            }
        }
        let inner = CSqliteInner {
            curr_txn_id: 0,
            txn_queue: vec![],
            txn_results: HashMap::new(),
        };
        let structure = CSqliteActor {
            pool,
            inner: Arc::new(Mutex::new(inner)),
            execute_lock: Arc::new(Mutex::new(())),
        };
        println!("Made CSQLITE Actor.");
        structure
    }

    /// Handle request.
    async fn handle_req(&self, dbtxn: DBTxn) -> (String, Vec<u8>) {
        let (resp, rows) = self.execute(dbtxn).await;
        (serde_json::to_string(&resp).unwrap(), rows)
    }

    /// Execute a transaction.
    pub async fn execute(&self, db_txn: DBTxn) -> (DBResp, Vec<u8>) {
        // TODO: Real preprocessing.
        if let Some(q) = &db_txn.query {
            let q = q.to_lowercase();
            if !q.contains("where") || !q.contains("=") {
                return (DBResp::Err("Invalid Query".into()), vec![]);
            }
        }
        // println!("Executing: {db_txn:?}");
        let txn_id = {
            let mut inner = self.inner.lock().await;
            let txn_id = inner.curr_txn_id;
            inner.curr_txn_id += 1;
            inner.txn_queue.push((db_txn, txn_id));
            txn_id
        };
        // Execute.
        let (resp, rows) = self.execute_queue(txn_id).await;
        let rows = serde_json::to_vec(&rows).unwrap();
        (resp, rows)
    }

    /// Run a query.
    pub fn query<'a>(svp: &Savepoint<'a>, query: String) -> Result<Vec<Vec<Value>>, String> {
        let mut stmt = match svp.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                return Err(format!("{e:?}"));
            }
        };
        let res_iter = stmt.query_map([], |r| {
            let mut row = Vec::<Value>::new();
            let mut i = 0;
            loop {
                let res = r.get::<usize, rusqlite::types::Value>(i);
                if let Ok(v) = res {
                    let v = match v {
                        rusqlite::types::Value::Null => Value::Null,
                        rusqlite::types::Value::Integer(i) => serde_json::json!(i),
                        rusqlite::types::Value::Real(f) => serde_json::json!(f),
                        rusqlite::types::Value::Text(s) => serde_json::json!(s),
                        rusqlite::types::Value::Blob(b) => serde_json::json!(b),
                    };
                    row.push(v);
                } else {
                    return Ok(row);
                }
                i += 1;
            }
        });
        match res_iter {
            Ok(res_iter) => {
                let mut results = Vec::new();
                for res in res_iter {
                    match res {
                        Ok(res) => results.push(res),
                        Err(e) => {
                            return Err(format!("{e:?}"));
                        }
                    };
                }
                Ok(results)
            }
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// Execute the txns in the queue, potentially for recovery.
    pub async fn perform_execute_queue(&self, txn_queue: Vec<(DBTxn, usize)>) {
        // Now execute in order in a deterministic fashion.
        let pool = self.pool.clone();
        let start_time = std::time::Instant::now();
        let results = tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{e:?}")),
            };
            let mut txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(e) => return Err(format!("{e:?}")),
            };
            let mut results = HashMap::new();
            for (db_txn, txn_id) in txn_queue {
                // TODO: Instead of all the unwrap(), check failure and rollback savepoint.
                let svp = match txn.savepoint() {
                    Ok(svp) => svp,
                    Err(e) => return Err(format!("{e:?}")),
                };
                // Check conditions.
                let mut satisfies_conditions = true;
                let mut err_str = String::new();
                for (condition_idx, condition) in db_txn.conditions.into_iter().enumerate() {
                    let res = svp.query_row(&condition, [], |row| row.get::<usize, i64>(0));
                    match res {
                        Ok(res) => {
                            if res < 1 {
                                satisfies_conditions = false;
                                err_str = format!("ConditionCheckFailed: Idx={condition_idx}.");
                                break;
                            }
                        }
                        Err(e) => {
                            satisfies_conditions = false;
                            err_str = format!("{e:?}");
                            break;
                        }
                    }
                }
                if !satisfies_conditions {
                    let resp = DBResp::Err(err_str);
                    let rows = Vec::new();
                    results.insert(txn_id, (resp, rows));
                    continue;
                }
                // Do the updates.
                let mut all_ok = true;
                for update in db_txn.updates {
                    let resp = svp.execute(&update, []);
                    if let Err(e) = resp {
                        err_str = format!("{e:?}");
                        all_ok = false;
                        break;
                    }
                }
                // Do the query.
                let rows = if let Some(q) = db_txn.query {
                    let res = Self::query(&svp, q);
                    match res {
                        Ok(rows) => rows,
                        Err(e) => {
                            all_ok = false;
                            err_str = e;
                            vec![]
                        }
                    }
                } else {
                    vec![]
                };

                if all_ok {
                    // Commit savepoint.
                    let resp = svp.commit();
                    if let Err(e) = resp {
                        err_str = format!("{e:?}");
                        all_ok = false;
                    }
                }
                let resp = if all_ok {
                    DBResp::Ok
                } else {
                    DBResp::Err(err_str)
                };
                results.insert(txn_id, (resp, rows));
            }
            let end_time = std::time::Instant::now();
            let svp_duration = end_time.duration_since(start_time);
            // This is where the main IO happens.
            match txn.commit() {
                Ok(_) => Ok((results, svp_duration)),
                Err(e) => {
                    let err_str = format!("{e:?}");
                    let mut new_results = HashMap::new();
                    for (txn_id, _result) in results {
                        new_results.insert(txn_id, (DBResp::Err(err_str.clone()), vec![]));
                    }
                    Ok((new_results, svp_duration))
                }
            }
        })
        .await;
        match results {
            Err(e) => eprintln!("{e:?}"),
            Ok(results) => match results {
                Err(e) => eprintln!("{e:?}"),
                Ok((results, svp_duration)) => {
                    let end_time: std::time::Instant = std::time::Instant::now();
                    let txn_duration = end_time.duration_since(start_time);
                    println!(
                        "SVP={svp_duration:?}. TXN={txn_duration:?}. Res={:?}.",
                        results.len()
                    );
                    let mut inner = self.inner.lock().await;
                    for (txn_id, db_txn_result) in results {
                        inner.txn_results.insert(txn_id, db_txn_result);
                    }
                }
            },
        }
    }

    /// Perform the txns in the queue, if the given id hasn't been executed yet.
    pub async fn execute_queue(&self, at_txn_id: usize) -> (DBResp, Vec<Vec<Value>>) {
        let _l = self.execute_lock.lock().await;
        // Drain queue.
        let txn_queue: Vec<(DBTxn, usize)> = {
            let mut inner = self.inner.lock().await;
            if let Some(res) = inner.txn_results.remove(&at_txn_id) {
                return res;
            }
            inner.txn_queue.drain(..).collect()
        };
        self.perform_execute_queue(txn_queue).await;
        // Get result.
        {
            let mut inner = self.inner.lock().await;
            match inner.txn_results.remove(&at_txn_id) {
                Some(resp) => resp,
                None => (
                    DBResp::Err("Uncaught Txn Error. Likely lost actor.".into()),
                    vec![],
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obelisk::{HandlerKit, InstanceInfo, ServerlessStorage};
    use serde_json::Value;

    use crate::{
        client::{DBResp, DBTxn},
        CSqliteActor,
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
            handler_name: Some("dbactor".into()),
            subsystem: "functional".into(),
            namespace: "csqlite".into(),
            identifier: "dbactor0".into(),
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
    async fn simple_db_test() {
        run_simple_db_test().await;
    }

    async fn run_simple_db_test() {
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        let kit = make_test_actor_kit(true).await;
        let actor = CSqliteActor::new(kit).await;
        let (resp, rows) = actor
            .handle_req(DBTxn {
                conditions: Vec::new(),
                updates: vec!["REPLACE INTO keyvalues VALUES ('Amadou1', 'Ngom1')".into()],
                query: Some("SELECT * FROM keyvalues;".into()),
                caller_mem: 512,
            })
            .await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
        let (resp, rows) = actor
            .handle_req(DBTxn {
                conditions: vec![
                    "SELECT EXISTS(SELECT * FROM keyvalues WHERE key='Amadou3')".into()
                ],
                updates: vec!["REPLACE INTO keyvalues VALUES ('Amadou2', 'Ngom2')".into()],
                query: None,
                caller_mem: 512,
            })
            .await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_bench_test() {
        run_simple_bench_test().await;
    }

    async fn run_simple_bench_test() {
        let kit = make_test_actor_kit(true).await;
        let actor = Arc::new(CSqliteActor::new(kit).await);
        let txn1 = DBTxn {
            conditions: Vec::new(),
            updates: vec!["REPLACE INTO keyvalues VALUES ('Amadou1', 'Ngom1')".into()],
            query: Some("SELECT * FROM keyvalues;".into()),
            caller_mem: 512,
        };
        let txn2 = DBTxn {
            conditions: Vec::new(),
            updates: vec!["REPLACE INTO keyvalues VALUES ('Amadou2', 'Ngom2')".into()],
            query: Some("SELECT * FROM keyvalues;".into()),
            caller_mem: 512,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn datagen_test() {
        run_datagen_test().await;
    }

    fn make_key_val(key: usize, key_size_kb: usize, val_size_kb: usize) -> (String, String) {
        let key = key.to_string();
        let key = key.repeat((key_size_kb * 1000) / key.len());
        let val = key.repeat((val_size_kb * 1000) / key.len());
        (key, val)
    }

    async fn run_datagen_test() {
        let reset = false;
        let kit = make_test_actor_kit(reset).await;
        let actor = Arc::new(CSqliteActor::new(kit).await);
        let start_key = 1_000_000_000; // Large enough to guarantee same key size.
        let key_size_kb: usize = 1;
        let val_size_kb: usize = 9;
        if reset {
            let start_time = std::time::Instant::now();
            let data_size_kb: usize = 20 * 1000 * 1000; // Approx ~20GB.
            let num_keys = data_size_kb / (val_size_kb + key_size_kb); // Approx.
            let batch_size: usize = 1000;
            let end_key = start_key + num_keys;
            let mut i: usize = start_key;
            while i < end_key {
                let batch_start = i;
                let batch_end = batch_start + batch_size;
                let keyvals = (batch_start..batch_end)
                    .map(|k| make_key_val(k, key_size_kb, val_size_kb))
                    .collect::<Vec<_>>();
                let updates = keyvals
                    .into_iter()
                    .map(|(k, v)| format!("REPLACE INTO keyvalues VALUES ('{k}', '{v}');"))
                    .collect::<Vec<_>>();
                let db_txn = DBTxn {
                    conditions: vec![],
                    updates,
                    query: None,
                    caller_mem: 512,
                };
                let (resp, _) = actor.execute(db_txn).await;
                assert!(matches!(resp, DBResp::Ok));
                i += batch_size;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Gen Duration: {duration:?}");
        }
        let (key, _) = make_key_val(start_key + 37, 1, val_size_kb);
        let db_txn = DBTxn {
            conditions: vec![],
            updates: vec![],
            query: Some(format!("SELECT * FROM keyvalues WHERE key='{key}';")),
            caller_mem: 512,
        };
        let (resp, rows) = actor.execute(db_txn).await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
    }
}
