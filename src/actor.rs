use crate::client::DBOp;

use super::client::{DBResp, DBTxn};
use obelisk::{HandlerKit, PersistentLog, ScalingState, ServerlessHandler, ServerlessStorage};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Savepoint;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex};

#[derive(Serialize, Deserialize, Clone, Debug)]
enum LogEntry {
    Txn(DBTxn),
    Completion(usize),
}

pub struct CSqliteActor {
    plog: Arc<PersistentLog>,
    pool: Pool<SqliteConnectionManager>,
    inner: Arc<Mutex<CSqliteInner>>,
    execute_lock: Arc<Mutex<()>>,
}

struct CSqliteInner {
    txn_queue: Vec<(DBTxn, usize)>,
    last_executed_lsn: usize,
    txn_results: HashMap<usize, (DBResp, Vec<Vec<Value>>)>,
}

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
        let dbop: DBOp = serde_json::from_slice(&payload).unwrap();
        self.handle_req(dbop).await
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        if terminating {
            self.plog.terminate().await;
        }
        self.truncate_log().await;
    }
}

impl CSqliteActor {
    /// Create sqlite structure.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            serverless_storage,
        } = kit;
        let serverless_storage = serverless_storage.unwrap();
        let plog = PersistentLog::new(instance_info.clone(), serverless_storage.clone())
            .await
            .unwrap();
        let shared_dir = obelisk::common::shared_storage_prefix();
        let identifier = &instance_info.identifier;
        let storage_dir = format!("{shared_dir}/csqlite/{identifier}");
        let res = std::fs::create_dir_all(&storage_dir);
        let _res = strong_unwrap!(res);
        let pool = ServerlessStorage::try_exclusive_file(&storage_dir, 2);
        let pool = strong_unwrap!(pool);
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
            last_executed_lsn: 0,
            txn_queue: vec![],
            txn_results: HashMap::new(),
        };
        let structure = CSqliteActor {
            plog: Arc::new(plog),
            pool,
            inner: Arc::new(Mutex::new(inner)),
            execute_lock: Arc::new(Mutex::new(())),
        };
        // Drop conn for recovery.
        std::mem::drop(conn);
        structure.recover().await;
        structure
    }

    /// Handle request.
    async fn handle_req(&self, dbop: DBOp) -> (String, Vec<u8>) {
        let (resp, rows) = match dbop {
            DBOp::Txn(dbtxn) => self.execute(dbtxn).await,
            DBOp::Query(query) => self.snapshot_query(query).await,
        };
        (serde_json::to_string(&resp).unwrap(), rows)
    }

    /// Execute a transaction.
    pub async fn execute(&self, db_txn: DBTxn) -> (DBResp, Vec<u8>) {
        let entry = LogEntry::Txn(db_txn.clone());
        let entry = bincode::serialize(&entry).unwrap();
        let lsn = {
            let mut inner = self.inner.lock().await;
            let lsn = self.plog.enqueue(entry, Some(db_txn.caller_mem)).await;
            inner.txn_queue.push((db_txn, lsn));
            lsn
        };
        let (resp, rows) = self.execute_queue(lsn).await.unwrap();
        let rows = serde_json::to_vec(&rows).unwrap();
        (resp, rows)
    }

    pub async fn snapshot_query(&self, query: String) -> (DBResp, Vec<u8>) {
        let pool = self.pool.clone();
        let res = tokio::task::spawn_blocking(move || {
            let conn = {
                let conn = match pool.get() {
                    Ok(conn) => conn,
                    Err(e) => return Err(format!("{e:?}")),
                };
                unsafe {
                    let handle = conn.handle();
                    rusqlite::Connection::from_handle(handle)
                }
            };
            let mut conn = match conn {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{e:?}")),
            };
            let svp = conn.savepoint().unwrap();
            Self::query(&svp, query)
        }).await.unwrap();
        match res {
            Ok(res) => {
                let resp = DBResp::Rows;
                let rows = serde_json::to_vec(&res).unwrap();
                (resp, rows)
            },
            Err(e) => {
                (DBResp::Err(e), vec![])
            }
        }
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
                    results.push(res.unwrap());
                }
                Ok(results)
            }
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// Execute the txns in the queue, potentially for recovery.
    pub async fn perform_execute_queue(&self, txn_queue: Vec<(DBTxn, usize)>, recovering: bool) {
        let last_lsn = txn_queue.last().unwrap().1;
        // Flush if not recovering.
        if !recovering {
            self.plog.flush_at(Some(last_lsn)).await;
        }
        // Now execute in order in a deterministic fashion.
        let pool = self.pool.clone();
        let (send_ch, rcv_ch) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let mut conn = {
                let conn = pool.get().unwrap();
                unsafe {
                    let handle = conn.handle();
                    rusqlite::Connection::from_handle(handle).unwrap()
                }
            };
            println!("Starting txn!");
            let mut txn = strong_unwrap!(conn.transaction());
            println!("Started txn!");
            let prev_executed_lsn =
                txn.query_row("SELECT lsn FROM system__last_lsn", [], |row| row.get(0));
            let prev_executed_lsn: usize = match prev_executed_lsn {
                Ok(prev_executed_lsn) => prev_executed_lsn,
                Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                err => {
                    println!("Error: {err:?}");
                    std::process::exit(1);
                }
            };
            let mut results = HashMap::new();
            for (db_txn, lsn) in txn_queue {
                if prev_executed_lsn > lsn {
                    continue;
                }
                // TODO: Instead of all the unwrap(), check failure and rollback savepoint.
                let svp = strong_unwrap!(txn.savepoint());
                // Check conditions.
                let mut satisfies_conditions = true;
                let mut err_str = String::new();
                for condition in db_txn.conditions {
                    let res = svp.query_row(&condition, [], |row| row.get::<usize, i64>(0));
                    match res {
                        Ok(res) => {
                            if res < 1 {
                                satisfies_conditions = false;
                                err_str = "ConditionCheckFailed".into();
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
                    if !recovering {
                        let resp = DBResp::Err(err_str);
                        let rows = Vec::new();
                        results.insert(lsn, (resp, rows));
                    }
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
                if !recovering {
                    let resp = if all_ok {
                        DBResp::Ok
                    } else {
                        DBResp::Err(err_str)
                    };
                    results.insert(lsn, (resp, rows));
                }
            }
            if last_lsn > prev_executed_lsn {
                txn.execute("REPLACE INTO system__last_lsn VALUES (0, ?)", [last_lsn])
                    .unwrap();
            }
            send_ch.send(results).unwrap();
            // This is where the main IO happens.
            txn.commit().unwrap();
        });
        let results = rcv_ch.await.unwrap();
        let completion = LogEntry::Completion(last_lsn);
        let entry = bincode::serialize(&completion).unwrap();
        self.plog.enqueue(entry, Some(1)).await;
        // Mark results.
        {
            let mut inner = self.inner.lock().await;
            if last_lsn > inner.last_executed_lsn {
                // May not always be true during recovery.
                inner.last_executed_lsn = last_lsn;
            }
            if !recovering {
                for (lsn, db_txn_result) in results {
                    inner.txn_results.insert(lsn, db_txn_result);
                }
            }
        }
    }

    /// Perform the txns in the queue, if the given lsn hasn't been executed yet.
    pub async fn execute_queue(&self, at_lsn: usize) -> Option<(DBResp, Vec<Vec<Value>>)> {
        let _l = self.execute_lock.lock().await;
        // Drain queue.
        let txn_queue: Vec<(DBTxn, usize)> = {
            let mut inner = self.inner.lock().await;
            if inner.last_executed_lsn >= at_lsn {
                // Get already computed result.
                return inner.txn_results.remove(&at_lsn);
            }
            inner.txn_queue.drain(..).collect()
        };
        assert!(!txn_queue.is_empty());
        self.perform_execute_queue(txn_queue, false).await;
        // Get result.
        {
            let mut inner = self.inner.lock().await;
            return inner.txn_results.remove(&at_lsn);
        }
    }

    /// Truncate log file.
    pub async fn truncate_log(&self) {
        let mut curr_start_lsn = 0;
        let mut still_active = BTreeSet::<usize>::new();
        println!("SqliteStructure::truncate_log().");
        loop {
            let entries = self.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("SqliteStructure::truncate_log(): {x:?}");
                    // Must own db at this point.
                    std::process::exit(1);
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                let entry: LogEntry = bincode::deserialize(&entry).unwrap();
                match entry {
                    LogEntry::Completion(lsn) => {
                        let mut to_remove = Vec::new();
                        for key in still_active.range(0..(lsn + 1)) {
                            to_remove.push(*key);
                        }
                        for key in to_remove {
                            still_active.remove(&key);
                        }
                    }
                    LogEntry::Txn(_db_txn) => {
                        still_active.insert(lsn);
                    }
                }
            }
        }
        let first_lsn = still_active.first();
        if let Some(first_lsn) = first_lsn {
            if *first_lsn > 0 {
                println!("Truncating up to: {}!", *first_lsn - 1);
                strong_unwrap!(self.plog.truncate(*first_lsn - 1).await);
            }
        }
    }

    /// Perform recovery.
    pub async fn recover(&self) {
        let mut curr_start_lsn = 0;
        let mut to_replay: BTreeMap<usize, DBTxn> = BTreeMap::new();
        println!("SqliteStructure::recover(). Starting recovery.");
        loop {
            let entries = self.plog.replay(curr_start_lsn).await;
            let entries = match entries {
                Ok(entries) => entries,
                Err(x) => {
                    println!("SqliteStructure::recover(): {x:?}");
                    // Must own db at this point.
                    std::process::exit(1);
                }
            };
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                curr_start_lsn = lsn;
                let entry: LogEntry = bincode::deserialize(&entry).unwrap();
                println!("SqliteStructure::recover(): Found entry ({lsn}): {entry:?}.");
                match entry {
                    LogEntry::Completion(lsn) => {
                        let mut to_remove = Vec::new();
                        for key in to_replay.keys() {
                            if *key <= lsn {
                                to_remove.push(*key);
                            }
                        }
                        for key in to_remove {
                            to_replay.remove(&key);
                        }
                        let mut inner = self.inner.lock().await;
                        inner.last_executed_lsn = lsn;
                    }
                    LogEntry::Txn(db_txn) => {
                        to_replay.insert(lsn, db_txn);
                    }
                }
            }
        }
        let txn_queue: Vec<(DBTxn, usize)> = to_replay.into_iter().map(|(k, v)| (v, k)).collect();
        if !txn_queue.is_empty() {
            println!("Recovering: {txn_queue:?}.");
            self.perform_execute_queue(txn_queue, true).await;
            self.plog.flush().await;
        }
        println!("Done with recovery!");
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obelisk::{ServerlessStorage, HandlerKit, InstanceInfo};
    use serde_json::Value;

    use crate::{CSqliteActor, client::{DBTxn, DBOp}};


    async fn make_test_actor_kit() -> HandlerKit {
        std::env::set_var("OBK_EXTERNAL_ACCESS", false.to_string()); // Prevent log rescaling.
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
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
        let shared_dir = obelisk::common::shared_storage_prefix();
        let _ = std::fs::remove_dir_all(&shared_dir);
        let kit = make_test_actor_kit().await;
        let actor = CSqliteActor::new(kit).await;
        let (resp, rows) = actor.handle_req(DBOp::Txn(DBTxn {
            conditions: Vec::new(),
            updates: vec![
                "REPLACE INTO keyvalues VALUES ('Amadou1', 'Ngom1')".into()
            ],
            query: Some(
                "SELECT * FROM keyvalues;".into()
            ),
            caller_mem: 512,
        })).await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
        let (resp, rows) = actor.handle_req(DBOp::Txn(DBTxn {
            conditions: vec![
                "SELECT EXISTS(SELECT * FROM keyvalues WHERE key='Amadou3')".into(),
            ],
            updates: vec![
                "REPLACE INTO keyvalues VALUES ('Amadou2', 'Ngom2')".into()
            ],
            query: None,
            caller_mem: 512,
        })).await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
        actor.truncate_log().await;
        let (resp, rows) = actor.handle_req(DBOp::Query(
            "SELECT * FROM keyvalues".into()
        )).await;
        println!("Resp: {resp:?}");
        let rows: Vec<Vec<Value>> = serde_json::from_slice(&rows).unwrap();
        println!("{rows:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_local_bench_test() {
        run_simple_local_bench_test().await;
    }

    async fn run_simple_local_bench_test() {
        let key = "Amadou";
        let value = "Ngom";
        let shared_dir = obelisk::common::shared_storage_prefix();
        let _ = std::fs::remove_dir_all(&shared_dir);
        let kit = make_test_actor_kit().await;
        let actor = Arc::new(CSqliteActor::new(kit).await);
        let op1 = DBOp::Query(format!("SELECT * FROM keyvalues WHERE key='{key}'"));
        let op2 = DBOp::Txn(DBTxn {
            conditions: vec![],
            updates: vec![
                format!("REPLACE INTO keyvalues VALUES ('{key}', '{value}')"),
            ],
            query: None,
            caller_mem: 512,
        });
        let mut ts = Vec::new();
        for op in [op1, op2] {
            let actor = actor.clone();
            ts.push(tokio::spawn(async move {
                let start_time = std::time::Instant::now();
                for _ in 0..10000 {
                    actor.handle_req(op.clone()).await;
                }
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                println!("Duration: {duration:?}. Op={op:?}");
            }));
        }
        for t in ts {
            let _ = t.await;
        }
    }
}