use std::sync::Arc;

use obelisk::FunctionalClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Client.
#[derive(Clone)]
pub struct DBClient {
    fc: Arc<FunctionalClient>,
    caller_mem: i32,
}

/// Client.
#[derive(Clone)]
pub struct KVClient {
    fc: Arc<FunctionalClient>,
}

/// A txn.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBTxn {
    pub conditions: Vec<String>,
    pub updates: Vec<String>,
    pub query: Option<String>,
    pub caller_mem: i32,
}

/// A key value store transaction.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KVTxn {
    pub conditions: Vec<(String, Option<String>)>,
    pub updates: Vec<(String, Option<String>)>,
    pub reads: Vec<String>,
}

/// A Response.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DBResp {
    Ok,
    Err(String),
}

/// A Response.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum KVResp {
    Ok,
    Err(String),
}

impl DBClient {
    /// New client.
    pub async fn new(db_id: Option<usize>, caller_mem: Option<i32>) -> Self {
        let db_id = db_id.unwrap_or(0);
        let caller_mem = caller_mem.or(Some(512));
        let fc = FunctionalClient::new("csqlite", "dbactor", Some(db_id), caller_mem).await;
        DBClient {
            fc: Arc::new(fc),
            caller_mem: caller_mem.unwrap(),
        }
    }

    /// Perform a txn.
    pub async fn txn(
        &self,
        conditions: Vec<String>,
        updates: Vec<String>,
        query: Option<String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let dbtxn = DBTxn {
            conditions,
            updates,
            query,
            caller_mem: self.caller_mem,
        };
        let payload = serde_json::to_vec(&dbtxn).unwrap();
        let (resp_meta, resp_payload) = self.fc.invoke("", &payload).await?;
        let resp_meta: DBResp = serde_json::from_str(&resp_meta).unwrap();
        println!("Resp Payload: {}.", resp_payload.len());
        println!("Resp Meta: {resp_meta:?}.");
        match resp_meta {
            DBResp::Ok => Ok(serde_json::from_slice(&resp_payload).unwrap()),
            DBResp::Err(s) => Err(s),
        }
    }
}

impl KVClient {
    /// New client.
    pub async fn new(db_id: Option<usize>, caller_mem: Option<i32>) -> Self {
        let db_id = db_id.unwrap_or(0);
        let caller_mem = caller_mem.or(Some(512));
        let fc = FunctionalClient::new("csqlite", "rkvactor", Some(db_id), caller_mem).await;
        KVClient { fc: Arc::new(fc) }
    }

    pub async fn txn(
        &self,
        conditions: Vec<(String, Option<String>)>,
        updates: Vec<(String, Option<String>)>,
        reads: Vec<String>,
    ) -> Result<Vec<Option<String>>, String> {
        let kvtxn = KVTxn {
            conditions,
            updates,
            reads,
        };
        let payload = serde_json::to_vec(&kvtxn).unwrap();
        let (resp_meta, resp_payload) = self.fc.invoke("", &payload).await?;
        let resp_meta: DBResp = serde_json::from_str(&resp_meta).unwrap();
        println!("Resp Payload: {}.", resp_payload.len());
        println!("Resp Meta: {resp_meta:?}.");
        match resp_meta {
            DBResp::Ok => Ok(serde_json::from_slice(&resp_payload).unwrap()),
            DBResp::Err(s) => Err(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{DBClient, KVClient};
    use crate::{
        bench::{DATA_SIZE_KB, KEY_SIZE_B, NUM_KEYS, START_KEY, VAL_SIZE_B},
        BenchFn,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_db_test() {
        run_simple_db_test().await;
    }

    async fn run_simple_db_test() {
        let dbcl = DBClient::new(Some(0), Some(512)).await;
        let resp = dbcl.txn(vec![], vec![
            "REPLACE INTO keyvalues(key, value) VALUES ('Amadou0', 'Ngom0'), ('Amadou1', 'Ngom1');".into()
        ], Some(
            "SELECT * FROM keyvalues WHERE key='Amadou0';".into()
        )).await;
        println!("Resp: {resp:?}");
        let resp = dbcl
            .txn(
                vec![],
                vec!["DELETE FROM keyvalues WHERE key IN ('Amadou0', 'Amadou1');".into()],
                Some("SELECT * FROM keyvalues WHERE key='Amadou0';".into()),
            )
            .await;
        println!("Resp: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_scaling_test() {
        run_simple_scaling_test().await;
    }

    async fn run_simple_scaling_test() {
        // Large mem to force scale up.
        let dbcl = DBClient::new(Some(0), Some(20000)).await;
        for _ in 0..10000 {
            let resp = dbcl.txn(vec![], vec![
                "REPLACE INTO keyvalues(key, value) VALUES ('Amadou0', 'Ngom0'), ('Amadou1', 'Ngom1');".into()
            ], Some(
                "SELECT * FROM keyvalues WHERE key='Amadou0';".into()
            )).await;
            println!("Resp: {resp:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn datagen_test() {
        run_datagen_test().await;
    }

    async fn run_datagen_test() {
        let reset = false;
        let dbcl = DBClient::new(Some(0), Some(2048)).await;
        let start_key = START_KEY; // Large enough to guarantee same key size.
        let key_size_b: usize = KEY_SIZE_B;
        let val_size_b: usize = VAL_SIZE_B;
        if reset {
            let start_time = std::time::Instant::now();
            let data_size_kb: usize = DATA_SIZE_KB; // Approx ~20GB.
            let num_keys = data_size_kb / ((val_size_b + key_size_b) * 1000); // Approx.
            let batch_size: usize = (4 * 1000) / ((val_size_b + key_size_b) * 1000); // 4MB
            let end_key = start_key + num_keys;
            let mut i: usize = start_key;
            while i < end_key {
                let batch_start = i;
                let batch_end = batch_start + batch_size;
                println!("Batch: ({batch_start}, {batch_end}).");
                let keyvals = (batch_start..batch_end)
                    .map(|k| BenchFn::make_key_val(k, key_size_b, val_size_b))
                    .collect::<Vec<_>>();
                let updates = keyvals
                    .into_iter()
                    .map(|(k, v)| format!("REPLACE INTO keyvalues VALUES ('{k}', '{v}');"))
                    .collect::<Vec<_>>();
                loop {
                    let r = dbcl.txn(vec![], updates.clone(), None).await;
                    if r.is_ok() {
                        println!("Ok.");
                        break;
                    } else {
                        println!("Err: {r:?}. Retrying.");
                    }
                }
                i += batch_size;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Gen Duration: {duration:?}");
        }
        let (key, _) = BenchFn::make_key_val(start_key + 37, key_size_b, val_size_b);
        let resp = dbcl
            .txn(
                vec![],
                vec![],
                Some(format!("SELECT * FROM keyvalues WHERE key='{key}';")),
            )
            .await;
        println!("Resp: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_kv_test() {
        run_simple_kv_test().await;
    }

    async fn run_simple_kv_test() {
        let kvcl = KVClient::new(Some(0), Some(512)).await;
        let resp = kvcl
            .txn(
                vec![],
                vec![
                    ("Amadou0".into(), Some("Ngom0".into())),
                    ("Amadou1".into(), Some("Ngom1".into())),
                ],
                vec!["Amadou0".into(), "Amadou1".into()],
            )
            .await;
        println!("Resp: {resp:?}");
        let resp = kvcl
            .txn(
                vec![],
                vec![("Amadou0".into(), None)],
                vec!["Amadou0".into(), "Amadou1".into()],
            )
            .await;
        println!("Resp: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn kv_datagen_test() {
        run_kv_datagen_test().await;
    }

    async fn run_kv_datagen_test() {
        let reset = false;
        let kvcl = KVClient::new(Some(0), Some(2048)).await;
        let start_key = START_KEY; // Large enough to guarantee same key size.
        let key_size_b: usize = KEY_SIZE_B;
        let val_size_b: usize = VAL_SIZE_B;
        if reset {
            let start_time = std::time::Instant::now();
            let num_keys = NUM_KEYS;
            let batch_size: usize = (4 * 1000) / ((val_size_b + key_size_b) / 1000); // 4MB
            let end_key = start_key + num_keys;
            let mut i: usize = start_key;
            while i < end_key {
                let batch_start = i;
                let batch_end = batch_start + batch_size;
                println!("Batch: ({batch_start}, {batch_end}).");
                let keyvals = (batch_start..batch_end)
                    .map(|k| BenchFn::make_key_val(k, key_size_b, val_size_b))
                    .collect::<Vec<_>>();
                let updates = keyvals
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<Vec<_>>();
                loop {
                    let r = kvcl.txn(vec![], updates.clone(), vec![]).await;
                    if r.is_ok() {
                        println!("Ok.");
                        break;
                    } else {
                        println!("Err: {r:?}. Retrying.");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                i += batch_size;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Gen Duration: {duration:?}");
        }
        let (key, _) = BenchFn::make_key_val(start_key + 37, key_size_b, val_size_b);
        let resp = kvcl.txn(vec![], vec![], vec![key]).await;
        println!("Resp: {resp:?}");
    }
}
