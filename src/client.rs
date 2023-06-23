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

/// A txn.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBTxn {
    pub conditions: Vec<String>,
    pub updates: Vec<String>,
    pub query: Option<String>,
    pub caller_mem: i32,
}

/// Op.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DBOp {
    Txn(DBTxn),
    Query(String),
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DBResp {
    Ok,
    Err(String),
    Rows,
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

    /// Send operation.
    async fn send_op(&self, dbop: DBOp) -> Result<Vec<Vec<Value>>, String> {
        let payload = serde_json::to_vec(&dbop).unwrap();
        let (resp_meta, resp_payload) = self.fc.invoke("", &payload).await?;
        let resp_meta: DBResp = serde_json::from_str(&resp_meta).unwrap();
        println!("Resp Payload: {}.", resp_payload.len());
        match resp_meta {
            DBResp::Ok => Ok(vec![]),
            DBResp::Err(s) => Err(s),
            DBResp::Rows => Ok(bincode::deserialize(&resp_payload).unwrap()),
        }
    }

    /// Snapshot query.
    pub async fn snapshot_query(&self, query: &str) -> Result<Vec<Vec<Value>>, String> {
        let dbop = DBOp::Query(query.into());
        self.send_op(dbop).await
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
        let dbop = DBOp::Txn(dbtxn);
        self.send_op(dbop).await
    }
}

#[cfg(test)]
mod tests {
    use obelisk::ServerlessStorage;

    use super::DBClient;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_db_test() {
        run_simple_db_test().await;
    }

    async fn run_simple_db_test() {
        let dbcl = DBClient::new(Some(0), Some(512)).await;
        let resp = dbcl.txn(vec![], vec![
            "REPLACE INTO keyvalues(key, value) VALUES ('Amadou0', 'Ngom0'), ('Amadou1', 'Ngom1');".into()
        ], Some(
            "SELECT * FROM keyvalues;".into()
        )).await;
        println!("Resp: {resp:?}");
        println!("Resp: {resp:?}");
    }


    /// This is just to check if the `unsafe` use of sqlite connections works.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn concurrent_st_test() {
        run_concurrent_st_test().await;
    }

    async fn run_concurrent_st_test() {
        let shared_dir = obelisk::common::shared_storage_prefix();
        let identifier = "essai";
        let storage_dir = format!("{shared_dir}/csqlite/{identifier}");
        let _ = std::fs::remove_dir_all(&storage_dir);
        let _res = std::fs::create_dir_all(&storage_dir).unwrap();
        let pool = ServerlessStorage::try_exclusive_file(&storage_dir, 2).unwrap();
        {
            let conn = pool.get().unwrap();
            conn.execute("CREATE TABLE IF NOT EXISTS essai (key INT PRIMARY KEY)", []).unwrap();
        }
        {
            let pool = pool.clone();
            tokio::task::spawn_blocking(move || {
                let mut conn = {
                    let conn = pool.get().unwrap();
                    unsafe {
                        let handle = conn.handle();
                        rusqlite::Connection::from_handle(handle).unwrap()
                    }
                };
                println!("Getting Write Conn!");
                println!("Got Write Conn!");
                let txn = conn.transaction().unwrap();
                let res = txn.execute("REPLACE INTO essai VALUES (37)", []).unwrap();
                println!("Insert res: {res:?}.");
                std::thread::sleep(std::time::Duration::from_secs(10));
                txn.commit().unwrap();
            });
        }
        {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let pool = pool.clone();
            tokio::task::block_in_place(move || {
                println!("Getting Read Conn!");
                let conn = pool.get().unwrap();
                println!("Got Read Conn!");
                let res = conn.query_row("SELECT * FROM essai LIMIT 1", [], |r| r.get::<usize, usize>(0)).unwrap();
                println!("Select res: {res:}");    
            });
        }
    }
}
