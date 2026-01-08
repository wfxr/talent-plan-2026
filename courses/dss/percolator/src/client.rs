use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use futures::executor::block_on;
use labrpc::*;

use crate::{
    msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest},
    service::{TSOClient, TransactionClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,

    start_ts: u64,
    buffer:   HashMap<Vec<u8>, Vec<u8>>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client { tso_client, txn_client, start_ts: 0, buffer: HashMap::new() }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut backoff_ms = BACKOFF_TIME_MS;
        let mut retries = RETRY_TIMES;
        with_retry("get_timestamp request", || {
            block_on(async { self.tso_client.get_timestamp(&TimestampRequest {}).await })
        })
        .map(|resp| resp.tso)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        // TODO: Should this method return a Result?
        self.buffer.clear();
        self.start_ts = self.get_timestamp().expect("Failed to get timestamp");
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let req = GetRequest { start_ts: self.start_ts, key };
        with_retry("get request", || {
            block_on(async { self.txn_client.get(&req).await.map(|resp| resp.value) })
        })
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.buffer.insert(key, value);
    }

    fn request_precommit(&self, key: Vec<u8>, value: Vec<u8>, pkey: Vec<u8>) -> Result<bool> {
        let req = PrewriteRequest { start_ts: self.start_ts, pkey, key, value };
        with_retry("precommit request", || {
            block_on(async { self.txn_client.prewrite(&req).await })
        })
        .map(|resp| resp.success)
    }

    fn request_commit(&self, commit_ts: u64, key: Vec<u8>, is_primary: bool) -> Result<bool> {
        let req = CommitRequest { start_ts: self.start_ts, commit_ts, key, is_primary };
        with_retry("commit request", || {
            block_on(async {
                match self.txn_client.commit(&req).await {
                    Ok(resp) => Ok(resp.success),
                    Err(Error::Other(msg)) if msg == "reqhook" => Ok(false),
                    Err(e) => Err(e),
                }
            })
        })
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        let mut keys = self.buffer.keys();
        let (primary, secondaries) = match keys.next() {
            None => return Ok(true),
            Some(key) => (key, keys),
        };

        // 1. do prewrite for each entry
        for (key, value) in &self.buffer {
            match self.request_precommit(key.clone(), value.clone(), primary.clone()) {
                Ok(true) => continue,
                failed => return failed,
            }
        }

        // 2. get commit timestamp
        let commit_ts = self.get_timestamp()?;

        // 3. commit primary first
        if !self.request_commit(commit_ts, primary.clone(), true)? {
            return Ok(false);
        }

        // 4. commit secondaries
        // PERF: do this asynchronously for better latency
        for key in secondaries {
            let resp = self.request_commit(commit_ts, key.clone(), false);

            // Log a warning if committing a secondary fails
            if !matches!(resp, Ok(true)) {
                warn!("Failed to commit secondary key {:?}, resp: {:?}", key, resp);
            }
        }

        Ok(true)
    }
}

fn with_retry<T>(op: impl AsRef<str>, f: impl Fn() -> Result<T>) -> Result<T> {
    let mut backoff_ms = BACKOFF_TIME_MS;
    let mut retries = RETRY_TIMES;
    loop {
        let res = f();
        retries -= 1;

        match res {
            Err(Error::Timeout) if retries > 0 => {
                warn!("{} timeout, remaining retries: {}", op.as_ref(), retries);
                // Exponential backoff
                thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms *= 2;
            }
            res => return res,
        }
    }
}
