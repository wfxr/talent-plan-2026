use std::{
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
    entries:  Vec<(Vec<u8>, Vec<u8>)>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client { tso_client, txn_client, start_ts: 0, entries: Vec::new() }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut backoff_ms = BACKOFF_TIME_MS;
        let mut retries = RETRY_TIMES;
        loop {
            let res = block_on(async {
                self.tso_client
                    .get_timestamp(&TimestampRequest {})
                    .await
                    .map(|resp| resp.tso)
            });
            retries -= 1;

            match res {
                Ok(tso) => return Ok(tso),
                Err(Error::Timeout) if retries > 0 => {
                    debug!("Retrying to get timestamp, remaining retries: {}", retries);
                    // Exponential backoff
                    thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms *= 2;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        // TODO: Should this method return a Result?
        self.entries.clear();
        self.start_ts = self.get_timestamp().expect("Failed to get timestamp");
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let req = GetRequest { key, start_ts: self.start_ts };
        block_on(async { self.txn_client.get(&req).await.map(|resp| resp.value) })
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.entries.push((key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.

        if self.entries.is_empty() {
            return Ok(true);
        }

        // Do prewrite for each entry
        let pkey = &self.entries[0].0;
        for (key, value) in &self.entries {
            let req = PrewriteRequest {
                pkey:     pkey.clone(),
                key:      key.clone(),
                value:    value.clone(),
                start_ts: self.start_ts,
            };

            let resp = block_on(async { self.txn_client.prewrite(&req).await })?;
            if !resp.ok {
                return Ok(false);
            }
        }

        // Get commit timestamp
        let commit_ts = self.get_timestamp()?;

        // Commit primary first
        let req = CommitRequest {
            is_primary: true,
            key: pkey.clone(),
            start_ts: self.start_ts,
            commit_ts,
        };
        let resp = block_on(async { self.txn_client.commit(&req).await });
        match resp {
            Err(Error::Other(msg)) if msg == "reqhook" => return Ok(false),
            Err(e) => return Err(e),
            Ok(_) => {}
        }

        // Commit secondaries
        // PERF: We can do this asynchronously for better latency
        let secondaries = &self.entries[1..];
        for (key, _) in secondaries {
            let req = CommitRequest {
                is_primary: false,
                key: key.clone(),
                start_ts: self.start_ts,
                commit_ts,
            };
            let resp = block_on(async { self.txn_client.commit(&req).await });
            // Log a warning if committing a secondary fails
            if resp.is_err() {
                warn!("Failed to commit secondary key: {:?}", key);
            }
        }

        Ok(true)
    }
}
