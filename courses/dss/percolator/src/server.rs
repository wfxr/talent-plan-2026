use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
        Mutex,
    },
    thread,
    time::Duration,
};

use crate::{msg::*, service::*, *};
use Value::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    seq: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        // +1 to make tso start from 1
        Ok(TimestampResponse { tso: seq + 1 })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data:  BTreeMap<Key, Value>,
    lock:  BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let ts_start = ts_start_inclusive.unwrap_or(u64::MIN);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);
        self.cf(column)
            .range((key.clone(), ts_start)..=(key, ts_end))
            .last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        self.cf_mut(column).insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        self.cf_mut(column).remove(&(key, commit_ts));
    }

    #[inline]
    // Get mutable reference to the specified column family.
    fn cf_mut(&mut self, column: Column) -> &mut BTreeMap<Key, Value> {
        match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }

    #[inline]
    // Get reference to the specified column family.
    fn cf(&self, column: Column) -> &BTreeMap<Key, Value> {
        match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }

    fn find_commit_ts(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        self.cf(Column::Write)
            .range((key.clone(), start_ts)..=(key, u64::MAX))
            .find_map(|((_, commit_ts), value)| match value {
                Timestamp(ts) if *ts == start_ts => Some(*commit_ts),
                _ => None,
            })
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        let GetRequest { start_ts, key } = req;

        // try to clean up pending locks
        self.back_off_maybe_clean_up_lock(start_ts, key.clone());

        let store = self.data.lock().unwrap();
        match store.read(key.clone(), Column::Write, None, Some(start_ts)) {
            Some(((_, commit_ts), v)) => match v {
                Timestamp(start_ts) =>
                    match store.read(key, Column::Data, Some(*start_ts), Some(*start_ts)) {
                        Some((_, Vector(body))) => Ok(GetResponse { value: body.clone() }),
                        Some(_) => unreachable!("data cf should only have Vector values"),
                        None => Ok(GetResponse { value: vec![] }),
                    },
                _ => unreachable!("write cf should only have Timestamp values"),
            },
            None => Ok(GetResponse { value: vec![] }),
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let PrewriteRequest { start_ts, pkey, key, value } = req;
        let mut store = self.data.lock().unwrap();

        // abort if there is a committed write with commit_ts >= start_ts
        if store
            .read(key.clone(), Column::Write, Some(start_ts), None)
            .is_some()
        {
            return Ok(PrewriteResponse { success: false });
        }

        // abort if there is already a lock
        if store.read(key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse { success: false });
        }

        store.write(key.clone(), Column::Data, start_ts, Vector(value));
        store.write(key.clone(), Column::Lock, start_ts, Vector(pkey));

        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let CommitRequest { start_ts, commit_ts, key, .. } = req;
        let mut store = self.data.lock().unwrap();

        // abort if there is no lock
        if store
            .read(key.clone(), Column::Lock, Some(start_ts), Some(start_ts))
            .is_none()
        {
            return Ok(CommitResponse { success: false });
        }

        store.write(key.clone(), Column::Write, commit_ts, Timestamp(start_ts));
        store.erase(key, Column::Lock, start_ts);

        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    // Inspect the primary lock for a given key and max_start_ts.
    // Return (primary_key, pkey_start_ts, pkey_lock_exists).
    fn inspect_primary_lock(
        &self,
        key: Vec<u8>,
        max_start_ts: u64,
    ) -> Option<(Vec<u8>, u64, bool)> {
        let mut store = self.data.lock().unwrap();

        let (pkey, start_ts) =
            match store.read(key.clone(), Column::Lock, None, Some(max_start_ts))? {
                ((_, start_ts), Vector(pkey)) => (pkey.clone(), *start_ts),
                _ => unreachable!("lock cf should only have Vector values"),
            };

        if pkey == key {
            // fastpath: this is the primary lock, no need to read again
            return Some((pkey, start_ts, true));
        }

        let plock = store.read(pkey.clone(), Column::Lock, Some(start_ts), Some(start_ts));
        Some((pkey, start_ts, plock.is_some()))
    }

    fn back_off_maybe_clean_up_lock(&self, max_start_ts: u64, key: Vec<u8>) {
        // Your code here.
        loop {
            match self.inspect_primary_lock(key.clone(), max_start_ts) {
                Some((pkey, start_ts, true)) => {
                    // primary lock exists, wait for it to be committed or expired
                    thread::sleep(Duration::from_nanos(TTL));

                    // now the lock must committed or expired, so just clean it up
                    let mut store = self.data.lock().unwrap();
                    store.erase(key.clone(), Column::Lock, start_ts);
                }
                Some((pkey, start_ts, false)) => {
                    // primary lock does not exist, try to find the commit ts of the primary key
                    let mut store = self.data.lock().unwrap();
                    if let Some(commit_ts) = store.find_commit_ts(pkey.clone(), start_ts) {
                        store.write(key.clone(), Column::Write, commit_ts, Timestamp(start_ts));
                    }

                    // clean up the secondary lock
                    store.erase(key.clone(), Column::Lock, start_ts);
                }
                None => return,
            }
        }
    }
}
