use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
        Mutex,
    },
    time::Duration,
};

use crate::{msg::*, service::*, *};

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    next_tso: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let tso = self.next_tso.fetch_add(1, Ordering::SeqCst);
        Ok(TimestampResponse { tso: tso + 1 })
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
        let cf = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        let ts_start = ts_start_inclusive.unwrap_or(u64::MIN);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);
        cf.range((key.clone(), ts_start)..=(key, ts_end)).next_back()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let cf = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        cf.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let cf = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        cf.remove(&(key, commit_ts));
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
        let GetRequest { key, start_ts } = req;

        // try to clean up pending locks
        self.back_off_maybe_clean_up_lock(start_ts, key.clone());

        let store = self.data.lock().unwrap();
        match store.read(key.clone(), Column::Write, None, Some(start_ts)) {
            Some(((k, commit_ts), v)) => match v {
                Value::Timestamp(start_ts) => match store.read(key, Column::Data, Some(*start_ts), Some(*start_ts)) {
                    Some((_, Value::Vector(body))) => Ok(GetResponse { value: body.clone() }),
                    Some(_) => unreachable!("data column should only have Vector values"),
                    _ => Ok(GetResponse { value: vec![] }),
                },
                _ => unreachable!("write column should only have Timestamp values"),
            },
            None => Ok(GetResponse { value: vec![] }),
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let PrewriteRequest { pkey, key, value, start_ts } = req;
        let mut store = self.data.lock().unwrap();

        // check if there is already a lock
        if store.read(key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse { ok: false });
        }

        // check if there is a write with commit_ts > start_ts already committed
        if store
            .read(key.clone(), Column::Write, Some(start_ts + 1), None)
            .is_some()
        {
            return Ok(PrewriteResponse { ok: false });
        }

        store.write(key.clone(), Column::Lock, start_ts, Value::Vector(pkey));
        store.write(key.clone(), Column::Data, start_ts, Value::Vector(value));

        Ok(PrewriteResponse { ok: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let CommitRequest { is_primary, key, start_ts, commit_ts } = req;
        let mut store = self.data.lock().unwrap();

        store.write(key.clone(), Column::Write, commit_ts, Value::Timestamp(start_ts));
        store.erase(key.clone(), Column::Lock, start_ts);

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        loop {
            let mut store = self.data.lock().unwrap();

            let (prev_start_ts, pkey) = match store.read(key.clone(), Column::Lock, None, Some(start_ts)) {
                Some(((_, prev_start_ts), Value::Vector(pkey))) => (*prev_start_ts, pkey.clone()),
                Some((_, Value::Timestamp(_))) => unreachable!("lock column should only have Vector values"),
                None => return,
            };

            if pkey == key {
                // it's a primary lock
                // for simpifity, just wait until prev txn committed
                drop(store);
                std::thread::sleep(Duration::from_nanos(TTL));

                // The lock must committed or expired now, so just clean it up
                let mut store = self.data.lock().unwrap();
                store.erase(key.clone(), Column::Lock, prev_start_ts);
            } else {
                // it's a secondary lock
                // check if the primary has been committed
                let plock = store.read(pkey.clone(), Column::Lock, Some(prev_start_ts), Some(prev_start_ts));
                if plock.is_some() {
                    // primary lock still exists, so back off
                    drop(store);
                    std::thread::sleep(Duration::from_millis(10));

                    // The lock must committed or expired now, so just clean it up
                    let mut store = self.data.lock().unwrap();
                    store.erase(key.clone(), Column::Lock, prev_start_ts);
                } else {
                    // primary lock does not exist

                    // 1. find the commit ts of the primary
                    let mut rbound = None;
                    let mut prev_commit_ts;
                    loop {
                        match store.read(pkey.clone(), Column::Write, Some(prev_start_ts), rbound) {
                            Some(((pkey, commit_ts), Value::Timestamp(start_ts))) => {
                                if *start_ts == prev_start_ts {
                                    prev_commit_ts = Some(*commit_ts);
                                    break;
                                }
                                rbound = Some(*commit_ts - 1);
                            }
                            Some(_) => unreachable!("write column should only have Timestamp values"),
                            None => {
                                // no write record found, primary must have not been committed
                                prev_commit_ts = None;
                                break;
                            }
                        }
                    }

                    // 2. if found the commit record, amend the secondary's write record
                    if let Some(prev_commit_ts) = prev_commit_ts {
                        store.write(
                            key.clone(),
                            Column::Write,
                            prev_commit_ts,
                            Value::Timestamp(prev_start_ts),
                        );
                    }

                    // 3. clean up the secondary lock
                    store.erase(key.clone(), Column::Lock, prev_start_ts);
                }
            }
        }
    }
}
