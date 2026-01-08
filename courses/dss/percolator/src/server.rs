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
    seq: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
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
            .next_back()
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
                Value::Timestamp(start_ts) =>
                    match store.read(key, Column::Data, Some(*start_ts), Some(*start_ts)) {
                        Some((_, Value::Vector(body))) => Ok(GetResponse { value: body.clone() }),
                        Some(_) => unreachable!("data column should only have Vector values"),
                        None => Ok(GetResponse { value: vec![] }),
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

        // fail if there is already a lock
        if store.read(key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse { ok: false });
        }

        // fail if there is a committed write with commit_ts > start_ts
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

        store.write(
            key.clone(),
            Column::Write,
            commit_ts,
            Value::Timestamp(start_ts),
        );
        store.erase(key.clone(), Column::Lock, start_ts);

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        loop {
            let mut store = self.data.lock().unwrap();

            // inspect if there is a lock on this key
            let (pkey_start_ts, pkey) =
                match store.read(key.clone(), Column::Lock, None, Some(start_ts)) {
                    Some(((_, pkey_start_ts), Value::Vector(pkey))) =>
                        (*pkey_start_ts, pkey.clone()),
                    Some((_, Value::Timestamp(_))) =>
                        unreachable!("lock column should only have Vector values"),
                    // no lock found, just return
                    None => return,
                };

            if pkey == key {
                // it's a primary lock, wait for TTL
                drop(store);
                std::thread::sleep(Duration::from_nanos(TTL));

                // the lock must committed or expired now, so just clean it up
                let mut store = self.data.lock().unwrap();
                store.erase(key.clone(), Column::Lock, pkey_start_ts);
            } else {
                // it's a secondary lock, check if the primary has been committed
                let plock = store.read(
                    pkey.clone(),
                    Column::Lock,
                    Some(pkey_start_ts),
                    Some(pkey_start_ts),
                );
                if plock.is_some() {
                    // primary lock still exists, back off
                    drop(store);
                    std::thread::sleep(Duration::from_millis(10));

                    // The lock must committed or expired now, so just clean it up
                    let mut store = self.data.lock().unwrap();
                    store.erase(key.clone(), Column::Lock, pkey_start_ts);
                } else {
                    // primary lock does not exist, try to find the commit ts of the primary key
                    let mut rbound = None;
                    let mut pkey_commit_ts;
                    loop {
                        match store.read(pkey.clone(), Column::Write, Some(pkey_start_ts), rbound) {
                            None => {
                                // no write record found, primary must have not been committed
                                pkey_commit_ts = None;
                                break;
                            }
                            Some(((pkey, commit_ts), Value::Timestamp(start_ts))) => {
                                if *start_ts == pkey_start_ts {
                                    // found the commit ts of the primary
                                    pkey_commit_ts = Some(*commit_ts);
                                    break;
                                }
                                // Shrink the right bound and continue searching
                                rbound = Some(*commit_ts - 1);
                            }
                            Some(_) =>
                                unreachable!("write column should only have Timestamp values"),
                        }
                    }

                    // 2. amend the secondary's write record if the primary has been committed
                    if let Some(prev_commit_ts) = pkey_commit_ts {
                        store.write(
                            key.clone(),
                            Column::Write,
                            prev_commit_ts,
                            Value::Timestamp(pkey_start_ts),
                        );
                    }

                    // 3. clean up the secondary lock
                    store.erase(key.clone(), Column::Lock, pkey_start_ts);
                }
            }
        }
    }
}
