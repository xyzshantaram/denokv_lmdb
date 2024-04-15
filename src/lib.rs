use std::{borrow::Cow, path::Path};

use anyhow::Error;
use async_trait::async_trait;

use denokv_proto::{
    AtomicWrite, CommitResult, KvEntry, KvValue, QueueMessageHandle, ReadRange, ReadRangeOutput,
    SnapshotReadOptions, WatchStream,
};
use heed::{BytesDecode, BytesEncode};

pub struct LmdbMessageHandle;

#[derive(Clone)]
pub struct LmdbDatabase {
    env: heed::Env,
    db: heed::Database<LmdbDKvKey, LmdbDKvValue>,
}

struct LmdbDKvKey(Vec<u8>);
struct LmdbDKvValue(KvValue);

impl<'a> BytesDecode<'a> for LmdbDKvKey {
    type DItem = LmdbDKvKey;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, Box<dyn std::error::Error>> {
        let mut vec = Vec::<u8>::new();
        vec.extend_from_slice(bytes);
        Ok(LmdbDKvKey(vec))
    }
}

impl BytesEncode<'_> for LmdbDKvKey {
    type EItem = LmdbDKvKey;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn std::error::Error>> {
        Ok(Cow::Owned(item.0.clone()))
    }
}

impl BytesDecode<'_> for LmdbDKvValue {
    type DItem = LmdbDKvValue;
    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, Box<dyn std::error::Error>> {
        let mut vec = Vec::<u8>::new();
        vec.extend_from_slice(bytes);
        let (_, list) = vec.split_at(1);
        if vec[0] == 0 {
            Ok(LmdbDKvValue(KvValue::U64(u64::from_le_bytes(
                list.try_into()
                    .expect("Wrong number of bytes for LmdbDKvValue"),
            ))))
        } else if vec[0] == 1 {
            Ok(LmdbDKvValue(KvValue::Bytes(list.to_owned())))
        } else {
            Ok(LmdbDKvValue(KvValue::V8(list.to_owned())))
        }
    }
}

impl<'a> BytesEncode<'a> for LmdbDKvValue {
    type EItem = LmdbDKvValue;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn std::error::Error>> {
        let mut res = vec![match &item.0 {
            KvValue::V8(_) => 2u8,
            KvValue::Bytes(_) => 1u8,
            _ => 0u8,
        }];

        let contents = match &item.0 {
            KvValue::V8(val) | KvValue::Bytes(val) => val.to_owned(),
            KvValue::U64(val) => val.to_le_bytes().to_vec(),
        };

        res.extend(contents);

        Ok(Cow::Owned(res))
    }
}

impl LmdbDatabase {
    pub fn new(path: &Path) -> Result<LmdbDatabase, Error> {
        let options = heed::EnvOpenOptions::new();
        let env = options.open(path).map_err(|e| Error::msg(e.to_string()))?;
        let db = env
            .open_database::<LmdbDKvKey, LmdbDKvValue>(None)
            .map_err(|e| Error::msg(e.to_string()))?
            .expect("Database was None while opening!");
        Ok(LmdbDatabase { env, db })
    }
}

#[async_trait(?Send)]
impl QueueMessageHandle for LmdbMessageHandle {
    async fn take_payload(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        todo!()
    }
    async fn finish(&self, success: bool) -> Result<(), anyhow::Error> {
        todo!()
    }
}

#[async_trait(?Send)]
impl denokv_proto::Database for LmdbDatabase {
    type QMH = LmdbMessageHandle;

    async fn snapshot_read(
        &self,
        requests: Vec<ReadRange>,
        _: SnapshotReadOptions,
    ) -> Result<Vec<ReadRangeOutput>, anyhow::Error> {
        let mut res = Vec::<ReadRangeOutput>::new();
        let txn = self.env.read_txn().map_err(|e| Error::msg(e.to_string()))?;
        for req in requests {
            let start_key = LmdbDKvKey(req.start);
            let end_key = LmdbDKvKey(req.end);
            let range = &(&start_key..&end_key);

            let results: Box<dyn Iterator<Item = (LmdbDKvKey, LmdbDKvValue)>> = if req.reverse {
                Box::new(
                    (self
                        .db
                        .rev_range(&txn, range)
                        .map_err(|e| Error::msg(e.to_string()))?)
                    .flatten(),
                )
            } else {
                Box::new(
                    (self
                        .db
                        .range(&txn, range)
                        .map_err(|e| Error::msg(e.to_string()))?)
                    .flatten(),
                )
            };

            res.push(ReadRangeOutput {
                entries: results
                    .map(|(k, v)| KvEntry {
                        key: k.0,
                        value: v.0,
                        versionstamp: [0; 10],
                    })
                    .collect(),
            });
        }

        Ok(res)
    }

    async fn atomic_write(
        &self,
        write: AtomicWrite,
    ) -> Result<Option<CommitResult>, anyhow::Error> {
        todo!()
    }

    async fn dequeue_next_message(&self) -> Result<Option<Self::QMH>, anyhow::Error> {
        todo!()
    }

    fn watch(&self, keys: Vec<Vec<u8>>) -> WatchStream {
        todo!()
    }

    fn close(&self) {
        todo!()
    }
}
