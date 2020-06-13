//!
//! A rust implementation of a key-value store using [Log Structured Merge Trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree#:~:text=In%20computer%20science%2C%20the%20log,%2C%20maintain%20key%2Dvalue%20pairs.)
//!
//!
//!
//! ## Example Usage
//!  ```
//! use lsm_engine::{LSMEngine, LSMBuilder} ;
//! fn main() -> Result<(), Box< dyn std::error::Error>> {
//!
//!    let mut lsm = LSMBuilder::new().
//!          persist_data(false).
 //!         segment_size(2).
 //!         inmemory_capacity(1).
 //!         sparse_offset(2).
 //!
 //!         build();
//!     lsm.write("k1".to_owned(), "v1".to_owned())?;
//!     lsm.write("k2".to_owned(), "k2".to_owned())?;
//!     lsm.write("k1".to_owned(), "v_1_1".to_owned())?;
//!     let value = lsm.read("k1")?;
//!     assert_eq!(value, Some("v_1_1".to_owned()));
//!     Ok(())
//! }
//! ```
//! ## Design
//!
//! `lsm_engine` is an embedded key-value store that uses LSM-trees and leverages a [Write-Ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL) file for
//! data recovery.
//!
//! The basic architecture is illustrated below:
//!
//! ### Write
//! When a write comes in, the following happens
//! * The entry is written into the WAL file (unless an explicit request is made not to)
//! * If the size of the internal is at full capacity, the contents are dumped into a segment file, with compaction performed in the end.
//! * The entry is then inserted into the now-empty memtable.
//!
//! ### Read
//! When a request for a read is made, the following happens:
//! * It first checks its internal memtable for the value corresponding to the requested key. If it exists, it returns the value
//! * Otherwise, it looks up the offset of the closest key with its sparse mememory index. This is a balanced tree that maintains
//! that position of  1 out of every `sparse_offset` entries in memeory.
//! * It then linearly scans forward from that offset, looking for the desired key-value entry.
//!
//! ### Delete
//! This is just a special case of write, with value being a special tombstone string.
//!
//! For more details with visual illustrations, check out my [blog post](https://navyazaveri.github.io/algorithms/2020/01/12/write-a-kv-store-from-scratch.html)
//!

use crate::memtable::{Memtable};
use crate::sst::{Segment};
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use rand::Rng;
use thiserror::Error;
use rand::distributions::Alphanumeric;
use crate::kv::{KVPair, KVFileWriter, KVFileReader};
use crate::wal::Wal;
use std::fs::{File, OpenOptions};
use std::path::Path;
use rand::{SeedableRng};

use rand::rngs::StdRng;


#[macro_use]
extern crate lazy_static;


mod memtable;
mod sst;
mod wal;
mod kv;
lazy_static! {

static ref TOMBSTONE_VALUE: String = {
    let rng:StdRng = SeedableRng::seed_from_u64(20);
    rng.sample_iter(&Alphanumeric).take(20).collect::<String>()
    };
}


type KeyOffset = u64;
type SegmentIndex = usize;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SstError(#[from] sst::SstError),
    #[error(transparent)]
    KvError(#[from] kv::KvError),
}


pub type Result<T> = std::result::Result<T, self::Error>;

pub struct LSMEngine {
    memtable: Memtable<String, String>,
    segments: Vec<Segment>,
    segment_size: usize,
    sparse_memory_index: BTreeMap<String, (KeyOffset, SegmentIndex)>,
    sparse_offset: usize,
    wal: Option<Wal>,

}


pub struct LSMBuilder {
    persist_data: bool,
    segment_size: usize,
    sparse_offset: usize,
    inmemory_capacity: usize,
    wal: Option<Wal>,
}

impl LSMBuilder {
    pub fn new() -> LSMBuilder {
        return Self {
            persist_data: false,
            segment_size: 1500,
            sparse_offset: 35,
            inmemory_capacity: 500,
            wal: None,
        };
    }

    pub fn persist_data(mut self, persist: bool) -> Self {
        self.persist_data = persist;
        return self;
    }

    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        return self;
    }

    pub fn sparse_offset(mut self, sparse_offset: usize) -> Self {
        self.sparse_offset = sparse_offset;
        return self;
    }
    pub fn wal_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        self.wal = Some(Wal::new(file));
        return self;
    }

    pub fn inmemory_capacity(mut self, inmemory_capacity: usize) -> Self {
        self.inmemory_capacity = inmemory_capacity;
        return self;
    }
    pub fn build(self) -> LSMEngine {
        return LSMEngine::new(self.inmemory_capacity, self.segment_size, self.sparse_offset, self.wal);
    }
}

impl LSMEngine {
    fn new(inmemory_capacity: usize, segment_size: usize, sparse_offset: usize, wal: Option<Wal>) -> Self {
        if segment_size < inmemory_capacity {
            panic!("segment size {} cannot be less than in-memory capacity {}", segment_size, inmemory_capacity)
        }

        LSMEngine {
            memtable: Memtable::new(inmemory_capacity),
            segments: Vec::new(),
            sparse_memory_index: BTreeMap::new(),
            segment_size,
            sparse_offset,
            wal,
        }
    }


    fn recover_from(&mut self, wal_file: File) -> Result<()> {
        self.clear();
        let mut wal_file = Wal::new(wal_file);

        for maybe_kv in wal_file.read_from_start()? {
            let kv = maybe_kv?;
            self.write(kv.key, kv.value)?;
        }
        self.wal = Some(wal_file);
        Ok(())
    }

    fn clear(&mut self) {
        self.segments.clear();
        self.sparse_memory_index.clear();
    }


    fn flush_memtable(&mut self) -> Result<Segment> {
        let mut new_segment = Segment::temp();
        for (key, value) in self.memtable.drain() {
            new_segment.write(KVPair { key, value })?;
        }
        return Ok(new_segment);
    }


    fn merge_segments(&mut self) -> Result<()> {
        self.sparse_memory_index.clear();
        let mut count = 0;
        self.segments = sst::merge(std::mem::take(&mut self.segments), self.segment_size,
                                   |segment_index, key_offset, key| {
                                       if count % self.sparse_offset == 0 {
                                           self.sparse_memory_index.insert(key, (key_offset, segment_index));
                                       }
                                       count += 1;
                                   })?;
        Ok(())
    }

    pub fn write(&mut self, key: String, value: String) -> Result<()> {
        if self.wal.is_some() {
            self.wal.as_mut().unwrap().persist(KVPair { key: key.clone(), value: value.clone() })?;
        }
        if self.memtable.at_capacity() & &!self.memtable.contains(&key) {
            let new_segment = self.flush_memtable()?;
            self.segments.push(new_segment);
            self.memtable.insert(key, value);
            self.merge_segments()?;
        } else {
            self.memtable.insert(key, value);
        }
        Ok(())
    }

    ///Unfortunately this is marked as mutable since relies on rust's seek api, which is also
    /// mutable. In the future, this might change to immutable if the seek api changes
    /// or it the issue becomes significant enough to warrant  using `Rc<RefCell<>>`
    pub fn read(&mut self, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.memtable.get(key) {
            if value == &*TOMBSTONE_VALUE {
                return Ok(None);
            }
            return Ok(Some(value.to_owned()));
        }


        //get the biggest element less than or equal to the key
        let mut before = self.sparse_memory_index.range((Unbounded, Included(key.to_owned())));
        let maybe_closest_key = before.next_back();

        if maybe_closest_key.is_none() {
            return Ok(None);
        }

        let (closest_key, (key_offset, segment_index)) = maybe_closest_key.unwrap();

        for index in *segment_index..self.segments.len() {
            let segment = &mut self.segments[index];
            let maybe_value = if index == *segment_index { segment.search_from(key, *key_offset)? } else { segment.search_from_start(key)? };
            if maybe_value.is_some() {
                if maybe_value.as_ref().map(|x| x != &*TOMBSTONE_VALUE).unwrap() { return Ok(maybe_value); };

                //if it's marked with a tombstone value, it's a "deleted" key
                return Ok(None);
            }
        }

        Ok(None)
    }
    pub fn delete(&mut self, key: &str) -> Result<()> {
        if self.wal.is_some() {
            self.wal.as_mut().unwrap().persist(KVPair { key: key.to_owned(), value: TOMBSTONE_VALUE.to_string() })?;
        }
        self.write(key.to_owned(), TOMBSTONE_VALUE.to_string())?;
        Ok(())
    }

    fn contains(&mut self, key: &str) -> Result<bool> {

//TODO: use a scalable bloom filter for faster lookups
        let maybe_value = self.read(key)?;
        return Ok(maybe_value.is_some());
    }
}

impl Default for LSMEngine {
    fn default() -> Self {
        return LSMBuilder::new().build();
    }
}

#[cfg(test)]
mod tests {
    use crate::{LSMEngine, LSMBuilder};
    use crate::{TOMBSTONE_VALUE};
    use rand::seq::SliceRandom;
    use rand::{SeedableRng};

    use rand::rngs::StdRng;
    use std::collections::{HashMap};


    #[test]
    fn it_works() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMBuilder::new().
            persist_data(false).
            segment_size(100).
            sparse_offset(2).
            inmemory_capacity(3).
            build();
        lsm.write("k1".to_owned(), "v1".to_owned())?;
        lsm.write("k2".to_owned(), "v2".to_owned())?;
        lsm.write("k3".to_owned(), "v3".to_owned())?;

        for (k, v) in vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
            assert_eq!(lsm.read(k)?, Some(v.to_owned()));
        }
        Ok(())
    }


    #[test]
    fn test_deletions() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMBuilder::new()
            .persist_data(false)
            .segment_size(2)
            .inmemory_capacity(1)
            .sparse_offset(2)
            .build();
        lsm.write("k1".to_owned(), "v1".to_owned())?;
        lsm.write("k2".to_owned(), "v2".to_owned())?;
        lsm.delete("k1")?;
        let value = lsm.read("k1")?;
        assert!(value.is_none());

        Ok(())
    }

    #[test]
    fn test_reads_on_duplicate_keys() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMBuilder::new().
            persist_data(false).
            segment_size(2).
            inmemory_capacity(1).
            sparse_offset(2).
            build();

        lsm.write("k1".to_owned(), "v1".to_owned())?;
        lsm.write("k2".to_owned(), "k2".to_owned())?;
        lsm.write("k1".to_owned(), "v_1_1".to_owned())?;
        lsm.write("k3".to_owned(), "v3".to_owned())?;

        let value = lsm.read("k1")?;
        assert_eq!(value, Some("v_1_1".to_owned()));
        Ok(())
    }

    #[test]
    fn test_on_large_dataset() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMEngine::default();
        let dataset: Vec<_> = (0..5000).map(|i| ("k".to_owned() + &i.to_string(), "v".to_owned() + &i.to_string())).collect();
        let mut rng: StdRng = SeedableRng::seed_from_u64(20);
        let mut seen = HashMap::new();


        for (k, v) in dataset.iter() {
            lsm.write(k.clone(), v.clone())?;
            seen.insert(k, v.clone());

            let (random_key, random_value) = dataset.choose(&mut rng).unwrap();
            let mut value = None;

            if seen.contains_key(random_key) {
                value = seen.get(random_key);
            }
            assert_eq!(lsm.read(random_key)?.as_ref(), value);
        }

        Ok(())
    }

    #[test]
    fn test_recovery_with_wal() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMBuilder::new().wal_path("foo").build();
        let dataset: Vec<_> = (0..20).map(|i| ("k".to_owned() + &i.to_string(), "v".to_owned() + &i.to_string())).collect();

        for (key, v) in dataset.iter() {
            lsm.write(key.to_string(), v.to_string())?;
        }


        for i in 10..dataset.len() {
            let (k, v) = &dataset[i];
            lsm.delete(k)?;
        }

        let mut new_lsm = LSMBuilder::new().build();
        new_lsm.recover_from(lsm.wal.unwrap().file)?;
        for i in 0..10 {
            let (k, v) = &dataset[i];
            assert_eq!(new_lsm.read(k)?, Some(v.to_owned()));
        }

        for i in 10..dataset.len() {
            let (k, v) = &dataset[i];
            assert_eq!(new_lsm.read(k)?, None);
        }
        std::fs::remove_file("foo")?;
        Ok(())
    }

    #[test]
    fn test_contains() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMBuilder::new().inmemory_capacity(1).build();
        lsm.write("k1".to_owned(), "v1".to_owned())?;
        lsm.delete("k1")?;
        assert_eq!(lsm.contains("k1")?, false);
        assert_eq!(lsm.contains("k2")?, false);
        Ok(())
    }
}
