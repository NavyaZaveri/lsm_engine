use crate::memtable::{Memtable, ValueStatus};
use crate::sst::{Segment};
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

use thiserror::Error;

#[macro_use]
extern crate lazy_static;

mod memtable;
mod sst;

lazy_static! {
    static ref TOMBSTONE_VALUE: &'static str = "TOMBSTONE"; //TODO: change this
}


type key_offset = u64;
type segment_index = usize;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SstError(#[from] sst::SstError),
}

type Result<T> = std::result::Result<T, self::Error>;

pub struct LSMEngine {
    memtable: Memtable<String, String>,
    segments: Vec<Segment>,
    persist_data: bool,
    segment_size: usize,
    sparse_memory_index: BTreeMap<String, (key_offset, segment_index)>,
    sparse_offset: usize,
}


pub struct LSMBuilder {
    persist_data: bool,
    segment_size: usize,
    sparse_offset: usize,
    inmemory_capacity: usize,

}

impl LSMBuilder {
    pub fn new() -> LSMBuilder {
        return Self {
            persist_data: true,
            segment_size: 1500,
            sparse_offset: 35,
            inmemory_capacity: 500,
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

    pub fn inmemory_capacity(mut self, inmemory_capacity: usize) -> Self {
        self.inmemory_capacity = inmemory_capacity;
        return self;
    }
    pub fn build(self) -> LSMEngine {
        return LSMEngine::new(self.inmemory_capacity, self.segment_size, self.sparse_offset, self.persist_data);
    }
}

impl LSMEngine {
    fn new(inmemory_capacity: usize, segment_size: usize, sparse_offset: usize, persist_data: bool) -> Self {
        if segment_size < inmemory_capacity {
            panic!("segment size {} cannot be less than in-memory capacity {}", segment_size, inmemory_capacity)
        }

        LSMEngine {
            memtable: Memtable::new(inmemory_capacity),
            segments: Vec::new(),
            sparse_memory_index: BTreeMap::new(),
            persist_data,
            segment_size,
            sparse_offset,
        }
    }

    fn flush_memtable(&mut self) -> Result<Segment> {
        let mut new_segment = if self.persist_data {
            Segment::default()
        } else {
            Segment::temp()
        };
        for (k, value_status) in self.memtable.drain() {
            match value_status {
                ValueStatus::Present(value) => {
                    new_segment.write(k, value)?;
                }
                ValueStatus::Tombstone => {
                    new_segment.write(k, TOMBSTONE_VALUE.to_string())?;
                }
            }
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
        if self.memtable.at_capacity() && !self.memtable.contains(&key) {
            let new_segment = self.flush_memtable()?;
            self.segments.push(new_segment);
            self.memtable.insert(key, value);
            self.merge_segments()?;
        } else {
            self.memtable.insert(key, value);
        }
        Ok(())
    }


    pub fn read(&mut self, key: &str) -> Result<Option<String>> {
        if let Some(value_status) = self.memtable.get(key) {
            return match value_status {
                ValueStatus::Present(value) => Ok(Some(value.to_owned())),
                ValueStatus::Tombstone => Ok(None),
            };
        }

        let mut before = self.sparse_memory_index.range((Unbounded, Included(key.to_owned())));
        let maybe_closest_key = before.next_back();
        if maybe_closest_key.is_none() {
            return Ok(None);
        }

        let (closest_key, (key_offset, segment_index)) = maybe_closest_key.unwrap();

        let segment = &self.segments[*segment_index];
        let maybe_value = segment.search_from(key, *key_offset)?;

        if maybe_value.is_some() && maybe_value.as_ref().map(|value| value != &TOMBSTONE_VALUE.to_string()).unwrap() {
            return Ok(maybe_value);
        }
        for segment in &self.segments[segment_index + 1..] {
            let maybe_value = segment.search_from_start(key)?;
            if maybe_value.is_some() {
                return Ok(maybe_value);
            }
        }

        Ok(None)
    }
    pub fn delete(&mut self, key: &str) {
        self.memtable.delete(key.to_owned());
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
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

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
        lsm.delete("k1");
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
}
