use crate::memtable::{Memtable, ValueStatus};
use std::collections::{HashSet, HashMap};
use crate::sst::{Segment, SST_Error};

#[macro_use]
extern crate lazy_static;


mod memtable;
mod sst;

lazy_static! {
    static ref TOMBSTONE_VALUE:&'static str= "TOMBSTONE";
}

pub struct LSMEngine<'a> {
    memtable: Memtable<String, String>,
    segments: Vec<Segment>,
    persist_data: bool,
    sparse_memory_index: HashMap<u64, &'a Segment>,

}


impl<'a> LSMEngine<'a> {
    fn new(inmemory_capacity: usize, persist_data: bool) -> Self {
        return LSMEngine {
            memtable: Memtable::new(inmemory_capacity),
            segments: Vec::new(),
            sparse_memory_index: HashMap::new(),
            persist_data,
        };
    }


    fn flush_memtable(&mut self) -> Result<Segment, SST_Error> {
        let mut new_segment = if self.persist_data { Segment::default() } else { Segment::temp() };
        for (k, value_status) in self.memtable.drain() {
            match value_status {
                ValueStatus::Present(value) => {
                    new_segment.write(k, value)?;
                }
                ValueStatus::Tombstone => {
                    new_segment.write(k, TOMBSTONE_VALUE.to_string())?;
                }
            }
        };
        return Ok(new_segment);
    }
    pub fn write(&mut self, key: String, value: String) -> Result<(), SST_Error> {
        if self.memtable.at_capacity() && !self.memtable.contains(&key) {
            let new_segment = self.flush_memtable()?;
            self.segments.push(new_segment);
            self.memtable.insert(key, value);
        } else {
            self.memtable.insert(key, value);
        }
        Ok(())
    }


    pub fn read<'b>(&'b mut self, key: &str) -> Result<Option<String>, sst::SST_Error> {
        if let Some(value_status) = self.memtable.get(key) {
            return match value_status {
                ValueStatus::Present(value) => { Ok(Some(value.to_owned())) }
                ValueStatus::Tombstone => { Ok(None) }
            };
        }

        for seg in self.segments.iter().rev() {
            //replace with call to sparse memory index
            let value = seg.search_from_start(key)?;
            if value.is_some() && value.as_ref().unwrap() != &TOMBSTONE_VALUE.to_string() {
                return Ok(value);
            }
        }
        return Ok(None);
    }
    pub fn delete(&mut self, key: &str) {
        self.memtable.delete(key.to_owned());
    }
}


#[cfg(test)]
mod tests {
    use crate::LSMEngine;

    #[test]
    fn it_works() -> Result<(), Box<dyn std::error::Error>> {
        let mut lsm = LSMEngine::new(2, false);
        lsm.write("k1".to_owned(), "v1".to_owned())?;
        lsm.write("k2".to_owned(), "v2".to_owned())?;
        lsm.write("k3".to_owned(), "v3".to_owned())?;

        for (k, v) in vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
            assert_eq!(lsm.read(k)?, Some(v.to_owned()));
        }

        Ok(())
    }
}