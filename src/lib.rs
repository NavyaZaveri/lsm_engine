use crate::memtable::{Memtable, ValueStatus};
use std::collections::{HashSet, HashMap};
use crate::sst::Segment;

#[macro_use]
extern crate derive_builder;


mod memtable;
mod sst;


pub struct LSMEngine {
    memtable: Memtable<String, String>,
    segments: Vec<Segment>,
    persist_data: bool,
}


impl LSMEngine {
    fn new(inmemory_capacity: usize, persist_data: bool) -> Self {
        return LSMEngine {
            memtable: Memtable::<String, String>::new(inmemory_capacity),
            segments: Vec::new(),
            persist_data,
        };
    }
    pub fn write(&mut self, key: String, value: String) -> Result<(), Box<dyn std::error::Error>> {
        if self.memtable.at_capacity() {
            let mut new_segment = if self.persist_data { Segment::default() } else { Segment::temp() };
            for (k, value_status) in self.memtable.drain() {
                match value_status {
                    ValueStatus::Present(value) => {
                        new_segment.write(k, value)?;
                    }
                    ValueStatus::Tombstone => {
                        new_segment.write(k, "tombstone".to_string())?;
                    }
                }
            };
            self.segments.push(new_segment);
            self.memtable.insert(key, value);
        } else {
            self.memtable.insert(key, value);
        }
        Ok(())
    }


    pub fn read(&mut self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if let Some(value_status) = self.memtable.get(key) {
            return match value_status {
                ValueStatus::Present(value) => { Ok(Some(value.to_owned())) }
                ValueStatus::Tombstone => { Ok(None) }
            };
        }


        for seg in self.segments.iter().rev() {
            let value = seg.search_from_start(key)?;
            if value.is_some() && value.as_ref().unwrap() != "tombstone" {
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
    fn it_works() {}
}