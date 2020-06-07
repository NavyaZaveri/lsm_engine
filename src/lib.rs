use crate::memtable::{Memtable, ValueStatus};
use std::collections::{HashSet, HashMap};
use crate::sst::Segment;


mod memtable;
mod sst;


struct LSMEngine {
    memtable: Memtable<String, String>,
    inmemory_capacity: usize,
    segments: Vec<Segment>,
}


fn generate_segment_name() -> &'static str {
    unimplemented!()
}


impl LSMEngine {
    pub fn write(&mut self, key: String, value: String) {
        if self.memtable.at_capacity() {
            let mut new_segment = Segment::new("foo");
            self.memtable.drain().for_each(|(k, v)| {
                match v {
                    ValueStatus::Present(x) => {
                        new_segment.write(k, x);
                    }
                    ValueStatus::Tombstone => {
                        new_segment.write(k, "tombstone".to_string());
                    }
                }
            });
            self.segments.push(new_segment);
            self.memtable.insert(key, value);
        } else {
            self.memtable.insert(key, value);
        }
    }


    pub fn read(&mut self, key: &str) -> Option<&str> {
        if let Some(value_status) = self.memtable.get(key) {
            match value_status {
                ValueStatus::Present(value) => { return Some(value); }
                ValueStatus::Tombstone => { return None; }
            }
        }



        //go through all segments starting from the most recent one, and find the kv pair


        unimplemented!()
    }

    pub fn delete(&mut self, key: &str) {}
}

fn main() {}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Write, Seek, Read};
}
