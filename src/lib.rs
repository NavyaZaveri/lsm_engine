use crate::memtable::{Memtable, ValueStatus};
use std::collections::{HashSet, HashMap};


mod memtable;
mod sst;


struct LSMEngine {
    memtable: Memtable<String, String>,
    inmemory_capacity: usize,
}


impl LSMEngine {
    pub fn write(&mut self, key: String, value: String) {}
    pub fn read(&mut self, key: &str) -> Option<&str> {
        let status = self.memtable.get(key)?;
        match status {
            ValueStatus::Present(value) => { Some(value) }
            ValueStatus::Tombstone => { None }
        }
    }
    pub fn delete(&mut self, key: &str) {}
}

fn main() {}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Write, Seek, Read};
}
