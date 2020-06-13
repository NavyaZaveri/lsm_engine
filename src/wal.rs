use std::fs::File;
use crate::kv::{KVFileWriter, KVFileIterator, KVFileReader};


pub struct Wal {
    pub file: File
}


impl KVFileIterator for Wal {
    fn file_as_mut(&mut self) -> &mut File {
        return &mut self.file;
    }
}

impl KVFileReader for Wal {}

impl KVFileWriter for Wal {}

impl Wal {
    pub fn new(f: File) -> Self {
        return Wal {
            file: f
        };
    }
}