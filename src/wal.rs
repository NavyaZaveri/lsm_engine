use std::fs::File;
use crate::kv::{KVPair, KVFileWriter, KVFileIterator, KVFileReader};
use std::io::{BufReader, BufRead, Write, SeekFrom, Seek};
use serde_json;
use std::convert::TryFrom;

#[macro_use]
use thiserror::Error;
use crate::kv;
use std::cell::RefCell;
use std::io;


pub struct Wal {
    pub file: File
}

pub type Result<T> = std::result::Result<T, WalError>;


#[derive(Error, Debug)]
pub enum WalError {
    #[error(transparent)]
    WriteError(#[from] kv::KvError),

    #[error("Unable to seek to the start of the file")]
    SeekError(#[from] io::Error),
}

impl KVFileIterator for Wal {
    fn file(&mut self) -> &mut File {
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