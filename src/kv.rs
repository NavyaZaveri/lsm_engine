use serde::{Deserialize, Serialize};
#[macro_use]
use thiserror::Error;
use std::fs::File;
use serde::export::TryFrom;
use std::cell::{RefCell, Ref};
use std::io::{SeekFrom, Seek, BufReader, BufRead, Write};


pub(crate) type Result<T> = std::result::Result<T, KvError>;


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}


impl KVPair {
    pub fn persist_to_file(self, file: &mut File) -> Result<()> {
        serde_json::to_writer(file, &self)?;
        Ok(())
    }
}


impl TryFrom<String> for KVPair {
    type Error = KvError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        let kv_pair = serde_json::from_str::<KVPair>(&value)?;
        Ok(kv_pair)
    }
}


#[derive(Error, Debug)]
pub enum KvError {
    #[error(transparent)]
    JsonError(#[from] serde_json::error::Error),

    #[error(transparent)]
    FileIOError(#[from] std::io::Error),

}

pub trait KVFileIterator {
    fn file(&mut self) -> &mut File;
    fn seek(&mut self, pos: u64) -> Result<()> {
        self.file().seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn tell(&mut self) -> Result<u64> {
        let offset = self.file().seek(SeekFrom::Current(0))?;
        return Ok(offset);
    }
}

pub trait KVFileReader: KVFileIterator {
    fn read(&mut self) -> Box<dyn Iterator<Item=Result<KVPair>> + '_> {
        let reader = BufReader::new(self.file());

        return Box::new(reader.lines().map(|string| {
            KVPair::try_from(string.expect("the segment file should not be tampered with"))
        }));
    }

    fn read_from_start(&mut self) -> Result<Box<dyn Iterator<Item=Result<KVPair>> + '_>> {
        self.seek(0)?;
        let reader = BufReader::new(self.file());
        return Ok(Box::new(reader.lines().map(|string| {
            KVPair::try_from(string?)
        })));
    }
}

pub trait KVFileWriter: KVFileIterator {
    fn persist(&mut self, kv: KVPair) -> Result<u64> {
        let current_offset = self.tell()?;
        serde_json::to_writer(self.file(), &kv)?;
        self.file().write(b"\n")?;
        return Ok(20);
    }
}
