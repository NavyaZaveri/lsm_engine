use std::fs::{File, read};

use std::io::{Read, Write, BufReader, BufRead, SeekFrom, Error, Seek};
use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::time::SystemTime;
use std::collections::BinaryHeap;
use std::{fmt, io};
#[macro_use]
use thiserror::Error;


type Result<T> = std::result::Result<T, SST_Error>;

#[derive(Error, Debug)]
pub enum SST_Error {
    #[error("Attempted to write {} but previous entry is {}", current, previous)]
    UNSORTED_WRTE { previous: String, current: String },

    #[error(transparent)]
    Disconnect(#[from] io::Error),

    #[error(transparent)]
    JSON_PARSING(#[from] serde_json::error::Error),
}

pub struct Segment {
    fd: File,
    size: usize,
    previous_key: Option<String>,
}


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct KVPair {
    key: String,
    value: String,
}


impl Segment {
    pub fn new(path: &str) -> Segment {
        return Segment {
            fd: OpenOptions::new().read(true).write(true).create(true).open(path).unwrap(),
            size: 0,
            previous_key: None,
        };
    }

    pub fn temp() -> Segment {
        let temp = tempfile::tempfile().unwrap();
        return Segment::with_file(temp);
    }


    pub fn default() -> Segment {
        let now = SystemTime::now();
        let s = format!("{:?}", now);
        return Segment::new(&s);
    }


    pub fn with_file(f: File) -> Segment {
        return Segment {
            fd: f,
            size: 0,
            previous_key: None,
        };
    }


    pub fn write(&mut self, key: String, value: String) -> Result<u64> {
        if self.previous_key.as_ref().map_or(false, |prev| prev > &key) {
            return Err(SST_Error::UNSORTED_WRTE { previous: self.previous_key.as_ref().unwrap().to_string(), current: key.clone() });
        }
        self.previous_key = Some(key.clone());

        let kv = KVPair { key, value };

        let current_offset = self.tell()?;

        serde_json::to_writer(&self.fd, &kv)?;
        self.fd.write(b"\n")?;
        self.size += 1;
        return Ok(current_offset);
    }


    pub fn at(&self, pos: u64) -> Result<Option<String>> {
        let current = self.tell()?;
        self.seek(pos)?;
        let value = self.read().take(1).last().map(|kv| kv.value);
        self.seek(current)?;
        Ok(value)
    }

    fn seek(&self, pos: u64) -> Result<()> {
        RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Start(pos))?;
        Ok(())
    }


    fn peek(&self) -> Result<Option<KVPair>> {
        let current = self.tell()?;
        let maybe_entry = self.read().take(1).last();

        self.seek(current)?;
        Ok(maybe_entry)
    }


    pub fn search_from(&self, key: &str, offset: u64) -> Result<Option<String>> {
        let current_pos = self.tell()?;
        self.seek(offset)?;
        let maybe_value = self.
            read().
            find(|x| x.key.as_str() >= key).
            filter(|x| x.key == key).
            map(|kv| kv.value);


        self.seek(current_pos)?;
        return Ok(maybe_value);
    }


    pub fn search_from_start(&self, key: &str) -> Result<Option<String>> {
        return self.search_from(key, 0);
    }


    fn tell(&self) -> Result<u64> {
        let offset = RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Current(0))?;
        Ok(offset)
    }


    fn reset(&self) -> Result<()> {
        self.seek(0)?;
        Ok(())
    }


    pub fn read(&self) -> impl Iterator<Item=KVPair> + '_ {
        let reader = BufReader::new(&self.fd);
        return reader.lines().
            map(|string| serde_json::from_str::<KVPair>(&string.expect("the segment file should not be tampered with")).expect("something went deserializing the contents of the segment file"));
    }


    pub fn read_from_start(&self) -> Result<impl Iterator<Item=KVPair> + '_> {
        self.reset()?;
        return Ok(self.read());
    }
}


#[cfg(test)]
mod tests {
    use std::io::{Write, Seek, Read};
    use crate::sst::Segment;

    extern crate tempfile;


    #[test]
    fn test_search() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.write("k2".to_owned(), "v2".to_owned())?;
        assert_eq!(Some("v2".to_owned()), sst.search_from_start("k2")?);
        Ok(())
    }

    #[test]
    fn test_peek() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.reset()?;
        let x = sst.peek()?;
        let y = sst.peek()?;
        assert_eq!(x, y);
        Ok(())
    }


    #[test]
    fn test_seek() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        let first_offset = sst.write("k1".to_owned(), "v1".to_owned())?;
        let second_offset = sst.write("k2".to_owned(), "v2".to_owned())?;
        sst.write("k3".to_owned(), "v3".to_owned())?;


        sst.seek(first_offset)?;
        let first = sst.read().take(1).last();
        assert_eq!(Some("v1".to_owned()), first.map(|x| x.value));

        sst.seek(second_offset)?;
        let first = sst.read().take(1).last();
        assert_eq!(Some("v2".to_owned()), first.map(|x| x.value));

        Ok(())
    }

    #[test]
    fn test_read() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.write("k2".to_owned(), "v2".to_owned())?;
        let iterator = &mut sst.read_from_start()?;

        let first = iterator.next();
        assert_eq!(Some("v1".to_owned()), first.map(|kv| kv.value));

        let second = iterator.next();
        assert_eq!(Some("v2".to_owned()), second.map(|kv| kv.value));

        assert_eq!(None, iterator.next());

        Ok(())
    }

    #[test]
    fn test_interspersed_seek_and_search() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        let first_offset = sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.write("k2".to_owned(), "v2".to_owned())?;
        let value_v1 = sst.at(first_offset)?;
        let value = sst.search_from_start("k2")?;

        assert_eq!(value, Some("v2".to_owned()));
        assert_eq!(value_v1, Some("v1".to_owned()));

        sst.write("k3".to_owned(), "v3".to_owned())?;
        for k in vec!["k1", "k2", "k3"] {
            assert!(sst.search_from_start(k)?.is_some());
        }
        Ok(())
    }

    #[test]
    fn test_search_range() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        let offset_1 = sst.write("k1".to_owned(), "v1".to_owned())?;
        let offset_2 = sst.write("k2".to_owned(), "v2".to_owned())?;
        sst.write("k3".to_owned(), "v3".to_owned())?;

        for key in vec!["k2", "k3"] {
            assert!(sst.search_from(key, offset_2)?.is_some());
        }
        assert!(sst.search_from("k1", offset_2)?.is_none());
        Ok(())
    }

    #[test]
    fn test_unsorted_writes() {
        let mut sst = Segment::with_file(tempfile::tempfile().unwrap());
        sst.write("k2".to_owned(), "v2".to_owned()).unwrap();
        let result = sst.write("k1".to_owned(), "v1".to_owned());
        assert!(result.is_err())
    }
}
