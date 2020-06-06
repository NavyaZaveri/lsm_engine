use std::fs::{File, read};
use std::path::Path;
use std::io::{Read, Write, BufReader, BufRead, SeekFrom, Error, Seek};
use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io;


pub struct Segment {
    fd: File,
    size: usize,
}


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct KVPair {
    key: String,
    value: String,
}


impl Segment {
    pub fn new(path: &'static str) -> Segment {
        return Segment {
            fd: OpenOptions::new().read(true).write(true).create(true).open(path).unwrap(),
            size: 0,
        };
    }

    pub fn with_file(f: File) -> Segment {
        return Segment {
            fd: f,
            size: 0,
        };
    }


    fn write(&mut self, key: String, value: String) -> Result<u64, Box<dyn std::error::Error>> {
        let kv = KVPair {
            key,
            value,
        };
        let current_offset = self.tell()?;

        serde_json::to_writer(&self.fd, &kv)?;
        self.fd.write(b"\n")?;
        self.size += 1;
        return Ok(current_offset);
    }


    fn seek(&self, pos: u64) -> Result<(), std::io::Error> {
        RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn peek(&self) -> Result<Option<KVPair>, Box<dyn std::error::Error>> {
        let current = self.tell()?;
        let maybe_entry = self.read(false)?.take(1).last();

        //reset back to current offset
        self.seek(current)?;
        Ok(maybe_entry)
    }


    fn search(&self, key: &str) -> Result<Option<String>, std::io::Error> {
        let current_pos = self.tell()?;
        let result = self.
            read(true).
            map(|mut stream| stream.find(|kv| &kv.key == key)).
            map(|kv| kv.map(|found| found.value));
        self.seek(current_pos)?;
        return result;
    }


    fn tell(&self) -> Result<u64, std::io::Error> {
        let offset = RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Current(0))?;
        Ok(offset)
    }


    fn reset(&self) -> Result<(), std::io::Error> {
        self.seek(0)?;
        Ok(())
    }


    fn read(&self, from_start: bool) -> Result<impl Iterator<Item=KVPair> + '_, std::io::Error> {
        if from_start {
            self.reset()?;
        }
        let reader = BufReader::new(&self.fd);
        return Ok(reader.lines().
            map(|x| serde_json::from_str::<KVPair>(&x.unwrap()).unwrap()));
    }
}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Write, Seek, Read, SeekFrom};
    use crate::sst::{KVPair, Segment};
    use std::fs;

    extern crate tempfile;


    #[test]
    fn test_search() {
        let mut sst = Segment::with_file(tempfile::tempfile().unwrap());
        assert_eq!(true, sst.write("foo".to_owned(), "bar".to_owned()).is_ok());
        assert_eq!(true, sst.write("hello".to_owned(), "world".to_owned()).is_ok());
        assert_eq!(Some("world".to_owned()), sst.search("hello").unwrap());
    }

    #[test]
    fn test_peek() {
        let mut sst = Segment::with_file(tempfile::tempfile().unwrap());
        sst.write("k1".to_owned(), "v1".to_owned()).unwrap();
        sst.reset().unwrap();
        let x = sst.peek();
        let y = sst.peek();
        assert_eq!(x.is_ok(), true);
        assert_eq!(y.is_ok(), true);
        assert_eq!(x.unwrap(), y.unwrap());
    }


    #[test]
    fn test_seek() {
        let mut sst = Segment::with_file(tempfile::tempfile().unwrap());
        let first_offset = sst.write("k1".to_owned(), "v1".to_owned()).unwrap();
        let second_offset = sst.write("k2".to_owned(), "v2".to_owned()).unwrap();
        sst.write("k3".to_owned(), "v3".to_owned());

        sst.seek(first_offset);
        let first = sst.read(false).unwrap().take(1).last();
        assert_eq!(Some("v1".to_owned()), first.map(|x| x.value));

        sst.seek(second_offset);
        let first = sst.read(false).unwrap().take(1).last();
        assert_eq!(Some("v2".to_owned()), first.map(|x| x.value));
    }

    #[test]
    fn test_read() {
        let mut sst = Segment::with_file(tempfile::tempfile().unwrap());
        sst.write("k1".to_owned(), "v1".to_owned());
        sst.write("k2".to_owned(), "v2".to_owned());
        let iterator = &mut sst.read(true).unwrap();

        let first = iterator.take(1).last();
        assert_eq!(first.unwrap().value, "v1".to_owned());

        let second = iterator.take(2).last();
        assert_eq!(second.unwrap().value, "v2".to_owned());
    }
}
