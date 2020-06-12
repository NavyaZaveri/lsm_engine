#![allow(dead_code)]

use std::fs::File;

use binary_heap_plus::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::time::Instant;
use std::time::SystemTime;

use std::io;
#[macro_use]
use thiserror::Error;

use std::cmp::Ordering;
use std::iter::Peekable;


type Result<T> = std::result::Result<T, SstError>;

#[derive(Error, Debug)]
pub enum SstError {
    #[error("Attempted to write {} but previous key is {}", current, previous)]

    UnsortedWrite { previous: String, current: String },

    #[error(transparent)]
    Disconnect(#[from] io::Error),

    #[error(transparent)]
    JsonParsing(#[from] serde_json::error::Error),
}

pub struct Segment {
    fd: File,
    size: usize,
    previous_key: Option<String>,
    created_at: Instant,
}

struct MetaKey {
    key: String,
    value: String,
    timestamp: Instant,
    which_segment: usize,
}

impl Ord for MetaKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(self.timestamp.cmp(&other.timestamp).reverse())
    }
}

impl PartialEq for MetaKey {
    fn eq(&self, other: &Self) -> bool {
        return self.key == other.key && self.timestamp == other.timestamp;
    }
}

impl PartialOrd for MetaKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(self));
    }
}

impl Eq for MetaKey {}

struct SstMerger<I: Iterator<Item=KVPair>> {
    heap: BinaryHeap<MetaKey, MinComparator>,
    segment_iterators: Vec<Peekable<I>>,
    previous_key: Option<String>,
}

impl<I: Iterator<Item=KVPair>> SstMerger<I> {
    fn new(
        mut heap: BinaryHeap<MetaKey, MinComparator>,
        mut segment_iterators_with_timestamp: Vec<(Peekable<I>, Instant)>,
    ) -> Self {
        //initialize the heap
        for (index, (it, timestamp)) in segment_iterators_with_timestamp.iter_mut().enumerate() {
            if it.peek().is_some() {
                let kv = it.next().unwrap();
                let meta_key = MetaKey {
                    key: kv.key,
                    value: kv.value,
                    timestamp: *timestamp,
                    which_segment: index,
                };
                heap.push(meta_key);
            }
        }
        return Self {
            heap,
            segment_iterators: segment_iterators_with_timestamp.into_iter().map(|x| x.0).collect(),
            previous_key: None,
        };
    }
}

impl<I: Iterator<Item=KVPair>> Iterator for SstMerger<I> {
    type Item = KVPair;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.heap.is_empty() {
            let meta_key = self.heap.pop().unwrap();
            let segment_iterator = &mut self.segment_iterators[meta_key.which_segment];
            if Some(meta_key.key.clone()) == self.previous_key {
                continue;
            }
            self.previous_key = Some(meta_key.key.clone());
            if segment_iterator.peek().is_some() {
                let next = segment_iterator.next().unwrap();
                self.heap.push(MetaKey {
                    key: next.key,
                    value: next.value,
                    timestamp: meta_key.timestamp,
                    which_segment: meta_key.which_segment,
                });
                return Some(KVPair {
                    key: meta_key.key,
                    value: meta_key.value,
                });
            }

            return Some(KVPair {
                key: meta_key.key,
                value: meta_key.value,
            });
        }
        None
    }
}

pub fn merge<F: FnMut(usize, u64, String) -> ()>(
    segments: Vec<Segment>,
    segment_size: usize,
    persist: bool,
    mut callback_on_write: F,
) -> Result<Vec<Segment>> {
    let segment_timestamps = segments.iter().map(|s| s.created_at).collect::<Vec<_>>();

    let iterators = segments
        .iter()
        .map(|s| s.read_from_start())
        .map(|maybe_it| maybe_it.map(|it| it.peekable()))
        .collect::<Result<Vec<_>>>()?;

    let heap = BinaryHeap::<MetaKey, MinComparator>::new_min();
    let iterator_with_timestamp = iterators
        .into_iter()
        .zip(segment_timestamps)
        .collect::<Vec<_>>();

    let merger = SstMerger::new(heap, iterator_with_timestamp);
    let mut res = vec![];
    let mut segment = if persist { Segment::default() } else { Segment::temp() };
    let mut segment_count: usize = 0;

    for kv in merger.into_iter() {
        if segment.size() == segment_size {
            res.push(segment);
            segment = if persist { Segment::default() } else { Segment::temp() };
            segment_count += 1;
        }
        let cloned_key = kv.key.clone();
        let offset = segment.write(kv.key, kv.value)?;
        callback_on_write(segment_count, offset, cloned_key);
    }
    if segment.size() > 0 {
        res.push(segment);
    }
    Ok(res)
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

impl Segment {
    pub fn new(path: &str) -> Segment {
        return Segment {
            fd: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap(),
            size: 0,
            previous_key: None,
            created_at: Instant::now(),
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

    pub fn timestamp(&self) -> Instant {
        return self.created_at;
    }

    pub fn with_file(f: File) -> Segment {
        return Segment {
            fd: f,
            size: 0,
            previous_key: None,
            created_at: Instant::now(),
        };
    }

    fn validate(&self, key: &str) -> Result<()> {
        if self
            .previous_key
            .as_ref()
            .map_or(false, |prev| prev.as_str() > key)
        {
            return Err(SstError::UnsortedWrite {
                previous: self.previous_key.as_ref().unwrap().to_string(),
                current: key.to_owned(),
            });
        }
        Ok(())
    }

    pub fn write(&mut self, key: String, value: String) -> Result<u64> {
        //check if the previously written key is bigger than the current key
        self.validate(&key)?;

        self.previous_key = Some(key.clone());

        let kv = KVPair { key, value };

        let current_offset = self.tell()?;

        serde_json::to_writer(&self.fd, &kv)?;
        self.fd.write(b"\n")?;
        self.size += 1;
        return Ok(current_offset);
    }

    fn size(&self) -> usize {
        return self.size;
    }

    pub fn at(&self, pos: u64) -> Result<Option<String>> {
        let current = self.tell()?;
        self.seek(pos)?;
        let value = self.read().take(1).last().map(|kv| kv.value);
        self.seek(current)?;
        Ok(value)
    }


    fn seek(&self, pos: u64) -> Result<()> {
        RefCell::new(&self.fd)
            .borrow_mut()
            .seek(SeekFrom::Start(pos))?;
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
        let maybe_value = self
            .read()
            .find(|x| x.key.as_str() >= key)
            .filter(|x| x.key == key)
            .map(|kv| kv.value);

        self.seek(current_pos)?;
        return Ok(maybe_value);
    }

    pub fn search_from_start(&self, key: &str) -> Result<Option<String>> {
        return self.search_from(key, 0);
    }

    pub(crate) fn tell(&self) -> Result<u64> {
        let offset = RefCell::new(&self.fd)
            .borrow_mut()
            .seek(SeekFrom::Current(0))?;
        Ok(offset)
    }

    fn reset(&self) -> Result<()> {
        self.seek(0)?;
        Ok(())
    }

    pub fn read(&self) -> impl Iterator<Item=KVPair> + '_ {
        let reader = BufReader::new(&self.fd);
        return reader.lines().map(|string| {
            serde_json::from_str::<KVPair>(
                &string.expect("the segment file should not be tampered with"),
            )
                .expect("something went wrong deserializing the contents of the segment file")
        });
    }
    pub fn read_from_start(&self) -> Result<impl Iterator<Item=KVPair> + '_> {
        self.reset()?;
        return Ok(self.read());
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::{merge, Segment};

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
        assert!(result.is_err());
    }

    #[test]
    fn test_merges() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst_1 = Segment::temp();
        sst_1.write("k1".to_owned(), "v1".to_owned())?;
        let mut sst_2 = Segment::temp();
        sst_2.write("k2".to_owned(), "v2".to_owned())?;
        let v = vec![sst_1, sst_2];
        let mut merged = merge(v, 20, false, |index, offset, _| {})?;
        assert_eq!(merged.len(), 1);
        let segment = merged.pop().unwrap();
        let pairs: Vec<_> = segment
            .read_from_start()?
            .map(|kv| (kv.key, kv.value))
            .collect();

        assert_eq!(
            pairs,
            vec![
                ("k1".to_owned(), "v1".to_owned()),
                ("k2".to_owned(), "v2".to_owned())
            ]
        );

        Ok(())
    }

    #[test]
    fn test_merge_with_same_keys_different_timestamps() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst_1 = Segment::temp();
        let mut sst_2 = Segment::temp();
        sst_1.write("k1".to_owned(), "v1".to_owned())?;
        sst_2.write("k1".to_owned(), "v2".to_owned())?;
        let v = vec![sst_1, sst_2];
        let merged = merge(v, 100, false, |index, offset, _| {})?;
        let expected = vec![("k1".to_owned(), "v2".to_owned())];
        let actual: Vec<_> = merged[0].read_from_start()?.map(|kv| (kv.key, kv.value)).collect();
        assert_eq!(expected, actual);
        Ok(())
    }
}
