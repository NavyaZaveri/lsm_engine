# lsm_engine
A rust implementation of a [Log Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree#:~:text=In%20computer%20science%2C%20the%20log,%2C%20maintain%20key%2Dvalue%20pairs.). 


### Install 

```

```

### Example
```

let mut lsm = LSMBuilder::new().
    persist_data(false).
    segment_size(2).
    inmemory_capacity(1).
    sparse_offset(2).
    build();

lsm.write("k1".to_owned(), "v1".to_owned())?;
lsm.write("k2".to_owned(), "k2".to_owned())?;
lsm.write("k1".to_owned(), "v_1_1".to_owned())?;
lsm.write("k3".to_owned(), "v3".to_owned())?;

let value = lsm.read("k1")?;
assert_eq!(value, Some("v_1_1".to_owned()));

```


