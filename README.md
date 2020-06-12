# lsm_engine
A rust implementation of a Log Structured Merge Tree (LSM-tree). 


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


