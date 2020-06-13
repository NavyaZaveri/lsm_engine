# lsm_engine
A rust implementation of a key-value store that uses  [Log Structured Merge Trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree#:~:text=In%20computer%20science%2C%20the%20log,%2C%20maintain%20key%2Dvalue%20pairs.)
and leverages a [Write-Ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL) for data recovery.

 [![Docs.rs][crate_img]][crate_link]
 [![Docs.rs][doc_img]][doc_link]



[crate_link]: https://crates.io/crates/lsm_engine
[crate_img]: https://img.shields.io/badge/crates.io-lsm__engine-blue
[doc_link]: https://docs.rs/lsm_engine
[doc_img]: https://img.shields.io/badge/docs.rs-lsm__engine-red


### Install 

```
[dependencies]
lsm_engine = "*"
```

### Docs 
https://docs.rs/lsm_engine/0.1.1/lsm_engine/
