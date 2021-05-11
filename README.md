# hdfesse

Rust ~~async~~ HDFS client library with libhdfs.so and hdfs util drop-in
replacements.

This project is in its early stage and is not yet functional.

## Incompatibilities

  + hdfesse uses system timezone database that is usually updated; the
    JRE uses bundled timezone database that becomes obsolete if you do
    not update the JRE.  Thus ls output may have different date/time
    fields.
  + hdfesse: for files with same atime/mtime, order for time sort may
    produce different results.  hdfesse uses stable sort, Java version
    uses quicksort without any stability and reproducibility
    guarantee.
  + Rust strings consider surrogate chars as invalid, so it your paths
    contain them, hdfesse/libhdfesse/libhfs will immediately complain.

## Features

  + `serde_support` for `libhdfesse`: Serde serialize/deserialize for HA config
    structs.  So, you may load the Config from Hadoop configs, or deserialize
    it from your own data.
