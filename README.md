# hdfesse

Rust async HDFS client library with libhdfs.so and hdfs util drop-in
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
