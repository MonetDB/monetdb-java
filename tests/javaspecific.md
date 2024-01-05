# Java-specific tests

Test settings that are only in monetdb-java.

```test
ONLY jdbc
EXPECT so_timeout=0
SET so_timeout=42
EXPECT so_timeout=42
ACCEPT monetdb://?so_timeout=99
EXPECT so_timeout=99
```

```test
ONLY jdbc
EXPECT treat_clob_as_varchar=true
SET treat_clob_as_varchar=off
EXPECT treat_clob_as_varchar=false
ACCEPT monetdb://?treat_clob_as_varchar=yes
EXPECT treat_clob_as_varchar=on
```

```test
ONLY jdbc
EXPECT treat_blob_as_binary=true
SET treat_blob_as_binary=off
EXPECT treat_blob_as_binary=false
ACCEPT monetdb://?treat_blob_as_binary=yes
EXPECT treat_blob_as_binary=on
```
