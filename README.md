# MonetDB-java

<!-- this repo -->
This repository contains the Java source code of the official **MonetDB JDBC driver**
and some test- and example programs.

<!-- the driver -->
The **MonetDB JDBC driver** allows Java programs to connect to a [MonetDB](https://www.monetdb.org/)
database server using standard, database independent Java code.
It is an open source JDBC driver implementing JDBC API 4.2, written in Pure Java (Type 4),
and communicates using the native MonetDB network protocol.
It support Java version 8 and higher and MonetDB 
version 11.45 (Sep2022) and higher.

<!-- Sep2022 is the highest version currently tested in GitHub CI.
     We could probably go back further if necessary, as long as suitable Docker
     images are available. Really old Docker images are not compatible with our
     test suite. -->

<!-- more on the driver -->
The driver jar file can also be run from the command line 
as a standalone interactive SQL client.
For example, `java -jar monetdb-jdbc-12.3.jre8.jar`. Pass the `--help` flag
or see the [JdbcClient documentation](https://www.monetdb.org/documentation/user-guide/client-interfaces/jdbcclient/) for more information.

<!-- who maintains it and where -->
The sources are actively maintained by the MonetDB team at [MonetDB Solutions](https://www.monetdbsolutions.com/).
The master repository is [our Mercurial repository](https://www.monetdb.org/hg/monetdb-java/).
A read-only copy is available on [GitHub](https://github.com/MonetDB/monetdb-java).

<!-- where to get it -->
The latest released jar files can be downloaded from the [MonetDB Java Download Area](https://www.monetdb.org/downloads/Java/).
It is also available on Maven with coordinates `org.monetdb:monetdb-jdbc:X.Y` where `X.Y` is the version number.

<!-- where to read the docs -->
See [JDBC driver info](https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/jdbc-driver/) 
for more information on using the JDBC driver with MonetDB.


## Reporting issues

Before reporting an issue, please check if you are using the latest version.
Some issues may already have been fixed in the latest released jar files.

If you find a bug in the latest released jar files or have a request, please log it as an issue at:
[github monetdb-java issues](https://github.com/MonetDB/monetdb-java/issues).
Include which versions of the released JDBC driver and MonetDB server you are using and on which platforms.  
For bugs also include a small standalone java reproduction program.

**Note** we do not accept Pull requests on Github as it is a read-only copy.


## Copyright Notice

SPDX-License-Identifier: MPL-2.0

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this  
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2024 - present MonetDB Foundation;  
Copyright August 2008 - 2023 MonetDB B.V.;  
Copyright 1997 - July 2008 CWI.


## Build Process

<!-- copied from the documentation site, we should try to keep the two in sync  -->

To build the JDBC driver yourself, clone the
[Mercurial repository](https://www.monetdb.org/hg/monetdb-java/file/tip)
or its read-only [GitHub mirror](https://github.com/MonetDB/monetdb-java/).

The project is built using [Maven], which should be invoked using the provided
[maven wrappers] `./mvnw` and `.\mvnw.bat`. For convenience, the provided
Makefile offers *jar*, *alljars*, *test*, *testall* and *clean* targets that
wrap the appropriate Maven commands. The difference between *jar* and *alljars*
is that *jar* does not build documentation- and source jars. The difference
between *test* and *testall* is that *test* skips a few slow tests.

Running `make jar` leaves the file `monetdb-jdbc-X.Y-SNAPSHOT.jar` in the `target/` directory.
Running `make alljars` also creates the corresponding `-javadoc` and `-sources` jars,
plus `-tests` and `-test-jar-with-deps` jars which are used in our test environment.

Note that only development versions have the `-SNAPSHOT` suffix, in release
builds it is omitted.

For backward compatibility, differently named copies of these files are left in
the `jars/` directory: `monetdb-jdbc-X.Y.jre8.jar` and `jdbcclient.jre8.jar` are
both copies of` monetdb-jdbc-X.Y-SNAPSHOT.jar`, and `jdbctests.jar` is a copy of
`monetdb-jdbc-X.Y-SNAPSHOT-test-jar-with-deps.jar`.

The tests need access to a scratch MonetDB instance. By default they try to
connect to `jdbc:monetdb:///testjdbc` but this can be configured using the
environment variable `MONETDB_TEST_URL` or the System property `test.url`.
Instead of setting environment variables or system properties, you can also
create a Properties file `test.properties` in the directory where `make test` is
invoked. For example,

```
echo 'test.url=monetdb:///demo' >test.properties
```

There are various other things that can be configured, see
`src/test/java/org/monetdb/testinfra/Config.java` in the source code.

To run a single test, run for example `./mvnw test -Dtest='ApiTests#testAutocommit`.


[Maven]: https://maven.apache.org/
[maven wrappers]: https://maven.apache.org/tools/wrapper/




