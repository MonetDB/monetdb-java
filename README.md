## MonetDB-java

This repository contains the java source code of the official `MonetDB JDBC driver`,
the `MonetDB JdbcClient program`, and some Java test and example programs.

The master repository is: [hg monetdb-java](https://www.monetdb.org/hg/monetdb-java/file/tip).

A read-only copy is available on: [github monetdb-java](https://github.com/MonetDB/monetdb-java).


These Java programs are designed to work only with the [MonetDB Database System](https://www.monetdb.org/).
They support MonetDB servers from version Jul2015 (11.21) and higher.
However only the latest MonetDB server versions are tested actively.

The `MonetDB JDBC driver` allows Java programs to connect to a MonetDB
database server using standard, database independent Java code.  
It is an open source JDBC driver implementing JDBC API 4.2, written in Pure Java (Type 4),
and communicates in the MonetDB native network protocol.

The sources are actively maintained by the MonetDB team at [MonetDB Solutions](https://www.monetdbsolutions.com/).

The latest released jar files can be downloaded from [MonetDB Java Download Area](https://www.monetdb.org/downloads/Java/).

## Tools
To build the jar files yourself, you need `JDK 8` (or higher), `ant` and `make` tools installed.

## Build Process
To build, simply run `make` command from a shell.  
See contents of `Makefile` for other `make` targets, such as `make test` or `make doc`.

The `.class` files will be stored in the `build/` and `tests/build/` subdirectories.  
The `.jar` files will be stored in the `jars/` subdirectory.

By default debug symbols are **not** included in the compiled class and jar files.
To include debug symbols, edit file `build.properties` change line `enable_debug=true` and rebuild.

## Tests
**Note** For the tests to succeed you first have to startup a MonetDB server (on localhost, port 50000).

To test, simply run `make test` command from a shell.

The `JdbcClient CLI program` (jdbcclient.jre8.jar) includes and uses the MonetDB JDBC driver,
so it can also be used to test connectivity and sending SQL statements to a MonetDB server.

The `JdbcClient CLI program` can be started via shell commands:
```
cd jars

java -jar jdbcclient.jre8.jar
```

To get a list of JdbcClient startup options simply run:
```
java -jar jdbcclient.jre8.jar --help
```

See [JdbcClient doc](https://www.monetdb.org/documentation/user-guide/client-interfaces/jdbcclient/) for more info.

## JDBC Driver
The JDBC driver consists of one single jar file: `monetdb-jdbc-##.#.jre8.jar`.

We recommend to always use the latest released jar file.
The latest released JDBC driver can be downloaded from [MonetDB Java Download Area](https://www.monetdb.org/downloads/Java/).

See [JDBC driver info](https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/jdbc-driver/) for more info.

## Reporting issues
Before reporting an issue, please check if you have used the latest released jar files.
Some issues may already have been fixed in the latest released jar files.

If you find a bug in the latest released jar files or have a request, please log it as an issue at:
[github monetdb-java issues](https://github.com/MonetDB/monetdb-java/issues).

Include which versions of the released JDBC driver and MonetDB server you are using and on which platforms.  
For bugs also include a small standalone java reproduction program.

**Note** we do not accept Pull requests on Github.

## Copyright Notice
SPDX-License-Identifier: MPL-2.0

This Source Code Form is subject to the terms of the Mozilla Public  
License, v. 2.0.  If a copy of the MPL was not distributed with this  
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2024 MonetDB Foundation;  
Copyright August 2008 - 2023 MonetDB B.V.;  
Copyright 1997 - July 2008 CWI.

