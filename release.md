RELEASE NOTES
=============

MonetDB JDBC driver version 3.3 (Liberica)
<br>
Release date: 2023-02-23

The Java Database Connectivity (JDBC) API provides universal data access from
the Java programming language.

The MonetDB JDBC driver is designed for use with MonetDB, an Open-Source column-store RDBMS.
For more information see https://www.monetdb.org/

The latest MonetDB JDBC driver can be downloaded from
https://www.monetdb.org/downloads/Java/

The sources for this JDBC driver and related Java programs can be found at:
https://dev.monetdb.org/hg/monetdb-java/file/tip


URL Format
----------

The MonetDB JDBC connection URL format is:

<pre>
jdbc:<b>monetdb</b>://[&lt;host>[:&lt;port>]]/&lt;databasename>[?&lt;properties>]  (not encrypted)
OR
jdbc:<b>monetdbs</b>://[&lt;host>[:&lt;port>]]/&lt;databasename>[?&lt;properties>] (encrypted)

where &lt;properties> are &-separated: prop1=value1[&prop2=value2[&prop3=value3...]]
</pre>

The second form (monetdbs) is for creating a TLS-protected connection. TLS (Transport Layer Security)
is the mechanism that is also used for HTTPS.

Property keys and values support percent-escaped bytes. For example, the
password 'chocolate&cookies' can be passed as follows: `jdbc:monetdb:///demo?user=me&password=chocolate%26cookies`.

Note: MonetDB-Java version 3.3 and earlier did not support percent-escapes.
If your password contains percent-characters, these must now be encoded as `%25`.

Supported connection properties are:

| Property                                  | Default | Notes                                               |
| ----------------------------------------- | ------- | --------------------------------------------------- |
| user=&lt;login name>                      | -       | required                                            |
| password=&lt;secret value>                | -       | required                                            |
| so_timeout=&lt;time in milliseconds>      | -       |                                                     |
| treat_clob_as_varchar=&lt;bool>           | true    |                                                     |
| treat_blob_as_binary=&lt;bool>            | true    |                                                     |
| language=&lt;sql or mal>                  | sql     |                                                     |
| replysize=&lt;nr of rows>                 | 250     | -1 means fetch everything at once                   |
| autocommit=&lt;bool>                      | true    |                                                     |
| schema=&lt;schema name>                   | -       | initial schema to select                            |
| timezone=&lt;minutes east of UTC>         | system  |                                                     |
| debug=true                                | false   |                                                     |
| logfile=&lt;name of logfile>              |         |                                                     |
| hash=&lt;SHA512, SHA384, SHA256 or SHA1>  |         |                                                     |
| cert=&lt;path to certificate>             | -       | TLS certificate must be in PEM format               |
| certhash=sha256:&lt;hexdigits and colons> | -       | required hash of server TLS certificate in DER form |


Booleans &lt;bool> can be written as 'true', 'false', 'yes', 'no', 'on' and 'off'. Property 'fetchsize' is accepted as an alias of 'replysize'.

Client authentication (Mutual TLS, or MTLS) is not yet supported.

When the properties 'treat_clob_as_varchar' and 'treat_blob_as_binary' are enabled,

The properties 'treat_clob_as_varchar' and 'treat_blob_as_binary' control which
type is returned by ResultSetMetaData.getColumnType(int) for CLOB and BLOB columns.
When 'treat_clob_as_varchar' is enabled, Types.VARCHAR is returned instead of Types.CLOB
for CLOB columns. When 'treat_blob_as_binary' is enabled, Types.VARBINARY is returned instead
of Types.BLOB for BLOB columns.
This will cause generic JDBC applications such as SQuirrel SQL and DBeaver to use
the more efficient '#getString()' and '#getBytes()' API rather than '#getClob()' and
'#getClob()'.
These properties are enabled by default since MonetDB-Java version 3.0.


Use
---

See also: https://www.monetdb.org/Documentation/SQLreference/Programming/JDBC

The MonetDB JDBC driver class name is `org.monetdb.jdbc.MonetDriver`.
This has been changed as of release 3.0 (monetdb-jdbc-3.0.jre8.jar).
The old driver class (nl.cwi.monetdb.jdbc.MonetDriver) has been deprecated
since 12 Nov 2020 and has been removed in release 3.4 (monetdb-jdbc-3.4.jre8.jar).


Notes and tips
--------------

- After creating a Connection object check for SQLWarnings via conn.getWarnings();

- Close JDBC ResultSet, Statement, PreparedStatement, CallableStatement and
  Connection objects immediately (via close()) when they are no longer needed,
  in order to release resources and memory on the server and client side.
  Especially ResultSets can occupy large amounts of memory on the server and
  client side.

- By default the ResultSets created by methods in DatabaseMetaData
  which return a ResultSet (such as dbmd.getColumns(...)) are
  TYPE_SCROLL_INSENSITIVE, so they cache their ResultSet data to
  allow absolute, relative and random access to data rows and fields.
  To free heap memory and server resources, close those ResultSets
  immediately when no longer needed.

- By default the ResultSets created by stmt.executeQuery(...) or
  stmt.execute(...) are TYPE_FORWARD_ONLY, to reduce the potentially large
  amount of client memory needed to cache the whole ResultSet data.

- When you need to execute many SQL queries sequentially reuse the Statement
  object instead of creating a new Statement for each single SQL query.
  Alternatively you can execute the SQL queries as one script (each SQL query
  must be separated by a ; character) string via stmt.execute(script),
  stmt.getResultSet() and stmt.getMoreResults().
  Or you can use the batch execution functionality, see stmt.addBatch() and
  stmt.executeBatch() methods.

- The fastest way to retrieve data from a MonetDB ResultSet is via the
  getString(int columnIndex) method, because internally all data
  values (of all types) are stored as Strings, so no conversions are needed.

- Avoid using rs.getObject() as it will need to construct a new Object for
  each value, even for primitive types such as int, long, boolean.

- Avoid using rs.getClob(). Instead use getString() for all CLOB
  columns, which is much faster and uses much (3 times) less memory.

- Avoid using rs.getBlob(). Instead use getBytes() to get a byte array
  or use getString() to get a string containing hex pairs, for all BLOB
  columns. These methods are much faster and use much less memory.
  The getString() is the fastest way as no conversions are done at all.
  The getBytes() will need to convert the hex char string into a new bytes[].

- Try to avoid calling "rs.get...(String columnLabel)" methods inside the
   while(rs.next()) {...} loop. Instead resolve the columnLabels to column
  numbers before the loop via method "int findColumn(String columnLabel)"
  and use the int variables with the rs.get...(int columnIndex) methods.
  This eliminates the call to findColumn(String columnLabel) for
  each value of every column for every row in the ResultSet.
  See also the example Java JDBC program on:
  https://www.monetdb.org/Documentation/SQLreference/Programming/JDBC

**WARNING**:
 The current implementation of the MonetDB JDBC driver is *NOT*
 multi-thread safe. If your program uses multiple threads concurrently on
 the same Connection (so one MapiSocket), this may lead to incorrect behavior
 and results (due to race conditions).
 You will need to serialize the processing of the threads in your Java program.
 Alternatively you can use a separate JDBC Connection for each thread.

Note: as of version 3.0 (monetdb-jdbc-3.0.jre8.jar) we compile all
 the java sources to target: Java SE 8 (profile compact2), so
 you need a JRE/JDK JVM of version 8 or higher to use it.

