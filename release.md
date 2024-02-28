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


JDBC COMPLIANCE
---------------

The MonetDB JDBC driver is a type 4 driver (100% pure Java) and
complies to the JDBC 4.2 definition, see
 http://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/index.html
and
 https://en.wikipedia.org/wiki/Java_Database_Connectivity

Within the current implementation not all functionalities of the JDBC
interface are available.  It is believed however, that this
implementation is rich enough to be suitable for a majority of
application settings.

Below a list of (un)supported features can be found.
Please read this list if you intend to use this JDBC driver.

If you feel some features are missing or have encountered an issue/bug,
please let us know at our bugtracker:
  https://github.com/MonetDB/monetdb-java/issues

Currently implemented JDBC 4.2 interfaces include:
  * java.sql.Driver
    The following method is NOT useable/supported:
    - getParentLogger

  * java.sql.Connection
    The following features/methods are NOT useable/supported:
    - createArrayOf, createNClob, createStruct, createSQLXML
    - prepareStatement with array of column indices or column names
    - setHoldability (close/hold cursors over commit is not configurable)

    NOTE: be sure to check for warnings after setting concurrencies or
          isolation levels; MonetDB currently does not support anything
          else but "fully serializable" transactions.

  * java.sql.DatabaseMetaData
    NOTE: the column SPECIFIC_NAME as returned by getProcedures,
    getProcedureColumns, getFunctions and getFunctionColumns contains
    the internal id of the procedure or function. Use it for overloaded
    procedure and function names to match the proper columns info as
    returned by getProcedureColumns or getFunctionColumns to a specifc
    procedure or function name as returned by getProcedures or getFunctions.
    For example, getProcedures(null, "sys", "analyze") will return 4 rows
    as there exists 4 overloaded system procedures called analyze, with
    different (from 0 to 3) parameters. When calling
    getProcedureColumns(null, "sys", "analyze", "%") you will get all the
    6 (0+1+2+3) parameters of the 4 system procedures combined. So you will
    need to use the value of column SPECIFIC_NAME to properly match the right
    parameters to a specific procedure.

  * java.sql.Statement
    The following methods/options are NOT useable/supported:
    - cancel (query execution cannot be terminated, once started)
       see also: https://github.com/MonetDB/monetdb-java/issues/7
       or https://www.monetdb.org/bugzilla/show_bug.cgi?id=6222
    - execute with column indices or names
    - executeUpdate with column indices or names
    - setMaxFieldSize
    - setCursorName
    The following methods will add an SQLWarning:
    - setEscapeProcessing(true)  for Sep2022 (11.45) and older servers
    - setEscapeProcessing(false) for Jun2023 (11.47) and newer servers

  * java.sql.PreparedStatement
    The following methods are NOT useable/supported:
    - setArray
    - setAsciiStream
    - setBinaryStream
    - setBlob
    - setNClob
    - setRef
    - setRowId
    - setSQLXML
    - setUnicodeStream (note: this method is Deprecated)

  * java.sql.ParameterMetaData

  * java.sql.CallableStatement
    The following methods are NOT useable/supported:
    - all getXyz(parameterIndex/parameterName, ...) methods because
      output parameters in stored procedures are not supported by MonetDB
    - all registerOutParameter(parameterIndex/parameterName, int sqlType, ...) methods
      because output parameters in stored procedures are not supported by MonetDB
    - wasNull() method because output parameters in stored procedures are
      not supported by MonetDB
    - setArray
    - setAsciiStream
    - setBinaryStream
    - setBlob
    - setNClob
    - setRef
    - setRowId
    - setSQLXML
    - setUnicodeStream (note: this method is Deprecated)

  * java.sql.ResultSet
    The following methods are NOT useable/supported:
    - getArray
    - getAsciiStream, getUnicodeStream
    - getNClob
    - getRef, getRowId, getSQLXML
    - moveToCurrentRow, moveToInsertRow,
    - All methods related to updateable result sets such as:
      updateArray ... updateTimestamp, cancelRowUpdates,
      deleteRow, insertRow, refreshRow

  * java.sql.ResultSetMetaData

  * java.sql.SavePoint

  * java.sql.Wrapper

  * java.sql.Blob
    A simple implementation using a byte[] to store the whole BLOB.
    The following method is NOT useable/supported:
    - setBinaryStream

  * java.sql.Clob
    A simple implementation using a StringBuilder to store the whole CLOB.
    The following methods are NOT useable/supported:
    - setAsciiStream
    - setCharacterStream

  * java.sql.SQLData
    implemented by class: org.monetdb.jdbc.types.INET
            and by class: org.monetdb.jdbc.types.URL

  * javax.sql.DataSource (not tested)
    The following method is NOT useable/supported:
    - getParentLogger


The following java.sql.* interfaces are NOT implemented:
  * java.sql.Array
  * java.sql.DriverAction
  * java.sql.NClob
  * java.sql.Ref
  * java.sql.Rowid
  * java.sql.SQLInput
  * java.sql.SQLOutput
  * java.sql.SQLType
  * java.sql.SQLXML
  * java.sql.Struct


ON CLIENT support
-----------------

Since release 3.2 (monetdb-jdbc-3.2.jre8.jar), the MonetDB JDBC driver has
support for the ON CLIENT clause of the COPY statement. To use
this functionality you must register handlers for upload and download of data.
The MonetConnection class has been extended with 2 methods:

* public void setUploadHandler(UploadHandler uploadHandler)
* public void setDownloadHandler(DownloadHandler downloadHandler)

The API has been extended with some further interfaces and utility classes:
* public interface org.monetdb.jdbc.MonetConnection.UploadHandler
* public interface org.monetdb.jdbc.MonetConnection.DownloadHandler
* public static class org.monetdb.jdbc.MonetConnection.Upload
* public static class org.monetdb.jdbc.MonetConnection.Download
* public class org.monetdb.util.FileTransferHandler
  which implements the UploadHandler and DownloadHandler interfaces.

See file  onclient.txt  for more information on how to use these from Java.

The JdbcClient application has also been extended to support COPY ...
ON CLIENT functionality. However for security reasons you must provide an
explicit new startup argument
  --csvdir "/path/to/csvdatafiles"
or on MS Windows
  --csvdir "C:\\path\\to\\csvdatafiles"
in order to allow the JdbcClient to access local files.


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

