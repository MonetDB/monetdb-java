package nl.cwi.monetdb.jdbc;

import nl.cwi.monetdb.jdbc.types.INET;
import nl.cwi.monetdb.jdbc.types.URL;
import nl.cwi.monetdb.mcl.connection.MCLException;
import nl.cwi.monetdb.mcl.connection.Debugger;
import nl.cwi.monetdb.mcl.connection.MonetDBLanguage;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.responses.ResponseList;
import nl.cwi.monetdb.responses.SendThread;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * A {@link Connection} suitable for the MonetDB database.
 *
 * This connection represents a connection (session) to a MonetDB
 * database. SQL statements are executed and results are returned within
 * the context of a connection. This Connection object holds a physical
 * connection to the MonetDB database.
 *
 * A Connection object's database should able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the getMetaData method.
 *
 * Note: By default a Connection object is in auto-commit mode, which
 * means that it automatically commits changes after executing each
 * statement. If auto-commit mode has been disabled, the method commit
 * must be called explicitly in order to commit changes; otherwise,
 * database changes will not be saved.
 *
 * The current state of this connection is that it nearly implements the
 * whole Connection interface.
 *
 * @author Fabian Groffen
 * @version 1.2
 */
public abstract class MonetConnection extends MonetWrapper implements Connection {

    /** the successful processed input properties */
    private final Properties conn_props;

    /** The language to connect with */
    protected MonetDBLanguage currentMonetDBLanguage = MonetDBLanguage.LANG_SQL;
    /** The database to connect to */
    protected String database;
    /** Authentication hash method */
    protected final String hash;
    /** An optional thread that is used for sending large queries */
    private SendThread sendThread;
    /** Whether this Connection is closed (and cannot be used anymore) */
    private boolean closed;
    /** Whether this Connection is in autocommit mode */
    private boolean autoCommit = true;
    /** The stack of warnings for this Connection object */
    private SQLWarning warnings;
    /** The Connection specific mapping of user defined types to Java types */
    private Map<String,Class<?>> typeMap = new HashMap<String,Class<?>>() {
        private static final long serialVersionUID = 1L; {
            put("inet", INET.class);
            put("url",  URL.class);
        }
    };

    // See javadoc for documentation about WeakHashMap if you don't know what
    // it does !!!NOW!!! (only when you deal with it of course)
    /** A Map containing all (active) Statements created from this Connection */
    private Map<Statement,?> statements = new WeakHashMap<Statement, Object>();

    /** The number of results we receive from the server at once */
    private int curReplySize = -1; // the server by default uses -1 (all)

    /** Whether or not BLOB is mapped to BINARY within the driver */
    private final boolean blobIsBinary;

    protected boolean isDebugging;

    protected Debugger ourSavior;

    /**
     * Constructor of a Connection for MonetDB. At this moment the
     * current implementation limits itself to storing the given host,
     * database, username and password for later use by the
     * createStatement() call.  This constructor is only accessible to
     * classes from the jdbc package.
     *
     * @throws IOException if an error occurs
     */
    public MonetConnection(Properties props, String database, String hash, String language, boolean blobIsBinary, boolean isDebugging) throws IOException {
        this.conn_props = props;
        this.database = database;
        this.hash = hash;
        this.currentMonetDBLanguage = MonetDBLanguage.GetLanguageFromString(language);
        this.blobIsBinary = blobIsBinary;
        this.isDebugging = isDebugging;
    }

    public MonetDBLanguage getCurrentMonetDBLanguage() {
        return currentMonetDBLanguage;
    }

    public void setCurrentMonetDBLanguage(MonetDBLanguage currentMonetDBLanguage) {
        this.currentMonetDBLanguage = currentMonetDBLanguage;
    }

    public void setDebugging(String filename) throws IOException {
        ourSavior = new Debugger(filename);
    }

    /**
     * Connects to the given host and port, logging in as the given
     * user.  If followRedirect is false, a RedirectionException is
     * thrown when a redirect is encountered.
     *
     * @return A List with informational (warning) messages. If this
     * 		list is empty; then there are no warnings.
     * @throws IOException if an I/O error occurs when creating the
     *         socket
     * @throws MCLParseException if bogus data is received
     * @throws MCLException if an MCL related error occurs
     */
    public abstract List<String> connect(String user, String pass) throws IOException, MCLParseException, MCLException;

    public abstract int getBlockSize();

    public abstract int getSoTimeout() throws SocketException;

    public abstract void setSoTimeout(int s) throws SocketException;

    public abstract void closeUnderlyingConnection() throws IOException;

    public abstract String getJDBCURL();

    /**
     * Releases this Connection object's database and JDBC resources
     * immediately instead of waiting for them to be automatically
     * released. All Statements created from this Connection will be
     * closed when this method is called.
     *
     * Calling the method close on a Connection object that is already
     * closed is a no-op.
     */
    @Override
    public synchronized void close() {
        for (Statement st : statements.keySet()) {
            try {
                st.close();
            } catch (SQLException e) {
                // better luck next time!
            }
        }
        //close the debugger
        try {
            if (ourSavior != null) {
                ourSavior.close();
            }
        } catch (IOException e) {
            // ignore it
        }
        // close the socket or the embedded server
        try {
            this.closeUnderlyingConnection();
        } catch (IOException e) {
            // ignore it
        }
        // close active SendThread if any
        if (sendThread != null) {
            sendThread.shutdown();
            sendThread = null;
        }
        // report ourselves as closed
        closed = true;
    }

    /**
     * Destructor called by garbage collector before destroying this
     * object tries to disconnect the MonetDB connection if it has not
     * been disconnected already.
     */
    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    //== methods of interface Connection

    /**
     * Clears all warnings reported for this Connection object. After a
     * call to this method, the method getWarnings returns null until a
     * new warning is reported for this Connection object.
     */
    @Override
    public void clearWarnings() {
        warnings = null;
    }

    /**
     * Makes all changes made since the previous commit/rollback
     * permanent and releases any database locks currently held by this
     * Connection object.  This method should be used only when
     * auto-commit mode has been disabled.
     *
     * @throws SQLException if a database access error occurs or this
     *         Connection object is in auto-commit mode
     * @see #setAutoCommit(boolean)
     */
    @Override
    public void commit() throws SQLException {
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives

        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send commit to the server
        try {
            l.processQuery("COMMIT");
        } finally {
            l.close();
        }
    }

    /**
     * Factory method for creating Array objects.
     *
     * Note: When createArrayOf is used to create an array object that
     * maps to a primitive data type, then it is implementation-defined
     * whether the Array object is an array of that primitive data type
     * or an array of Object.
     *
     * Note: The JDBC driver is responsible for mapping the elements
     * Object array to the default JDBC SQL type defined in
     * java.sql.Types for the given class of Object. The default mapping
     * is specified in Appendix B of the JDBC specification. If the
     * resulting JDBC type is not the appropriate type for the given
     * typeName then it is implementation defined whether an
     * SQLException is thrown or the driver supports the resulting
     * conversion.
     *
     * @param typeName the SQL name of the type the elements of the
     *        array map to. The typeName is a database-specific name
     *        which may be the name of a built-in type, a user-defined
     *        type or a standard SQL type supported by this database.
     *        This is the value returned by Array.getBaseTypeName
     * @return an Array object whose elements map to the specified SQL
     *         type
     * @throws SQLException if a database error occurs, the JDBC type
     *         is not appropriate for the typeName and the conversion is
     *         not supported, the typeName is null or this method is
     *         called on a closed connection
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support this data type
     *
     * @since 1.6
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf(String, Object[]) not supported", "0A000");
    }

    /**
     * Creates a Statement object for sending SQL statements to the
     * database.  SQL statements without parameters are normally
     * executed using Statement objects. If the same SQL statement is
     * executed many times, it may be more efficient to use a
     * PreparedStatement object.
     *
     * Result sets created using the returned Statement object will by
     * default be type TYPE_FORWARD_ONLY and have a concurrency level of
     * CONCUR_READ_ONLY.
     *
     * @return a new default Statement object
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a Statement object that will generate ResultSet objects
     * with the given type and concurrency. This method is the same as
     * the createStatement method above, but it allows the default
     * result set type and concurrency to be overridden.
     *
     * @param resultSetType a result set type; one of
     *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     *        or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency a concurrency type; one of
     *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new Statement object that will generate ResultSet objects with
     *         the given type and concurrency
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a Statement object that will generate ResultSet objects
     * with the given type, concurrency, and holdability.  This method
     * is the same as the createStatement method above, but it allows
     * the default result set type, concurrency, and holdability to be
     * overridden.
     *
     * @param resultSetType one of the following ResultSet constants:
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     * or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency one of the following ResultSet
     * constants: ResultSet.CONCUR_READ_ONLY or
     * ResultSet.CONCUR_UPDATABLE
     * @param resultSetHoldability one of the following ResultSet
     * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
     * ResultSet.CLOSE_CURSORS_AT_COMMIT
     *
     * @return a new Statement      object that will generate ResultSet
     * objects with the given type, concurrency, and holdability
     * @throws SQLException if a database access error occurs or the
     * given parameters are not ResultSet constants indicating type,
     * concurrency, and holdability
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            Statement ret = new MonetStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
            // store it in the map for when we close...
            statements.put(ret, null);
            return ret;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
        // we don't have to catch SQLException because that is declared to
        // be thrown
    }

    /**
     * Constructs an object that implements the Clob interface. The
     * object returned initially contains no data. The setAsciiStream,
     * setCharacterStream and setString methods of the Clob interface
     * may be used to add data to the Clob.
     *
     * @return a MonetClob instance
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support MonetClob objects that can be filled in
     * @since 1.6
     */
    @Override
    public Clob createClob() throws SQLException {
        return new MonetClob("");
    }

    /**
     * Constructs an object that implements the Blob interface. The
     * object returned initially contains no data. The setBinaryStream
     * and setBytes methods of the Blob interface may be used to add
     * data to the Blob.
     *
     * @return a MonetBlob instance
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support MonetBlob objects that can be filled in
     */
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob() not supported", "0A000");
    }

    /**
     * Constructs an object that implements the NClob interface. The
     * object returned initially contains no data. The setAsciiStream,
     * setCharacterStream and setString methods of the NClob interface
     * may be used to add data to the NClob.
     *
     * @return an NClob instance
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support MonetClob objects that can be filled in
     */
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob() not supported", "0A000");
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName the SQL type name of the SQL structured type that
     *        this Struct object maps to. The typeName is the name of a
     *        user-defined type that has been defined for this database.
     *        It is the value returned by Struct.getSQLTypeName.
     * @param attributes the attributes that populate the returned
     *        object
     * @return a Struct object that maps to the given SQL type and is
     *         populated with the given attributes
     * @throws SQLException if a database error occurs, the typeName
     *         is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support this data type
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct() not supported", "0A000");
    }

    /**
     * Constructs an object that implements the SQLXML interface. The
     * object returned initially contains no data. The
     * createXmlStreamWriter object and setString method of the SQLXML
     * interface may be used to add data to the SQLXML object.
     *
     * @return An object that implements the SQLXML interface
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support this data type
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML() not supported", "0A000");
    }

    /**
     * Retrieves the current auto-commit mode for this Connection
     * object.
     *
     * @return the current state of this Connection object's auto-commit
     *         mode
     * @see #setAutoCommit(boolean)
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    /**
     * Retrieves this Connection object's current catalog name.
     *
     * @return the current catalog name or null if there is none
     * @throws SQLException if a database access error occurs or the
     *         current language is not SQL
     */
    @Override
    public String getCatalog() throws SQLException {
        // MonetDB does NOT support catalogs
        return null;
    }

    /**
     * Not implemented by MonetDB's JDBC driver.
     *
     * @param name The name of the client info property to retrieve
     * @return The value of the client info property specified
     */
    @Override
    public String getClientInfo(String name) {
        // This method will also return null if the specified client
        // info property name is not supported by the driver.
        return null;
    }

    /**
     * Not implemented by MonetDB's JDBC driver.
     *
     * @return A Properties object that contains the name and current
     *         value of each of the client info properties supported by
     *         the driver.
     */
    @Override
    public Properties getClientInfo() {
        return new Properties();
    }

    /**
     * Retrieves the current holdability of ResultSet objects created
     * using this Connection object.
     *
     * @return the holdability, one of
     *         ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *         ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public int getHoldability() {
        // TODO: perhaps it is better to have the server implement
        //       CLOSE_CURSORS_AT_COMMIT
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Retrieves a DatabaseMetaData object that contains metadata about
     * the database to which this Connection object represents a
     * connection. The metadata includes information about the
     * database's tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, and so on.
     *
     * @throws SQLException if the current language is not SQL
     * @return a DatabaseMetaData object for this Connection object
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (this.currentMonetDBLanguage != MonetDBLanguage.LANG_SQL) {
            throw new SQLException("This method is only supported in SQL mode", "M0M04");
        }
        return new MonetDatabaseMetaData(this);
    }

    /**
     * Retrieves this Connection object's current transaction isolation
     * level.
     *
     * @return the current transaction isolation level, which will be
     *         Connection.TRANSACTION_SERIALIZABLE
     */
    @Override
    public int getTransactionIsolation() {
        return TRANSACTION_SERIALIZABLE;
    }

    /**
     * Retrieves the Map object associated with this Connection object.
     * Unless the application has added an entry, the type map returned
     * will be empty.
     *
     * @return the java.util.Map object associated with this Connection
     *         object
     */
    @Override
    public Map<String,Class<?>> getTypeMap() {
        return typeMap;
    }

    /**
     * Retrieves the first warning reported by calls on this Connection
     * object.  If there is more than one warning, subsequent warnings
     * will be chained to the first one and can be retrieved by calling
     * the method SQLWarning.getNextWarning on the warning that was
     * retrieved previously.
     *
     * This method may not be called on a closed connection; doing so
     * will cause an SQLException to be thrown.
     *
     * Note: Subsequent warnings will be chained to this SQLWarning.
     *
     * @return the first SQLWarning object or null if there are none
     * @throws SQLException if a database access error occurs or this method is
     *         called on a closed connection
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        // if there are no warnings, this will be null, which fits with the
        // specification.
        return warnings;
    }

    /**
     * Retrieves whether this Connection object has been closed.  A
     * connection is closed if the method close has been called on it or
     * if certain fatal errors have occurred.  This method is guaranteed
     * to return true only when it is called after the method
     * Connection.close has been called.
     *
     * This method generally cannot be called to determine whether a
     * connection to a database is valid or invalid.  A typical client
     * can determine that a connection is invalid by catching any
     * exceptions that might be thrown when an operation is attempted.
     *
     * @return true if this Connection object is closed; false if it is
     *         still open
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Retrieves whether this Connection object is in read-only mode.
     * MonetDB currently doesn't support updateable result sets, but
     * updates are possible.  Hence the Connection object is never in
     * read-only mode.
     *
     * @return true if this Connection object is read-only; false otherwise
     */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Returns true if the connection has not been closed and is still
     * valid. The driver shall submit a query on the connection or use
     * some other mechanism that positively verifies the connection is
     * still valid when this method is called.
     *
     * The query submitted by the driver to validate the connection
     * shall be executed in the context of the current transaction.
     *
     * @param timeout The time in seconds to wait for the database
     *        operation used to validate the connection to complete. If
     *        the timeout period expires before the operation completes,
     *        this method returns false. A value of 0 indicates a
     *        timeout is not applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the value supplied for timeout is less
     *         than 0
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("timeout is less than 0", "M1M05");
        if (closed)
            return false;
        // ping db using select 1;
        Statement stmt = null;
        try {
            stmt = createStatement();
            // the timeout parameter is ignored here, since
            // MonetStatement.setQueryTimeout(timeout) is not supported.
            stmt.executeQuery("SELECT 1");
            stmt.close();
            return true;
        } catch (Exception e) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e2) {}
            }
        }
        return false;
    }

    @Override
    public String nativeSQL(String sql) {return sql;}

    @Override
    public CallableStatement prepareCall(String sql) {return null;}

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return null;}

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return null;}

    /**
     * Creates a PreparedStatement object for sending parameterized SQL
     * statements to the database.
     *
     * A SQL statement with or without IN parameters can be pre-compiled
     * and stored in a PreparedStatement object. This object can then be
     * used to efficiently execute this statement multiple times.
     *
     * Note: This method is optimized for handling parametric SQL
     * statements that benefit from precompilation. If the driver
     * supports precompilation, the method prepareStatement will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the PreparedStatement object is
     * executed. This has no direct effect on users; however, it does
     * affect which methods throw certain SQLException objects.
     *
     * Result sets created using the returned PreparedStatement object
     * will by default be type TYPE_FORWARD_ONLY and have a concurrency
     * level of CONCUR_READ_ONLY.
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @return a new default PreparedStatement object containing the
     *         pre-compiled SQL statement
     * @throws SQLException if a database access error occurs
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a PreparedStatement object that will generate ResultSet
     * objects with the given type and concurrency.  This method is the
     * same as the prepareStatement method above, but it allows the
     * default result set type and concurrency to be overridden.
     *
     * @param sql a String object that is the SQL statement to be sent to the
     *            database; may contain one or more ? IN parameters
     * @param resultSetType a result set type; one of
     *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     *        or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency a concurrency type; one of
     *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new PreparedStatement object containing the pre-compiled SQL
     *         statement that will produce ResultSet objects with the given
     *         type and concurrency
     * @throws SQLException if a database access error occurs or the given
     *                      parameters are not ResultSet constants indicating
     *                      type and concurrency
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a PreparedStatement object that will generate ResultSet
     * objects with the given type, concurrency, and holdability.
     *
     * This method is the same as the prepareStatement method above, but
     * it allows the default result set type, concurrency, and
     * holdability to be overridden.
     *
     * @param sql a String object that is the SQL statement to be sent
     * to the database; may contain one or more ? IN parameters
     * @param resultSetType one of the following ResultSet constants:
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     * or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency one of the following ResultSet
     * constants: ResultSet.CONCUR_READ_ONLY or
     * ResultSet.CONCUR_UPDATABLE
     * @param resultSetHoldability one of the following ResultSet
     * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
     * ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new PreparedStatement object, containing the
     * pre-compiled SQL statement, that will generate ResultSet objects
     * with the given type, concurrency, and holdability
     * @throws SQLException if a database access error occurs or the
     * given parameters are not ResultSet constants indicating type,
     * concurrency, and holdability
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            PreparedStatement ret = new MonetPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, sql);
            // store it in the map for when we close...
            statements.put(ret, null);
            return ret;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
        // we don't have to catch SQLException because that is declared to
        // be thrown
    }

    /**
     * Creates a default PreparedStatement object that has the
     * capability to retrieve auto-generated keys.  The given constant
     * tells the driver whether it should make auto-generated keys
     * available for retrieval.  This parameter is ignored if the SQL
     * statement is not an INSERT statement.
     *
     * Note: This method is optimized for handling parametric SQL
     * statements that benefit from precompilation.  If the driver
     * supports precompilation, the method prepareStatement will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation.  In this case, the statement may
     * not be sent to the database until the PreparedStatement object is
     * executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     *
     * Result sets created using the returned PreparedStatement object
     * will by default be type TYPE_FORWARD_ONLY and have a concurrency
     * level of CONCUR_READ_ONLY.
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param autoGeneratedKeys a flag indicating whether auto-generated
     *        keys should be returned; one of
     *        Statement.RETURN_GENERATED_KEYS or
     *        Statement.NO_GENERATED_KEYS
     * @return a new PreparedStatement object, containing the
     *         pre-compiled SQL statement, that will have the capability
     *         of returning auto-generated keys
     * @throws SQLException - if a database access error occurs or the
     *         given parameter is not a Statement  constant indicating
     *         whether auto-generated keys should be returned
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLException("Invalid argument, expected RETURN_GENERATED_KEYS or NO_GENERATED_KEYS", "M1M05");
        }
		/* MonetDB has no way to disable this, so just do the normal thing ;) */
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {return null;}

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {return null;}

    /**
     * Removes the given Savepoint object from the current transaction.
     * Any reference to the savepoint after it have been removed will
     * cause an SQLException to be thrown.
     *
     * @param savepoint the Savepoint object to be removed
     * @throws SQLException if a database access error occurs or the given
     *         Savepoint object is not a valid savepoint in the current
     *         transaction
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (!(savepoint instanceof MonetSavepoint)) {
            throw new SQLException("This driver can only handle savepoints it created itself", "M0M06");
        }
        MonetSavepoint sp = (MonetSavepoint) savepoint;
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send the appropriate query string to the database
        try {
            l.processQuery("RELEASE SAVEPOINT " + sp.getName());
        } finally {
            l.close();
        }
    }

    /**
     * Undoes all changes made in the current transaction and releases
     * any database locks currently held by this Connection object. This
     * method should be used only when auto-commit mode has been
     * disabled.
     *
     * @throws SQLException if a database access error occurs or this
     *         Connection object is in auto-commit mode
     * @see #setAutoCommit(boolean)
     */
    @Override
    public void rollback() throws SQLException {
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send rollback to the server
        try {
            l.processQuery("ROLLBACK");
        } finally {
            l.close();
        }
    }

    /**
     * Undoes all changes made after the given Savepoint object was set.
     *
     * This method should be used only when auto-commit has been
     * disabled.
     *
     * @param savepoint the Savepoint object to roll back to
     * @throws SQLException if a database access error occurs, the
     *         Savepoint object is no longer valid, or this Connection
     *         object is currently in auto-commit mode
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (!(savepoint instanceof MonetSavepoint)) {
            throw new SQLException("This driver can only handle savepoints it created itself", "M0M06");
        }

        MonetSavepoint sp = (MonetSavepoint)savepoint;
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send the appropriate query string to the database
        try {
            l.processQuery("ROLLBACK TO SAVEPOINT " + sp.getName());
        } finally {
            l.close();
        }
    }

    /**
     * Sets this connection's auto-commit mode to the given state. If a
     * connection is in auto-commit mode, then all its SQL statements
     * will be executed and committed as individual transactions.
     * Otherwise, its SQL statements are grouped into transactions that
     * are terminated by a call to either the method commit or the
     * method rollback. By default, new connections are in auto-commit
     * mode.
     *
     * The commit occurs when the statement completes or the next
     * execute occurs, whichever comes first. In the case of statements
     * returning a ResultSet object, the statement completes when the
     * last row of the ResultSet object has been retrieved or the
     * ResultSet object has been closed. In advanced cases, a single
     * statement may return multiple results as well as output parameter
     * values. In these cases, the commit occurs when all results and
     * output parameter values have been retrieved.
     *
     * NOTE: If this method is called during a transaction, the
     * transaction is committed.
     *
     * @param autoCommit true to enable auto-commit mode; false to disable it
     * @throws SQLException if a database access error occurs
     * @see #getAutoCommit()
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autoCommit != autoCommit) {
            sendControlCommand("auto_commit " + (autoCommit ? "1" : "0"));
            this.autoCommit = autoCommit;
        }
    }

    /**
     * Sets the given catalog name in order to select a subspace of this
     * Connection object's database in which to work.  If the driver
     * does not support catalogs, it will silently ignore this request.
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        // silently ignore this request as MonetDB does not support catalogs
    }

    /**
     * Not implemented by MonetDB's JDBC driver.
     *
     * @param name The name of the client info property to set
     * @param value The value to set the client info property to. If the
     *        value is null, the current value of the specified property
     *        is cleared.
     */
    @Override
    public void setClientInfo(String name, String value) {
        addWarning("clientInfo: " + name + "is not a recognised property", "01M07");
    }

    /**
     * Not implemented by MonetDB's JDBC driver.
     *
     * @param props The list of client info properties to set
     */
    @Override
    public void setClientInfo(Properties props) {
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            setClientInfo(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    /**
     * Changes the default holdability of ResultSet objects created using this
     * Connection object to the given holdability. The default holdability of
     * ResultSet objects can be be determined by invoking DatabaseMetaData.getResultSetHoldability().
     *
     * @param holdability - a ResultSet holdability constant; one of
     *	ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *	ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @see #getHoldability()
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        // we only support ResultSet.HOLD_CURSORS_OVER_COMMIT
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("setHoldability(CLOSE_CURSORS_AT_COMMIT) not supported", "0A000");
    }

    /**
     * Puts this connection in read-only mode as a hint to the driver to
     * enable database optimizations.  MonetDB doesn't support any mode
     * here, hence an SQLWarning is generated if attempted to set
     * to true here.
     *
     * @param readOnly true enables read-only mode; false disables it
     * @throws SQLException if a database access error occurs or this
     *         method is called during a transaction.
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            addWarning("cannot setReadOnly(true): read-only Connection mode not supported", "01M08");
        }
    }

    /**
     * Creates an unnamed savepoint in the current transaction and
     * returns the new Savepoint object that represents it.
     *
     * @return the new Savepoint object
     * @throws SQLException if a database access error occurs or this Connection
     *         object is currently in auto-commit mode
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        // create a new Savepoint object
        MonetSavepoint sp = new MonetSavepoint();
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send the appropriate query string to the database
        try {
            l.processQuery("SAVEPOINT " + sp.getName());
        } finally {
            l.close();
        }
        return sp;
    }

    /**
     * Creates a savepoint with the given name in the current
     * transaction and returns the new Savepoint object that represents
     * it.
     *
     * @param name a String containing the name of the savepoint
     * @return the new Savepoint object
     * @throws SQLException if a database access error occurs or this Connection
     *         object is currently in auto-commit mode
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        // create a new Savepoint object
        MonetSavepoint sp;
        try {
            sp = new MonetSavepoint(name);
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.getMessage(), "M0M03");
        }
        // note: can't use sendIndependentCommand here because we need
        // to process the auto_commit state the server gives
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send the appropriate query string to the database
        try {
            l.processQuery("SAVEPOINT " + sp.getName());
        } finally {
            l.close();
        }
        return sp;
    }

    /**
     * Attempts to change the transaction isolation level for this
     * Connection object to the one given.  The constants defined in the
     * interface Connection are the possible transaction isolation
     * levels.
     *
     * @param level one of the following Connection constants:
     *        Connection.TRANSACTION_READ_UNCOMMITTED,
     *        Connection.TRANSACTION_READ_COMMITTED,
     *        Connection.TRANSACTION_REPEATABLE_READ, or
     *        Connection.TRANSACTION_SERIALIZABLE.
     */
    @Override
    public void setTransactionIsolation(int level) {
        if (level != TRANSACTION_SERIALIZABLE) {
            addWarning("MonetDB only supports fully serializable " + "transactions, continuing with transaction level " +
                    "raised to TRANSACTION_SERIALIZABLE", "01M09");
        }
    }

    /**
     * Installs the given TypeMap object as the type map for this
     * Connection object. The type map will be used for the custom
     * mapping of SQL structured types and distinct types.
     *
     * @param map the java.util.Map object to install as the replacement for
     *        this Connection  object's default type map
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        typeMap = map;
    }

    /**
     * Returns a string identifying this Connection to the MonetDB
     * server.
     *
     * @return a String representing this Object
     */
    @Override
    public String toString() {
        return "MonetDB Connection (" + this.getJDBCURL() + ") " + (closed ? "connected" : "disconnected");
    }

    //== 1.7 methods (JDBC 4.1)

    /**
     * Sets the given schema name to access.
     *
     * @param schema the name of a schema in which to work
     * @throws SQLException if a database access error occurs or this
     *         method is called on a closed connection
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        if (schema != null) {
            createStatement().execute("SET SCHEMA \"" + schema + "\"");
        }
    }

    /**
     * Retrieves this Connection object's current schema name.
     *
     * @return the current schema name or null if there is none
     * @throws SQLException if a database access error occurs or this
     *         method is called on a closed connection
     */
    @Override
    public String getSchema() throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        String cur_schema;
        Statement st = createStatement();
        ResultSet rs = null;
        try {
            rs = st.executeQuery("SELECT CURRENT_SCHEMA");
            if (!rs.next())
                throw new SQLException("Row expected", "02000");
            cur_schema = rs.getString(1);
        } finally {
            if (rs != null)
                rs.close();
            st.close();
        }
        return cur_schema;
    }

    /**
     * Terminates an open connection. Calling abort results in:
     *  * The connection marked as closed
     *  * Closes any physical connection to the database
     *  * Releases resources used by the connection
     *  * Insures that any thread that is currently accessing the
     *    connection will either progress to completion or throw an
     *    SQLException.
     * Calling abort marks the connection closed and releases any
     * resources. Calling abort on a closed connection is a no-op.
     *
     * @param executor The Executor implementation which will be used by
     *        abort
     * @throws SQLException if a database access error occurs or the
     *         executor is null
     * @throws SecurityException if a security manager exists and
     *         its checkPermission method denies calling abort
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        if (closed)
            return;
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        // this is really the simplest thing to do, it destroys
        // everything (in particular the server connection)
        close();
    }

    /**
     * Sets the maximum period a Connection or objects created from the
     * Connection will wait for the database to reply to any one
     * request. If any request remains unanswered, the waiting method
     * will return with a SQLException, and the Connection or objects
     * created from the Connection will be marked as closed. Any
     * subsequent use of the objects, with the exception of the close,
     * isClosed or Connection.isValid methods, will result in a
     * SQLException.
     *
     * @param executor The Executor implementation which will be used by
     *        setNetworkTimeout
     * @param millis The time in milliseconds to wait for the
     *        database operation to complete
     * @throws SQLException if a database access error occurs, this
     *         method is called on a closed connection, the executor is
     *         null, or the value specified for seconds is less than 0.
     */
    @Override
    public void setNetworkTimeout(Executor executor, int millis) throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        if (millis < 0)
            throw new SQLException("milliseconds is less than zero", "M1M05");
        try {
            this.setSoTimeout(millis);
        } catch (SocketException e) {
            throw new SQLException(e.getMessage(), "08000");
        }
    }

    /**
     * Retrieves the number of milliseconds the driver will wait for a
     * database request to complete. If the limit is exceeded, a
     * SQLException is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means
     *         there is no limit
     * @throws SQLException if a database access error occurs or
     *         this method is called on a closed Connection
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        try {
            return this.getSoTimeout();
        } catch (SocketException e) {
            throw new SQLException(e.getMessage(), "08000");
        }
    }

    //== end methods of interface Connection

    /**
     * Returns whether the BLOB type should be mapped to BINARY type.
     */
    public boolean getBlobAsBinary() {
        return blobIsBinary;
    }

    /**
     * Sends the given string to MonetDB as regular statement, making
     * sure there is a prompt after the command is sent.  All possible
     * returned information is discarded.  Encountered errors are
     * reported.
     *
     * @param command the exact string to send to MonetDB
     * @throws SQLException if an IO exception or a database error occurs
     */
    public void sendIndependentCommand(String command) throws SQLException {
        synchronized (this) {
            try {
                out.writeLine(server.getQueryTemplateHeader(0) + command + server.getQueryTemplateHeader(1));
                String error = in.waitForPrompt();
                if (error != null)
                    throw new SQLException(error.substring(6),
                            error.substring(0, 5));
            } catch (SocketTimeoutException e) {
                close(); // JDBC 4.1 semantics: abort()
                throw new SQLException("connection timed out", "08M33");
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), "08000");
            }
        }
    }

    /**
     * Sends the given string to MonetDB as control statement, making
     * sure there is a prompt after the command is sent.  All possible
     * returned information is discarded.  Encountered errors are
     * reported.
     *
     * @param command the exact string to send to MonetDB
     * @throws SQLException if an IO exception or a database error occurs
     */
    public void sendControlCommand(String command) throws SQLException {
        // send X command
        synchronized (this) {
            try {
                out.writeLine(server.getCommandTemplateHeader(0) + command + server.getCommandTemplateHeader(1));
                String error = in.waitForPrompt();
                if (error != null)
                    throw new SQLException(error.substring(6),
                            error.substring(0, 5));
            } catch (SocketTimeoutException e) {
                close(); // JDBC 4.1 semantics, abort()
                throw new SQLException("connection timed out", "08M33");
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), "08000");
            }
        }
    }

    /**
     * Adds a warning to the pile of warnings this Connection object
     * has.  If there were no warnings (or clearWarnings was called)
     * this warning will be the first, otherwise this warning will get
     * appended to the current warning.
     *
     * @param reason the warning message
     */
    public void addWarning(String reason, String sqlstate) {
        if (warnings == null) {
            warnings = new SQLWarning(reason, sqlstate);
        } else {
            warnings.setNextWarning(new SQLWarning(reason, sqlstate));
        }
    }
}
