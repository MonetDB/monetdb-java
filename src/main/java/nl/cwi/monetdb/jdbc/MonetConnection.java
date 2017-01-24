package nl.cwi.monetdb.jdbc;

import nl.cwi.monetdb.mcl.connection.*;
import nl.cwi.monetdb.mcl.connection.SenderThread;
import nl.cwi.monetdb.mcl.connection.mapi.MapiLanguage;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.responses.*;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.*;
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
 * @author Fabian Groffen, Martin van Dinther, Pedro Ferreira
 * @version 1.3
 */
public abstract class MonetConnection extends MonetWrapper implements Connection {

    /** The sequence counter */
    private static int SeqCounter = 0;

    /**
     * Gets the current sequence counter.
     *
     * @return The current sequence counter
     */
    public static int GetSeqCounter() {
        return SeqCounter;
    }

    /** The successful processed input properties */
    protected final Properties conn_props;
    /** The language to connect with */
    protected IMonetDBLanguage language;
    /** Authentication hash method */
    protected final String hash;
    /** An optional thread that is used for sending large queries */
    private SenderThread senderThread;
    /** Whether this Connection is closed (and cannot be used anymore) */
    private boolean closed;
    /** Whether this Connection is in autocommit mode */
    private boolean autoCommit = true;
    /** The stack of warnings for this Connection object */
    private SQLWarning warnings;
    /** The Connection specific mapping of user defined types to Java types */
    private Map<String,Class<?>> typeMap = new HashMap<String,Class<?>>() {
        private static final long serialVersionUID = 1L; {
            put("inet", MonetINET.class);
            put("url", MonetURL.class);
        }
    };

    // See javadoc for documentation about WeakHashMap if you don't know what it does !!!NOW!!!
    // (only when you deal with it of course)
    /** A Map containing all (active) Statements created from this Connection */
    private Map<Statement,?> statements = new WeakHashMap<Statement, Object>();
    /** The number of results we receive from the server at once */
    private int curReplySize = -1; // the server by default uses -1 (all)
    /** Whether or not BLOB is mapped to LONGVARBINARY within the driver */
    private final boolean blobIsBinary;
    /** Whether or not CLOB is mapped to LONGVARCHAR within the driver */
    private final boolean clobIsLongChar;
    /** The underlying proticol provided by the connection (MAPI or embedded) */
    protected AbstractProtocol protocol;
    /** Tells if the connection is embedded or not */
    private final boolean isEmbedded;

    /**
     * Constructor of a Connection for MonetDB. At this moment the current implementation limits itself to storing the
     * given host, database, username and password for later use by the createStatement() call.  This constructor is
     * only accessible to classes from the jdbc package.
     *
     * @throws IOException if an error occurs
     */
    public MonetConnection(Properties props, String hash, IMonetDBLanguage language, boolean blobIsBinary,
                           boolean clobIsLongChar) throws IOException {
        this.conn_props = props;
        this.hash = hash;
        this.language = language;
        this.blobIsBinary = blobIsBinary;
        this.clobIsLongChar = clobIsLongChar;
        String embedded = props.getProperty("embedded");
        this.isEmbedded = embedded != null && embedded.equals("true");
    }

    /**
     * Checks if the conection is embedded or not
     *
     * @return If the connection is embedded
     */
    public boolean isEmbedded() {
        return isEmbedded;
    }

    /**
     * Gets the connection's language data.
     *
     * @return The connection's language data
     */
    public IMonetDBLanguage getLanguage() {
        return language;
    }

    /**
     * Gets the connection's protocol.
     *
     * @return The connection's protocol
     */
    public AbstractProtocol getProtocol() {
        return this.protocol;
    }

    /**
     * Connects to the server, authenticating the user.
     *
     * @param user The user name to authenticate
     * @param pass The user's password
     * @return A List with informational (warning) messages. If this list is empty; then there are no warnings.
     * @throws IOException if an I/O error occurs when creating the socket
     * @throws ProtocolException if bogus data is received
     * @throws MCLException if an MCL related error occurs
     */
    public abstract List<String> connect(String user, String pass) throws IOException, ProtocolException, MCLException;

    /**
     * Gets the underlying connection block size length.
     *
     * @return The block size length
     */
    public abstract int getBlockSize();

    /**
     * Gets the underlying connection default fetch size for DataBlock responses.
     *
     * @return The default fetch size
     */
    public abstract int getDefFetchsize();

    /**
     * Gets the underlying connection socket timeout.
     *
     * @return The underlying connection socket timeout
     */
    public abstract int getSoTimeout();

    /**
     * Sets the underlying connection socket timeout.
     *
     * @param timeout The specified timeout, in milliseconds. A timeout of zero is interpreted as an infinite timeout
     */
    public abstract void setSoTimeout(int timeout);

    /**
     * Closes the underlying connection implementation.
     *
     * @throws IOException if an I/O error occurs while closing the connection
     */
    public abstract void closeUnderlyingConnection() throws IOException;

    /**
     * Gets the underlying connection JDBC String URL.
     *
     * @return The underlying connection JDBC String URL
     */
    public abstract String getJDBCURL();

    /**
     * Sends a control command to the server.
     *
     * @param commandID the command identifier according to {@link ControlCommands} listing
     * @param data The integer to send according to the control command
     * @throws SQLException if an IO exception or a database error occurs
     */
    public abstract void sendControlCommand(int commandID, int data) throws SQLException;

    /**
     * Releases this Connection object's database and JDBC resources immediately instead of waiting for them to be
     * automatically released. All Statements created from this Connection will be closed when this method is called.
     *
     * Calling the method close on a Connection object that is already closed is a no-op.
     */
    @Override
    public void close() {
        for (Statement st : statements.keySet()) {
            try {
                st.close();
            } catch (SQLException e) {
                // better luck next time!
            }
        }
        // close the socket or the embedded server
        try {
            this.closeUnderlyingConnection();
        } catch (IOException e) {
            // ignore it
        }
        // close active SendThread if any
        if (senderThread != null) {
            senderThread.shutdown();
            senderThread = null;
        }
        // report ourselves as closed
        closed = true;
    }

    /**
     * Destructor called by garbage collector before destroying this object tries to disconnect the MonetDB connection
     * if it has not been disconnected already.
     */
    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    //== methods of interface Connection

    /**
     * Clears all warnings reported for this Connection object. After a call to this method, the method getWarnings
     * returns null until a new warning is reported for this Connection object.
     */
    @Override
    public void clearWarnings() {
        warnings = null;
    }

    private void createResponseList(String query) throws SQLException {
        // create a container for the result
        ResponseList l = new ResponseList(0, 0, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        // send commit to the server
        try {
            l.processQuery(query);
        } finally {
            l.close();
        }
    }

    /**
     * Makes all changes made since the previous commit/rollback permanent and releases any database locks currently
     * held by this Connection object. This method should be used only when auto-commit mode has been disabled.
     *
     * @throws SQLException if a database access error occurs or this Connection object is in auto-commit mode
     * @see #setAutoCommit(boolean)
     */
    @Override
    public void commit() throws SQLException {
        // note: can't use sendIndependentCommand here because we need to process the auto_commit state the server gives
        this.createResponseList("COMMIT");
    }

    /**
     * Creates a Statement object for sending SQL statements to the database. SQL statements without parameters are
     * normally executed using Statement objects. If the same SQL statement is executed many times, it may be more
     * efficient to use a PreparedStatement object.
     *
     * Result sets created using the returned Statement object will by default be type TYPE_FORWARD_ONLY and have a
     * concurrency level of CONCUR_READ_ONLY.
     *
     * @return a new default Statement object
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a Statement object that will generate ResultSet objects with the given type and concurrency. This method
     * is the same as the createStatement method above, but it allows the default result set type and concurrency to be
     * overridden.
     *
     * @param resultSetType a result set type; one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     * or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency a concurrency type; one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new Statement object that will generate ResultSet objects with the given type and concurrency
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
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
     * Retrieves the current auto-commit mode for this Connection object.
     *
     * @return the current state of this Connection object's auto-commit mode
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
     * @throws SQLException if a database access error occurs or the current language is not SQL
     */
    @Override
    public String getCatalog() throws SQLException {
        // MonetDB does NOT support catalogs
        return null;
    }

    /**
     * Retrieves the current holdability of ResultSet objects created using this Connection object.
     *
     * @return the holdability, one of ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
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
        if (this.language != MapiLanguage.LANG_SQL) {
            throw new SQLException("This method is only supported in SQL mode", "M0M04");
        }
        return new MonetDatabaseMetaData(this);
    }

    /**
     * Retrieves this Connection object's current transaction isolation level.
     *
     * @return the current transaction isolation level, which will be Connection.TRANSACTION_SERIALIZABLE
     */
    @Override
    public int getTransactionIsolation() {
        return TRANSACTION_SERIALIZABLE;
    }

    /**
     * Retrieves the Map object associated with this Connection object. Unless the application has added an entry,
     * the type map returned will be empty.
     *
     * @return the java.util.Map object associated with this Connection object
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
     * This method may not be called on a closed connection; doing so will cause an SQLException to be thrown.
     *
     * Note: Subsequent warnings will be chained to this SQLWarning.
     *
     * @return the first SQLWarning object or null if there are none
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        // if there are no warnings, this will be null, which fits with the specification.
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
     * @return true if this Connection object is closed; false if it is still open
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

    @Override
    public String nativeSQL(String sql) {return sql;}

    @Override
    public CallableStatement prepareCall(String sql) {return null;}

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return null;}

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) {return null;}

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
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
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
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
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
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        try {
            PreparedStatement ret = new MonetPreparedStatement(this, resultSetType, resultSetConcurrency,
                    resultSetHoldability, sql);
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
        this.createResponseList("RELEASE SAVEPOINT " + sp.getName());
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
        this.createResponseList("ROLLBACK");
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
        this.createResponseList("ROLLBACK TO SAVEPOINT " + sp.getName());
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
            this.sendControlCommand(ControlCommands.AUTO_COMMIT, (autoCommit ? 1 : 0));
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
        throw new SQLFeatureNotSupportedException("setCatalog(String catalog) not supported", "0A000");
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
        this.createResponseList("SAVEPOINT " + sp.getName());
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
        this.createResponseList("SAVEPOINT " + sp.getName());
        return sp;
    }

    /**
     * Attempts to change the transaction isolation level for this
     * Connection object to the one given.  The constants defined in the
     * interface Connection are the possible transaction isolation
     * levels.
     *
     * @param level one of the following Connection constants: Connection.TRANSACTION_READ_UNCOMMITTED,
     *        Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ, or
     *        Connection.TRANSACTION_SERIALIZABLE.
     */
    @Override
    public void setTransactionIsolation(int level) {
        if (level != TRANSACTION_SERIALIZABLE) {
            addWarning("MonetDB only supports fully serializable " +
                    "transactions, continuing with transaction level raised to TRANSACTION_SERIALIZABLE", "01M09");
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
     * Returns a string identifying this Connection to the MonetDB server.
     *
     * @return a String representing this Object
     */
    @Override
    public String toString() {
        return "MonetDB Connection (" + this.getJDBCURL() + ") " + (closed ? "disconnected" : "connected");
    }

    //== Java 1.6 methods (JDBC 4.0)

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
     * SQLException is thrown or the driver supports the resulting conversion.
     *
     * @param typeName the SQL name of the type the elements of the
     *        array map to. The typeName is a database-specific name
     *        which may be the name of a built-in type, a user-defined
     *        type or a standard SQL type supported by this database.
     *        This is the value returned by Array.getBaseTypeName
     * @return an Array object whose elements map to the specified SQL type
     * @throws SQLException if a database error occurs, the JDBC type
     *         is not appropriate for the typeName and the conversion is
     *         not supported, the typeName is null or this method is
     *         called on a closed connection
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support this data type
     * @since 1.6
     */
    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf() not supported", "0A000");
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
    public java.sql.Clob createClob() throws SQLException {
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
     * @since 1.6
     */
    @Override
    public java.sql.Blob createBlob() throws SQLException {
        return new MonetBlob(new byte[1]);
    }

    /**
     * Constructs an object that implements the NClob interface. The
     * object returned initially contains no data. The setAsciiStream,
     * setCharacterStream and setString methods of the NClob interface
     * may be used to add data to the NClob.
     *
     * @return an NClob instance
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support MonetNClob objects that can be filled in
     * @since 1.6
     */
    @Override
    public java.sql.NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob() not supported", "0A000");
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName the SQL type name of the SQL structured type that
     *        this Struct object maps to. The typeName is the name of a
     *        user-defined type that has been defined for this database.
     *        It is the value returned by Struct.getSQLTypeName.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is
     *         populated with the given attributes
     * @throws SQLException if a database error occurs, the typeName
     *         is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException the JDBC driver does
     *         not support this data type
     * @since 1.6
     */
    @Override
    public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
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
     * @since 1.6
     */
    @Override
    public java.sql.SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML() not supported", "0A000");
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
     * @throws SQLException if the value supplied for timeout is less than 0
     * @since 1.6
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("timeout is less than 0", "M1M05");
        if (closed)
            return false;

        // ping db using query: select 1;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = createStatement();
            stmt.setQueryTimeout(timeout);
            rs = stmt.executeQuery("SELECT 1");
            rs.close();
            rs = null;
            stmt.close();
            return true;
        } catch (Exception e) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e2) {}
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e2) {}
            }
        }
        return false;
    }

    /**
     * Returns the value of the client info property specified by name.
     * This method may return null if the specified client info property
     * has not been set and does not have a default value.
     * This method will also return null if the specified client info
     * property name is not supported by the driver.
     * Applications may use the DatabaseMetaData.getClientInfoProperties method
     * to determine the client info properties supported by the driver.
     *
     * @param name - The name of the client info property to retrieve
     * @return The value of the client info property specified or null
     * @throws SQLException - if the database server returns an error
     *	when fetching the client info value from the database
     *	or this method is called on a closed connection
     * @since 1.6
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        if (name == null || name.isEmpty())
            return null;
        return conn_props.getProperty(name);
    }

    /**
     * Returns a list containing the name and current value of each client info
     * property supported by the driver. The value of a client info property may
     * be null if the property has not been set and does not have a default value.
     *
     * @return A Properties object that contains the name and current value
     *         of each of the client info properties supported by the driver.
     * @throws SQLException - if the database server returns an error
     *	when fetching the client info value from the database
     *	or this method is called on a closed connection
     * @since 1.6
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        // return a clone of the connection properties object
        return new Properties(conn_props);
    }

    /**
     * Sets the value of the client info property specified by name to the value specified by value.
     * Applications may use the DatabaseMetaData.getClientInfoProperties method to determine
     * the client info properties supported by the driver and the maximum length that may be specified
     * for each property.
     *
     * The driver stores the value specified in a suitable location in the database. For example
     * in a special register, session parameter, or system table column. For efficiency the driver
     * may defer setting the value in the database until the next time a statement is executed
     * or prepared. Other than storing the client information in the appropriate place in the
     * database, these methods shall not alter the behavior of the connection in anyway.
     * The values supplied to these methods are used for accounting, diagnostics and debugging purposes only.
     *
     * The driver shall generate a warning if the client info name specified is not recognized by the driver.
     *
     * If the value specified to this method is greater than the maximum length for the property
     * the driver may either truncate the value and generate a warning or generate a SQLClientInfoException.
     * If the driver generates a SQLClientInfoException, the value specified was not set on the connection.
     *
     * The following are standard client info properties. Drivers are not required to support these
     * properties however if the driver supports a client info property that can be described by one
     * of the standard properties, the standard property name should be used.
     *
     *	ApplicationName - The name of the application currently utilizing the connection
     *	ClientUser - The name of the user that the application using the connection is performing work for.
     *		This may not be the same as the user name that was used in establishing the connection.
     *	ClientHostname - The hostname of the computer the application using the connection is running on.
     *
     * @param name - The name of the client info property to set
     * @param value - The value to set the client info property to. If the
     *        value is null, the current value of the specified property is cleared.
     * @throws SQLClientInfoException - if the database server returns an error
     *         while setting the clientInfo values on the database server
     *         or this method is called on a closed connection
     * @since 1.6
     */
    @Override
    public void setClientInfo(String name, String value) throws java.sql.SQLClientInfoException {
        if (name == null || name.isEmpty()) {
            addWarning("setClientInfo: missing property name", "01M07");
            return;
        }
        // If the value is null, the current value of the specified property is cleared.
        if (value == null) {
            if (conn_props.containsKey(name))
                conn_props.remove(name);
            return;
        }
        // only set value for supported property names
        if (name.equals("host") || name.equals("port") || name.equals("user") || name.equals("password") ||
                name.equals("database") || name.equals("language") || name.equals("so_timeout") ||
                name.equals("hash") || name.equals("treat_blob_as_binary") || name.equals("follow_redirects") ||
                name.equals("treat_clob_as_longvarchar") || name.equals("embedded") || name.equals("directory")) {
            conn_props.setProperty(name, value);
        } else {
            addWarning("setClientInfo: " + name + "is not a recognised property", "01M07");
        }
    }

    /**
     * Sets the value of the connection's client info properties.
     * The Properties object contains the names and values of the client info
     * properties to be set. The set of client info properties contained in the
     * properties list replaces the current set of client info properties on the connection.
     * If a property that is currently set on the connection is not present in the
     * properties list, that property is cleared. Specifying an empty properties list
     * will clear all of the properties on the connection.
     * See setClientInfo (String, String) for more information.
     *
     * If an error occurs in setting any of the client info properties, a
     * SQLClientInfoException is thrown. The SQLClientInfoException contains information
     * indicating which client info properties were not set. The state of the client
     * information is unknown because some databases do not allow multiple client info
     * properties to be set atomically. For those databases, one or more properties may
     * have been set before the error occurred.
     *
     * @param props - The list of client info properties to set
     * @throws SQLClientInfoException - if the database server returns an error
     *	while setting the clientInfo values on the database server
     *	or this method is called on a closed connection
     * @since 1.6
     */
    @Override
    public void setClientInfo(Properties props) throws java.sql.SQLClientInfoException {
        if (props != null) {
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                setClientInfo(entry.getKey().toString(), entry.getValue().toString());
            }
        }
    }

    //== Java 1.7 methods (JDBC 4.1)

    /**
     * Sets the given schema name to access.
     *
     * @param schema the name of a schema in which to work
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     * @since 1.7
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        if (closed)
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        if (schema == null)
            throw new SQLException("Missing schema name", "M1M05");

        Statement st = createStatement();
        try {
            st.execute("SET SCHEMA \"" + schema + "\"");
        } finally {
            st.close();
        }
    }

    /**
     * Retrieves this Connection object's current schema name.
     *
     * @return the current schema name or null if there is none
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
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
     * @param executor The Executor implementation which will be used by abort
     * @throws SQLException if a database access error occurs or the executor is null
     * @throws SecurityException if a security manager exists and its checkPermission method denies calling abort
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
     * @param executor The Executor implementation which will be used by setNetworkTimeout
     * @param millis The time in milliseconds to wait for the database operation to complete
     * @throws SQLException if a database access error occurs, this method is called on a closed connection, the
     * executor is null, or the value specified for seconds is less than 0.
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
        this.setSoTimeout(millis);
    }

    /**
     * Retrieves the number of milliseconds the driver will wait for a database request to complete. If the limit is
     * exceeded, a SQLException is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means there is no limit
     * @throws SQLException if a database access error occurs or this method is called on a closed Connection
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        if (closed) {
            throw new SQLException("Cannot call on closed Connection", "M1M20");
        }
        return this.getSoTimeout();
    }

    //== end methods of interface Connection

    /**
     * Returns whether the BLOB type should be mapped to BINARY type.
     */
    public boolean getBlobAsBinary() {
        return blobIsBinary;
    }

    /**
     * Returns whether the CLOB type should be mapped to LONGVARCHAR type.
     */
    public boolean getClobAsLongChar() {
        return clobIsLongChar;
    }

    /**
     * Sends the given string to MonetDB as regular statement, making sure there is a prompt after the command is sent.
     * All possible returned information is discarded. Encountered errors are reported.
     *
     * @param command the exact string to send to MonetDB
     * @throws SQLException if an IO exception or a database error occurs
     */
    public void sendIndependentCommand(String command) throws SQLException {
        try {
            protocol.writeNextQuery(language.getQueryTemplateIndex(0), command, language.getQueryTemplateIndex(1));
            protocol.waitUntilPrompt();
            int csrh = protocol.getCurrentServerResponse();
            if (csrh == ServerResponses.ERROR) {
                String error = protocol.getRemainingStringLine(0);
                throw new SQLException(error.substring(6), error.substring(0, 5));
            }
        } catch (SocketTimeoutException e) {
            close(); // JDBC 4.1 semantics: abort()
            throw new SQLException("connection timed out", "08M33");
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), "08000");
        }
    }

    /**
     * Adds a warning to the pile of warnings this Connection object has. If there were no warnings (or clearWarnings
     * was called) this warning will be the first, otherwise this warning will get appended to the current warning.
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

    /**
     * A list of Response objects. Responses are added to this list. Methods of this class are not synchronized. This is
     * left as responsibility to the caller to prevent concurrent access.
     */
    public class ResponseList {

        /** The cache size (number of rows in a DataBlockResponse object) */
        private final int cachesize;
        /** The maximum number of results for this query */
        private final int maxrows;
        /** The ResultSet type to produce */
        private final int rstype;
        /** The ResultSet concurrency to produce */
        private final int rsconcur;
        /** The sequence number of this ResponseList */
        private final int seqnr;
        /** A list of the Responses associated with the query, in the right order */
        private final List<IResponse> responses = new ArrayList<>();
        /** A map of ResultSetResponses, used for additional DataBlockResponse mapping */
        private Map<Integer, ResultSetResponse> rsresponses;
        /** The current header returned by getNextResponse() */
        private int curResponse = -1;

        /**
         * Main constructor. The query argument can either be a String or List.  An SQLException is thrown if another
         * object instance is supplied.
         *
         * @param cachesize overall cachesize to use
         * @param maxrows maximum number of rows to allow in the set
         * @param rstype the type of result sets to produce
         * @param rsconcur the concurrency of result sets to produce
         */
        public ResponseList(int cachesize, int maxrows, int rstype, int rsconcur) {
            this.cachesize = cachesize;
            this.maxrows = maxrows;
            this.rstype = rstype;
            this.rsconcur = rsconcur;
            this.seqnr = SeqCounter++;
        }

        public int getCachesize() {
            return cachesize;
        }

        public int getRstype() {
            return rstype;
        }

        public int getRsconcur() {
            return rsconcur;
        }

        public int getMaxrows() {
            return maxrows;
        }

        /**
         * Retrieves the next available response, or null if there are no more responses.
         *
         * @return the next Response available or null
         */
        IResponse getNextResponse() throws SQLException {
            if (rstype == ResultSet.TYPE_FORWARD_ONLY) {
                // free resources if we're running forward only
                if (curResponse >= 0 && curResponse < responses.size()) {
                    IResponse tmp = responses.get(curResponse);
                    if (tmp != null) {
                        tmp.close();
                    }
                    responses.set(curResponse, null);
                }
            }
            curResponse++;
            if (curResponse >= responses.size()) {
                // ResponseList is obviously completed so, there are no more responses
                return null;
            } else {
                // return this response
                return responses.get(curResponse);
            }
        }

        /**
         * Closes the Response at index i, if not null.
         *
         * @param i the index position of the header to close
         */
        void closeResponse(int i) {
            if (i < 0 || i >= responses.size()) return;
            IResponse tmp = responses.set(i, null);
            if (tmp != null)
                tmp.close();
        }

        /**
         * Closes the current response.
         */
        void closeCurrentResponse() {
            closeResponse(curResponse);
        }

        /**
         * Closes the current and previous responses.
         */
        void closeCurOldResponses() {
            for (int i = curResponse; i >= 0; i--) {
                closeResponse(i);
            }
        }

        /**
         * Closes this ResponseList by closing all the Responses in this ResponseList.
         */
        public void close() {
            for (int i = 0; i < responses.size(); i++) {
                closeResponse(i);
            }
        }

        /**
         * Returns whether this ResponseList has still unclosed Responses.
         */
        boolean hasUnclosedResponses() {
            for (IResponse r : responses) {
                if (r != null)
                    return true;
            }
            return false;
        }

        /**
         * Executes the query contained in this ResponseList, and stores the Responses resulting from this query in this
         * ResponseList.
         *
         * @throws SQLException if a database error occurs
         */
        void processQuery(String query) throws SQLException {
            this.executeQuery(language.getQueryTemplates(), query);
        }

        /**
         * Internal executor of queries.
         *
         * @param templ the template to fill in
         * @param query the query to execute
         * @throws SQLException if a database error occurs
         */
        @SuppressWarnings("fallthrough")
        public void executeQuery(String[] templ, String query) throws SQLException {
            String error = null;

            try {
                // make sure we're ready to send query; read data till we have the prompt it is possible (and most
                // likely) that we already have the prompt and do not have to skip any lines. Ignore errors from
                // previous result sets.
                protocol.waitUntilPrompt();

                // {{{ set reply size
                /**
                 * Change the reply size of the server.  If the given value is the same as the current value known
                 * to use, then ignore this call.  If it is set to 0 we get a prompt after the server sent it's
                 * header.
                 *
                 * 2017: For now, in the embedded connection, the value set cachesize will be always the default one.
                 */
                int size = (cachesize != 0 && !isEmbedded) ? cachesize : MonetConnection.this.getDefFetchsize();
                size = maxrows != 0 ? Math.min(maxrows, size) : size;
                // don't do work if it's not needed
                if (language == MapiLanguage.LANG_SQL && size != curReplySize &&
                        !Arrays.deepEquals(templ, language.getCommandTemplates())) {
                    sendControlCommand(ControlCommands.REPLY_SIZE, size);
                    // store the reply size after a successful change
                    curReplySize = size;
                }
                // }}} set reply size

                // If the query is larger than the TCP buffer size, use a special send thread to avoid deadlock with
                // the server due to blocking behaviour when the buffer is full. Because the server will be writing
                // back results to us, it will eventually block as well when its TCP buffer gets full, as we are
                // blocking an not consuming from it. The result is a state where both client and server want to
                // write, but block.
                if (query.length() > getBlockSize()) {
                    // get a reference to the send thread
                    if (senderThread == null) {
                        senderThread = new SenderThread(protocol);
                    }
                    // tell it to do some work!
                    senderThread.runQuery(templ, query);
                } else {
                    // this is a simple call, which is a lot cheaper and will always succeed for small queries.
                    protocol.writeNextQuery((templ[0] == null) ? "" : templ[0], query,
                            (templ[1] == null) ? "" : templ[1]);
                }

                // go for new results
                protocol.fetchNextResponseData();
                int nextResponse = protocol.getCurrentServerResponse();
                IResponse res = null;
                while (nextResponse != ServerResponses.PROMPT) {
                    // each response should start with a start of header (or error)
                    switch (nextResponse) {
                        case ServerResponses.SOHEADER:
                            // make the response object, and fill it
                            int nextStartHeader = protocol.getNextStarterHeader();
                            try {
                                switch (nextStartHeader) {
                                    case StarterHeaders.Q_PARSE:
                                        throw new ProtocolException("Q_PARSE header not allowed here");
                                    case StarterHeaders.Q_TABLE:
                                    case StarterHeaders.Q_PREPARE: {
                                        res = protocol.getNextResultSetResponse(MonetConnection.this,
                                                ResponseList.this, this.seqnr);
                                        ResultSetResponse rsreponse = (ResultSetResponse) res;
                                        // only add this resultset to the hashmap if it can possibly
                                        // have an additional datablock
                                        if (rsreponse.getRowcount() < rsreponse.getTuplecount()) {
                                            if (rsresponses == null) {
                                                rsresponses = new HashMap<>();
                                            }
                                            rsresponses.put(rsreponse.getId(), rsreponse);
                                        }
                                    }
                                    break;
                                    case StarterHeaders.Q_UPDATE:
                                        res = protocol.getNextUpdateResponse();
                                        break;
                                    case StarterHeaders.Q_SCHEMA:
                                        res = protocol.getNextSchemaResponse();
                                        break;
                                    case StarterHeaders.Q_TRANS:
                                        res = protocol.getNextAutoCommitResponse();
                                        boolean isAutoCommit = ((AutoCommitResponse) res).isAutocommit();

                                        if (MonetConnection.this.getAutoCommit() && isAutoCommit) {
                                            MonetConnection.this.addWarning("Server enabled auto commit mode " +
                                                    "while local state already was auto commit.", "01M11");
                                        }
                                        MonetConnection.this.autoCommit = isAutoCommit;
                                        break;
                                    case StarterHeaders.Q_BLOCK: {
                                        DataBlockResponse next = protocol.getNextDatablockResponse(rsresponses);
                                        if (next == null) {
                                            error = "M0M12!No ResultSetResponse for a DataBlock found";
                                            break;
                                        }
                                        res = next;
                                    }
                                    break;
                                }
                            } catch (ProtocolException e) {
                                error = "M0M10!error while parsing start of header:\n" + e.getMessage() + " found: '"
                                        + protocol.getRemainingStringLine(0).charAt(e.getErrorOffset()) + "'" +
                                        " in: \"" + protocol.getRemainingStringLine(0) + "\"" + " at pos: "
                                        + e.getErrorOffset();
                                // flush all the rest
                                protocol.waitUntilPrompt();
                                nextResponse = protocol.getCurrentServerResponse();
                                break;
                            }

                            // immediately handle errors after parsing the header (res may be null)
                            if (error != null) {
                                protocol.waitUntilPrompt();
                                nextResponse = protocol.getCurrentServerResponse();
                                break;
                            }

                            // here we have a res object, which we can start filling
                            if (res instanceof IIncompleteResponse) {
                                IIncompleteResponse iter = (IIncompleteResponse) res;
                                while (iter.wantsMore()) {
                                    try {
                                        protocol.fetchNextResponseData();
                                        iter.addLines(protocol);
                                    } catch (ProtocolException ex) {
                                        // right, some protocol violation, skip the rest of the result
                                        error = "M0M10!" + ex.getMessage();
                                        protocol.waitUntilPrompt();
                                        nextResponse = protocol.getCurrentServerResponse();
                                        break;
                                    }
                                }
                            }

                            if (error != null) {
                                break;
                            }

                            // it is of no use to store DataBlockResponses, you never want to retrieve them directly
                            // anyway
                            if (!(res instanceof DataBlockResponse)) {
                                responses.add(res);
                            }
                            // read the next line (can be prompt, new result, error, etc.) before we start the loop over
                            protocol.fetchNextResponseData();
                            nextResponse = protocol.getCurrentServerResponse();
                            break;
                        case ServerResponses.INFO:
                            addWarning(protocol.getRemainingStringLine(0), "01000");
                            // read the next line (can be prompt, new result, error, etc.) before we start the loop over
                            protocol.fetchNextResponseData();
                            nextResponse = protocol.getCurrentServerResponse();
                            break;
                        case ServerResponses.ERROR:
                            // read everything till the prompt (should be error) we don't know if we ignore some
                            // garbage here... but the log should reveal that
                            error = protocol.getRemainingStringLine(0);
                            protocol.waitUntilPrompt();
                            nextResponse = protocol.getCurrentServerResponse();
                            break;
                        default:
                            throw new SQLException("Protocol violation, unexpected line!", "M0M10");
                    }
                }

                // if we used the senderThread, make sure it has finished
                if (senderThread != null) {
                    String tmp = senderThread.getErrors();
                    if (tmp != null) {
                        if (error == null) {
                            error = "08000!" + tmp;
                        } else {
                            error += "\n08000!" + tmp;
                        }
                    }
                }
                if (error != null) {
                    SQLException ret = null;
                    String[] errors = error.split("\n");
                    for (String error1 : errors) {
                        if (ret == null) {
                            ret = new SQLException(error1.substring(6), error1.substring(0, 5));
                        } else {
                            ret.setNextException(new SQLException(error1.substring(6), error1.substring(0, 5)));
                        }
                    }
                    throw ret;
                }
            } catch (SocketTimeoutException e) {
                this.close(); // JDBC 4.1 semantics, abort()
                throw new SQLException("connection timed out", "08M33");
            } catch (IOException e) {
                closed = true;
                throw new SQLException(e.getMessage() + " (mserver still alive?)", "08000");
            }
        }
    }
}
