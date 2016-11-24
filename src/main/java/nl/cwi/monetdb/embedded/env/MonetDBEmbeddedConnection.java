/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import nl.cwi.monetdb.embedded.resultset.*;
import nl.cwi.monetdb.embedded.tables.MonetDBTable;
import nl.cwi.monetdb.embedded.utils.StringEscaper;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A single connection to a MonetDB database instance
 * Communication between Java and native C is done via JNI.
 * <br/>
 * <strong>Note</strong>: You can have only one Embedded MonetDB database running per JVM process.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedConnection implements IEmbeddedConnection {

	private final long connectionPointer;

    private final ConcurrentHashMap<Long, AbstractConnectionResult> results = new ConcurrentHashMap<>();

	protected MonetDBEmbeddedConnection(long connectionPointer) { this.connectionPointer = connectionPointer; }

    public long getConnectionPointer() { return connectionPointer; }

    /**
     * Gets the current schema set on the connection.
     *
     * @return A Java String with the name of the schema
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public String getCurrentSchema() throws MonetDBEmbeddedException {
        QueryResultSet eqr = this.sendQuery("SELECT current_schema FROM sys.var();");
        QueryResultSetObjectColumn<String> col = eqr.getObjectColumnByIndex(0);
        String res = col.fetchFirstNColumnValues(1)[0];
        eqr.close();
        return res;
    }

    /**
     * Sets the current schema on the connection.
     *
     * @param newSchema Java String with the name of the schema
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void setCurrentSchema(String newSchema) throws MonetDBEmbeddedException {
        newSchema = StringEscaper.SQLStringEscape(newSchema);
        this.sendUpdate("SET SCHEMA " + newSchema + ";").close();
    }

    /**
     * Begins a transaction.
     *
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void startTransaction() throws MonetDBEmbeddedException {
        this.sendUpdate("START TRANSACTION;").close();
    }

    /**
     * Commits the current transaction.
     *
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void commit() throws MonetDBEmbeddedException {
        this.sendUpdate("COMMIT;").close();
    }

    /**
     * Rollbacks the current transaction.
     *
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void rollback() throws MonetDBEmbeddedException {
        this.sendUpdate("ROLLBACK;").close();
    }

    /**
     * Executes a SQL query without a result set.
     *
     * @param query The SQL query string
     * @return The update result object
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public UpdateResultSet sendUpdate(String query) throws MonetDBEmbeddedException {
        if (!query.endsWith(";")) {
            query += ";";
        }
        UpdateResultSet res = this.sendUpdateInternal(this.connectionPointer, query, true);
        results.put(res.getRandomIdentifier(), res);
        return res;
    }

    /**
     * Executes a SQL query without a result set asynchronously.
     *
     * @param query The SQL query string
     * @return The update result object
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<UpdateResultSet> sendUpdateAsync(String query) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.sendUpdate(query));
    }*/

    /**
	 * Executes a SQL query with a result set.
	 * 
	 * @param query The SQL query string
	 * @return The query result object
	 * @throws MonetDBEmbeddedException If an error in the database occurred
	 */
	public QueryResultSet sendQuery(String query) throws MonetDBEmbeddedException {
		if (!query.endsWith(";")) {
            query += ";";
		}
        QueryResultSet res = this.sendQueryInternal(this.connectionPointer, query, true);
        results.put(res.getRandomIdentifier(), res);
        return res;
	}

    /**
     * Executes an SQL query with a result set asynchronously.
     *
     * @param query The SQL query string
     * @return The query result object
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<QueryResultSet> sendQueryAsync(String query) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.sendQuery(query));
    }*/

    /**
     * Retrieves a MonetDB Table for further operations
     *
     * @param schemaName The schema of the table
     * @param tableName The name of the table
     * @return A MonetDBTable instance with column details
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public MonetDBTable getMonetDBTable(String schemaName, String tableName) throws MonetDBEmbeddedException {
        MonetDBTable res = this.getMonetDBTableInternal(this.connectionPointer, schemaName, tableName);
        results.put(res.getRandomIdentifier(), res);
        return res;
    }

    /**
     * Retrieves a MonetDB Table for further operations asynchronously.
     *
     * @param schemaName The schema of the table
     * @param tableName The name of the table
     * @return A MonetDBTable instance with column details
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<MonetDBTable> getMonetDBTableAsync(String schemaName, String tableName) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.getMonetDBTable(schemaName, tableName));
    }*/

    /**
     * Performs a listing of the existing tables with schemas.
     *
     * @param listSystemTables List system's tables as well (default true)
     * @return The query result object, {@code null} if the database is not running
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSet listTables(boolean listSystemTables) throws MonetDBEmbeddedException {
        String query = "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on tables.schema_id=schemas.id";
        if (!listSystemTables) {
            query += " where tables.system=false order by sn, tn";
        }
        return this.sendQuery(query + ";");
    }

    /**
     * Check if a table it exists.
     *
     * @param schemaName The schema of the table
     * @param tableName The name of the table
     * @return If a the table exists or not
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public boolean checkIfTableExists(String schemaName, String tableName) throws MonetDBEmbeddedException {
        schemaName = StringEscaper.SQLStringEscape(schemaName);
        tableName = StringEscaper.SQLStringEscape(tableName);
        String query =
                "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on sys.tables.schema_id=schemas.id where tables.system=true order by sn, tn and schemas.name ='" +
                        schemaName + "' and tables.name ='" + tableName + "';";
        QueryResultSet eqr = this.sendQuery(query);
        eqr.close();
        return eqr.getNumberOfRows() > 0;
    }

    /**
     * Deletes a table if it exists.
     *
     * @param schemaName The schema of the table
     * @param tableName The name of the table
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void removeTable(String schemaName, String tableName) throws MonetDBEmbeddedException {
        schemaName = StringEscaper.SQLStringEscape(schemaName);
        tableName = StringEscaper.SQLStringEscape(tableName);
        String query = "drop table " + schemaName + "." + tableName + ";";
        this.sendUpdate(query).close();
    }

    /**
     * When the database is shuts down, this method is called instead
     */
    public void closeConnectionImplementation() {
        for(AbstractConnectionResult res : this.results.values()) {
            res.closeImplementation();
        }
        this.closeConnectionInternal(this.connectionPointer);
    }

    /**
     * Shuts down this connection. Any pending queries connections will be immediately closed as well.
     */
    public void closeConnection() {
        this.closeConnectionImplementation();
        MonetDBEmbeddedDatabase.RemoveConnection(this);
    }

    /**
     * Shuts down this connection asynchronously. Any pending queries connections will be immediately closed as well.
     */
    /*public CompletableFuture<Void> closeConnectionAsync() {
        return CompletableFuture.runAsync(() -> this.closeConnection());
    }*/

    /**
     * Removes a query result from this connection.
     */
    protected void removeQueryResult(AbstractConnectionResult res) { this.results.remove(res.getRandomIdentifier()); }

    /**
     * Internal implementation of sendUpdate.
     */
    private native UpdateResultSet sendUpdateInternal(long connectionPointer, String query, boolean execute)
            throws MonetDBEmbeddedException;

    /**
     * Internal implementation of sendQuery.
     */
    private native QueryResultSet sendQueryInternal(long connectionPointer, String query, boolean execute)
            throws MonetDBEmbeddedException;

    /**
     * Internal implementation of getMonetDBTable.
     */
    private native MonetDBTable getMonetDBTableInternal(long connectionPointer, String schemaName, String tableName)
            throws MonetDBEmbeddedException;

    /**
     * Internal implementation to close a connection.
     */
    private native void closeConnectionInternal(long connectionPointer);
}
