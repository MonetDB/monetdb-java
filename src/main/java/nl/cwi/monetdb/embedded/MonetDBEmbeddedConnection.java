/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.util.ArrayList;
import java.util.List;

/**
 * A single connection to a MonetDB database instance
 * Communication between Java and native C is done via JNI.
 * <br/>
 * <strong>Note</strong>: You can have only one Embedded MonetDB database running per JVM process.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedConnection {

    private final MonetDBEmbeddedDatabase database;

	private final long connectionPointer;

    private final List<AbstractStatementResult> results = new ArrayList<>();

    //TODO add autocommit
	protected MonetDBEmbeddedConnection(MonetDBEmbeddedDatabase database, long connectionPointer) {
        this.database = database;
        this.connectionPointer = connectionPointer;
	}

    /**
     * Gets the current schema set on the connection.
     *
     * @return A Java String with the name of the schema
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public String getCurrentSchema() throws MonetDBEmbeddedException {
        QueryResultSet eqr = this.sendQuery("select current_schema from sys.var();");
        QueryResultSetColumn<String> col = eqr.getColumn(0);
        String res = col.fetchFirstNColumnValues(1)[0];
        eqr.close();
        return res;
    }

    /**
     * Sets the current schema on the connection.
     *
     * @param currentSchema Java String with the name of the schema
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public void setCurrentSchema(String currentSchema) throws MonetDBEmbeddedException {
        String valueToSubmit = "'" + currentSchema.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "';";
        this.sendUpdate("SET SCHEMA " + valueToSubmit).close();
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
        UpdateResultSet res = this.createEmptyResultSetInternal(this.connectionPointer, query, true);
        results.add(res);
        return res;
    }

    /**
     * Executes a SQL query without a result set asynchronously.
     *
     * @param query The SQL query string
     * @return The update result object
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public UpdateResultSet sendUpdateAsync(String query) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.sendUpdate(query)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

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
        QueryResultSet res = this.createNonEmptyResultSetInternal(this.connectionPointer, query, true);
        results.add(res);
        return res;
	}

    /**
     * Executes an SQL query with a result set asynchronously.
     *
     * @param query The SQL query string
     * @return The query result object
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSet sendQueryAsync(String query) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.sendQuery(query)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /**
     * Creates a prepared query statement likewise the PreparedStatement in JDBC.
     *
     * @param query The SQL query with ? indicating the parameters to replace in the query
     * @return An instance of EmbeddedPreparedStatement
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public EmbeddedPreparedStatement createPreparedStatement(String query) throws MonetDBEmbeddedException {
        if (!query.endsWith(";")) {
            query += ";";
        }
        return this.createPreparedStatementInternal(this.connectionPointer, query);
    }

    /**
     * Creates a prepared query statement likewise the PreparedStatement in JDBC asynchronously.
     *
     * @param query The SQL query with ? indicating the parameters to replace in the query
     * @return An instance of EmbeddedPreparedStatement
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public EmbeddedPreparedStatement createPreparedStatementAsync(String query) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.createPreparedStatement(query)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /*public MonetDBTable getMonetDBTable(String schemaName, String tableName) throws MonetDBEmbeddedException {
        MonetDBTable res = this.getMonetDBTableInternal(schemaName, tableName, this.connectionPointer);
        //results.add(res);
        return res;
    }
        add the method from current schema
    public MonetDBTable getMonetDBTableAsync(String schema, String tableName) throws MonetDBEmbeddedException {
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
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
        String query =
                "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on tables.schema_id=schemas.id where tables.system=true order by sn, tn and schemas.name ='" +
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
     * @throws MonetDBEmbeddedException
     */
    public void removeTable(String schemaName, String tableName) throws MonetDBEmbeddedException {
        String query = "drop table " + schemaName + "." + tableName + ";";
        this.sendUpdate(query).close();
    }

    /**
     * Shuts down this connection. Any pending queries connections will be immediately closed as well.
     */
    public void shutdownConnection() {
        for(AbstractStatementResult res : this.results) {
            res.close();
        }
        this.shutdownConnectionInternal(this.connectionPointer);
        this.database.removeConnection(this);
    }

    /**
     * Shuts down this connection asynchronously. Any pending queries connections will be immediately closed as well.
     */
    public void shutdownConnectionAsync() {
        /* CompletableFuture.supplyAsync(() -> this.shutdownConnection()); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /**
     * Removes a query result from this connection.
     */
    protected void removeQueryResult(AbstractStatementResult res) {
        this.results.remove(res);
    }

	private native UpdateResultSet createEmptyResultSetInternal(long connectionPointer, String query, boolean execute)
            throws MonetDBEmbeddedException;

    private native QueryResultSet createNonEmptyResultSetInternal(long connectionPointer, String query, boolean execute)
            throws MonetDBEmbeddedException;

    private native EmbeddedPreparedStatement createPreparedStatementInternal(long connectionPointer, String query)
            throws MonetDBEmbeddedException;

    /*private native MonetDBTable getMonetDBTableInternal(long connectionPointer, String schemaName, String tableName)
            throws MonetDBEmbeddedException;*/

    private native void shutdownConnectionInternal(long connectionPointer);
}
