/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import nl.cwi.monetdb.embedded.column.Column;

import java.io.*;
import java.sql.SQLException;

/**
 * A single connection to a MonetDB database instance
 * Communication between Java and native C is done via JNI.
 * <br/>
 * <strong>Note</strong>: You can have only one nl.cwi.monetdb.embedded MonetDB database running per JVM process.
 */
public class MonetDBEmbeddedConnection {

	private final long connectionPointer;

	public MonetDBEmbeddedConnection(long connectionPointer) {
		this.connectionPointer = connectionPointer;
	}

	protected long getConnectionPointer() {
		return connectionPointer;
	}

	/**
	 * Execute an SQL query in an nl.cwi.monetdb.embedded database.
	 * 
	 * @param query The SQL query string
	 * @return The query result object, {@code null} if the database is not running
	 * @throws SQLException
	 */
	public EmbeddedQueryResult createQuery(String query) throws SQLException {
		String queryString = query;
		if (!queryString.endsWith(";")) {
			queryString += ";";
		}
		return queryWrapper(queryString, true, this.connectionPointer);
	}

    /**
     * Begins a transaction in nl.cwi.monetdb.embedded database.
     *
     * @throws SQLException
     */
	public void startTransaction() throws SQLException {
		this.createQuery("START TRANSACTION;").close();
	}

    /**
     * Commits a transaction in nl.cwi.monetdb.embedded database.
     *
     * @throws SQLException
     */
	public void commit() throws SQLException {
		this.createQuery("COMMIT;").close();
	}

    /**
     * Rollbacks a transaction in nl.cwi.monetdb.embedded database.
     *
     * @throws SQLException
     */
	public void rollback() throws SQLException {
		this.createQuery("ROLLBACK;").close();
	}

    /**
     * Performs a Lists the the existing tables with schemas on the system
     *
     * @param listSystemTables List system's tables as well (default true)
     * @return The query result object, {@code null} if the database is not running
     * @throws SQLException
     */
    public EmbeddedQueryResult listTables(boolean listSystemTables) throws SQLException {
        String query = "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on tables.schema_id=schemas.id";
        if (!listSystemTables) {
            query += " where tables.system=false order by sn, tn";
        }
        return this.createQuery(query + ";");
    }

    /**
     * Performs a SELECT * FROM a table  in nl.cwi.monetdb.embedded database.
     *
     * @param tableName The name of the table
     * @return The query result object, {@code null} if the database is not running
     * @throws SQLException
     */
	public EmbeddedQueryResult readTable(String tableName) throws SQLException {
        return this.createQuery("SELECT * FROM " + tableName + ";");
    }

    /**
     * Check if a table exists  in nl.cwi.monetdb.embedded database.
     *
     * @param tableName The name of the table
     * @return If a the table exists or not
     * @throws SQLException
     */
    public boolean checkTableExists(String tableName) throws SQLException {
        EmbeddedQueryResult eqr = this.listTables(true);
        Column<String> tablenames = (Column<String>) eqr.getColumn(0);
        boolean res = false;
        for (String str: tablenames.getAllValues()) {
            if(str.equals(tableName)) {
                res = true;
            }
        }
        eqr.close();
        return res;
    }

    /**
     * Lists the table fields and types in nl.cwi.monetdb.embedded database.
     *
     * @param tableName The name of the table
     * @return
     * @throws SQLException
     */
    public String[] listFields(String tableName) throws SQLException {
        if(!this.checkTableExists(tableName)) {
            throw  new SQLException("The table " + tableName + " doesn't exist!!");
        }
        EmbeddedQueryResult eqr = this.createQuery("select columns.name as name from sys.columns join sys.tables on columns.table_id=tables.id where tables.name='" + tableName + "';");
        String[] res = (String[]) eqr.getColumn(0).getAllValues();
        eqr.close();
        return res;
    }

	/**
	 * Execute an SQL query in an nl.cwi.monetdb.embedded database.
	 * 
	 * @param query The SQL query string
	 * @return The query result object, {@code null} if the database is not running
	 * @throws SQLException
	 */
	private native EmbeddedQueryResult queryWrapper(String query, boolean execute, long connectionPointer) throws SQLException;

}
