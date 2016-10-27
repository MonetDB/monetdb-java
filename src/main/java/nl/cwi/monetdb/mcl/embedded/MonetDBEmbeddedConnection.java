/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded;

import java.io.*;
import java.sql.SQLException;

import nl.cwi.monetdb.mcl.embedded.result.EmbeddedQueryResult;

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
	public EmbeddedQueryResult createQuery(String query) throws SQLException, IOException {
		String queryString = query;
		if (!queryString.endsWith(";")) {
			queryString += ";";
		}
		return queryWrapper(queryString, true, this.connectionPointer);
	}

	/**
	 * Execute an SQL query in an nl.cwi.monetdb.embedded database.
	 * 
	 * @param query The SQL query string
	 * @return The query result object, {@code null} if the database is not running
	 * @throws SQLException
	 */
	private native EmbeddedQueryResult queryWrapper(String query, boolean execute, long connectionPointer) throws SQLException, IOException;

}
