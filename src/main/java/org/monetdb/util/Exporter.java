/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.util;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Exporter {
	protected PrintWriter out;
	protected boolean useSchema;

	protected Exporter(final PrintWriter out) {
		this.out = out;
	}

	public abstract void dumpSchema(
			final java.sql.DatabaseMetaData dbmd,
			final String type,
			final String schema,
			final String name) throws SQLException;

	public abstract void dumpResultSet(final ResultSet rs) throws SQLException;

	public abstract void setProperty(final int type, final int value) throws Exception;
	public abstract int getProperty(final int type) throws Exception;


	//=== shared utilities

	public final void useSchemas(final boolean use) {
		useSchema = use;
	}

	/**
	 * returns the given string between two double quotes for usage as
	 * identifier such as column or table or schema name in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static final String dq(final String in) {
		return "\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"";
	}

	/**
	 * returns the given string between two single quotes for usage as
	 * string literal in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static final String q(final String in) {
		return "'" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
	}

	/**
	 * Simple helper function to repeat a given character a number of times.
	 *
	 * @param chr the character to repeat
	 * @param cnt the number of times to repeat chr
	 * @return a String holding cnt times chr
	 */
	protected static final String repeat(final char chr, final int cnt) {
		final char[] buf = new char[cnt];
		java.util.Arrays.fill(buf, chr);
		return new String(buf);
	}

	/**
	 * Utility method to fetch the "query" value from sys.tables for a specific view or table in a specific schema
	 * The "query" value contains the original SQL view creation text or the ON clause text when it is a REMOTE TABLE
	 *
	 * @param con the JDBC connection, may not be null
	 * @param schema the schem name, may not be null or empty
	 * @param name the view or table name, may not be null or empty
	 * @return the value of the "query" field for the specified view/table name and schema. It can return null.
	 */
	protected static final String fetchSysTablesQueryValue(
		final java.sql.Connection con,
		final String schema,
		final String name)
	{
		java.sql.Statement stmt = null;
		ResultSet rs = null;
		String val = null;
		try {
			stmt = con.createStatement();
			final String cmd = "SELECT query FROM sys.tables WHERE name = '" + name
				+ "' and schema_id IN (SELECT id FROM sys.schemas WHERE name = '" + schema + "')";
			rs = stmt.executeQuery(cmd);
			if (rs != null) {
				if (rs.next()) {
					val = rs.getString(1);
				}
			}
		} catch (SQLException se) {
			/* ignore */
		} finally {
			// free resources
			if (rs != null) {
				try { rs.close(); } catch (SQLException se) { /* ignore */ }
			}
			if (stmt != null) {
				try { stmt.close(); } catch (SQLException se) { /* ignore */ }
			}
		}
		return val;
	}
}
