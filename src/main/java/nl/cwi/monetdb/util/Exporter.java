/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

import java.io.PrintWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Exporter {
	protected PrintWriter out;
	protected boolean useSchema;

	protected Exporter(final PrintWriter out) {
		this.out = out;
	}

	public abstract void dumpSchema(
			final DatabaseMetaData dbmd,
			final String type,
			final String schema,
			final String name) throws SQLException;

	public abstract void dumpResultSet(final ResultSet rs) throws SQLException;

	public abstract void setProperty(final int type, final int value) throws Exception;
	public abstract int getProperty(final int type) throws Exception;

	//=== shared utilities

	public void useSchemas(final boolean use) {
		useSchema = use;
	}

	/**
	 * returns the given string between two double quotes for usage as
	 * identifier such as column or table or schema name in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static String dq(final String in) {
		return "\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"";
	}

	/**
	 * returns the given string between two single quotes for usage as
	 * string literal in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static String q(final String in) {
		return "'" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
	}

	/**
	 * Simple helper function to repeat a given character a number of
	 * times.
	 *
	 * @param chr the character to repeat
	 * @param cnt the number of times to repeat chr
	 * @return a String holding cnt times chr
	 */
	protected static String repeat(final char chr, final int cnt) {
		final char[] buf = new char[cnt];
		java.util.Arrays.fill(buf, chr);
		return new String(buf);
	}
}
