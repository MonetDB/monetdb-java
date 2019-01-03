/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc.types;

import java.net.MalformedURLException;
import java.sql.SQLData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 * The URL class represents the URL datatype in MonetDB.
 * It represents an URL, that is, a well-formed string conforming to RFC2396.
 */
public class URL implements SQLData {
	private String url;

	@Override
	public String getSQLTypeName() {
		return "url";
	}

	@Override
	public void readSQL(SQLInput stream, String typeName) throws SQLException {
		if (!"url".equals(typeName))
			throw new SQLException("can only use this class with 'url' type", "M1M05");
		url = stream.readString();
	}

	@Override
	public void writeSQL(SQLOutput stream) throws SQLException {
		stream.writeString(url);
	}

	@Override
	public String toString() {
		return url;
	}

	public void fromString(String newurl) throws Exception {
		if (newurl == null) {
			url = newurl;
			return;
		}
		new java.net.URL(newurl);
		// if above doesn't fail (throws an Exception), it is fine
		url = newurl;
	}

	public java.net.URL getURL() throws SQLException {
		if (url == null)
			return null;

		try {
			return new java.net.URL(url);
		} catch (MalformedURLException mue) {
			throw new SQLDataException("data is not a valid URL: " + mue.getMessage(), "22M30");
		}
	}

	public void setURL(java.net.URL nurl) throws Exception {
		url = nurl.toString();
	}
}
