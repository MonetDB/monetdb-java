/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc.types;

import java.sql.SQLException;

/**
 * The URL class represents the URL datatype in MonetDB.
 * It represents an URL, that is, a well-formed string conforming to RFC2396.
 */
public final class URL implements java.sql.SQLData {
	private String url;

	@Override
	public String getSQLTypeName() {
		return "url";
	}

	@Override
	public void readSQL(final java.sql.SQLInput stream, final String typeName) throws SQLException {
		if (!"url".equals(typeName))
			throw new SQLException("can only use this class with 'url' type", "M1M05");
		url = stream.readString();
	}

	@Override
	public void writeSQL(final java.sql.SQLOutput stream) throws SQLException {
		stream.writeString(url);
	}

	@Override
	public String toString() {
		return url;
	}

	public void fromString(final String newurl) throws Exception {
		if (newurl == null) {
			url = newurl;
			return;
		}

		// parse the newurl on validity
		new java.net.URL(newurl);
		// if above doesn't fail (throws an Exception), it is fine
		url = newurl;
	}

	public java.net.URL getURL() throws SQLException {
		if (url == null)
			return null;

		try {
			return new java.net.URL(url);
		} catch (java.net.MalformedURLException mue) {
			throw new java.sql.SQLDataException("data is not a valid URL: " + mue.getMessage(), "22M30");
		}
	}

	public void setURL(final java.net.URL nurl) throws Exception {
		url = nurl.toString();
	}
}
