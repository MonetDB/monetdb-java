/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.jdbc.types;

import java.sql.SQLDataException;
import java.sql.SQLException;

/**
 * The URL class represents the URL datatype in MonetDB.
 * It represents an URL, that is, a well-formed string conforming to RFC2396.
 */
public final class URL implements java.sql.SQLData {
	/** String url */
	private String url;

	/**
	 * String getSQLTypeName()
	 * @return String the type name
	 */
	@Override
	public String getSQLTypeName() {
		return "url";
	}

	/**
	 * void readSQL(final java.sql.SQLInput stream, final String typeName)
	 * @param stream a java.sql.SQLInput stream
	 * @param typeName the type name
	 */
	@Override
	public void readSQL(final java.sql.SQLInput stream, final String typeName) throws SQLException {
		if (!"url".equals(typeName))
			throw new SQLException("can only use this class with 'url' type", "M1M05");
		url = stream.readString();
	}

	/**
	 * void writeSQL(final java.sql.SQLOutput stream)
	 * @param stream a java.sql.SQLOutput stream
	 */
	@Override
	public void writeSQL(final java.sql.SQLOutput stream) throws SQLException {
		stream.writeString(url);
	}

	/**
	 * String toString()
	 * @return String the url string
	 */
	@Override
	public String toString() {
		return url;
	}

	/**
	 * void fromString(final String newurl)
	 * @param newurl the new url string
	 * @throws Exception when conversion of newurl string to URI or URL object fails
	 */
	public void fromString(final String newurl) throws Exception {
		if (newurl == null) {
			url = newurl;
			return;
		}

		// parse the newurl on validity
		// Note: as of Java version 20 java.net.URL(String) constructor is deprecated.
		// https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/net/URL.html#%3Cinit%3E(java.lang.String)
		new java.net.URI(newurl).toURL();
		// if above doesn't fail (throws an java.net.URISyntaxException | java.net.MalformedURLException), it is fine
		url = newurl;
	}

	/**
	 * java.net.URL getURL()
	 * @return URL an url object
	 * @throws SQLDataException when conversion of url string to URL object fails
	 */
	public java.net.URL getURL() throws SQLDataException {
		if (url == null)
			return null;

		try {
			// Note: as of java 20 java.net.URL(String) constructor is deprecated.
			// https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/net/URL.html#%3Cinit%3E(java.lang.String)
			return new java.net.URI(url).toURL();
		} catch (java.net.URISyntaxException | java.net.MalformedURLException mue) {
			throw new SQLDataException("data is not a valid URL: " + mue.getMessage(), "22M30");
		}
	}

	/**
	 * void setURL(final java.net.URL nurl)
	 * @param nurl a java.net.URL object
	 */
	public void setURL(final java.net.URL nurl) {
		url = nurl.toString();
	}
}
