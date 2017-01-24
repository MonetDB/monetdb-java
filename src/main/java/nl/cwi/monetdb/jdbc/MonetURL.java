/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 * The URL class represents the URL datatype in MonetDB. It represents an URL, that is, a well-formed string conforming
 * to RFC2396.
 *
 * @author Fabian Groffen
 */
public class MonetURL implements SQLData {

	public static String FromString(String newurl) throws Exception {
		if (newurl == null) {
			return null;
		}
		// if above doesn't fail (throws an Exception), it is fine
		new URL(newurl);
		return newurl;
	}

	private String url;

	public MonetURL(String inet) throws Exception {
		this.url = FromString(inet);
	}

	@Override
	public String getSQLTypeName() {
		return "url";
	}

	@Override
	public void readSQL(SQLInput stream, String typeName) throws SQLException {
		if (typeName.compareTo("url") != 0)
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

	public URL getURL() throws SQLException {
		if (url == null)
			return null;

		try {
			return new URL(url);
		} catch (MalformedURLException mue) {
			throw new SQLException("data is not a valid URL", "M0M27");
		}
	}

	public void setURL(URL nurl) throws Exception {
		url = nurl.toString();
	}
}
