/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.jdbc.types;

import java.net.InetAddress;
import java.net.Inet6Address; // https://docs.oracle.com/javase/8/docs/api/java/net/Inet6Address.html
import java.sql.SQLDataException;
import java.sql.SQLException;

/**
 * The Inet6 class represents the 'inet6' datatype in MonetDB.
 * It represents an Internet Protocol version 6 (IPv6) address.
 *
 * The input format for Inet6 is x:x:x:x:x:x:x:x where the 'x's are
 * the hexadecimal values of the eight 16-bit pieces of the address.
 *
 * This class allows to retrieve the value of this Inet6 as InetAddress.
 */
public final class Inet6 implements java.sql.SQLData {
	private String inet6;

	@Override
	public String getSQLTypeName() {
		return "inet6";
	}

	@Override
	public void readSQL(final java.sql.SQLInput stream, final String typeName) throws SQLException {
		if (!"inet6".equals(typeName))
			throw new SQLException("can only use this class with 'inet6' type", "M1M05");
		fromString(stream.readString());
	}

	@Override
	public void writeSQL(final java.sql.SQLOutput stream) throws SQLException {
		stream.writeString(inet6);
	}

	@Override
	public String toString() {
		return inet6;
	}

	public void fromString(final String newinet) throws SQLException {
		if (newinet == null) {
			inet6 = null;
			return;
		}

		InetAddress inet;
		try {
			 inet = InetAddress.getByName(newinet);
		} catch (java.net.UnknownHostException uhe) {
			throw new SQLDataException("could not resolve IP address", "22M29");
		}
		if (!(inet instanceof Inet6Address))
			throw new SQLDataException("IPv6 address expected", "22M29");

		inet6 = inet.toString();
	}

	public String getAddress() {
		return inet6;
	}

	public void setAddress(final String newinet) throws SQLException {
		fromString(newinet);
	}

	public InetAddress getInetAddress() throws SQLException {
		if (inet6 == null)
			return null;

		try {
			return InetAddress.getByName(getAddress());
		} catch (java.net.UnknownHostException uhe) {
			throw new SQLDataException("could not resolve IP address", "22M29");
		}
	}

	public void setInetAddress(final InetAddress iaddr) throws SQLException {
		if (!(iaddr instanceof Inet6Address))
			throw new SQLDataException("IPv6 address expected", "22M29");
		fromString(iaddr.getHostAddress());
	}
}
