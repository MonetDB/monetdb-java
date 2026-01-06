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
import java.net.Inet4Address; // https://docs.oracle.com/javase/8/docs/api/java/net/Inet4Address.html
import java.sql.SQLDataException;
import java.sql.SQLException;

/**
 * The Inet4 class represents the 'inet4' datatype in MonetDB.
 * It represents an Internet Protocol version 4 (IPv4) address.
 *
 * The input format for Inet4 is a dotted quad string: d.d.d.d
 *
 * This class allows to retrieve the value of this Inet4 as InetAddress.
 */
public final class Inet4 implements java.sql.SQLData {
	private String inet4;

	@Override
	public String getSQLTypeName() {
		return "inet4";
	}

	@Override
	public void readSQL(final java.sql.SQLInput stream, final String typeName) throws SQLException {
		if (!"inet4".equals(typeName))
			throw new SQLException("can only use this class with 'inet4' type", "M1M05");
		fromString(stream.readString());
	}

	@Override
	public void writeSQL(final java.sql.SQLOutput stream) throws SQLException {
		stream.writeString(inet4);
	}

	@Override
	public String toString() {
		return inet4;
	}

	public void fromString(final String newinet) throws SQLException {
		if (newinet == null) {
			inet4 = null;
			return;
		}

		InetAddress inet;
		try {
			 inet = InetAddress.getByName(newinet);
		} catch (java.net.UnknownHostException uhe) {
			throw new SQLDataException("could not resolve IP address", "22M29");
		}
		if (!(inet instanceof Inet4Address))
			throw new SQLDataException("IPv4 address expected", "22M29");

		inet4 = inet.toString();
	}

	public String getAddress() {
		return inet4;
	}

	public void setAddress(final String newinet) throws SQLException {
		fromString(newinet);
	}

	public InetAddress getInetAddress() throws SQLException {
		if (inet4 == null)
			return null;

		try {
			return InetAddress.getByName(getAddress());
		} catch (java.net.UnknownHostException uhe) {
			throw new SQLDataException("could not resolve IP address", "22M29");
		}
	}

	public void setInetAddress(final InetAddress iaddr) throws SQLException {
		if (!(iaddr instanceof Inet4Address))
			throw new SQLDataException("IPv4 address expected", "22M29");
		fromString(iaddr.getHostAddress());
	}
}
