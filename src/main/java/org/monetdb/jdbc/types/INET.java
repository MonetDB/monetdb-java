/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.jdbc.types;

import java.net.InetAddress;
import java.sql.SQLDataException;
import java.sql.SQLException;

/**
 * The INET class represents the INET datatype in MonetDB.
 * It represents a IPv4 address with a certain mask applied.
 * Currently, IPv6 is not supported.
 *
 * The input format for INET is x.x.x.x/y where x.x.x.x is an IP address
 * and y is the number of bits in the netmask. If the /y part is left
 * off, then the netmask is 32, and the value represents just a single
 * host. On display, the /y portion is suppressed if the netmask is 32.
 *
 * This class allows to retrieve the value of this INET as InetAddress.
 * This is probably meaningful only and only if the netmask is 32.
 * The getNetmaskBits() method can be used to retrieve the subnet bits.
 */
public class INET implements java.sql.SQLData {	/* cannot (yet) be final as nl.cwi.monetdb.jdbc.types.INET extends this class */
	private String inet;

	@Override
	public String getSQLTypeName() {
		return "inet";
	}

	@Override
	public void readSQL(final java.sql.SQLInput stream, final String typeName) throws SQLException {
		if (!"inet".equals(typeName))
			throw new SQLException("can only use this class with 'inet' type", "M1M05");
		inet = stream.readString();
	}

	@Override
	public void writeSQL(final java.sql.SQLOutput stream) throws SQLException {
		stream.writeString(inet);
	}

	@Override
	public String toString() {
		return inet;
	}

	public void fromString(final String newinet) throws SQLException {
		if (newinet == null) {
			inet = newinet;
			return;
		}

		String tinet = newinet;
		final int slash = newinet.indexOf('/');
		if (slash != -1) {
			final int netmask;
			// ok, see if it is a valid netmask
			try {
				netmask = Integer.parseInt(newinet.substring(slash + 1));
			} catch (NumberFormatException nfe) {
				throw new SQLDataException("cannot parse netmask bits: " +
						newinet.substring(slash + 1), "22M29");
			}
			if (netmask <= 0 || netmask > 32)
				throw new SQLDataException("netmask must be >0 and <32", "22M29");
			tinet = newinet.substring(0, slash);
		}
		// check dotted quad
		final String quads[] = tinet.split("\\.");
		if (quads.length != 4)
			throw new SQLDataException("expected dotted quad (xxx.xxx.xxx.xxx)", "22M29");
		for (int i = 0; i < 4; i++) {
			final int quadv;
			try {
				quadv = Integer.parseInt(quads[i]);
			} catch (NumberFormatException nfe) {
				throw new SQLDataException("cannot parse number: " + quads[i], "22M29");
			}
			if (quadv < 0 || quadv > 255)
				throw new SQLDataException("value must be between 0 and 255: " + quads[i], "22M29");
		}
		// everything is fine
		inet = newinet;
	}

	public String getAddress() {
		if (inet == null)
			return null;

		// inet optionally has a /y part, if y < 32, chop it off
		final int slash = inet.indexOf('/');
		if (slash != -1)
			return inet.substring(0, slash);
		return inet;
	}

	public void setAddress(final String newinet) throws Exception {
		if (newinet == null) {
			inet = newinet;
			return;
		}
		if (newinet.indexOf('/') != -1)
			throw new Exception("IPv4 address cannot contain '/' " +
					"(use fromString() instead)");
		fromString(newinet);
	}

	public int getNetmaskBits() throws SQLException {
		if (inet == null)
			return 0;

		// if netmask is 32, it is omitted in the output
		int slash = inet.indexOf('/');
		if (slash == -1)
			return 32;
		try {
			return Integer.parseInt(inet.substring(slash + 1));
		} catch (NumberFormatException nfe) {
			throw new SQLDataException("cannot parse netmask bits: " +
					inet.substring(slash + 1), "22M29");
		}
	}

	public void setNetmaskBits(final int bits) throws Exception {
		String newinet = inet;
		if (newinet == null) {
			newinet = "0.0.0.0/" + bits;
		} else {
			final int slash = newinet.indexOf('/');
			if (slash != -1) {
				newinet = newinet.substring(0, slash + 1) + bits;
			} else {
				newinet = newinet + "/" + bits;
			}
		}
		fromString(newinet);
	}

	public InetAddress getInetAddress() throws SQLException {
		if (inet == null)
			return null;

		try {
			return InetAddress.getByName(getAddress());
		} catch (java.net.UnknownHostException uhe) {
			throw new SQLDataException("could not resolve IP address", "22M29");
		}
	}

	public void setInetAddress(final InetAddress iaddr) throws Exception {
		if (!(iaddr instanceof java.net.Inet4Address))
			throw new Exception("only IPv4 are supported currently");
		fromString(iaddr.getHostAddress());
	}
}
