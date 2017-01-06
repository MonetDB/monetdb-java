/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.io.*;

/**
 * The MonetClob class implements the {@link java.sql.Clob} interface.  Because
 * MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a {@link StringBuilder}.  It is more or less supplied to
 * enable an application that depends on it to run.  It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * sole reason for a Clob: avoidance of huge resource consumption.
 * <b>Use of this class is highly discouraged.</b>
 *
 * @author Fabian Groffen
 */
public class MonetClob implements Clob, Serializable, Comparable<MonetClob> {
	
	private final StringBuilder buffer;

	public MonetClob(String in) {
		buffer = new StringBuilder(in);
	}

	public MonetClob(char[] toParse, int startPosition, int count) {
		buffer = new StringBuilder(new String(toParse, startPosition, count));
	}

	//== begin interface Clob
	
	/**
	 * This method frees the Clob object and releases the resources the resources that it holds. The object is invalid
	 * once the free method is called.
	 *
	 * After free has been called, any attempt to invoke a method other than free will result in a SQLException being
	 * thrown. If free is called multiple times, the subsequent calls to free are treated as a no-op.
	 */
	@Override
	public void free() {
		buffer.setLength(0);
	}

	/**
	 * Retrieves the CLOB value designated by this Clob object as an ascii stream.
	 *
	 * @return a java.io.InputStream object containing the CLOB data
     * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public InputStream getAsciiStream() throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		return new ByteArrayInputStream(buffer.toString().getBytes());
	}

	/**
	 * Retrieves the CLOB value designated by this Clob object as a java.io.Reader object
	 * (or as a stream of characters).
	 *
	 * @return a java.io.Reader object containing the CLOB data
     * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public Reader getCharacterStream() throws SQLException {
        if (buffer.length() == 0)
            throw new SQLException("This Clob has been freed", "M1M20");
        return new StringReader(buffer.toString());
	}

	/**
	 * Returns a Reader object that contains a partial Clob value, starting with the character specified by pos, which
	 * is length characters in length.
	 *
	 * @param pos the offset to the first character of the partial value to be retrieved. The first character in the
	 * Clob is at position 1.
	 * @param length the length in characters of the partial value to be retrieved.
	 * @return Reader through which the partial Clob value can be read.
     * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public Reader getCharacterStream(long pos, long length) throws SQLException {
        if (buffer.length() == 0)
            throw new SQLException("This Clob has been freed", "M1M20");
        return new StringReader(buffer.substring((int)(pos - 1), (int)(pos - 1 + length)));
	}

	/**
	 * Retrieves a copy of the specified substring in the CLOB value designated by this Clob object. The substring
	 * begins at position pos and has up to length consecutive characters.
	 *
	 * @param pos the first character of the substring to be extracted. The first character is at position 1.
	 * @param length the number of consecutive characters to be copied
	 * @return a String that is the specified substring in the CLOB value designated by this Clob object
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public String getSubString(long pos, int length) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		try {
			return buffer.substring((int)(pos - 1), (int)(pos - 1 + length));
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage());
		}
	}

	/**
	 * Retrieves the number of characters in the CLOB value designated by this Clob object.
	 *
	 * @return length of the CLOB in characters
	 * @throws SQLException if there is an error accessing the length of the CLOB value
	 */
	@Override
	public long length() throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		return (long) buffer.length();
	}

	/**
	 * Retrieves the character position at which the specified Clob object searchstr appears in this Clob object.
	 * The search begins at position start.
	 *
	 * @param searchstr the Clob object for which to search
	 * @param start the position at which to begin searching; the first position is 1
	 * @return the position at which the Clob object appears or -1 if it is not present; the first position is 1
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public long position(Clob searchstr, long start) throws SQLException {
		return position(searchstr.getSubString(1L, (int)(searchstr.length())), start);
	}

	/**
	 * Retrieves the character position at which the specified substring searchstr appears in the SQL CLOB value
	 * represented by this Clob object.  The search begins at position start.
	 *
	 * @param searchstr the substring for which to search
	 * @param start the position at which to begin searching; the first position is 1
	 * @return the position at which the substring appears or -1 if it is not present; the first position is 1
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public long position(String searchstr, long start) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		return (long)(buffer.indexOf(searchstr, (int)(start - 1)));
	}

	@Override
	public OutputStream setAsciiStream(long pos) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		throw new SQLException("Operation setAsciiStream(long pos) currently not supported", "0A000");
	}

	@Override
	public Writer setCharacterStream(long pos) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		throw new SQLException("Operation setCharacterStream(long pos) currently not supported", "0A000");
	}

	/**
	 * Writes the given Java String to the CLOB  value that this Clob object designates at the position pos.
	 *
	 * @param pos the position at which to start writing to the CLOB value that this Clob object represents
	 * @param str the string to be written to the CLOB value that this Clob designates
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public int setString(long pos, String str) throws SQLException {
		return setString(pos, str, 1, str.length());
	}

	/**
	 * Writes len characters of str, starting at character offset, to the CLOB value that this Clob represents.
	 *
	 * @param pos the position at which to start writing to this CLOB object
	 * @param str the string to be written to the CLOB value that this Clob object represents
	 * @param offset the offset into str to start reading the characters to be written
	 * @param len the number of characters to be written
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public int setString(long pos, String str, int offset, int len) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");

		int buflen = buffer.length();
		int retlen = Math.min(buflen, (int)(pos - 1 + len));
		
		if (retlen > 0) {
			buffer.replace((int)(pos - 1), (int)(pos + retlen), str.substring(offset - 1, (offset + len)));
			return retlen;
		} else {
			return 0;
		}
	}

	/**
	 * Truncates the CLOB value that this Clob designates to have a length of len characters.
	 *
	 * @param len the length, in bytes, to which the CLOB value should be truncated
	 * @throws SQLException if there is an error accessing the CLOB value
	 */
	@Override
	public void truncate(long len) throws SQLException {
		if (buffer.length() == 0)
			throw new SQLException("This Clob has been freed", "M1M20");
		buffer.setLength((int) len);
	}

	/**
	 * Returns the String behind this Clob.  This is a MonetClob extension that does not violate nor is described in
	 * the Clob interface.
	 *
	 * @return the String this Clob wraps.
	 */
	@Override
	public String toString() {
		if (buffer.length() == 0)
			return "<a freed MonetClob instance>";
		return buffer.toString();
	}

	/**
	 * Overriding the equals method for the byte array.
	 */
	@Override
	public boolean equals(Object obj) {
		return obj instanceof MonetClob && this.toString().equals(obj.toString());
	}

	/**
	 * Overriding the hashCode method for the byte array.
	 */
	@Override
	public int hashCode() { return this.buffer.toString().hashCode(); }

	/**
	 * Adding the compare to method.
	 */
	@Override
	public int compareTo(MonetClob o) {
		return this.toString().compareTo(o.toString());
	}
}
