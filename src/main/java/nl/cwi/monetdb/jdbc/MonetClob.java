/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * The MonetClob class implements the {@link java.sql.Clob} interface.
 *
 * Because MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a {@link StringBuilder}.  It is more or less supplied to
 * enable an application that depends on it to run.  It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * sole reason for a Clob: avoidance of huge resource consumption.
 * <b>Use of this class is highly discouraged.</b>
 *
 * @author Fabian Groffen
 */
public final class MonetClob implements Clob {
	private StringBuilder buf;

	protected MonetClob(final String in) {
		buf = new StringBuilder(in);
	}

	/* internal utility method */
	final private void checkBufIsNotNull() throws SQLException {
		if (buf == null)
			throw new SQLException("This MonetClob has been freed", "M1M20");
	}

	//== begin interface Clob

	/**
	 * This method frees the Clob object and releases the resources the
	 * resources that it holds. The object is invalid once the free
	 * method is called.
	 *
	 * After free has been called, any attempt to invoke a method other
	 * than free will result in a SQLException being thrown. If free is
	 * called multiple times, the subsequent calls to free are treated
	 * as a no-op.
	 */
	@Override
	public void free() {
		buf = null;
	}

	/**
	 * Retrieves the CLOB value designated by this Clob object as an
	 * ascii stream.
	 *
	 * @return a java.io.InputStream object containing the CLOB data
	 * @throws SQLException - if there is an error accessing the CLOB value
	 */
	@Override
	public InputStream getAsciiStream() throws SQLException {
		checkBufIsNotNull();
		return new java.io.ByteArrayInputStream(buf.toString().getBytes());
	}

	/**
	 * Retrieves the CLOB value designated by this Clob object as a
	 * java.io.Reader object (or as a stream of characters).
	 *
	 * @return a java.io.Reader object containing the CLOB data
	 * @throws SQLException - if there is an error accessing the CLOB value
	 */
	@Override
	public Reader getCharacterStream() throws SQLException {
		checkBufIsNotNull();
		return new StringReader(buf.toString());
	}

	/**
	 * Returns a Reader object that contains a partial Clob value,
	 * starting with the character specified by pos, which is length
	 * characters in length.
	 *
	 * @param pos the offset to the first character of the partial value
	 *        to be retrieved. The first character in the Clob is at position 1.
	 * @param length the length in characters of the partial value to be retrieved.
	 * @return Reader through which the partial Clob value can be read.
	 * @throws SQLException - if pos is less than 1
	 *         or if pos is greater than the number of characters in the Clob
	 *         or if pos + length is greater than the number of characters in the Clob
	 */
	@Override
	public Reader getCharacterStream(final long pos, final long length) throws SQLException {
		// buf and input argument pos will be checked in method getSubString(long, int)
		if (length < 0 || length > Integer.MAX_VALUE) {
			throw new SQLException("Invalid length value: " + length, "M1M05");
		}
		return new StringReader(getSubString(pos, (int)length));
	}

	/**
	 * Retrieves a copy of the specified substring in the CLOB value
	 * designated by this Clob object. The substring begins at
	 * position pos and has up to length consecutive characters.
	 *
	 * @param pos the first character of the substring to be
	 *        extracted. The first character is at position 1.
	 * @param length the number of consecutive characters to be copied
	 * @return a String that is the specified substring in the
	 *         CLOB value designated by this Clob object
	 * @throws SQLException - if pos is less than 1
	 *         or if pos is greater than the number of characters in the Clob
	 *         or if pos + length is greater than the number of characters in the Clob
	 * @throws SQLException - if there is an error accessing the CLOB value
	 */
	@Override
	public String getSubString(final long pos, final int length) throws SQLException {
		checkBufIsNotNull();
		if (pos < 1 || pos > buf.length()) {
			throw new SQLException("Invalid pos value: " + pos, "M1M05");
		}
		if (length < 0 || pos -1 + length > buf.length()) {
			throw new SQLException("Invalid length value: " + length, "M1M05");
		}
		try {
			return buf.substring((int)(pos - 1), (int)(pos - 1 + length));
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M1M05");
		}
	}

	/**
	 * Retrieves the number of characters in the CLOB value designated
	 * by this Clob object.
	 *
	 * @return length of the CLOB in characters
	 * @throws SQLException if there is an error accessing the length
	 *         of the CLOB value
	 */
	@Override
	public long length() throws SQLException {
		checkBufIsNotNull();
		return (long)buf.length();
	}

	/**
	 * Retrieves the character position at which the specified Clob
	 * object searchstr appears in this Clob object. The search
	 * begins at position start.
	 *
	 * @param searchstr the Clob object for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the Clob object appears or
	 *         -1 if it is not present; the first position is 1
	 * @throws SQLException - if there is an error accessing the CLOB value
	 */
	@Override
	public long position(final Clob searchstr, final long start) throws SQLException {
		// buf and input argument start will be checked in method position(String, long)
		if (searchstr == null) {
			throw new SQLException("Missing searchstr object", "M1M05");
		}
		return position(searchstr.toString(), start);
	}

	/**
	 * Retrieves the character position at which the specified
	 * substring searchstr appears in the SQL CLOB value represented
	 * by this Clob object. The search begins at position start.
	 *
	 * @param searchstr the substring for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the substring appears or
	 *         -1 if it is not present; the first position is 1
	 * @throws SQLException - if there is an error accessing the CLOB value
	 */
	@Override
	public long position(final String searchstr, final long start) throws SQLException {
		checkBufIsNotNull();
		if (searchstr == null) {
			throw new SQLException("Missing searchstr object", "M1M05");
		}
		if (start < 1 || start > buf.length()) {
			throw new SQLException("Invalid start value: " + start, "M1M05");
		}
		return (long)(buf.indexOf(searchstr, (int)(start - 1)));
	}

	/**
	 * Retrieves a stream to be used to write Ascii characters to the CLOB value that this
	 * Clob object represents, starting at position pos. Characters written to the stream
	 * will overwrite the existing characters in the Clob object starting at the position pos.
	 * If the end of the Clob value is reached while writing characters to the stream,
	 * then the length of the Clob value will be increased to accomodate the extra characters.
	 *
	 * Note: If the value specified for pos is greater then the length+1 of the CLOB value
	 * then the behavior is undefined. Some JDBC drivers may throw a SQLException while
	 * other drivers may support this operation.
	 *
	 * @param pos - the position at which to start writing to this CLOB object; The first position is 1
	 * @return the stream to which ASCII encoded characters can be written
	 * @throws SQLException - if there is an error accessing the CLOB value or if pos is less than 1
	 * @throws SQLFeatureNotSupportedException - if the JDBC driver does not support this method
	 */
	@Override
	public java.io.OutputStream setAsciiStream(final long pos) throws SQLException {
		throw MonetWrapper.newSQLFeatureNotSupportedException("setAsciiStream");
	}

	/**
	 * Retrieves a stream to be used to write a stream of Unicode characters to the CLOB value that
	 * this Clob object represents, starting at position pos. Characters written to the stream
	 * will overwrite the existing characters in the Clob object starting at the position pos.
	 * If the end of the Clob value is reached while writing characters to the stream,
	 * then the length of the Clob value will be increased to accomodate the extra characters.
	 *
	 * Note: If the value specified for pos is greater then the length+1 of the CLOB value
	 * then the behavior is undefined. Some JDBC drivers may throw a SQLException while
	 * other drivers may support this operation.
	 *
	 * @param pos - the position at which to start writing to this CLOB object; The first position is 1
	 * @return the stream to which Unicode encoded characters can be written
	 * @throws SQLException - if there is an error accessing the CLOB value or if pos is less than 1
	 * @throws SQLFeatureNotSupportedException - if the JDBC driver does not support this method
	 */
	@Override
	public java.io.Writer setCharacterStream(final long pos) throws SQLException {
		throw MonetWrapper.newSQLFeatureNotSupportedException("setCharacterStream");
	}

	/**
	 * Writes the given Java String to the CLOB value that this Clob object designates at the position pos.
	 * The string will overwrite the existing characters in the Clob object starting at the position pos.
	 * If the end of the Clob value is reached while writing the given string,
	 * then the length of the Clob value will be increased to accomodate the extra characters.
	 *
	 * @param pos the position at which to start writing to the CLOB value that this Clob object represents
	 * @param str the string to be written to the CLOB value that this Clob designates
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the CLOB value or if pos is less than 1
	 */
	@Override
	public int setString(final long pos, final String str) throws SQLException {
		// buf will be checked in method setString(long, String, int, int)
		if (str == null) {
			throw new SQLException("Missing str object", "M1M05");
		}
		return setString(pos, str, 0, str.length());
	}

	/**
	 * Writes len characters of str, starting at character offset, to the CLOB value that this Clob represents.
	 * The string will overwrite the existing characters in the Clob object starting at the position pos.
	 * If the end of the Clob value is reached while writing the given string,
	 * then the length of the Clob value will be increased to accomodate the extra characters.
	 *
	 * @param pos the position at which to start writing to this CLOB object
	 * @param str the string to be written to the CLOB value that this Clob object represents
	 * @param offset the offset into str to start reading the characters to be written
	 * @param len the number of characters to be written
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the CLOB value or if pos is less than 1
	 */
	@Override
	public int setString(final long pos, final String str, final int offset, final int len)
		throws SQLException
	{
		checkBufIsNotNull();
		if (str == null) {
			throw new SQLException("Missing str object", "M1M05");
		}
		if (pos < 1 || pos > Integer.MAX_VALUE) {
			throw new SQLException("Invalid pos value: " + pos, "M1M05");
		}
		if (offset < 0 || offset > str.length()) {
			throw new SQLException("Invalid offset value: " + offset, "M1M05");
		}
		if (len < 1 || (offset + len) > str.length()) {
			throw new SQLException("Invalid len value: " + len, "M1M05");
		}

		final int ipos = (int) pos;
		if ((ipos + len) > buf.capacity()) {
			buf.ensureCapacity(ipos + len);
		}
		buf.replace(ipos - 1, ipos + len, str.substring(offset, (offset + len)));
		return len;
	}

	/**
	 * Truncates the CLOB value that this Clob designates to
	 * have a length of len characters.
	 *
	 * @param len the length, in bytes, to which the CLOB value should be truncated
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value or if len is less than 0
	 */
	@Override
	public void truncate(final long len) throws SQLException {
		checkBufIsNotNull();
		if (len < 0 || len > buf.length()) {
			throw new SQLException("Invalid len value: " + len, "M1M05");
		}
		buf.delete((int)len, buf.length());
		// Attempts to reduce storage used for the character sequence.
		buf.trimToSize();
	}


	/**
	 * Returns a String from this MonetClob buf.
	 *
	 * @return the String this MonetClob wraps or empty string when this MonetClob was freed.
	 */
	final public String toString() {
		return (buf != null) ? buf.toString() : "";
	}
}
