/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 *<pre>
 * The MonetBlob class implements the {@link java.sql.Blob} interface.
 *
 * Because MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a byte[]. It is more or less supplied to
 * enable an application that depends on it to run. It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * benefits for a Blob: avoidance of huge resource consumption.
 * <b>Use of this class is highly discouraged.</b>
 *</pre>
 *
 * @author Fabian Groffen
 */
public final class MonetBlob implements Blob {
	private byte[] buf;

	/* constructors */
	protected MonetBlob(final byte[] data) {
		buf = data;
	}

	protected MonetBlob(final String hexString) {
		buf = hexStrToByteArray(hexString);
	}


	/* class utility methods */
	static final byte[] hexStrToByteArray(final String hexString) {
		// unpack the HEX (BLOB) notation to real bytes
		final int len = hexString.length() / 2;
		final byte[] buf = new byte[len];
		for (int i = 0; i < len; i++) {
//	was		buf[i] = (byte)Integer.parseInt(hexString.substring(2 * i, (2 * i) + 2), 16);
			buf[i] = (byte) ((Character.digit(hexString.charAt(2 * i), 16) << 4)
					+ Character.digit(hexString.charAt((2 * i) +1), 16));
		}
		return buf;
	}

	/* internal utility method */
	private final void checkBufIsNotNull() throws SQLException {
		if (buf == null)
			throw new SQLException("This MonetBlob has been freed", "M1M20");
	}

	//== begin interface Blob

	/**
	 * This method frees the Blob object and releases the resources that
	 * it holds. The object is invalid once the free method is called.
	 *
	 * After free has been called, any attempt to invoke a method other
	 * than free will result in a SQLException being thrown. If free is
	 * called multiple times, the subsequent calls to free are treated
	 * as a no-op.
	 *
	 * @throws SQLException if an error occurs releasing the Blob's resources
	 */
	@Override
	public void free() throws SQLException {
		buf = null;
	}

	/**
	 * Retrieves the BLOB value designated by this Blob instance as a stream.
	 *
	 * @return a stream containing the BLOB data
	 * @throws SQLException if there is an error accessing the BLOB value
	 */
	@Override
	public InputStream getBinaryStream() throws SQLException {
		checkBufIsNotNull();
		return new ByteArrayInputStream(buf);
	}

	/**
	 * Returns an InputStream object that contains a partial Blob value,
	 * starting with the byte specified by pos, which is length bytes in
	 * length.
	 *
	 * @param pos the offset to the first byte of the partial value to
	 *        be retrieved. The first byte in the Blob is at position 1
	 * @param length the length in bytes of the partial value to be retrieved
	 * @return InputStream through which the partial Blob value can be read.
	 * @throws SQLException if pos is less than 1 or if pos is
	 *         greater than the number of bytes in the Blob or if pos +
	 *         length is greater than the number of bytes in the Blob
	 */
	@Override
	public InputStream getBinaryStream(final long pos, final long length)
		throws SQLException
	{
		checkBufIsNotNull();
		if (pos < 1 || pos > buf.length) {
			throw new SQLException("Invalid pos value: " + pos, "M1M05");
		}
		if (length < 0 || pos - 1 + length > buf.length) {
			throw new SQLException("Invalid length value: " + length, "M1M05");
		}
		return new ByteArrayInputStream(buf, (int) pos - 1, (int) length);
	}

	/**
	 * Retrieves all or part of the BLOB value that this Blob object
	 * represents, as an array of bytes. This byte array contains up to
	 * length consecutive bytes starting at position pos.
	 *
	 * @param pos the ordinal position of the first byte in the BLOB
	 *        value to be extracted; the first byte is at position 1.
	 * @param length the number of consecutive bytes to be copied
	 * @return a byte array containing up to length consecutive bytes
	 *         from the BLOB value designated by this Blob object,
	 *         starting with the byte at position pos.
	 * @throws SQLException if there is an error accessing the BLOB value
	 */
	@Override
	public byte[] getBytes(final long pos, final int length) throws SQLException {
		checkBufIsNotNull();
		if (pos < 1 || pos > buf.length) {
			throw new SQLException("Invalid pos value: " + pos, "M1M05");
		}
		if (length < 0 || pos - 1 + length > buf.length) {
			throw new SQLException("Invalid length value: " + length, "M1M05");
		}

		try {
			return java.util.Arrays.copyOfRange(buf, (int) pos - 1, (int) pos - 1 + length);
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
	}

	/**
	 * Returns the number of bytes in the BLOB value designated by this
	 * Blob object.
	 *
	 * @return length of the BLOB in bytes
	 * @throws SQLException if there is an error accessing the length
	 *         of the BLOB value
	 */
	@Override
	public long length() throws SQLException {
		checkBufIsNotNull();
		return (long)buf.length;
	}

	/**
	 * Retrieves the byte position in the BLOB value designated by this
	 * Blob object at which pattern begins. The search begins at position start.
	 *
	 * @param pattern the Blob object designating the BLOB value for
	 *        which to search
	 * @param start the position in the BLOB value at which to begin
	 *        searching; the first position is 1
	 * @return the position at which the pattern begins, else -1
	 * @throws SQLException if there is an error accessing the BLOB value
	 */
	@Override
	public long position(final Blob pattern, final long start) throws SQLException {
		if (pattern == null) {
			throw new SQLException("Missing pattern object", "M1M05");
		}
		// buf and input argument start will be checked in method position(byte{}, long)
		return position(pattern.getBytes(1L, (int)pattern.length()), start);
	}

	/**
	 * Retrieves the byte position at which the specified byte array
	 * pattern begins within the BLOB value that this Blob object
	 * represents. The search for pattern begins at position start.
	 *
	 * @param pattern the byte array for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the pattern appears, else -1
	 * @throws SQLException if there is an error accessing the BLOB value
	 */
	@Override
	public long position(final byte[] pattern, final long start) throws SQLException {
		checkBufIsNotNull();
		if (pattern == null) {
			throw new SQLException("Missing pattern object", "M1M05");
		}
		if (start < 1 || start > buf.length) {
			throw new SQLException("Invalid start value: " + start, "M1M05");
		}
		try {
			final int patternLength = pattern.length;
			final int maxPos = buf.length - patternLength;
			for (int i = (int)(start - 1); i < maxPos; i++) {
				int j;
				for (j = 0; j < patternLength; j++) {
					if (buf[i + j] != pattern[j])
						break;
				}
				if (j == patternLength)
					// found a match
					return i;
			}
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
		return -1;
	}

	/**
	 * Retrieves a stream that can be used to write to the BLOB value
	 * that this Blob object represents. The stream begins at position
	 * pos. The bytes written to the stream will overwrite the existing
	 * bytes in the Blob object starting at the position pos. If the end
	 * of the Blob value is reached while writing to the stream, then
	 * the length of the Blob value will be increased to accomodate the
	 * extra bytes.
	 *
	 * @param pos the position in the BLOB value at which to start
	 *            writing; the first position is 1
	 * @return a java.io.OutputStream object to which data can be written
	 * @throws SQLException if there is an error accessing the BLOB
	 *         value or if pos is less than 1
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does
	 *         not support this method
	 */
	@Override
	public OutputStream setBinaryStream(final long pos) throws SQLException {
		throw MonetWrapper.newSQLFeatureNotSupportedException("setBinaryStream");
	}

	/**
	 * Writes the given array of bytes to the BLOB value that this Blob
	 * object represents, starting at position pos, and returns the
	 * number of bytes written.
	 *
	 * @param pos the position in the BLOB object at which to start writing
	 * @param bytes the array of bytes to be written to the BLOB value
	 *        that this Blob object represents
	 * @return the number of bytes written
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value or if pos is less than 1
	 */
	@Override
	public int setBytes(final long pos, final byte[] bytes) throws SQLException {
		// buf and input arguments will be checked in method setBytes(long, byte{}, int, int)
		final int len = (bytes != null) ? bytes.length : 0;
		return setBytes(pos, bytes, 1, len);
	}

	/**
	 * Writes all or part of the given byte array to the BLOB value that
	 * this Blob object represents and returns the number of bytes written.
	 * Writing starts at position pos in the BLOB value; len bytes from
	 * the given byte array are written.
	 *
	 * @param pos the position in the BLOB object at which to start writing
	 * @param bytes the array of bytes to be written to this BLOB object
	 * @param offset the offset into the array bytes at which to start
	 *        reading the bytes to be set
	 * @param len the number of bytes to be written to the BLOB value
	 *        from the array of bytes bytes
	 * @return the number of bytes written
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value or if pos is less than 1
	 */
	@Override
	public int setBytes(final long pos, final byte[] bytes, int offset, final int len)
		throws SQLException
	{
		checkBufIsNotNull();
		if (bytes == null) {
			throw new SQLException("Missing bytes[] object", "M1M05");
		}
		if (pos < 1 || pos > buf.length) {
			throw new SQLException("Invalid pos value: " + pos, "M1M05");
		}
		if (len < 0 || pos + len > buf.length) {
			throw new SQLException("Invalid len value: " + len, "M1M05");
		}
		if (offset < 0 || offset > bytes.length) {
			throw new SQLException("Invalid offset value: " + offset, "M1M05");
		}

		try {
			offset--;
			/* transactions? what are you talking about? */
            if (len - (int) pos >= 0)
                System.arraycopy(bytes, offset + (int) pos, buf, (int) pos, len - (int) pos);
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
		return len;
	}

	/**
	 * Truncates the BLOB value that this Blob object represents to be
	 * len bytes in length.
	 *
	 * @param len the length, in bytes, to which the BLOB value
	 *        should be truncated
	 * @throws SQLException if there is an error accessing the BLOB value
	 */
	@Override
	public void truncate(final long len) throws SQLException {
		checkBufIsNotNull();
		if (len < 0 || len > buf.length) {
			throw new SQLException("Invalid len value: " + len, "M1M05");
		}
		if (buf.length > len) {
			buf = java.util.Arrays.copyOf(buf, (int)len);
		}
	}
}
