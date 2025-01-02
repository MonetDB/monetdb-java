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

package org.monetdb.mcl.parser;

import java.nio.Buffer;	// needed as some CharBuffer overridden methods (mark() and reset()) return type changed between Java 8 (return Buffer) and 9 (or higher) (return CharBuffer)
import java.nio.CharBuffer;

/**
 * The StartOfHeaderParser allows easy examination of a start of header
 * line.  It does not fit into the general MCLParser framework because
 * it uses a different interface.  While the parser is very shallow, it
 * requires the caller to know about the header lines that are parsed.
 * All this parser does is detect the (valid) type of a soheader, and
 * allow to return the fields in it as integer or string.  An extra
 * bonus is that it can return if another field should be present in the
 * soheader.
 *
 * @author Fabian Groffen
 */
public final class StartOfHeaderParser {
	private CharBuffer soh = null;
	private int len;
	private int pos;

	/* Query types (copied from sql/include/sql_querytype.h) */
	/** A parse response (not handled) */
	public final static int Q_PARSE   = '0';
	/** A tabular response (typical ResultSet) */
	public final static int Q_TABLE   = '1';
	/** A response to an update statement, contains number of affected
	 * rows and generated key-id */
	public final static int Q_UPDATE  = '2';
	/** A response to a schema update */
	public final static int Q_SCHEMA  = '3';
	/** A response to a transation statement (start, rollback, abort,
	 * commit) */
	public final static int Q_TRANS   = '4';
	/** A tabular response in response to a PREPARE statement containing
	 * information about the wildcard values that need to be supplied */
	public final static int Q_PREPARE = '5';
	/** A tabular continuation response (for a ResultSet) */
	public final static int Q_BLOCK   = '6';


	public final int parse(final String in) throws MCLParseException {
		soh = CharBuffer.wrap(in);
		soh.get();	// skip the &
		final int type = soh.get();
		switch (type) {
			case Q_PARSE:
			case Q_SCHEMA:
				len = 0;
				break;
			case Q_TABLE:
			case Q_PREPARE:
				len = 4;
				soh.get();
				break;
			case Q_UPDATE:
				len = 2;
				soh.get();
				break;
			case Q_TRANS:
				len = 1;
				soh.get();
				break;
			case Q_BLOCK:
				len = 3;
				soh.get();
				break;
			default:
				throw new MCLParseException("invalid or unknown header", 1);
		}
		pos = 0;
		return type;
	}

/* MvD: disabled hasNext() method as it is never called.
	public final boolean hasNext() {
		return pos < len;
	}
*/

	/**
	 * Returns the next token in the CharBuffer as integer. The value is
	 * considered to end at the end of the CharBuffer or at a space.  If
	 * a non-numeric character is encountered an MCLParseException is thrown.
	 *
	 * @return The next token in the CharBuffer as integer
	 * @throws MCLParseException if no numeric value could be read
	 */
	public final int getNextAsInt() throws MCLParseException {
		return (int) getNextAsLong();
	}

	/**
	 * Returns the next token in the CharBuffer as long integer. The value
	 * is considered to end at the end of the CharBuffer or at a space.
	 * If a non-numeric character is encountered an MCLParseException is thrown.
	 *
	 * @return The next token in the CharBuffer as long integer
	 * @throws MCLParseException if no numeric value could be read
	 */
	public final long getNextAsLong() throws MCLParseException {
		pos++;
		if (!soh.hasRemaining())
			throw new MCLParseException("unexpected end of string", soh.position() - 1);

		boolean positive = true;
		char chr = soh.get();
		// note: don't use Character.isDigit() here, because
		// we only want ISO-LATIN-1 digits
		if (chr == '-') {
			positive = false;
			if (!soh.hasRemaining())
				throw new MCLParseException("unexpected end of string", soh.position() - 1);
			chr = soh.get();
		}

		long tmp = 0;
		if (chr >= '0' && chr <= '9') {
			tmp = (int)chr - (int)'0';
		} else {
			throw new MCLParseException("expected a digit", soh.position() - 1);
		}

		while (soh.hasRemaining() && (chr = soh.get()) != ' ') {
			if (chr >= '0' && chr <= '9') {
				tmp *= 10;
				tmp += (int)chr - (int)'0';
			} else {
				throw new MCLParseException("expected a digit", soh.position() - 1);
			}
		}

		return positive ? tmp : -tmp;
	}

	public final String getNextAsString() throws MCLParseException {
		pos++;
		if (!soh.hasRemaining())
			throw new MCLParseException("unexpected end of string", soh.position() - 1);

		int cnt = 0;
		((Buffer)soh).mark();
		while (soh.hasRemaining() && soh.get() != ' ') {
			cnt++;
		}
		((Buffer)soh).reset();

		return soh.subSequence(0, cnt).toString();
	}
}
