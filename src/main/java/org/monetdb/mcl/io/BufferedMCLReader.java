/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.mcl.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

/**
 * Read text from a character-input stream, buffering characters so as
 * to provide a means for efficient reading of characters, arrays and
 * lines.  This class is based on the BufferedReader class, and provides
 * extra functionality useful for MCL.
 *
 * The BufferedMCLReader is typically used as layer inbetween an
 * InputStream and a specific interpreter of the data.
 * <pre>
 *                         / Response
 * BufferedMCLReader ---o &lt;- Tuple
 *                         \ DataBlock
 * </pre>
 * Because the BufferedMCLReader provides an efficient way to access the
 * data from the stream in a linewise fashion, whereby each line is
 * identified as a certain type, consumers can easily decide how to
 * parse each retrieved line.  The line parsers from
 * org.monetdb.mcl.parser are well suited to work with the lines
 * outputted by the BufferedMCLReader.
 * This class is client-oriented, as it doesn't take into account the
 * messages as the server receives them.
 *
 * @author Fabian Groffen
 * @see org.monetdb.mcl.net.MapiSocket
 * @see org.monetdb.mcl.io.BufferedMCLWriter
 */
public final class BufferedMCLReader /* extends BufferedReader */ {

	private final BufferedReader inner;
	private String current = null;
	private LineType lineType = LineType.UNKNOWN;

	/**
	 * Create a buffering character-input stream that uses a
	 * default-sized input buffer.
	 *
	 * @param in A Reader
	 */
	public BufferedMCLReader(final Reader in) {
		inner = new BufferedReader(in);
	}

	/**
	 * Create a buffering character-input stream that uses a
	 * default-sized input buffer, from an InputStream.
	 *
	 * @param in An InputStream
	 * @param enc Encoding
	 * @throws UnsupportedEncodingException If encoding is not supported
	 */
	public BufferedMCLReader(final InputStream in, final String enc)
		throws UnsupportedEncodingException
	{
		this(new java.io.InputStreamReader(in, enc));
	}

	public void advance() throws IOException {
		if (lineType == LineType.PROMPT)
			return;

		current = inner.readLine();
		lineType = LineType.classify(current);
		if (lineType == LineType.ERROR && current != null && !current.matches("^![0-9A-Z]{5}!.+")) {
			current = "!22000!" + current.substring(1);
		}
	}

	/**
	 * Resets the linetype to UNKNOWN.
	 */
	public void resetLineType() {
		lineType = LineType.UNKNOWN;
	}

	/**
	 * Return the current line, or null if we're at the end or before the beginning.
	 * @return the current line or null
	 */
	public String getLine() {
		return current;
	}

	/**
	 * getLineType returns the type of the current line.
	 *
	 * @return Linetype representing the kind of line this is, one of the
	 *         following enums: UNKNOWN, HEADER, ERROR, RESULT,
	 *         PROMPT, MORE, FILETRANSFER, SOHEADER, REDIRECT, INFO
	 */
	public LineType getLineType() {
		return lineType;
	}

	/**
	 * Discard the remainder of the response but collect any further error messages.
	 *
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 *
	 * TODO(Wouter): should probably not have to be synchronized.
	 */


	final public synchronized String discardRemainder() throws IOException {
		return discard(null);
	}

	final public synchronized String discardRemainder(String error) throws IOException {
		final StringBuilder sb;

		if (error != null) {
			sb = makeErrorBuffer();
			sb.append(error);
		} else {
			sb = null;
		}
		return discard(sb);
	}

	final synchronized String discard(StringBuilder errmsgs) throws IOException {
		while (lineType != LineType.PROMPT) {
			advance();
			if (getLine() == null)
				throw new IOException("Connection to server lost!");
			if (getLineType() == LineType.ERROR) {
				if (errmsgs == null)
					errmsgs = new StringBuilder(128);
				errmsgs.append('\n').append(getLine().substring(1));
			}
		}
		if (errmsgs == null)
			return null;
		return errmsgs.toString().trim();
	}

	private final StringBuilder makeErrorBuffer() {
		return new StringBuilder(128);
	}

	public void close() throws IOException {
		inner.close();
	}
}
