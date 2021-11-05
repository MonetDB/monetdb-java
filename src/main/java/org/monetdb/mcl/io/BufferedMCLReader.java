/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
public final class BufferedMCLReader extends BufferedReader {

	/** The type of the last line read */
	private LineType lineType = LineType.UNKNOWN;

	/**
	 * Create a buffering character-input stream that uses a
	 * default-sized input buffer.
	 *
	 * @param in A Reader
	 */
	public BufferedMCLReader(final Reader in) {
		super(in);
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
		super(new java.io.InputStreamReader(in, enc));
	}

	/**
	 * Read a line of text.  A line is considered to be terminated by
	 * any one of a line feed ('\n'), a carriage return ('\r'), or a
	 * carriage return followed immediately by a linefeed.  Before this
	 * method returns, it sets the linetype to any of the in MCL
	 * recognised line types.
	 *
	 * Warning: until the server properly prefixes all of its error
	 * messages with SQLSTATE codes, this method prefixes all errors it
	 * sees without sqlstate with the generic data exception code (22000).
	 *
	 * @return A String containing the contents of the line, not
	 *         including any line-termination characters, or null if the
	 *         end of the stream has been reached
	 * @throws IOException If an I/O error occurs
	 */
	@Override
	public String readLine() throws IOException {
		String r = super.readLine();
		setLineType(r);
		if (lineType == LineType.ERROR && r != null && !r.matches("^![0-9A-Z]{5}!.+")) {
			r = "!22000!" + r.substring(1);
		}
		return r;
	}

	/**
	 * Sets the linetype to the type of the string given.  If the string
	 * is null, lineType is set to UNKNOWN.
	 *
	 * @param line the string to examine
	 */
	public void setLineType(final String line) {
		lineType = LineType.classify(line);
	}

	/**
	 * getLineType returns the type of the last line read.
	 *
	 * @return an integer representing the kind of line this is, one of the
	 *         following constants: UNKNOWN, HEADER, ERROR, PROMPT, MORE,
	 *         RESULT, SOHEADER, REDIRECT, INFO
	 */
	public LineType getLineType() {
		return lineType;
	}

	/**
	 * Reads up till the MonetDB prompt, indicating the server is ready
	 * for a new command.  All read data is discarded.  If the last line
	 * read by readLine() was a prompt, this method will immediately return.
	 *
	 * If there are errors present in the lines that are read, then they
	 * are put in one string and returned <b>after</b> the prompt has
	 * been found. If no errors are present, null will be returned.
	 *
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 *
	 * TODO(Wouter): should probably not have to be synchronized.
	 */
	final public synchronized String waitForPrompt() throws IOException {
		final StringBuilder ret = new StringBuilder(128);
		String tmp;

		while (lineType != LineType.PROMPT) {
			tmp = readLine();
			if (tmp == null)
				throw new IOException("Connection to server lost!");
			if (lineType == LineType.ERROR)
				ret.append('\n').append(tmp.substring(1));
		}
		return ret.length() == 0 ? null : ret.toString().trim();
	}
}
