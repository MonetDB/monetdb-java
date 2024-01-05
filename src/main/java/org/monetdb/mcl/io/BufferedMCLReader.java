/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.mcl.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * Helper class to read and classify the lines of a query response.
 *
 * This class wraps and buffers the Reader on which the responses come in.
 * Use {@link #getLine()} to get the current line, {@link #getLineType()}
 * to get its {@link LineType} and {@link #advance()} to proceed to the
 * next line.
 *
 * Initially, the line type is set to LineType.UNKNOWN and the line
 * is set to null. When the end of the result set has been reached the line type
 * will remain LineType.PROMPT and the line will again be null.
 *
 * To start reading the next response, call {@link #resetLineType()}. This
 * is usually done automatically by the accompanying {@link BufferedMCLWriter}
 * whenever a new request is sent to the server.
 *
 * @author Fabian Groffen
 * @see org.monetdb.mcl.net.MapiSocket
 * @see org.monetdb.mcl.io.BufferedMCLWriter
 */
public final class BufferedMCLReader {

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
	 * @param cs A Charset
	 */
	public BufferedMCLReader(final InputStream in, final Charset cs) {
		this(new java.io.InputStreamReader(in, cs));
	}

	/**
	 * Proceed to the next line of the response.
	 * 
	 * Set line type to PROMPT and line to null if the end of the response
	 * has been reached.
	 * @throws IOException if exception occurred during reading
	 */
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
	 * Reset the linetype to UNKNOWN.
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
	 * Return a substring of the current line, or null if we're at the end or before the beginning.
	 *
	 * @return the current line or null
	 */
	public String getLine(int start) {
		return getLine().substring(start);
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
	 */
	final public String discardRemainder() throws IOException {
		return discard(null);
	}

	/**
	 * Discard the remainder of the response but collect any further error messages.
	 *
	 * @param error A string containing earlier error messages to include in the error report.
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 */
	final public String discardRemainder(String error) throws IOException {
		final StringBuilder sb;

		if (error != null) {
			sb = makeErrorBuffer();
			sb.append(error);
		} else {
			sb = null;
		}
		return discard(sb);
	}

	/**
	 * Discard the remainder of the response but collect any further error messages.
	 *
	 * @param errmsgs An optional StringBuilder object containing earlier error messages to include in the error report.
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 *
	 * TODO(Wouter): should probably not have to be synchronized.
	 */
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

	/**
	 * Close the wrapped Reader.
	 * @throws IOException if an IO exception occurs while talking to the server
	 */
	public void close() throws IOException {
		inner.close();
	}
}
