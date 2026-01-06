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

/**
 * Utility class to parse quoted strings in MAPI responses.
 *
 * We need both the unquoted string and the quoted size but
 * in Java you can only return a single result so we use
 * this little class to hold the other one.
 */
public class QuotedStringParser {
	private StringBuilder builder = null;
	public int size;

	/**
	 * Parse a quoted string from the given character array.
	 * When this method returns, the {@link #size} field indicates the number of
	 * characters it occupies in the array.
	 * Because of backslash escapes this may be more than the length of the
	 * returned string.
	 *
	 * This method recognizes the escape characters \\, \", \f, \n, \r, \t and \377.
	 * The previous implementation ignored invalid escapes, returning them as-is.
	 * This method throws an exception instead.
	 *
	 * @param line array to parse from
	 * @param start position of the opening quote character
	 * @param end do not parse beyond this position
	 * @return the parsed string
	 * @throws MCLParseException if invalid backslash escapes are found or the
	 * terminating quote character is missing
	 */
	public String parse(char[] line, int start, int end) throws MCLParseException {
		// Happy path: no backslashes
		for (int i = start + 1; i < end; i++) {
			char chr = line[i];
			if (chr == '"') {
				// size should include the quotes, return value shouldn't
				size = i + 1 - start;
				return new String(line, start + 1, i - start - 1);
			}
			if (chr == '\\') {
				// fall back to backslash unescaping code
				return parseEscapes(line, start, i, end);
			}
		}
		throw new MCLParseException("unterminated quoted string", end);
	}

	private String parseEscapes(char[] line, int start, int pos, int end) throws MCLParseException {
		if (builder != null)
			builder.setLength(0);
		else
			builder = new StringBuilder(end - start + 16);
		builder.append(line, start + 1, pos - start - 1);

		for (int i = pos; i < end; i++) {
			char chr = line[i];
			if (chr == '"') {
				// We reached the end
				size = i + 1 - start;
				return builder.toString();
			}
			if (chr == '\\') {
				// Parse the backslash escape
				int location = i;
				if (i + 1 == end)
					throw new MCLParseException("truncated escape sequence", location);
				char second = line[++i];
				switch (second) {
					case '\\':
					case '"':
						chr = second;
						break;
					case 'f':
						chr = '\f';
						break;
					case 'n':
						chr = '\n';
						break;
					case 'r':
						chr = '\r';
						break;
					case 't':
						chr = '\t';
						break;
					case '0':
					case '1':
					case '2':
					case '3':
						if (i + 3 >= end)
							throw new MCLParseException("truncated escape sequence", location);
						int digit1 = second - '0';
						int digit2 = line[++i] - '0';
						int digit3 = line[++i] - '0';
						if (digit2 < 0 | digit2 > 7 | digit3 < 0 | digit3 > 7)
							throw new MCLParseException("invalid escape sequence", i);
						chr = (char)(64 * digit1 + 8 * digit2 + digit3);
						break;
					default:
						throw new MCLParseException("unexpected escape sequence", location);
				}
			} // end of if (chr=='\\')
			builder.append(chr);
		}
		throw new MCLParseException("unterminated quoted string", end);
	}
}
