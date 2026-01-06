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
 * The HeaderLineParser is a generic MCLParser that extracts values from
 * a metadata header in the MCL protocol either as string or integer values.
 *
 * @author Fabian Groffen
 */
public final class HeaderLineParser extends MCLParser {
	/* types of meta data supported by MCL protocol */
	public final static int NAME   = 1;	// name of column
	public final static int LENGTH = 2;
	public final static int TABLE  = 3;	// may include the schema name
	public final static int TYPE   = 4;
	public final static int TYPESIZES = 5;	// precision and scale

	 /** The int values found while parsing.  Public, you may touch it. */
	public final int intValues[];

	private final QuotedStringParser qsparser = new QuotedStringParser();

	/**
	 * Constructs a HeaderLineParser which expects columncount columns.
	 *
	 * @param columncount the number of columns in the to be parsed string
	 */
	public HeaderLineParser(final int columncount) {
		super(columncount);
		intValues = new int[columncount];
	}

	/**
	 * Parses the given String source as header line.  If source cannot
	 * be parsed, an MCLParseException is thrown.  The columncount argument
	 * given during construction is used for allocation of the backing array.
	 *
	 * @param source a String which should be parsed
	 * @return the type of the parsed header line
	 * @throws MCLParseException if an error occurs during parsing
	 */
	@Override
	public int parse(final String source) throws MCLParseException {
		final char[] chrLine = source.toCharArray();
		int len = chrLine.length;
		int pos = 0;
		boolean foundChar = false;
		boolean nameFound = false;
		int i;

		// find header name searching from the end of the line
		for (i = len - 1; i >= 0; i--) {
			switch (chrLine[i]) {
				case ' ':
				case '\n':
				case '\t':
				case '\r':
					if (!foundChar) {
						len = i - 1;
					} else {
						pos = i + 1;
					}
					break;
				case '#':
					// found!
					nameFound = true;
					if (pos == 0)
						pos = i + 1;
					i = 0;	// force the loop to terminate
					break;
				default:
					foundChar = true;
					pos = 0;
					break;
			}
		}
		if (!nameFound)
			throw new MCLParseException("invalid header, no header name found", pos);

		// depending on the name of the header, we continue
		int type = 0;
		i = pos;
		switch (len - pos) {
			case 4:
				// source.regionMatches(pos + 1, "name", 0, 4)
				if (chrLine[i] == 'n' && chrLine[++i] == 'a' && chrLine[++i] == 'm' && chrLine[++i] == 'e') {
					getValues(chrLine, 2, pos - 3);
					type = NAME;
				} else
				// source.regionMatches(pos + 1, "type", 0, 4)
				if (chrLine[i] == 't' && chrLine[++i] == 'y' && chrLine[++i] == 'p' && chrLine[++i] == 'e') {
					getValues(chrLine, 2, pos - 3);
					type = TYPE;
				}
				break;
			case 6:
				// source.regionMatches(pos + 1, "length", 0, 6)
				if (chrLine[ i ] == 'l' && chrLine[++i] == 'e' && chrLine[++i] == 'n' && chrLine[++i] == 'g'
				 && chrLine[++i] == 't' && chrLine[++i] == 'h') {
					getIntValues(chrLine, 2, pos - 3);
					type = LENGTH;
				}
				break;
			case 9:
				// System.out.println("In HeaderLineParser.parse() case 9: source line = " + source);
				// source.regionMatches(pos + 1, "typesizes", 0, 9)
				if (chrLine[ i ] == 't' && chrLine[++i] == 'y' && chrLine[++i] == 'p' && chrLine[++i] == 'e'
				 && chrLine[++i] == 's' && chrLine[++i] == 'i' && chrLine[++i] == 'z' && chrLine[++i] == 'e' && chrLine[++i] == 's') {
					getValues(chrLine, 2, pos - 3);	/* these contain precision and scale values (separated by a space), so read them as strings */
					type = TYPESIZES;
				}
				break;
			case 10:
				// source.regionMatches(pos + 1, "table_name", 0, 10)
				if (chrLine[ i ] == 't' && chrLine[++i] == 'a' && chrLine[++i] == 'b' && chrLine[++i] == 'l' && chrLine[++i] == 'e'
				 && chrLine[++i] == '_' && chrLine[++i] == 'n' && chrLine[++i] == 'a' && chrLine[++i] == 'm' && chrLine[++i] == 'e') {
					getValues(chrLine, 2, pos - 3);
					type = TABLE;
				}
				break;
			default:
				throw new MCLParseException("unknown header: " + (new String(chrLine, pos, len - pos)));
		}

		// adjust colno
		colnr = 0;

		return type;
	}

	/**
	 * Fills an array of Strings containing the values between
	 * ',\t' separators.
	 *
	 * As of Oct2014-SP1 release MAPI adds double quotes around names when
	 * the name contains a comma or a tab or a space or a # or " or \ escape character.
	 * See issue: https://github.com/MonetDB/MonetDB/issues/3616
	 * If the parsed name string part has a " as first and last character,
	 * we remove those added double quotes here and expand any backslash escapes.
	 *
	 * @param chrLine a character array holding the input data
	 * @param start where the relevant data starts
	 * @param end where the relevant data stops
	 */
	private void getValues(final char[] chrLine, int start, final int end) throws MCLParseException {
		int elem = 0;

		int pos = start;
		while (pos < end) {
			// Extract a value and leave pos at its end
			String value;
			if (chrLine[pos] == '"') {
				value = qsparser.parse(chrLine, pos, end);
				pos += qsparser.size;
			} else {
				int i = pos;
				while (i < end && chrLine[i] != ',')
					i++;
				value = new String(chrLine, pos, i - pos);
				pos = i;
			}

			// Is it a suitable separator (or end?)
			if ((pos != end) && (pos > end - 2 || chrLine[pos] != ',' || chrLine[pos + 1] != '\t'))
				throw new MCLParseException("invalid separator", pos);

			// Append the value and skip the separator
			if (elem >= values.length)
				throw new MCLParseException("too many values", pos);
			values[elem++] = value;
			pos += 2;
		}

		if (elem != values.length)
			throw new MCLParseException("not enough values", end);
	}

	/**
	 * Fills an array of ints containing the values between
	 * ',\t' separators.
	 *
	 * Feb2017 note - This integer parser doesn't have to parse negative
	 * numbers, because it is only used to parse column lengths
	 * which are always greater than 0.
	 *
	 * @param chrLine a character array holding the input data
	 * @param start where the relevant data starts
	 * @param stop where the relevant data stops
	 * @throws MCLParseException if an error occurs during parsing
	 */
	private final void getIntValues(final char[] chrLine, final int start, final int stop) throws MCLParseException {
		int elem = 0;
		int tmp = 0;

		for (int i = start; i < stop; i++) {
			if (chrLine[i] == ',' && chrLine[i + 1] == '\t') {
				if (elem < intValues.length) {
					intValues[elem++] = tmp;
				}
				tmp = 0;
				i++;
			} else {
				// note: don't use Character.isDigit() here, because
				// we only want ISO-LATIN-1 digits
				if (chrLine[i] >= '0' && chrLine[i] <= '9') {
					tmp *= 10;
					tmp += (int)chrLine[i] - (int)'0';
				} else {
					throw new MCLParseException("expected a digit in " + new String(chrLine) + " at " + i);
				}
			}
		}
		// add the left over part (last column)
		if (elem < intValues.length)
			intValues[elem] = tmp;
	}
}
