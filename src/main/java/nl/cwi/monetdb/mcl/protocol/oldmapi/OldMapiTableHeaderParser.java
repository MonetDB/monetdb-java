/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

import java.nio.CharBuffer;

/**
 * The OldMapiTableHeaderParser is a generic parser that retrieves Q_TABLE, Q_PREPARE and Q_BLOCK responses data as
 * integers and Strings to fill the Tables' metadata.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
final class OldMapiTableHeaderParser {

	private OldMapiTableHeaderParser() {}

	/**
	 * Retrieves the next table result set header and fills the respective array of values.
	 *
	 * @param lineBuffer An Old Mapi Protocol's lineBuffer to retrieve data
	 * @param columnNames The result set column names
	 * @param columnLengths The result set column lengths
	 * @param types The result set column SQL types
	 * @param tableNames The result set columns schema and table names in format schema.table
	 * @return The integer representation of the Table Result Header retrieved
	 * @throws ProtocolException If an error while parsing occurred
	 */
	static int getNextTableHeader(CharBuffer lineBuffer, String[] columnNames, int[] columnLengths, String[] types,
								  String[] tableNames) throws ProtocolException {
		int res = TableResultHeaders.UNKNOWN;
		int currentLength = lineBuffer.limit();
		char[] array = lineBuffer.array();

		int pos = 0;
		boolean foundChar = false, nameFound = false;

		// find header name
		for (int i = currentLength - 1; i >= 0; i--) {
			switch (array[i]) {
				case ' ':
				case '\n':
				case '\t':
				case '\r':
					if (!foundChar) {
						currentLength = i - 1;
					} else {
						pos = i + 1;
					}
					break;
				case '#':
					// found!
					nameFound = true;
					if (pos == 0) pos = i + 1;
					i = 0;	// force the loop to terminate
					break;
				default:
					foundChar = true;
					pos = 0;
					break;
			}
		}
		if (!nameFound)
			throw new ProtocolException("Invalid header, no header name found", pos);

		// depending on the name of the header, we continue
		switch (array[pos]) {
			case 'n': //name
				if (currentLength - pos == 4) {
					getStringValues(array, pos - 3, columnNames);
					res = TableResultHeaders.NAME;
				}
				break;
			case 'l': //length
				if (currentLength - pos == 6) {
					getIntValues(array, pos - 3, columnLengths);
					res = TableResultHeaders.LENGTH;
				}
				break;
			case 't':
				if (currentLength - pos == 4) { //type
					getStringValues(array, pos - 3, types);
					res = TableResultHeaders.TYPE;
				} else if (currentLength - pos == 10) { //table_name
					getStringValues(array, pos - 3, tableNames);
					res = TableResultHeaders.TABLE;
				}
				break;
			default:
				throw new ProtocolException("Unknown header: " + new String(array, pos, currentLength - pos));
		}
		return res;
	}

	/**
	 * Fills a String array header with values.
	 *
	 * As of Oct2014-SP1 release MAPI adds double quotes around names when the name contains a comma or a tab or a space
	 * or a # or " character.
	 * See issue: https://www.monetdb.org/bugzilla/show_bug.cgi?id=3616 If the parsed name string part has a " as first
	 * and last character, we remove those added double quotes here.
	 *
	 * @param array The lineBuffer's backing array
	 * @param stop The position to stop parsing
	 * @param stringValues The String array to fill
	 */
	private static void getStringValues(char[] array, int stop, String[] stringValues) {
		int elem = 0, start = 2;
		boolean inString = false, escaped = false;

		for (int i = start; i < stop; i++) {
			switch(array[i]) {
				case '\\':
					escaped = !escaped;
					break;
				case '"':
					/**
					 * If all strings are wrapped between two quotes, a \" can
					 * never exist outside a string. Thus if we believe that we
					 * are not within a string, we can safely assume we're about
					 * to enter a string if we find a quote.
					 * If we are in a string we should stop being in a string if
					 * we find a quote which is not prefixed by a \, for that
					 * would be an escaped quote. However, a nasty situation can
					 * occur where the string is like "test \\" as obvious, a
					 * test for a \ in front of a " doesn't hold here for all
					 * cases. Because "test \\\"" can exist as well, we need to
					 * know if a quote is prefixed by an escaping slash or not.
					 */
					if (!inString) {
						inString = true;
					} else if (!escaped) {
						inString = false;
					}
					// reset escaped flag
					escaped = false;
					break;
				case ',':
					if (!inString && array[i + 1] == '\t') {
						// we found the field separator
						if (array[start] == '"')
							start++;  // skip leading double quote
						if (elem < stringValues.length) {
							stringValues[elem++] = new String(array, start, i - (array[i - 1] == '"' ? 1 : 0) - start);
						}
						i++;
						start = i + 1;	// reset start for the next name, skipping the field separator (a comma and tab)
					}
					// reset escaped flag
					escaped = false;
					break;
				default:
					escaped = false;
					break;
			}
		}
		// add the left over part (last column)
		if (array[start] == '"')
			start++;  // skip leading double quote
		if (elem < stringValues.length)
			stringValues[elem] = new String(array, start, stop - (array[stop - 1] == '"' ? 1 : 0) - start);
	}

	/**
	 * Fills an integer array header with values.
	 *
	 * @param array The lineBuffer's backing array
	 * @param stop The position to stop parsing
	 * @param intValues The integer array to fill
	 * @throws ProtocolException If an error while parsing occurred
	 */
	private static void getIntValues(char[] array, int stop, int[] intValues) throws ProtocolException {
		int elem = 0, tmp = 0, start = 2;

		for (int i = start; i < stop; i++) {
			if (array[i] == ',' && array[i + 1] == '\t') {
				intValues[elem++] = tmp;
				tmp = 0;
				i++;
			} else {
				tmp *= 10;
				// note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
				if (array[i] >= '0' && array[i] <= '9') {
					tmp += (int) array[i] - (int)'0';
				} else {
					throw new ProtocolException("Expected a digit in " + new String(array) + " at " + i);
				}
			}
		}
		// add the left over part
		intValues[elem] = tmp;
	}
}
