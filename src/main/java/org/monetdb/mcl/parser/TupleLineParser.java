/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.mcl.parser;

/**
 * The TupleLineParser extracts the values from a given tuple.
 * The number of values that are expected are known upfront to speed up
 * allocation and validation.
 *
 * @author Fabian Groffen
 * @author Martin van Dinther
 */
public final class TupleLineParser extends MCLParser {
	private StringBuilder uesc = null;	// used for building field string value when an escape is present in the field value

	/**
	 * Constructs a TupleLineParser which expects columncount columns.
	 * The columncount argument is used for allocation of the public values array.
	 * While this seems illogical, the caller should know this size, since the
	 * StartOfHeader contains this information.
	 *
	 * @param columncount the number of columns in the to be parsed string
	 */
	public TupleLineParser(final int columncount) {
		super(columncount);
	}

	/**
	 * Parses the given String source as tuple line.
	 * If source cannot be parsed, a MCLParseException is thrown.
	 *
	 * @param source a String representing a tuple line which should be parsed
	 * @return 0, as there is no 'type' of TupleLine
	 * @throws MCLParseException if source is not compliant to expected tuple/single value format
	 */
	@Override
	public int parse(final String source) throws MCLParseException {
		final int len = source.length();
		if (len <= 0)
			throw new MCLParseException("Missing tuple data");

		// first detect whether this is a single value line (=) or a real tuple ([)
		char chr = source.charAt(0);
		if (chr == '=') {
			if (values.length != 1)
				throw new MCLParseException(values.length +
						" columns expected, but only single value found");

			// return the whole string but without the leading =
			values[0] = source.substring(1);

			// reset colnr
			colnr = 0;
			return 0;
		}

		if (chr != '[')
			throw new MCLParseException("Expected a data row starting with [");

		// It is a tuple. Extract separate fields by examining the string data char for char
		// For parsing it is faster to use an char[] to avoid overhead of source.charAt(i) method calls
		final char[] chrLine = source.toCharArray();
		boolean inString = false, escaped = false, fieldHasEscape = false;
		int column = 0, cursor = 2;
		// scan the characters, when a field separator is found extract the field value as String dealing with possible escape characters
		for (int i = 2; i < len; i++) {
			switch(chrLine[i]) {
				case '\\':
					escaped = !escaped;
					fieldHasEscape = true;
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
				case '\t':		// potential field separator found
					if (!inString &&
						((chrLine[i - 1] == ',') ||		// found field separator: ,\t
						 ((i + 1 == len - 1) && chrLine[++i] == ']'))) // found last field: \t]
					{
						// extract the field value as a string, without the potential escape codes
						final int endpos = i - 2;	// minus the tab and the comma or ]
						if (chrLine[cursor] == '"' &&
						    chrLine[endpos] == '"')	// field is surrounded by double quotes, so a string with possible escape codes
						{
							cursor++;
							final int fieldlen = endpos - cursor;
							if (fieldHasEscape) {
								if (uesc == null) {
									// first time use, create it with enough capacity, minimum 1024
									uesc = new StringBuilder(Math.max(fieldlen, 1024));
								} else {
									// reuse the StringBuilder by cleaning it
									uesc.setLength(0);
									if (fieldlen > 1024) {
										// prevent multiple capacity increments during the append()'s in the inner loop
										uesc.ensureCapacity(fieldlen);
									}
								}
								// parse the field value (excluding the double quotes) and convert it to a string without any escape characters
								for (int pos = cursor; pos < endpos; pos++) {
									chr = chrLine[pos];
									if (chr == '\\' && pos + 1 < endpos) {
										// we detected an escape
										// escapedStr and GDKstrFromStr in gdk_atoms.c only
										// support \\ \f \n \r \t \" and \377
										pos++;
										chr = chrLine[pos];
										switch (chr) {
											case 'f':
												uesc.append('\f');
												break;
											case 'n':
												uesc.append('\n');
												break;
											case 'r':
												uesc.append('\r');
												break;
											case 't':
												uesc.append('\t');
												break;
											case '0': case '1': case '2': case '3':
												// this could be an octal number, let's check it out
												if (pos + 2 < endpos) {
													char chr2 = chrLine[pos + 1];
													char chr3 = chrLine[pos + 2];
													if (chr2 >= '0' && chr2 <= '7' && chr3 >= '0' && chr3 <= '7') {
														// we got an octal number between \000 and \377
														try {
															uesc.append((char)(Integer.parseInt(new String(chrLine, pos, 3), 8)));
															pos += 2;
														} catch (NumberFormatException e) {
															// hmmm, this point should never be reached actually...
															throw new AssertionError("Flow error, should never try to parse non-number");
														}
													} else {
														// do default action if number seems not to be an octal number
														uesc.append(chr);
													}
												} else {
													// do default action if number seems not to be an octal number
													uesc.append(chr);
												}
												break;
											/* case '\\':	optimisation: this code does the same as the default case, so not needed
												uesc.append('\\');
												break;
											*/
											/* case '"':	optimisation: this code does the same as the default case, so not needed
												uesc.append('"');
												break;
											*/
											default:
												// this is wrong usage of escape (except for '\\' and '"'), just ignore the \-escape and print the char
												uesc.append(chr);
												break;
										}
									} else {
										uesc.append(chr);
									}
								}
								// put the unescaped string in the right place
								values[column] = uesc.toString();
							} else {
								// the field is a string surrounded by double quotes and without escape chars
								values[column] = new String(chrLine, cursor, fieldlen);
								// if (values[column].contains("\\")) {
								//	throw new MCLParseException("Invalid parsing: detected a \\ in double quoted string: " + fieldVal);
								// }
							}
						} else {
							final int vlen = i - 1 - cursor;
							if (vlen == 4 &&
							    chrLine[cursor] == 'N' && chrLine[cursor+1] == 'U' && chrLine[cursor+2] == 'L' && chrLine[cursor+3] == 'L') {
								// the field contains NULL, so no value
								values[column] = null;
							} else {
								// the field is a string NOT surrounded by double quotes and thus without escape chars
								values[column] = new String(chrLine, cursor, vlen);
								// if (values[column].contains("\\")) {
								//	throw new MCLParseException("Invalid parsing: detected a \\ in unquoted string: " + fieldVal);
								// }
							}
						}
						cursor = i + 1;
						fieldHasEscape = false;		// reset for next field scan
						column++;
					}
					// reset escaped flag
					escaped = false;
					break;
				default:
					escaped = false;
					break;
			} // end of switch()
		} // end of for()

		// check if this result is of the size we expected it to be
		if (column != values.length)
			throw new MCLParseException("illegal result length: " + column + "\nlast read: " + (column > 0 ? values[column - 1] : "<none>"));

		// reset colnr
		colnr = 0;
		return 0;
	}
}
