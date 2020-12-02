/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetBlob;
import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.mcl.connection.helpers.BufferReallocator;
import nl.cwi.monetdb.mcl.connection.helpers.GregorianCalendarParser;
import nl.cwi.monetdb.mcl.connection.helpers.TimestampHelper;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.math.BigDecimal;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.sql.Types;
import java.util.Calendar;

/**
 * The OldMapiTupleLineParser extracts the values from a given tuple. The number of values that are expected are known
 * upfront to speed up allocation and validation. For null values we will map into Java minimum values for primitives
 * and null pointers for objects.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
final class OldMapiTupleLineParser {

	private OldMapiTupleLineParser() {}

	/**
	 * The character array representation of a NULL value found in a tuple.
	 */
	private static final char[] NULL_STRING = new char[]{'N','U','L','L'};

	/**
	 * Parses the given OldMapiProtocol's lineBuffer source as tuple line for a DataBlock. If source cannot be parsed, a
	 * ProtocolException is thrown. The OldMapiProtocol's tupleLineBuffer is used to help parsing a column value.
	 *
	 * @param protocol The OldMapiProtocol instance to parse its lineBuffer
	 * @param lineNumber The row line number on the DataBlock to insert the tuple
	 * @param typesMap The JDBC types mapping array
	 * @param values An array of columns to fill the parsed values
	 * @return The number of columns parsed
	 * @throws ProtocolException If an error occurs during parsing
	 */
	static int oldMapiParseTupleLine(OldMapiProtocol protocol, int lineNumber, int[] typesMap, Object[] values)
			throws ProtocolException {
		CharBuffer lineBuffer = protocol.lineBuffer;
		CharBuffer tupleLineBuffer = protocol.tupleLineBuffer;

		int len = lineBuffer.limit();
		char[] array = lineBuffer.array();

		// first detect whether this is a single value line (=) or a real tuple ([)
		if (array[0] == '=') {
			if (typesMap.length != 1) {
				throw new ProtocolException(typesMap.length + " columns expected, but only single value found");
			}
			// return the whole string but the leading =
			oldMapiStringToJavaDataConversion(protocol, array, 1, len - 1, lineNumber, values[0],
					typesMap[0]);
			return 1;
		}

		// extract separate fields by examining string, char for char
		boolean inString = false, escaped = false, fieldHasEscape = false;
		int cursor = 2, column = 0, i = 2;
		for (; i < len; i++) {
			switch(array[i]) {
				case '\\':
					escaped = !escaped;
					fieldHasEscape = true;
					break;
				case '"':
					/**
					 * If all strings are wrapped between two quotes, a \" can never exist outside a string. Thus if we
					 * believe that we are not within a string, we can safely assume we're about to enter a string if we
					 * find a quote. If we are in a string we should stop being in a string if we find a quote which is
					 * not prefixed by a \, for that would be an escaped quote. However, a nasty situation can occur
					 * where the string is like "test \\" as obvious, a test for a \ in front of a " doesn't hold here
					 * for all cases. Because "test \\\"" can exist as well, we need to know if a quote is prefixed by
					 * an escaping slash or not.
					 */
					if (!inString) {
						inString = true;
					} else if (!escaped) {
						inString = false;
					}
					// reset escaped flag
					escaped = false;
					break;
				case '\t':
					if (!inString && (i > 0 && array[i - 1] == ',') || (i + 1 == len - 1 && array[++i] == ']')) { // dirty
						// extract the field value as a string, without the potential escape codes
						final int endpos = i - 2;	// minus the tab and the comma or ]
						if (array[cursor] == '"' && array[endpos] == '"') {
							cursor++;
							final int fieldlen = endpos - cursor;
							if (fieldHasEscape) {
								// reuse the tupleLineBuffer by cleaning it and ensure the capacity
								tupleLineBuffer.clear();
								tupleLineBuffer = BufferReallocator.ensureCapacity(tupleLineBuffer, (i - 2) - (cursor + 1));
								// parse the field value (excluding the double quotes) and convert it to a string without any escape characters
								for (int pos = cursor; pos < endpos; pos++) {
									char chr = array[pos];
									if (chr == '\\' && pos + 1 < endpos) {
										// we detected an escape
										// escapedStr and GDKstrFromStr in gdk_atoms.c only
										// support \\ \f \n \r \t \" and \377
										pos++;
										chr = array[pos];
										switch (chr) {
											case 'f':
												tupleLineBuffer.put('\f');
												break;
											case 'n':
												tupleLineBuffer.put('\n');
												break;
											case 'r':
												tupleLineBuffer.put('\r');
												break;
											case 't':
												tupleLineBuffer.put('\t');
												break;
											case '0': case '1': case '2': case '3':
												// this could be an octal number, let's check it out
												if (pos + 2 < endpos) {
													char chr2 = array[pos + 1];
													char chr3 = array[pos + 2];
													if (chr2 >= '0' && chr2 <= '7' && chr3 >= '0' && chr3 <= '7') {
														// we got an octal number between \000 and \377
														try {
															tupleLineBuffer.put((char)(Integer.parseInt("" + array[pos] + array[pos + 1] + array[pos + 2], 8)));
															pos += 2;
														} catch (NumberFormatException e) {
															// hmmm, this point should never be reached actually...
															throw new AssertionError("Flow error, should never try to parse non-number");
														}
													} else {
														// do default action if number seems not to be an octal number
														tupleLineBuffer.put(array[pos]);
													}
												} else {
													// do default action if number seems not to be an octal number
													tupleLineBuffer.put(array[pos]);
												}
												break;
											default:
												// this is wrong usage of escape (except for '\\' and '"'), just ignore the \-escape and print the char
												tupleLineBuffer.put(array[pos]);
												break;
										}
									} else {
										tupleLineBuffer.put(array[pos]);
									}
								}
								// put the unescaped string in the right place
								((Buffer)tupleLineBuffer).flip();
								oldMapiStringToJavaDataConversion(protocol, tupleLineBuffer.array(), 0, tupleLineBuffer.limit(), lineNumber, values[column], typesMap[column]);
							} else {
								oldMapiStringToJavaDataConversion(protocol, array, cursor, fieldlen, lineNumber, values[column], typesMap[column]);
							}
						} else {
							final int vlen = i - 1 - cursor;
							if (vlen == 4 && OldMapiTupleLineParserHelper.charIndexOf(array, 0, array.length, NULL_STRING, 0, 4, cursor) == cursor) {
								setNullValue(lineNumber, values[column], typesMap[column]);
							} else {
								oldMapiStringToJavaDataConversion(protocol, array, cursor, vlen, lineNumber, values[column], typesMap[column]);
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
			}
		}
		protocol.tupleLineBuffer = tupleLineBuffer;
		// check if this result is of the size we expected it to be
		if (column != typesMap.length)
			throw new ProtocolException("illegal result length: " + column + "\nlast read: "
					+ (column > 0 ? values[column - 1] : "<none>"));
		return column;
	}

	/**
	 * Parses a BLOB String from a tuple column, converting it into a Java byte[] representation.
	 *
	 * @param toParse The CharBuffer's backing array
	 * @param startPosition The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @return A Java byte[] instance with the parsed BLOB
	 */
	private static byte[] binaryBlobConverter(char[] toParse, int startPosition, int count) {
		int len = (startPosition + count) / 2;
		byte[] res = new byte[len];
		for (int i = 0; i < len; i++) {
			res[i] = (byte) Integer.parseInt(new String(toParse, 2 * i, (2 * i) + 2), 16);
		}
		return res;
	}

	/**
	 * Converts a segment of a CharBuffer's backing array into a Java primitive or object depending on the JDBC Mapping.
	 *
	 * @param toParse The CharBuffer's backing array
	 * @param startPosition The first position in the array to parse
	 * @param count The number of characters to read from the starter position
	 * @param lineNumber The row line number on the DataBlock to insert the tuple
	 * @param columnArray The column array where the value will be appended
	 * @param jDBCMapping The JDBC mapping of the value
	 * @throws ProtocolException If the JDBC Mapping is unknown
	 */
	private static void oldMapiStringToJavaDataConversion(OldMapiProtocol protocol, char[] toParse, int startPosition,
														  int count, int lineNumber, Object columnArray,
														  int jDBCMapping) throws ProtocolException {
		switch (jDBCMapping) {
			case Types.BOOLEAN:
				((byte[]) columnArray)[lineNumber] = OldMapiTupleLineParserHelper.charArrayToBoolean(toParse, startPosition);
				break;
			case Types.TINYINT:
				((byte[]) columnArray)[lineNumber] = OldMapiTupleLineParserHelper.charArrayToByte(toParse, startPosition, count);
				break;
			case Types.SMALLINT:
				((short[]) columnArray)[lineNumber] = OldMapiTupleLineParserHelper.charArrayToShort(toParse, startPosition, count);
				break;
			case Types.INTEGER:
				((int[]) columnArray)[lineNumber] = OldMapiTupleLineParserHelper.charArrayToInt(toParse, startPosition, count);
				break;
			case Types.BIGINT:
				((long[]) columnArray)[lineNumber] = OldMapiTupleLineParserHelper.charArrayToLong(toParse, startPosition, count);
				break;
			case Types.REAL:
				((float[]) columnArray)[lineNumber] = Float.parseFloat(new String(toParse, startPosition, count));
				break;
			case Types.DOUBLE:
				((double[]) columnArray)[lineNumber] = Double.parseDouble(new String(toParse, startPosition, count));
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				((BigDecimal[]) columnArray)[lineNumber] = new BigDecimal(toParse, startPosition, count);
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.OTHER:
				((String[]) columnArray)[lineNumber] = new String(toParse, startPosition, count);
				break;
			case Types.DATE:
				((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.parseDate(new String(toParse, startPosition, count),
						protocol.getMonetParserPosition(), protocol.getMonetDate());
				break;
			case Types.TIME:
				((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.parseTime(new String(toParse, startPosition, count),
						protocol.getMonetParserPosition(), protocol.timeParser, false);
				break;
			case 2013: //Types.TIME_WITH_TIMEZONE:
				((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.parseTime(new String(toParse, startPosition, count),
						protocol.getMonetParserPosition(), protocol.timeParser, true);
				break;
			case Types.TIMESTAMP:
				((TimestampHelper[]) columnArray)[lineNumber] = GregorianCalendarParser.parseTimestamp(new String(toParse, startPosition, count),
						protocol.getMonetParserPosition(), protocol.timestampParser, false);
				break;
			case 2014: //Types.TIMESTAMP_WITH_TIMEZONE:
				((TimestampHelper[]) columnArray)[lineNumber] = GregorianCalendarParser.parseTimestamp(new String(toParse, startPosition, count),
						protocol.getMonetParserPosition(), protocol.timestampParser, true);
				break;
			case Types.CLOB:
				((MonetClob[]) columnArray)[lineNumber] = new MonetClob(toParse, startPosition, count);
				break;
			case Types.BLOB:
				((MonetBlob[]) columnArray)[lineNumber] = new MonetBlob(binaryBlobConverter(toParse, startPosition, count));
				break;
			case Types.LONGVARBINARY:
				((byte[][]) columnArray)[lineNumber] = binaryBlobConverter(toParse, startPosition, count);
				break;
			default:
				throw new ProtocolException("Unknown JDBC mapping!");
		}
	}

	/**
	 * Maps MonetDB's null values with their respective Java representation. For the primitive types, we will map them
	 * to their minimum values, while for objects we just map into null pointers.
	 *
	 * @param lineNumber The row line number on the DataBlock to insert the tuple
	 * @param columnArray The column array where the value will be appended
	 * @param jDBCMapping The JDBC mapping of the value
	 */
	private static void setNullValue(int lineNumber, Object columnArray, int jDBCMapping) {
		switch (jDBCMapping) {
			case Types.BOOLEAN:
			case Types.TINYINT:
				((byte[]) columnArray)[lineNumber] = Byte.MIN_VALUE;
				break;
			case Types.SMALLINT:
				((short[]) columnArray)[lineNumber] = Short.MIN_VALUE;
				break;
			case Types.INTEGER:
				((int[]) columnArray)[lineNumber] = Integer.MIN_VALUE;
				break;
			case Types.BIGINT:
				((long[]) columnArray)[lineNumber] = Long.MIN_VALUE;
				break;
			case Types.REAL:
				((float[]) columnArray)[lineNumber] = Float.MIN_VALUE;
				break;
			case Types.DOUBLE:
				((double[]) columnArray)[lineNumber] = Double.MIN_VALUE;
				break;
			default:
				((Object[]) columnArray)[lineNumber] = null;
		}
	}
}
