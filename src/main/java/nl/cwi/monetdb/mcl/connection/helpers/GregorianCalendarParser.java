/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.helpers;

import nl.cwi.monetdb.jdbc.MonetResultSet;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * A helper class to process MAPI dates, times and timestamps Strings into their Java representation.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class GregorianCalendarParser {

	/**
	 * Small helper method that returns the intrinsic value of a char if it represents a digit. If a non-digit character
	 * is encountered an MCLParseException is thrown.
	 *
	 * @param c the char
	 * @param pos the position
	 * @return the intrinsic value of the char
	 * @throws ProtocolException if c is not a digit
	 */
	private static int getIntrinsicValue(char c, int pos) throws ProtocolException {
		// note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
		if (c >= '0' && c <= '9') {
			return (int)c - (int)'0';
		} else {
			throw new ProtocolException("Expected a digit", pos);
		}
	}

	/** The time zone information, to be re-used to avoid memory allocations */
	private static final TimeZone defaultTimeZone = TimeZone.getDefault();

	/**
	 * Parses a date or time or timestamp MAPI String into a Java {@link Calendar} instance.
	 *
	 * @param mrs A MonetResultSet instance where warning can be added
	 * @param toParse The date or time or timestamp String to parse
	 * @param pos The position of the String to start the parsing
	 * @param parser The parser to use
	 * @param jdbcType The JDBC type of the column
	 * @return A {@link Calendar} instance of the parsed date
	 */
	public static Calendar parseDateString(MonetResultSet mrs, String toParse, ParsePosition pos,
										   SimpleDateFormat parser, int jdbcType) {
		pos.setIndex(0);
		Calendar res = new GregorianCalendar();
		if(jdbcType == Types.TIME || jdbcType == Types.TIMESTAMP || jdbcType == Types.DATE) {
			parser.setTimeZone(TimeZone.getTimeZone("GMT" + toParse.substring(toParse.length() - 6)));
		} else {
			parser.setTimeZone(defaultTimeZone);
		}

		Date aux = parser.parse(toParse, pos);
		if (aux == null) {
			// parsing failed
			int epos = pos.getErrorIndex();
			if (epos == -1) {
				mrs.addWarning("parsing '" + toParse + "' failed", "01M10");
			} else if (epos < toParse.length()) {
				mrs.addWarning("parsing failed, found: '" + toParse.charAt(epos) + "' in: \"" + toParse +
						"\" at pos: " + pos.getErrorIndex(), "01M10");
			} else {
				mrs.addWarning("parsing failed, expected more data after '" + toParse + "'", "01M10");
			}
			res.clear();
		} else {
			res.setTime(aux);
		}

		if (jdbcType != Types.DATE) {
			// parse additional nanos (if any)
			int pos1 = pos.getIndex(), nanos;
			char[] monDate = toParse.toCharArray();
			if (pos1 < monDate.length && monDate[pos1] == '.') {
				pos1++;
				int ctr;
				try {
					nanos = getIntrinsicValue(monDate[pos1], pos1++);
					for (ctr = 1; pos1 < monDate.length && monDate[pos1] >= '0' && monDate[pos1] <= '9'; ctr++) {
						if (ctr < 9) {
							nanos *= 10;
							nanos += (getIntrinsicValue(monDate[pos1], pos1));
						}
						if (ctr == 2) // we have three at this point
							res.set(Calendar.MILLISECOND, nanos);
						pos1++;
					}
					while (ctr++ < 9)
						nanos *= 10;
				} catch(ProtocolException e) {
					mrs.addWarning(e.getMessage() + " found: '" + monDate[e.getErrorOffset()] + "' in: \"" +
							toParse + "\" at pos: " + e.getErrorOffset(), "01M10");
					res.clear();
				}
			}
		}
		return res;
	}

	/**
	 * Parses a date MAPI String into a Java {@link Calendar} instance.
	 *
	 * @param toParse The date String to parse
	 * @param pos The position of the String to start the parsing
	 * @param parser The parser to use (date)
	 * @return A {@link Calendar} instance of the parsed date
	 * @throws ProtocolException If the String could not be parsed
	 */
	public static Calendar parseDate(String toParse, ParsePosition pos, SimpleDateFormat parser)
			throws ProtocolException {
		pos.setIndex(0);
		Calendar res = new GregorianCalendar();
		Date util = parser.parse(toParse, pos);
		if(util == null) {
			res.clear();
		} else {
			res.setTime(util);
		}
		return res;
	}

	/**
	 * Parses a time or a timestamp MAPI String into a Java {@link Calendar} instance.
	 *
	 * @param toParse The time String to parse
	 * @param pos The position of the String to start the parsing
	 * @param hasTimeZone If the time String has timezone information
	 * @param parser The parser to use (time)
	 * @return A {@link Calendar} instance of the parsed time
	 * @throws ProtocolException If the String could not be parsed
	 */
	public static Calendar parseTime(String toParse, ParsePosition pos, SimpleDateFormat parser, boolean hasTimeZone)
			throws ProtocolException {
		pos.setIndex(0);
		if(hasTimeZone) { // MonetDB/SQL99:  Sign TwoDigitHours : Minutes
			parser.setTimeZone(TimeZone.getTimeZone("GMT" + toParse.substring(toParse.length() - 6)));
		} else {
			parser.setTimeZone(defaultTimeZone);
		}

		Calendar res = new GregorianCalendar();
		Date util = parser.parse(toParse, pos);
		if(util == null) {
			res.clear();
		} else {
			res.setTime(util);
		}
		return res;
	}

	/**
	 * Parses a timestamp MAPI String into a {@link TimestampHelper} instance.
	 *
	 * @param toParse The timestamp String to parse
	 * @param pos The position of the String to start the parsing
	 * @param hasTimeZone If the timestamp String has timezone information
	 * @param parser The parser to use (timestamp)
	 * @return A {@link TimestampHelper} instance of the parsed timestamp with nanos information
	 * @throws ProtocolException If the String could not be parsed
	 */
	public static TimestampHelper parseTimestamp(String toParse, ParsePosition pos, SimpleDateFormat parser,
												 boolean hasTimeZone) throws ProtocolException {
		pos.setIndex(0);
		if(hasTimeZone) { // MonetDB/SQL99:  Sign TwoDigitHours : Minutes
			parser.setTimeZone(TimeZone.getTimeZone("GMT" + toParse.substring(toParse.length() - 6)));
		} else {
			parser.setTimeZone(defaultTimeZone);
		}

		GregorianCalendar res = new GregorianCalendar();
		Date util = parser.parse(toParse, pos);
		if(util != null) {
			res.setTime(util);
		} else {
			res.clear();
		}

		// parse additional nanos (if any)
		int pos1 = pos.getIndex(), nanos = 0;
		if (pos1 < toParse.length() && toParse.charAt(pos1) == '.') {
			pos1++;
			int ctr;

			nanos = getIntrinsicValue(toParse.charAt(pos1), pos1++);
			for (ctr = 1; pos1 < toParse.length() && toParse.charAt(pos1) >= '0' && toParse.charAt(pos1) <= '9'; ctr++) {
				if (ctr < 9) {
					nanos *= 10;
					nanos += (getIntrinsicValue(toParse.charAt(pos1), pos1));
				}
				if (ctr == 2) { // we have three at this point
					res.set(Calendar.MILLISECOND, nanos);
				}
				pos1++;
			}
			while (ctr++ < 9)
				nanos *= 10;
		}
		return new TimestampHelper(res, nanos);
	}
}
