/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.helpers;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public final class GregorianCalendarParser {

    private static final SimpleDateFormat DateParser = new SimpleDateFormat("yyyy-MM-dd");

    private static final SimpleDateFormat TimeParser = new SimpleDateFormat("HH:mm:ss");

    private static final SimpleDateFormat TimestampParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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

    public static Calendar ParseDate(String toParse, ParsePosition pos) throws ProtocolException {
        pos.setIndex(0);
        Calendar res = new GregorianCalendar();
        Date util = DateParser.parse(toParse, pos);
        if(util == null) {
            res.clear();
        } else {
            res.setTime(util);
        }
        return res;
    }

    private static Calendar ParseTimeIn(String toParse, ParsePosition pos, boolean hasTimeZone,
                                        SimpleDateFormat parser) throws ProtocolException {
        pos.setIndex(0);
        Calendar res = new GregorianCalendar();
        Date util = parser.parse(toParse, pos);
        if(util == null) {
            res.clear();
        } else {
            res.setTime(util);
        }

        // parse additional nanos (if any)
        int pos1 = pos.getIndex(), nanos;
        if (pos1 < toParse.length() && toParse.charAt(pos1) == '.') {
            pos1++;
            int ctr;

            nanos = getIntrinsicValue(toParse.charAt(pos1), pos1++);
            for (ctr = 1; pos1 < toParse.length() && toParse.charAt(pos1) >= '0' && toParse.charAt(pos1) <= '9'; ctr++) {
                if (ctr < 9) {
                    nanos *= 10;
                    nanos += (getIntrinsicValue(toParse.charAt(pos1), pos1));
                }
                if (ctr == 2) // we have three at this point
                    res.set(Calendar.MILLISECOND, nanos);
                pos1++;
            }
            while (ctr++ < 9)
                nanos *= 10;
        }

        if(hasTimeZone) {
            int vallen = toParse.length();
            if (vallen >= 6) { // MonetDB/SQL99:  Sign TwoDigitHours : Minutes
                res.setTimeZone(TimeZone.getTimeZone("GMT" + toParse.substring(vallen - 6, vallen)));
            }
        }
        return res;
    }

    public static Calendar ParseTime(String toParse, ParsePosition pos, boolean hasTimeZone) throws ProtocolException {
        return ParseTimeIn(toParse, pos, hasTimeZone, TimeParser);
    }

    public static Calendar ParseTimestamp(String toParse, ParsePosition pos, boolean hasTimeZone)
            throws ProtocolException {
        return ParseTimeIn(toParse, pos, hasTimeZone, TimestampParser);
    }
}
