/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetBlob;
import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.mcl.connection.helpers.GregorianCalendarParser;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.sql.Types;
import java.text.ParsePosition;
import java.util.Calendar;

final class OldMapiTupleLineParser {

    private static final char[] NULL_STRING = "NULL".toCharArray();

    private static int CharIndexOf(char[] source, int sourceCount, char[] target, int targetCount) {
        if (targetCount == 0) {
            return 0;
        }

        char first = target[0];
        int max = sourceCount - targetCount;

        for (int i = 0; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first);
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = 1; j < end && source[j] == target[k]; j++, k++);

                if (j == end) {
                    /* Found whole string. */
                    return i;
                }
            }
        }
        return -1;
    }

    static int OldMapiParseTupleLine(int lineNumber, CharBuffer lineBuffer, StringBuilder helper, int[] typesMap,
                                     Object[] values, boolean[] nulls) throws ProtocolException {
        int len = lineBuffer.limit();
        char[] array = lineBuffer.array();

        // first detect whether this is a single value line (=) or a real tuple ([)
        if (array[0] == '=') {
            if (typesMap.length != 1) {
                throw new ProtocolException(typesMap.length + " columns expected, but only single value found");
            }
            // return the whole string but the leading =
            OldMapiStringToJavaObjectConverter(new String(array, 1, len - 1), lineNumber, values[0], typesMap[0]);
            return 1;
        }

        // extract separate fields by examining string, char for char
        boolean inString = false, escaped = false;
        int cursor = 2, column = 0, i = 2;
        for (; i < len; i++) {
            switch(array[i]) {
                default:
                    escaped = false;
                    break;
                case '\\':
                    escaped = !escaped;
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
                        // split!
                        if (array[cursor] == '"' && array[i - 2] == '"') {
                            // reuse the StringBuilder by cleaning it
                            helper.setLength(0);
                            // prevent capacity increases
                            helper.ensureCapacity((i - 2) - (cursor + 1));
                            for (int pos = cursor + 1; pos < i - 2; pos++) {
                                if (array[cursor] == '\\' && pos + 1 < i - 2) {
                                    pos++;
                                    // strToStr and strFromStr in gdk_atoms.mx only support \t \n \\ \" and \377
                                    switch (array[pos]) {
                                        case '\\':
                                            helper.append('\\');
                                            break;
                                        case 'n':
                                            helper.append('\n');
                                            break;
                                        case 't':
                                            helper.append('\t');
                                            break;
                                        case '"':
                                            helper.append('"');
                                            break;
                                        case '0': case '1': case '2': case '3':
                                            // this could be an octal number, let's check it out
                                            if (pos + 2 < i - 2 && array[pos + 1] >= '0' && array[pos + 1] <= '7' &&
                                                    array[pos + 2] >= '0' && array[pos + 2] <= '7') {
                                                // we got the number!
                                                try {
                                                    helper.append((char)(Integer.parseInt("" + array[pos] + array[pos + 1] + array[pos + 2], 8)));
                                                    pos += 2;
                                                } catch (NumberFormatException e) {
                                                    // hmmm, this point should never be reached actually...
                                                    throw new AssertionError("Flow error, should never try to parse non-number");
                                                }
                                            } else {
                                                // do default action if number seems not to be correct
                                                helper.append(array[pos]);
                                            }
                                            break;
                                        default:
                                            // this is wrong, just ignore the escape, and print the char
                                            helper.append(array[pos]);
                                            break;
                                    }
                                } else {
                                    helper.append(array[pos]);
                                }
                            }
                            // put the unescaped string in the right place
                            OldMapiStringToJavaObjectConverter(helper.toString(), lineNumber, values[column], typesMap[column]);
                            nulls[column] = false;
                        } else if ((i - 1) - cursor == 4 && CharIndexOf(array, array.length, NULL_STRING, NULL_STRING.length) == cursor) {
                            SetNullValue(lineNumber, values[column], typesMap[column]);
                            nulls[column] = true;
                        } else {
                            OldMapiStringToJavaObjectConverter(new String(array, cursor, i - 1 - cursor), lineNumber, values[column], typesMap[column]);
                            nulls[column] = false;
                        }
                        column++;
                        cursor = i + 1;
                    }
                    // reset escaped flag
                    escaped = false;
                    break;
            }
        }
        // check if this result is of the size we expected it to be
        if (column != typesMap.length)
            throw new ProtocolException("illegal result length: " + column + "\nlast read: " + (column > 0 ? values[column - 1] : "<none>"));
        return column;
    }

    private static byte[] BinaryBlobConverter(String toParse) {
        int len = toParse.length() / 2;
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) {
            res[i] = (byte) Integer.parseInt(toParse.substring(2 * i, (2 * i) + 2), 16);
        }
        return res;
    }

    private static final ParsePosition Ppos = new ParsePosition(0);

    private static void OldMapiStringToJavaObjectConverter(String toParse, int lineNumber, Object columnArray,
                                                           int jDBCMapping) throws ProtocolException {
        switch (jDBCMapping) {
            case Types.BOOLEAN:
                ((boolean[]) columnArray)[lineNumber] = Boolean.parseBoolean(toParse);
                break;
            case Types.TINYINT:
                ((byte[]) columnArray)[lineNumber] = Byte.parseByte(toParse);
                break;
            case Types.SMALLINT:
                ((short[]) columnArray)[lineNumber] = Short.parseShort(toParse);
                break;
            case Types.INTEGER:
                ((int[]) columnArray)[lineNumber] = Integer.parseInt(toParse);
                break;
            case Types.BIGINT:
                ((long[]) columnArray)[lineNumber] = Long.parseLong(toParse);
                break;
            case Types.REAL:
                ((float[]) columnArray)[lineNumber] = Float.parseFloat(toParse);
                break;
            case Types.DOUBLE:
                ((double[]) columnArray)[lineNumber] = Double.parseDouble(toParse);
                break;
            case Types.DECIMAL:
                ((BigDecimal[]) columnArray)[lineNumber] = new BigDecimal(toParse);
                break;
            case Types.NUMERIC:
                ((BigInteger[]) columnArray)[lineNumber] = new BigInteger(toParse);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.OTHER:
                ((String[]) columnArray)[lineNumber] = toParse;
                break;
            case Types.DATE:
                ((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.ParseDate(toParse, Ppos);
                break;
            case Types.TIME:
                ((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.ParseTime(toParse, Ppos, false);
                break;
            case Types.TIME_WITH_TIMEZONE:
                ((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.ParseTime(toParse, Ppos, true);
                break;
            case Types.TIMESTAMP:
                ((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.ParseTimestamp(toParse, Ppos, false);
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                ((Calendar[]) columnArray)[lineNumber] = GregorianCalendarParser.ParseTimestamp(toParse, Ppos, true);
                break;
            case Types.CLOB:
                ((MonetClob[]) columnArray)[lineNumber] = new MonetClob(toParse);
                break;
            case Types.BLOB:
                ((MonetBlob[]) columnArray)[lineNumber] = new MonetBlob(BinaryBlobConverter(toParse));
                break;
            case Types.LONGVARBINARY:
                ((byte[][]) columnArray)[lineNumber] = BinaryBlobConverter(toParse);
                break;
            default:
                throw new ProtocolException("Unknown type!");
        }
    }

    private static void SetNullValue(int lineNumber, Object columnArray, int jDBCMapping) throws ProtocolException {
        switch (jDBCMapping) {
            case Types.BOOLEAN:
                ((boolean[]) columnArray)[lineNumber] = false;
                break;
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
