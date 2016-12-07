package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetBlob;
import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Types;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

/**
 * Created by ferreira on 12/6/16.
 */
final class OldMapiTupleLineParser {

    static int OldMapiParseTupleLine(StringBuilder line, Object[] values, StringBuilder helper, int[] jDBCTypesMap) throws ProtocolException {
        int len = line.length();

        // first detect whether this is a single value line (=) or a real tuple ([)
        if (line.charAt(0) == '=') {
            if (values.length != 1) {
                throw new ProtocolException(values.length + " columns expected, but only single value found");
            }
            // return the whole string but the leading =
            values[0] = line.substring(1);
            return 1;
        }

        // extract separate fields by examining string, char for char
        boolean inString = false, escaped = false;
        int cursor = 2, column = 0, i = 2;
        for (; i < len; i++) {
            switch(line.charAt(i)) {
                default:
                    escaped = false;
                    break;
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
                case '\t':
                    if (!inString && (i > 0 && line.charAt(i - 1) == ',') || (i + 1 == len - 1 && line.charAt(++i) == ']')) { // dirty
                        // split!
                        if (line.charAt(cursor) == '"' && line.charAt(i - 2) == '"') {
                            // reuse the StringBuilder by cleaning it
                            helper.setLength(0);
                            // prevent capacity increases
                            helper.ensureCapacity((i - 2) - (cursor + 1));
                            for (int pos = cursor + 1; pos < i - 2; pos++) {
                                if (line.charAt(pos) == '\\' && pos + 1 < i - 2) {
                                    pos++;
                                    // strToStr and strFromStr in gdk_atoms.mx only
                                    // support \t \n \\ \" and \377
                                    switch (line.charAt(pos)) {
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
                                            if (pos + 2 < i - 2 &&
                                                    line.charAt(pos + 1) >= '0' && line.charAt(pos + 1) <= '7' &&
                                                    line.charAt(pos + 2) >= '0' && line.charAt(pos + 2) <= '7') {
                                                // we got the number!
                                                try {
                                                    helper.append((char)(Integer.parseInt("" + line.charAt(pos) + line.charAt(pos + 1) + line.charAt(pos + 2), 8)));
                                                    pos += 2;
                                                } catch (NumberFormatException e) {
                                                    // hmmm, this point should never be reached actually...
                                                    throw new AssertionError("Flow error, should never try to parse non-number");
                                                }
                                            } else {
                                                // do default action if number seems not to be correct
                                                helper.append(line.charAt(pos));
                                            }
                                            break;
                                        default:
                                            // this is wrong, just ignore the escape, and print the char
                                            helper.append(line.charAt(pos));
                                            break;
                                    }
                                } else {
                                    helper.append(line.charAt(pos));
                                }
                            }

                            // put the unescaped string in the right place
                            values[column] = OldMapiStringToJavaObjectConverter(helper.toString(), jDBCTypesMap[column]);
                        } else if ((i - 1) - cursor == 4 && line.indexOf("NULL", cursor) == cursor) {
                            values[column] = null;
                        } else {
                            values[column] = OldMapiStringToJavaObjectConverter(line.substring(cursor, i - 1), jDBCTypesMap[column]);
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
        if (column != values.length)
            throw new ProtocolException("illegal result length: " + column + "\nlast read: " + (column > 0 ? values[column - 1] : "<none>"));
        return column;
    }

    private static final SimpleDateFormat DateParser = new SimpleDateFormat("yyyy-MM-dd");

    private static final SimpleDateFormat TimeParser = new SimpleDateFormat("HH:mm:ss");

    private static final SimpleDateFormat TimestampParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static byte[] BinaryBlobConverter(String toParse) {
        int len = toParse.length() / 2;
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) {
            res[i] = (byte) Integer.parseInt(toParse.substring(2 * i, (2 * i) + 2), 16);
        }
        return res;
    }

    private static Object OldMapiStringToJavaObjectConverter(String toParse, int jDBCMapping) throws ProtocolException {
        switch (jDBCMapping) {
            case Types.BIGINT:
                return Long.parseLong(toParse);
            case Types.BLOB:
                return new MonetBlob(BinaryBlobConverter(toParse));
            case Types.BINARY:
                return BinaryBlobConverter(toParse);
            case Types.BOOLEAN:
                return Boolean.parseBoolean(toParse);
            case Types.CHAR:
                return toParse;
            case Types.CLOB:
                return new MonetClob(toParse);
            case Types.DATE:
                try {
                    return DateParser.parse(toParse);
                } catch (ParseException e) {
                    throw new ProtocolException(e.getMessage());
                }
            case Types.DECIMAL:
                return new BigDecimal(toParse);
            case Types.DOUBLE:
                return Double.parseDouble(toParse);
            case Types.NUMERIC:
                return new BigInteger(toParse);
            case Types.INTEGER:
                return Integer.parseInt(toParse);
            case Types.REAL:
                return Float.parseFloat(toParse);
            case Types.SMALLINT:
                return Short.parseShort(toParse);
            case Types.TIME:
                try {
                    return TimeParser.parse(toParse);
                } catch (ParseException e) {
                    throw new ProtocolException(e.getMessage());
                }
            case Types.TIMESTAMP:
                try {
                    return TimestampParser.parse(toParse);
                } catch (ParseException e) {
                    throw new ProtocolException(e.getMessage());
                }
            case Types.TINYINT:
                return Byte.parseByte(toParse);
            case Types.VARCHAR:
                return toParse;
            default:
                return null;
        }
    }
}
