package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

/**
 * Created by ferreira on 12/6/16.
 */
public class OldMapiTableHeaderParser {

    static TableResultHeaders GetNextTableHeader(StringBuilder builder, String[] stringValues, int[] intValues) throws MCLParseException {
        TableResultHeaders res = TableResultHeaders.UNKNOWN;
        int len = builder.length(), pos = 0;
        boolean foundChar = false, nameFound = false;

        // find header name
        for (int i = len - 1; i >= 0; i--) {
            switch (builder.charAt(i)) {
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
            throw new MCLParseException("invalid header, no header name found", pos);

        // depending on the name of the header, we continue
        switch (builder.charAt(pos)) {
            case 'n': //name
                if (len - pos == 4) {
                    GetStringValues(builder, pos - 3, stringValues);
                    res = TableResultHeaders.NAME;
                }
                break;
            case 'l': //length
                if (len - pos == 6) {
                    GetIntValues(builder, pos - 3, intValues);
                    res = TableResultHeaders.LENGTH;
                }
                break;
            case 't':
                if (len - pos == 4) { //type
                    GetStringValues(builder, pos - 3, stringValues);
                    res = TableResultHeaders.TYPE;
                } else if (len - pos == 10) { //table_name
                    GetStringValues(builder, pos - 3, stringValues);
                    res = TableResultHeaders.TABLE;
                }
                break;
            default:
                throw new MCLParseException("unknown header: " + builder.substring(pos, len - pos));
        }
        return res;
    }

    private static void GetStringValues(StringBuilder builder, int stop, String[] stringValues) {
        int elem = 0, start = 2;

        for (int i = start + 1; i < stop; i++) {
            if (builder.charAt(i) == '\t' && builder.charAt(i - 1) == ',') {
                stringValues[elem++] = builder.substring(start, i - 1 - start);
                start = i + 1;
            }
        }
        // add the left over part
        stringValues[elem + 1] = builder.substring(start, stop - start);
    }

    private static void GetIntValues(StringBuilder builder, int stop, int[] intValues) throws MCLParseException {
        int elem = 0, tmp = 0, start = 2;

        for (int i = start; i < stop; i++) {
            if (builder.charAt(i) == ',' && builder.charAt(i + 1) == '\t') {
                intValues[elem++] = tmp;
                tmp = 0;
            } else {
                tmp *= 10;
                // note: don't use Character.isDigit() here, because
                // we only want ISO-LATIN-1 digits
                if (builder.charAt(i) >= '0' && builder.charAt(i) <= '9') {
                    tmp += (int) builder.charAt(i) - (int)'0';
                } else {
                    throw new MCLParseException("expected a digit in " + builder.toString() + " at " + i);
                }
            }
        }
        // add the left over part
        intValues[elem + 1] = tmp;
    }
}
