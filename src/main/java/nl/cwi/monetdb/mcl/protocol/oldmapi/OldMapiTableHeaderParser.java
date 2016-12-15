package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

import java.nio.CharBuffer;

/**
 * Created by ferreira on 12/6/16.
 */
final class OldMapiTableHeaderParser {

    static TableResultHeaders GetNextTableHeader(CharBuffer lineBuffer, String[] stringValues, int[] intValues)
            throws ProtocolException {
        TableResultHeaders res = TableResultHeaders.UNKNOWN;
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
            throw new ProtocolException("invalid header, no header name found", pos);

        // depending on the name of the header, we continue
        switch (array[pos]) {
            case 'n': //name
                if (currentLength - pos == 4) {
                    GetStringValues(array, pos - 3, stringValues);
                    res = TableResultHeaders.NAME;
                }
                break;
            case 'l': //length
                if (currentLength - pos == 6) {
                    GetIntValues(array, pos - 3, intValues);
                    res = TableResultHeaders.LENGTH;
                }
                break;
            case 't':
                if (currentLength - pos == 4) { //type
                    GetStringValues(array, pos - 3, stringValues);
                    res = TableResultHeaders.TYPE;
                } else if (currentLength - pos == 10) { //table_name
                    GetStringValues(array, pos - 3, stringValues);
                    res = TableResultHeaders.TABLE;
                }
                break;
            default:
                throw new ProtocolException("unknown header: " + new String(array, pos, currentLength - pos));
        }
        return res;
    }

    private static void GetStringValues(char[] array, int stop, String[] stringValues) {
        int elem = 0, start = 2;

        for (int i = start + 1; i < stop; i++) {
            if (array[i] == '\t' && array[i - 1] == ',') {
                stringValues[elem++] = new String(array, start, i - 1 - start);
                start = i + 1;
            }
        }
        // add the left over part
        stringValues[elem] = new String(array, start, stop - start);
    }

    private static void GetIntValues(char[] array, int stop, int[] intValues) throws ProtocolException {
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
                    throw new ProtocolException("expected a digit in " + new String(array) + " at " + i);
                }
            }
        }
        // add the left over part
        intValues[elem] = tmp;
    }
}
