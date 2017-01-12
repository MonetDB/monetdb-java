/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
    static int GetNextTableHeader(CharBuffer lineBuffer, String[] columnNames, int[] columnLengths,
                                  String[] types, String[] tableNames) throws ProtocolException {
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
                    GetStringValues(array, pos - 3, columnNames);
                    res = TableResultHeaders.NAME;
                }
                break;
            case 'l': //length
                if (currentLength - pos == 6) {
                    GetIntValues(array, pos - 3, columnLengths);
                    res = TableResultHeaders.LENGTH;
                }
                break;
            case 't':
                if (currentLength - pos == 4) { //type
                    GetStringValues(array, pos - 3, types);
                    res = TableResultHeaders.TYPE;
                } else if (currentLength - pos == 10) { //table_name
                    GetStringValues(array, pos - 3, tableNames);
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
     * @param array The lineBuffer's backing array
     * @param stop The position to stop parsing
     * @param stringValues The String array to fill
     */
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

    /**
     * Fills an integer array header with values.
     *
     * @param array The lineBuffer's backing array
     * @param stop The position to stop parsing
     * @param intValues The integer array to fill
     * @throws ProtocolException If an error while parsing occurred
     */
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
                    throw new ProtocolException("Expected a digit in " + new String(array) + " at " + i);
                }
            }
        }
        // add the left over part
        intValues[elem] = tmp;
    }
}
