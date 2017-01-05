/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;

final class OldMapiStartOfHeaderParser {

    static StarterHeaders GetNextStartHeaderOnOldMapi(OldMapiProtocol protocol) {
        StarterHeaders res;
        switch (protocol.lineBuffer.get()) {
            case '0':
                res = StarterHeaders.Q_PARSE;
                break;
            case '1':
                res = StarterHeaders.Q_TABLE;
                break;
            case '2':
                res = StarterHeaders.Q_UPDATE;
                break;
            case '3':
                res = StarterHeaders.Q_SCHEMA;
                break;
            case '4':
                res = StarterHeaders.Q_TRANS;
                break;
            case '5':
                res = StarterHeaders.Q_PREPARE;
                break;
            case '6':
                res = StarterHeaders.Q_BLOCK;
                break;
            default:
                res = StarterHeaders.Q_UNKNOWN;
        }
        return res;
    }

    static int GetNextResponseDataAsInt(OldMapiProtocol protocol) throws ProtocolException {
        int currentPointer = protocol.lineBuffer.position();
        int limit = protocol.lineBuffer.limit();
        char[] array = protocol.lineBuffer.array();

        if (currentPointer >= limit) {
            throw new ProtocolException("unexpected end of string", currentPointer - 1);
        }
        int tmp;
        char chr = array[currentPointer++];
        // note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = (int)chr - (int)'0';
        } else {
            throw new ProtocolException("expected a digit", currentPointer - 1);
        }

        while (currentPointer < limit) {
            chr = array[currentPointer++];
            if(chr == ' ') {
                break;
            }
            tmp *= 10;
            if (chr >= '0' && chr <= '9') {
                tmp += (int)chr - (int)'0';
            } else {
                throw new ProtocolException("expected a digit", currentPointer - 1);
            }
        }
        protocol.lineBuffer.position(currentPointer);
        return tmp;
    }

    static String GetNextResponseDataAsString(OldMapiProtocol protocol) throws ProtocolException {
        int currentPointer = protocol.lineBuffer.position();
        int limit = protocol.lineBuffer.limit();
        char[] array = protocol.lineBuffer.array();

        if (currentPointer >= limit) {
            throw new ProtocolException("unexpected end of string", currentPointer - 1);
        }
        int cnt = 0, mark = currentPointer;
        char chr;

        while (currentPointer < limit) {
            chr = array[currentPointer++];
            if(chr == ' ') {
                break;
            }
            cnt++;
        }

        protocol.lineBuffer.position(mark);
        return new String(array, 0, cnt);
    }
}
