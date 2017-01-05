/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ServerResponses;

final class OldMapiServerResponseParser {

    static ServerResponses ParseOldMapiServerResponse(OldMapiProtocol protocol) {
        ServerResponses res;
        switch (protocol.lineBuffer.get()) {
            case '!':
                res = ServerResponses.ERROR;
                break;
            case '&':
                res = ServerResponses.SOHEADER;
                break;
            case '%':
                res = ServerResponses.HEADER;
                break;
            case '[':
                res = ServerResponses.RESULT;
                break;
            case '=':
                res = ServerResponses.RESULT;
                break;
            case '^':
                res = ServerResponses.REDIRECT;
                break;
            case '#':
                res = ServerResponses.INFO;
                break;
            case '.':
                res = ServerResponses.PROMPT;
                break;
            case ',':
                res = ServerResponses.MORE;
                break;
            default:
                res = ServerResponses.UNKNOWN;
        }
        return res;
    }
}
