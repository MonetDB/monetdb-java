package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ServerResponses;

/**
 * Created by ferreira on 11/30/16.
 */
final class OldMapiServerResponseParser {

    static ServerResponses ParseOldMapiServerResponse(OldMapiProtocol protocol) {
        ServerResponses res;
        switch (protocol.builder.charAt(protocol.currentPointer)) {
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
