package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;

/**
 * Created by ferreira on 11/30/16.
 */
public final class OldMapiConverter {

    static ServerResponses GetNextResponseOnOldMapi(char nextChar) {
        switch (nextChar) {
            case '!':
                return ServerResponses.ERROR;
            case '&':
                return ServerResponses.SOHEADER;
            case '%':
                return ServerResponses.HEADER;
            case '[':
                return ServerResponses.RESULT;
            case '=':
                return ServerResponses.RESULT;
            case '^':
                return ServerResponses.REDIRECT;
            case '#':
                return ServerResponses.INFO;
            case '.':
                return ServerResponses.PROMPT;
            case ',':
                return ServerResponses.MORE;
            default:
                return ServerResponses.UNKNOWN;
        }
    }

    static StarterHeaders GetNextStartHeaderOnOldMapi(char nextChar) {
        switch (nextChar) {
            case '0':
                return StarterHeaders.Q_PARSE;
            case '1':
                return StarterHeaders.Q_TABLE;
            case '2':
                return StarterHeaders.Q_UPDATE;
            case '3':
                return StarterHeaders.Q_SCHEMA;
            case '4':
                return StarterHeaders.Q_TRANS;
            case '5':
                return StarterHeaders.Q_PREPARE;
            case '6':
                return StarterHeaders.Q_BLOCK;
            default:
                return StarterHeaders.Q_UNKNOWN;
        }
    }
}
