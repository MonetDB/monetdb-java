package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;

/**
 * Created by ferreira on 12/6/16.
 */
final class OldMapiStartOfHeaderParser {

    static StarterHeaders GetNextStartHeaderOnOldMapi(OldMapiProtocol protocol) {
        StarterHeaders res;
        switch (protocol.builder.charAt(protocol.currentPointer)) {
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

    static int GetNextResponseDataAsInt(OldMapiProtocol protocol) throws MCLParseException {
        protocol.currentPointer++;
        if (!protocol.hasRemaining()) {
            throw new MCLParseException("unexpected end of string", protocol.currentPointer - 1);
        }
        int tmp;
        char chr = protocol.builder.charAt(protocol.currentPointer);
        // note: don't use Character.isDigit() here, because
        // we only want ISO-LATIN-1 digits
        if (chr >= '0' && chr <= '9') {
            tmp = (int)chr - (int)'0';
        } else {
            throw new MCLParseException("expected a digit", protocol.currentPointer - 1);
        }

        while (protocol.hasRemaining()) {
            chr = protocol.builder.charAt(protocol.currentPointer);
            protocol.currentPointer++;
            if(chr == ' ') {
                break;
            }
            tmp *= 10;
            if (chr >= '0' && chr <= '9') {
                tmp += (int)chr - (int)'0';
            } else {
                throw new MCLParseException("expected a digit", protocol.currentPointer - 1);
            }
        }
        return tmp;
    }

    static String GetNextResponseDataAsString(OldMapiProtocol protocol) throws MCLParseException {
        protocol.currentPointer++;
        if (!protocol.hasRemaining()) {
            throw new MCLParseException("unexpected end of string", protocol.currentPointer - 1);
        }
        int cnt = 0, mark = protocol.currentPointer;
        char chr;

        while (protocol.hasRemaining()) {
            chr = protocol.builder.charAt(protocol.currentPointer);
            protocol.currentPointer++;
            if(chr == ' ') {
                break;
            }
            cnt++;
        }

        protocol.currentPointer = mark;
        return protocol.builder.subSequence(0, cnt).toString();
    }
}
