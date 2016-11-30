package nl.cwi.monetdb.mcl.parser.embedded;

import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;

/**
 * Created by ferreira on 11/25/16.
 */
public class EmbeddedStartOfHeaderParser extends StartOfHeaderParser {

    private int nextResponseType;

    private final int[] nextIntValues = new int[4];

    private String nextStringValue;

    @Override
    public int parse(String in) throws MCLParseException {
        this.pos = 0;
        switch (this.nextResponseType) {
            /*case Q_PARSE:*/
            case Q_SCHEMA:
                this.len = 0;
                break;
            case Q_TABLE:
            case Q_PREPARE:
                this.len = 4;
                break;
            case Q_UPDATE:
                this.len = 2;
                break;
            case Q_TRANS:
                this.len = 1;
                break;
            /*case Q_BLOCK:
                len = 3;
                break;*/
            default:
                throw new MCLParseException("invalid or unknown header", 1);
        }
        return this.nextResponseType;
    }

    @Override
    public int getNextAsInt() throws MCLParseException {
        int res = this.nextIntValues[this.pos];
        this.pos++;
        return res;
    }

    @Override
    public String getNextAsString() throws MCLParseException {
        return this.nextStringValue;
    }
}
