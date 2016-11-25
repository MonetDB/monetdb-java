package nl.cwi.monetdb.mcl.parser.embedded;

import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;

/**
 * Created by ferreira on 11/25/16.
 */
public class EmbeddedStartOfHeaderParser extends StartOfHeaderParser {

    @Override
    public int parse(String in) throws MCLParseException {
        return 0;
    }

    @Override
    public int getNextAsInt() throws MCLParseException {
        return 0;
    }

    @Override
    public String getNextAsString() throws MCLParseException {
        return null;
    }
}
