package nl.cwi.monetdb.mcl.parser.embedded;

import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;

/**
 * Created by ferreira on 11/25/16.
 */
public class EmbeddedHeaderLineParser extends HeaderLineParser {

    /**
     * Creates an MCLParser targeted at a given number of field values.
     * The lines parsed by an instance of this MCLParser should have
     * exactly capacity field values.
     *
     * @param capacity the number of field values to expect
     */
    public EmbeddedHeaderLineParser(int capacity) {
        super(capacity);
    }

    @Override
    public int parse(String source) throws MCLParseException {
        return 0;
    }
}
