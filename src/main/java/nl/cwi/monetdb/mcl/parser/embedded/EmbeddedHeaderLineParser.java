package nl.cwi.monetdb.mcl.parser.embedded;

import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;

/**
 * Created by ferreira on 11/25/16.
 */
public class EmbeddedHeaderLineParser extends HeaderLineParser {

    private long resultSetPointer;

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
        /*switch(this.colnr) {
            case HeaderLineParser.NAME:
                System.arraycopy(this.columnNames, 0, this.values, 0, this.values.length);
                break;
            case HeaderLineParser.LENGTH:
                System.arraycopy(this.columnLengths, 0, this.intValues, 0, this.intValues.length);
                break;
            case HeaderLineParser.TABLE:
                System.arraycopy(this.columnTables, 0, this.values, 0, this.values.length);
                break;
            case HeaderLineParser.TYPE:
                System.arraycopy(this.columnTypes, 0, this.values, 0, this.values.length);
                break;
        }*/
        return this.parseNextHeadLineInternal();
    }

    private native int parseNextHeadLineInternal();
}
