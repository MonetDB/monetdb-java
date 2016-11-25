package nl.cwi.monetdb.mcl.parser;

/**
 * Created by ferreira on 11/25/16.
 */
public abstract class TupleLineParser extends MCLParser {
    /**
     * Creates an MCLParser targeted at a given number of field values.
     * The lines parsed by an instance of this MCLParser should have
     * exactly capacity field values.
     *
     * @param capacity the number of field values to expect
     */
    protected TupleLineParser(int capacity) {
        super(capacity);
    }
}
