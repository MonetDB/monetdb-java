package nl.cwi.monetdb.mcl.parser;

/**
 * Created by ferreira on 11/25/16.
 */
public abstract class HeaderLineParser extends MCLParser {

    public final static int NAME       = 1;
    public final static int LENGTH     = 2;
    public final static int TABLE      = 3;
    public final static int TYPE       = 4;

    protected int type;

    /**
     * Creates an MCLParser targeted at a given number of field values.
     * The lines parsed by an instance of this MCLParser should have
     * exactly capacity field values.
     *
     * @param capacity the number of field values to expect
     */
    protected HeaderLineParser(int capacity) {
        super(capacity);
    }
}
