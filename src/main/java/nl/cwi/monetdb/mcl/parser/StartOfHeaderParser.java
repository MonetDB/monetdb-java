package nl.cwi.monetdb.mcl.parser;

/**
 * Created by ferreira on 11/25/16.
 */
public abstract class StartOfHeaderParser {

    /* Query types (copied from sql_query.mx) */

    /** A parse response (not handled) */
    public final static int Q_PARSE    = '0';
    /** A tabular response (typical ResultSet) */
    public final static int Q_TABLE    = '1';
    /** A response to an update statement, contains number of affected
     * rows and generated key-id */
    public final static int Q_UPDATE   = '2';
    /** A response to a schema update */
    public final static int Q_SCHEMA   = '3';
    /** A response to a transaction statement (start, rollback, abort,
     * commit) */
    public final static int Q_TRANS    = '4';
    /** A tabular response in response to a PREPARE statement containing
     * information about the wildcard values that need to be supplied */
    public final static int Q_PREPARE  = '5';
    /** A tabular continuation response (for a ResultSet) */
    public final static int Q_BLOCK    = '6';
    /** An unknown and unsupported response */
    public final static int Q_UNKNOWN  =  0;

    protected int len;

    protected int pos;

    public StartOfHeaderParser() {}

    public abstract int parse(String in) throws MCLParseException;

    public abstract int getNextAsInt() throws MCLParseException;

    public abstract String getNextAsString() throws MCLParseException;

    public final boolean hasNext() {
        return pos < len;
    }
}
