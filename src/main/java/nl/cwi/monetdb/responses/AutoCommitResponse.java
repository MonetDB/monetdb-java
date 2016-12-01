package nl.cwi.monetdb.responses;

/**
 * The AutoCommitResponse represents a transaction message.  It
 * stores (a change in) the server side auto commit mode.<br />
 * <tt>&amp;4 (t|f)</tt>
 */
public class AutoCommitResponse extends SchemaResponse {

    public final boolean autocommit;

    public AutoCommitResponse(boolean ac) {
        // fill the blank final
        this.autocommit = ac;
    }
}
