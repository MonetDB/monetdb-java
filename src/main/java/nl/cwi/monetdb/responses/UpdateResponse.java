package nl.cwi.monetdb.responses;

/**
 * The UpdateResponse represents an update statement response.  It
 * is issued on an UPDATE, INSERT or DELETE SQL statement.  This
 * response keeps a count field that represents the affected rows
 * and a field that contains the last inserted auto-generated ID, or
 * -1 if not applicable.<br />
 * <tt>&amp;2 0 -1</tt>
 */
public class UpdateResponse implements IResponse {
    public final int count;
    public final String lastid;

    public UpdateResponse(int cnt, String id) {
        // fill the blank finals
        this.count = cnt;
        this.lastid = id;
    }

    @Override
    public String addLine(String line, int linetype) {
        return "Header lines are not supported for an UpdateResponse";
    }

    @Override
    public boolean wantsMore() {
        return false;
    }

    @Override
    public void complete() {
        // empty, because there is nothing to check
    }

    @Override
    public void close() {
        // nothing to do here...
    }
}
