package nl.cwi.monetdb.mcl.responses;

/**
 * The UpdateResponse represents an update statement response.  It
 * is issued on an UPDATE, INSERT or DELETE SQL statement.  This
 * response keeps a count field that represents the affected rows
 * and a field that contains the last inserted auto-generated ID, or
 * -1 if not applicable.<br />
 * <tt>&amp;2 0 -1</tt>
 */
public class UpdateResponse implements IResponse {

    private final String lastid;

    private final int count;

    public UpdateResponse(String id, int cnt) {
        // fill the blank finals
        this.lastid = id;
        this.count = cnt;
    }

    public String getLastid() {
        return lastid;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void close() {
        // nothing to do here...
    }
}
