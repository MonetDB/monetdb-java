package nl.cwi.monetdb.responses;

import java.sql.Statement;

/**
 * The SchemaResponse represents an schema modification response.
 * It is issued on statements like CREATE, DROP or ALTER TABLE.
 * This response keeps a field that represents the success state, as
 * defined by JDBC, which is currently in MonetDB's case always
 * SUCCESS_NO_INFO.  Note that this state is not sent by the
 * server.<br />
 * <tt>&amp;3</tt>
 */
public class SchemaResponse implements IResponse {

    public final int state = Statement.SUCCESS_NO_INFO;

    @Override
    public String addLine(String line, int linetype) {
        return "Header lines are not supported for a SchemaResponse";
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
