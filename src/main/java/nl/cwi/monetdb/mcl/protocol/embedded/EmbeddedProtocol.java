package nl.cwi.monetdb.mcl.protocol.embedded;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ferreira on 11/30/16.
 */
public class EmbeddedProtocol extends AbstractProtocol<Object[]> {

    @Override
    public void fetchNextResponseData() {

    }

    @Override
    public Object[] getCurrentData() {
        return new Object[0];
    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        return null;
    }

    @Override
    public ResultSetResponse<Object[]> getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws MCLParseException {
        return null;
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws MCLParseException {
        return null;
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws MCLParseException {
        return null;
    }

    @Override
    public DataBlockResponse<Object[]> getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws MCLParseException {
        return null;
    }

    @Override
    public TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues) throws MCLParseException {
        return null;
    }

    @Override
    public void parseTupleLine(Object line, Object[] values) throws MCLParseException {

    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return null;
    }

    @Override
    public void writeNextCommand(byte[] prefix, byte[] query, byte[] suffix) throws IOException {

    }
}
