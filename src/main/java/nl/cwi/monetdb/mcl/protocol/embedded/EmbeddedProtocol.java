package nl.cwi.monetdb.mcl.protocol.embedded;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.embedded.JDBCEmbeddedConnection;
import nl.cwi.monetdb.mcl.protocol.*;
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

    private final JDBCEmbeddedConnection connection;

    public EmbeddedProtocol(JDBCEmbeddedConnection con) {
        this.connection = con;
    }

    public JDBCEmbeddedConnection getEmbeddedConnection() {
        return this.connection;
    }

    @Override
    public ServerResponses waitUntilPrompt() throws IOException {
        return null;
    }

    @Override
    public void fetchNextResponseData() throws IOException {

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
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws ProtocolException {
        return null;
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws ProtocolException {
        return null;
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException {
        return null;
    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws ProtocolException {
        return null;
    }

    @Override
    public TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues) throws ProtocolException {
        return null;
    }

    @Override
    public int parseTupleLine(Object line, Object[] values, int[] typesMap) throws ProtocolException {
        return 0;
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return null;
    }

    @Override
    public void writeNextQuery(String prefix, String query, String suffix) throws IOException {

    }
}
