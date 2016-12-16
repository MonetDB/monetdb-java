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
public class EmbeddedProtocol extends AbstractProtocol {

    private final JDBCEmbeddedConnection connection;

    public EmbeddedProtocol(JDBCEmbeddedConnection con) {
        this.connection = con;
    }

    public JDBCEmbeddedConnection getEmbeddedConnection() {
        return this.connection;
    }

    @Override
    public ServerResponses getCurrentServerResponseHeader() {
        return connection.getNextServerResponse();
    }

    @Override
    public void waitUntilPrompt() throws IOException {} //Nothing really :)

    @Override
    public void fetchNextResponseData() throws IOException {} //Nothing really :)

    @Override
    public StarterHeaders getNextStarterHeader() {
        return connection.getServerHeaderResponse();
    }

    @Override
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws ProtocolException {
        int[] array = connection.getLastServerResponseParameters();
        int id = array[0]; //The order cannot be switched!!
        int tuplecount = array[1];
        int columncount = array[2];
        int rowcount = array[3];
        return new ResultSetResponse(con, list, seqnr, id, rowcount, tuplecount, columncount);
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws ProtocolException {
        return (UpdateResponse) connection.getLastServerResponse();
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException {
        return (AutoCommitResponse) connection.getLastServerResponse();
    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws ProtocolException {
        int[] array = connection.getLastServerResponseParameters();
        int id = array[0]; //The order cannot be switched!!
        int columncount = array[1];
        int rowcount = array[2];
        int offset = array[3];

        ResultSetResponse rs = rsresponses.get(id);
        if (rs == null) {
            return null;
        }
        return rs.addDataBlockResponse(offset, rowcount, columncount, this);
    }

    @Override
    public TableResultHeaders getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types,
                                                 String[] tableNames) throws ProtocolException {
        return connection.fillTableHeaders(columnNames, columnLengths, types, tableNames);
    }

    @Override
    public int parseTupleLines(int lineNumber, int[] typesMap, Object[] values, boolean[][] nulls) throws ProtocolException {
        return connection.parseTupleLines(typesMap, values, nulls);
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return connection.getLastError();
    }

    @Override
    public void writeNextQuery(String prefix, String query, String suffix) throws IOException {
        connection.processNextQuery(query);
    }
}
