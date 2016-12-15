package nl.cwi.monetdb.mcl.protocol.embedded;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
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
        try {
            return connection.fillTableHeaders(columnNames, columnLengths, types, tableNames);
        } catch (MonetDBEmbeddedException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    @Override
    public int parseTupleLines(int lineNumber, int[] typesMap, Object[] values, boolean[][] nulls) throws ProtocolException {
        try {
            return connection.parseTupleLines(typesMap, values, nulls);
        } catch (MonetDBEmbeddedException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return connection.getLastError();
    }

    @Override
    public void writeNextQuery(String prefix, String query, String suffix) throws IOException {
        try {
            connection.processNextQuery(query);
        } catch (MonetDBEmbeddedException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    public void sendAutocommitCommand(int flag) throws ProtocolException { //1 or 0
        try {
            connection.sendAutocommitCommand(flag);
        } catch (MonetDBEmbeddedException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    public void sendReplySizeCommand(int size) throws ProtocolException {
        //do nothing for now :)
    }

    public void sendReleaseCommand(int commandId) throws ProtocolException {
        try {
            connection.sendReleaseCommand(commandId);
        } catch (MonetDBEmbeddedException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    public void sendCloseCommand(int commandId) throws ProtocolException {
        try {
            connection.sendCloseCommand(commandId);
        } catch (MonetDBEmbeddedException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }
}
