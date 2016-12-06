package nl.cwi.monetdb.mcl.protocol.newmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.protocol.MCLParseException;
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
public class NewMapiProtocol extends AbstractProtocol {

    private final SocketConnection connection;

    public NewMapiProtocol(SocketConnection con) {
        this.connection = con;
    }

    @Override
    public void fetchNextResponseData() {

    }

    @Override
    public Object getCurrentData() {
        return null;
    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        return null;
    }

    @Override
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws MCLParseException {
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
    public TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues) throws MCLParseException {
        return null;
    }

    @Override
    public int parseTupleLine(Object line, Object[] values) throws MCLParseException {
        return 0;
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return null;
    }

    @Override
    public void writeNextCommand(byte[] prefix, byte[] query, byte[] suffix) throws IOException {

    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map rsresponses) throws MCLParseException {
        return null;
    }
}
