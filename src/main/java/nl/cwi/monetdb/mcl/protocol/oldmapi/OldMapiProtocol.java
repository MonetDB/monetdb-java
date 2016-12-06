package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.protocol.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

/**
 * Created by ferreira on 11/30/16.
 */
public class OldMapiProtocol extends AbstractProtocol<StringBuilder> {

    private static final int STRING_BUILDER_INITIAL_SIZE = 128;

    private final SocketConnection connection;

    final StringBuilder builder;

    int currentPointer = 0;

    final StringBuilder tupleLineBuilder;

    public OldMapiProtocol(SocketConnection con) {
        this.connection = con;
        this.builder = new StringBuilder(STRING_BUILDER_INITIAL_SIZE);
        this.tupleLineBuilder = new StringBuilder(STRING_BUILDER_INITIAL_SIZE);
    }

    public SocketConnection getConnection() {
        return connection;
    }

    boolean hasRemaining() {
        return this.currentPointer < this.builder.length();
    }

    @Override
    public ServerResponses waitUntilPrompt() {
        this.builder.setLength(0);
        this.currentPointer = 0;
        return super.waitUntilPrompt();
    }

    @Override
    public void fetchNextResponseData() {
        ServerResponses res;
        try {
            int bytesRead = connection.readUntilChar(this.builder, '\n');
            res = OldMapiServerResponseParser.ParseOldMapiServerResponse(this);
            if(res == ServerResponses.ERROR && !this.builder.substring(bytesRead).matches("^![0-9A-Z]{5}!.+")) {
                this.builder.insert(bytesRead, "!22000!");
            }
        } catch (IOException e) {
            res = ServerResponses.ERROR;
            this.builder.setLength(0);
            this.currentPointer = 0;
            this.builder.append("!22000!").append(e.getMessage());
        }
        this.currentServerResponseHeader = res;
    }

    @Override
    public StringBuilder getCurrentData() {
        return this.builder;
    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        return OldMapiStartOfHeaderParser.GetNextStartHeaderOnOldMapi(this);
    }

    @Override
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr)
            throws MCLParseException {
        int id = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int tuplecount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int columncount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int rowcount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        return new ResultSetResponse(con, list, seqnr, id, rowcount, tuplecount, columncount);
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws MCLParseException {
        int count = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //The order cannot be switched!!
        String lastId = OldMapiStartOfHeaderParser.GetNextResponseDataAsString(this);
        return new UpdateResponse(lastId, count);
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws MCLParseException {
        boolean ac = OldMapiStartOfHeaderParser.GetNextResponseDataAsString(this).equals("t");
        return new AutoCommitResponse(ac);
    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws MCLParseException {
        int id = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int columncount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int rowcount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int offset = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);

        ResultSetResponse rs = rsresponses.get(id);
        if (rs == null) {
            return null;
        }
        return rs.addDataBlockResponse(offset, rowcount, columncount, this);
    }

    @Override
    public TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues) throws MCLParseException {
        return OldMapiTableHeaderParser.GetNextTableHeader((StringBuilder) line, stringValues, intValues);
    }

    @Override
    public int parseTupleLine(Object line, Object[] values, int[] typesMap) throws MCLParseException {
        return OldMapiTupleLineParser.OldMapiParseTupleLine((StringBuilder) line, values, this.tupleLineBuilder, typesMap);
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return this.builder.substring(startIndex);
    }

    @Override
    public void writeNextCommand(byte[] prefix, byte[] query, byte[] suffix) throws IOException {
        this.connection.writeNextLine(prefix, query, suffix);
    }
}
