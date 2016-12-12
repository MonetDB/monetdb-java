package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.socket.OldMapiSocket;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ferreira on 11/30/16.
 */
public class OldMapiProtocol extends AbstractProtocol<StringBuilder> {

    private static final int STRING_BUILDER_INITIAL_SIZE = 128;

    private final OldMapiSocket socket;

    final StringBuilder builder;

    int currentPointer = 0;

    private final StringBuilder tupleLineBuilder;

    public OldMapiProtocol(OldMapiSocket socket) {
        this.socket = socket;
        this.builder = new StringBuilder(STRING_BUILDER_INITIAL_SIZE);
        this.tupleLineBuilder = new StringBuilder(STRING_BUILDER_INITIAL_SIZE);
    }

    public OldMapiSocket getSocket() {
        return socket;
    }

    boolean hasRemaining() {
        return this.currentPointer < this.builder.length();
    }

    @Override
    public ServerResponses waitUntilPrompt() throws IOException {
        while(this.currentServerResponseHeader != ServerResponses.PROMPT) {
            if(this.socket.readLine(this.builder) == 0) {
                throw new IOException("Connection to server lost!");
            }
            this.currentPointer = 0;
            this.currentServerResponseHeader = OldMapiServerResponseParser.ParseOldMapiServerResponse(this);
            if (this.currentServerResponseHeader == ServerResponses.ERROR) {
                this.currentPointer = 1;
            }
        }
        return this.currentServerResponseHeader;
    }

    @Override
    public void fetchNextResponseData() throws IOException { //readLine equivalent
        this.socket.readLine(this.builder);
        this.currentPointer = 0;
        this.currentServerResponseHeader = OldMapiServerResponseParser.ParseOldMapiServerResponse(this);
        if (this.currentServerResponseHeader == ServerResponses.ERROR && !this.builder.toString().matches("^![0-9A-Z]{5}!.+")) {
            //this.builder.deleteCharAt(0);
            this.builder.insert(0, "!22000!");
        }
        this.currentPointer = 1;
    }

    @Override
    public StringBuilder getCurrentData() {
        return this.builder;
    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        StarterHeaders res = OldMapiStartOfHeaderParser.GetNextStartHeaderOnOldMapi(this);
        this.currentPointer += 2;
        return res;
    }

    @Override
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr)
            throws ProtocolException {
        int id = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //The order cannot be switched!!
        int tuplecount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int columncount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int rowcount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        return new ResultSetResponse(con, list, seqnr, id, rowcount, tuplecount, columncount);
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws ProtocolException {
        int count = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //The order cannot be switched!!
        String lastId = OldMapiStartOfHeaderParser.GetNextResponseDataAsString(this);
        return new UpdateResponse(lastId, count);
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException {
        boolean ac = OldMapiStartOfHeaderParser.GetNextResponseDataAsString(this).equals("t");
        return new AutoCommitResponse(ac);
    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
            throws ProtocolException {
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
    public TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues)
            throws ProtocolException {
        return OldMapiTableHeaderParser.GetNextTableHeader((StringBuilder) line, stringValues, intValues);
    }

    @Override
    public int parseTupleLine(Object line, Object[] values, int[] typesMap) throws ProtocolException {
        return OldMapiTupleLineParser.OldMapiParseTupleLine((StringBuilder) line, values, this.tupleLineBuilder,
                typesMap);
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return this.builder.substring(startIndex);
    }

    @Override
    public void writeNextQuery(String prefix, String query, String suffix) throws IOException {
        this.socket.writeNextLine(prefix, query, suffix);
        this.currentServerResponseHeader = ServerResponses.UNKNOWN; //reset reader state
    }
}
