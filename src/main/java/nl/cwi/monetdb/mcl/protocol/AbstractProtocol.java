package nl.cwi.monetdb.mcl.protocol;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.SchemaResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ferreira on 11/30/16.
 */
public abstract class AbstractProtocol<T> {

    protected ServerResponses currentServerResponseHeader = ServerResponses.UNKNOWN;

    public ServerResponses waitUntilPrompt() {
        while(this.currentServerResponseHeader != ServerResponses.PROMPT) {
           this.fetchNextResponseData();
        }
        return this.currentServerResponseHeader;
    }

    public abstract void fetchNextResponseData(); //UPDATE currentData!!!

    public ServerResponses getCurrentServerResponseHeader() {
        return currentServerResponseHeader;
    }

    public abstract T getCurrentData();

    public abstract StarterHeaders getNextStarterHeader();


    public abstract ResultSetResponse<T> getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws MCLParseException;

    public abstract UpdateResponse getNextUpdateResponse() throws MCLParseException;

    public SchemaResponse getNextSchemaResponse() {
        return new SchemaResponse();
    }

    public abstract AutoCommitResponse getNextAutoCommitResponse() throws MCLParseException;

    public abstract DataBlockResponse<T> getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws MCLParseException;


    public abstract TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues) throws MCLParseException;

    public abstract void parseTupleLine(Object line, Object[] values) throws MCLParseException;

    public abstract String getRemainingStringLine(int startIndex);

    public abstract void writeNextCommand(byte[] prefix, byte[] query, byte[] suffix) throws IOException;

}
