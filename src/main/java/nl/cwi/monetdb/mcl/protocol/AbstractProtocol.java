package nl.cwi.monetdb.mcl.protocol;

import nl.cwi.monetdb.jdbc.MonetConnection;
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
public abstract class AbstractProtocol {

    protected ServerResponses currentServerResponseHeader = ServerResponses.UNKNOWN;

    public abstract ServerResponses waitUntilPrompt() throws IOException;

    public abstract void fetchNextResponseData() throws IOException; //UPDATE currentData!!!

    public ServerResponses getCurrentServerResponseHeader() {
        return currentServerResponseHeader;
    }

    public abstract Object getCurrentData();

    public abstract StarterHeaders getNextStarterHeader();

    public abstract ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list,
                                                               int seqnr) throws ProtocolException;

    public abstract UpdateResponse getNextUpdateResponse() throws ProtocolException;

    public SchemaResponse getNextSchemaResponse() {
        return new SchemaResponse();
    }

    public abstract AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException;

    public abstract DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
            throws ProtocolException;

    public abstract TableResultHeaders getNextTableHeader(Object line, String[] stringValues, int[] intValues)
            throws ProtocolException;

    public abstract int parseTupleLine(int lineNumber, Object line, int[] typesMap, Object[] values, boolean[] nulls)
            throws ProtocolException;

    public abstract String getRemainingStringLine(int startIndex);

    public abstract void writeNextQuery(String prefix, String query, String suffix) throws IOException;
}
