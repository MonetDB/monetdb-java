package nl.cwi.monetdb.mcl.connection.embedded;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;
import nl.cwi.monetdb.mcl.responses.IResponse;

/**
 * Created by ferreira on 12/1/16.
 */
public class JDBCEmbeddedConnection extends MonetDBEmbeddedConnection {

    private int maxRows = Integer.MAX_VALUE;

    private long lastResultSetPointer;

    private final ServerResponses[] lineResponse = new ServerResponses[4];

    private int currentLineResponseState;

    private StarterHeaders serverHeaderResponse;

    private final int[] lastServerResponseParameters = new int[4]; //for ResultSetResponse and DataBlockResponse

    private IResponse lastServerResponse; //for Update and Autocommit

    private String lastError;

    protected JDBCEmbeddedConnection(long connectionPointer) {
        super(connectionPointer);
    }

    void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public ServerResponses getNextServerResponse() {
        return lineResponse[currentLineResponseState++];
    }

    public StarterHeaders getServerHeaderResponse() {
        return serverHeaderResponse;
    }

    public int[] getLastServerResponseParameters() {
        return lastServerResponseParameters;
    }

    public IResponse getLastServerResponse() {
        return lastServerResponse;
    }

    public TableResultHeaders fillTableHeaders(String[] columnNames, int[] columnLengths, String[] types,
                                               String[] tableNames) {
        this.getNextTableHeaderInternal(this.lastResultSetPointer, columnNames, columnLengths, types, tableNames);
        return TableResultHeaders.ALL;
    }

    public int parseTupleLines(int[] typesMap, Object[] values, boolean[][] nulls) {
        return this.parseTupleLinesInternal(this.lastResultSetPointer, typesMap, values, nulls);
    }

    public String getLastError() {
        return lastError;
    }

    public void processNextQuery(String query) {
        if (!query.endsWith(";")) {
            query += ";";
        }
        this.currentLineResponseState = 0;
        this.sendQueryInternal(this.connectionPointer, query, true);
    }

    void sendAutocommitCommand(int flag) { //1 or 0
        this.sendAutocommitCommandInternal(this.connectionPointer, flag);
    }

    void sendReleaseCommand(int commandId) {
        this.sendReleaseCommandInternal(this.connectionPointer, commandId);
    }

    void sendCloseCommand(int commandId) {
        this.sendCloseCommandInternal(this.connectionPointer, commandId);
    }

    private native void getNextTableHeaderInternal(long resultSetPointer, String[] columnNames, int[] columnLengths,
                                                   String[] types, String[] tableNames);

    private native int parseTupleLinesInternal(long resultSetPointer, int[] typesMap, Object[] values,
                                               boolean[][] nulls);

    private native void sendQueryInternal(long connectionPointer, String query, boolean execute);

    private native void sendAutocommitCommandInternal(long connectionPointer, int flag);

    private native void sendReleaseCommandInternal(long connectionPointer, int commandId);

    private native void sendCloseCommandInternal(long connectionPointer, int commandId);
}
