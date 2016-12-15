package nl.cwi.monetdb.mcl.connection.embedded;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;
import nl.cwi.monetdb.mcl.responses.IResponse;

/**
 * Created by ferreira on 12/1/16.
 */
public class JDBCEmbeddedConnection extends MonetDBEmbeddedConnection {

    private long lastResultSetPointer;

    private final ServerResponses[] lineResponse = new ServerResponses[8];

    private int currentLineResponseState;

    private StarterHeaders serverHeaderResponse;

    private final int[] lastServerResponseParameters = new int[4]; //for ResultSetResponse and DataBlockResponse

    private IResponse lastServerResponse; //for Update and Autocommit

    private String lastError;

    protected JDBCEmbeddedConnection(long connectionPointer) {
        super(connectionPointer);
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
                                               String[] tableNames) throws MonetDBEmbeddedException {
        this.getNextTableHeader(this.connectionPointer, this.lastResultSetPointer, columnNames, columnLengths,
                types, tableNames);
        return TableResultHeaders.TABLE;
    }

    public int parseTupleLines(int[] typesMap, Object[] values, boolean[][] nulls) throws MonetDBEmbeddedException {
        return this.parseTupleLines(this.connectionPointer, this.lastResultSetPointer, typesMap, values, nulls);
    }

    public String getLastError() {
        return lastError;
    }

    public void processNextQuery(String query) throws MonetDBEmbeddedException {
        if (!query.endsWith(";")) {
            query += ";";
        }
        this.sendQueryInternal(this.connectionPointer, query, true);
    }

    public void sendAutocommitCommand(int flag) throws MonetDBEmbeddedException { //1 or 0
        this.sendAutocommitCommandInternal(this.connectionPointer, flag);
    }

    public void sendReleaseCommand(int commandId) throws MonetDBEmbeddedException {
        this.sendReleaseCommandInternal(this.connectionPointer, commandId);
    }

    public void sendCloseCommand(int commandId) throws MonetDBEmbeddedException {
        this.sendCloseCommandInternal(this.connectionPointer, commandId);
    }

    private native void getNextTableHeader(long connectionPointer, long resultSetPointer, String[] columnNames,
                                           int[] columnLengths, String[] types, String[] tableNames)
            throws MonetDBEmbeddedException;

    private native int parseTupleLines(long connectionPointer, long resultSetPointer, int[] typesMap, Object[] values,
                                       boolean[][] nulls) throws MonetDBEmbeddedException;

    private native void sendQueryInternal(long connectionPointer, String query, boolean execute)
            throws MonetDBEmbeddedException;

    private native void sendAutocommitCommandInternal(long connectionPointer, int flag)
            throws MonetDBEmbeddedException;

    private native void sendReleaseCommandInternal(long connectionPointer, int commandId)
            throws MonetDBEmbeddedException;

    private native void sendCloseCommandInternal(long connectionPointer, int commandId) throws MonetDBEmbeddedException;
}
