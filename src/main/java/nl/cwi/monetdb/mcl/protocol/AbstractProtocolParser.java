package nl.cwi.monetdb.mcl.protocol;

/**
 * Created by ferreira on 11/30/16.
 */
public abstract class AbstractProtocolParser {

    protected ServerResponses currentServerResponseHeader = ServerResponses.UNKNOWN;

    protected StarterHeaders currentStarterHeader = StarterHeaders.Q_UNKNOWN;

    protected TableResultHeaders currentTableResultSetHeader = TableResultHeaders.UNKNOWN;

    public ServerResponses getCurrentServerResponseHeader() {
        return currentServerResponseHeader;
    }

    public StarterHeaders getCurrentStarterHeader() {
        return currentStarterHeader;
    }

    public TableResultHeaders getCurrentTableResultSetHeader() {
        return currentTableResultSetHeader;
    }

    public ServerResponses getNextResponseHeader() {
        this.currentServerResponseHeader = this.getNextResponseHeaderImplementation();
        return this.currentServerResponseHeader;
    }

    public StarterHeaders getNextStarterHeader() {
        this.currentStarterHeader = this.getNextStarterHeaderImplementation();
        return this.currentStarterHeader;
    }

    public TableResultHeaders getNextTableHeader() {
        this.currentTableResultSetHeader = this.getNextTableHeaderImplementation();
        return this.currentTableResultSetHeader;
    }

    protected abstract ServerResponses getNextResponseHeaderImplementation();

    protected abstract TableResultHeaders getNextTableHeaderImplementation();

    protected abstract StarterHeaders getNextStarterHeaderImplementation();
}
