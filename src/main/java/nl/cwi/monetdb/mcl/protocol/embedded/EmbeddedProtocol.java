package nl.cwi.monetdb.mcl.protocol.embedded;

import nl.cwi.monetdb.mcl.io.JDBCEmbeddedConnection;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocolParser;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

/**
 * Created by ferreira on 11/30/16.
 */
public class EmbeddedProtocol extends AbstractProtocolParser {

    private final JDBCEmbeddedConnection embeddedConnection;

    public EmbeddedProtocol(JDBCEmbeddedConnection embeddedConnection) {
        this.embeddedConnection = embeddedConnection;
    }

    @Override
    public ServerResponses getNextResponseHeaderImplementation() {
        return null;
    }

    @Override
    public StarterHeaders getNextStarterHeaderImplementation() {
        return null;
    }

    @Override
    public TableResultHeaders getNextTableHeaderImplementation() {
        return null;
    }
}
