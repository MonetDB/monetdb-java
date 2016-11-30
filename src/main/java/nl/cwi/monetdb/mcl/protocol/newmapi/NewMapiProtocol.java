package nl.cwi.monetdb.mcl.protocol.newmapi;

import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.io.SocketIOHandler;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocolParser;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

/**
 * Created by ferreira on 11/30/16.
 */
public class NewMapiProtocol extends AbstractProtocolParser {

    private final SocketIOHandler handler;

    public NewMapiProtocol(SocketConnection con) {
        this.handler = new SocketIOHandler(con);
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
