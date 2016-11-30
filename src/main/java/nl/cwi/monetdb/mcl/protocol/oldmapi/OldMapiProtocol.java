package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.io.SocketIOHandler;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocolParser;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

import java.io.IOException;

/**
 * Created by ferreira on 11/30/16.
 */
public class OldMapiProtocol extends AbstractProtocolParser {

    private final SocketIOHandler handler;

    public OldMapiProtocol(SocketConnection con) {
        this.handler = new SocketIOHandler(con);
    }

    public SocketIOHandler getHandler() {
        return handler;
    }

    @Override
    public ServerResponses getNextResponseHeaderImplementation() {
        try {
            char nextToken = handler.readNextChar();
            return OldMapiConverter.GetNextResponseOnOldMapi(nextToken);
        } catch (IOException e) {
            return ServerResponses.ERROR;
        }
    }

    @Override
    public StarterHeaders getNextStarterHeaderImplementation() {
        try {
            char nextToken = handler.readNextChar();
            return OldMapiConverter.GetNextStartHeaderOnOldMapi(nextToken);
        } catch (IOException e) {
            return StarterHeaders.Q_UNKNOWN;
        }
    }

    @Override
    public TableResultHeaders getNextTableHeaderImplementation() {
        return null;
    }
}
