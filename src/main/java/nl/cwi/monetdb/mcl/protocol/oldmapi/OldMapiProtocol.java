package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocolParser;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;
import nl.cwi.monetdb.mcl.protocol.TableResultHeaders;

import java.io.IOException;

/**
 * Created by ferreira on 11/30/16.
 */
public class OldMapiProtocol extends AbstractProtocolParser {

    private static final int STRING_BUILDER_INITIAL_SIZE = 128;

    private final SocketConnection connection;

    private int builderPointer;

    private final StringBuilder builder = new StringBuilder(STRING_BUILDER_INITIAL_SIZE);

    public OldMapiProtocol(SocketConnection con) {
        this.connection = con;
    }

    public SocketConnection getConnection() {
        return connection;
    }

    @Override
    public ServerResponses getNextResponseHeaderImplementation() {
        ServerResponses res = ServerResponses.UNKNOWN;
        try {
            while(res != ServerResponses.PROMPT) {
                connection.readUntilChar(this.builder, '\n');
                res = OldMapiConverter.GetNextResponseOnOldMapi(this.builder.charAt(0));
                if(res == ServerResponses.ERROR && !this.builder.toString().matches("^![0-9A-Z]{5}!.+")) {
                    this.builder.insert(1, "!22000!");
                }
            }
            this.builderPointer = 1;
        } catch (IOException e) {
            res = ServerResponses.ERROR;
            this.builder.setLength(0);
            this.builderPointer = 0;
            this.builder.append("!22000!").append(e.getMessage());
        }
        return res;
    }

    public String getEntireResponseLine() {
        String res = this.builder.toString();
        this.builderPointer = this.builder.length();
        return res;
    }

    public String getRemainingResponseLine(int startIndex) {
        String res = this.builder.substring(this.builderPointer + startIndex);
        this.builderPointer = this.builder.length();
        return res;
    }

    @Override
    public StarterHeaders getNextStarterHeaderImplementation() {
        try {
            char nextToken = connection.readNextChar();
            return OldMapiConverter.GetNextStartHeaderOnOldMapi(nextToken);
        } catch (IOException e) {
            return StarterHeaders.Q_UNKNOWN;
        }
    }

    @Override
    public TableResultHeaders getNextTableHeaderImplementation() {
        return null;
    }


    public void writeNextLine(byte[] line) throws IOException {
        connection.writeNextLine(line);
    }
}
