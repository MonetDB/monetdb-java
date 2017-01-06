/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.mapi.OldMapiSocket;
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
import java.nio.CharBuffer;
import java.util.Map;

public class OldMapiProtocol extends AbstractProtocol {

    private ServerResponses currentServerResponseHeader = ServerResponses.UNKNOWN;

    private final OldMapiSocket socket;

    CharBuffer lineBuffer;

    CharBuffer tupleLineBuffer;

    public OldMapiProtocol(OldMapiSocket socket) {
        this.socket = socket;
        this.lineBuffer = CharBuffer.wrap(new char[OldMapiSocket.BLOCK]);
        this.tupleLineBuffer = CharBuffer.wrap(new char[1024]);
    }

    public OldMapiSocket getSocket() {
        return socket;
    }

    @Override
    public ServerResponses getCurrentServerResponseHeader() {
        return currentServerResponseHeader;
    }

    @Override
    public void waitUntilPrompt() throws IOException {
        while(this.currentServerResponseHeader != ServerResponses.PROMPT) {
            this.lineBuffer = this.socket.readLine(this.lineBuffer);
            if(this.lineBuffer.limit() == 0) {
                throw new IOException("Connection to server lost!");
            }
            this.currentServerResponseHeader = OldMapiServerResponseParser.ParseOldMapiServerResponse(this);
            this.lineBuffer.position(0);
            if (this.currentServerResponseHeader == ServerResponses.ERROR) {
                this.lineBuffer.position(1);
            }
        }
    }

    @Override
    public void fetchNextResponseData() throws IOException { //readLine equivalent
        this.lineBuffer = this.socket.readLine(this.lineBuffer);
        if(this.lineBuffer.limit() == 0) {
            throw new IOException("Connection to server lost!");
        }
        this.currentServerResponseHeader = OldMapiServerResponseParser.ParseOldMapiServerResponse(this);
        if (this.currentServerResponseHeader == ServerResponses.ERROR && !this.lineBuffer.toString().matches("^[0-9A-Z]{5}!.+")) {
            CharBuffer newbuffer = CharBuffer.wrap(new char[this.lineBuffer.capacity() + 7]);
            newbuffer.put("!22000!");
            newbuffer.put(this.lineBuffer.array());
            newbuffer.flip();
            this.lineBuffer = newbuffer;
        }
        this.lineBuffer.position(1);
    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        return OldMapiStartOfHeaderParser.GetNextStartHeaderOnOldMapi(this);
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
        int lastId = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //TODO test this!!
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
        int id = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //The order cannot be switched!!
        OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this); //column count
        int rowcount = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);
        int offset = OldMapiStartOfHeaderParser.GetNextResponseDataAsInt(this);

        ResultSetResponse rs = rsresponses.get(id);
        if (rs == null) {
            return null;
        }
        return rs.addDataBlockResponse(offset, rowcount, this);
    }

    @Override
    public TableResultHeaders getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types, String[] tableNames) throws ProtocolException {
        return OldMapiTableHeaderParser.GetNextTableHeader(this.lineBuffer, columnNames, columnLengths, types, tableNames);
    }

    @Override
    public int parseTupleLines(int firstLineNumber, int[] typesMap, Object[] data, boolean[][] nulls)
            throws ProtocolException {
        OldMapiTupleLineParser.OldMapiParseTupleLine(this, firstLineNumber, typesMap, data, nulls);
        return firstLineNumber;
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        if(this.lineBuffer.limit() > startIndex) {
            return new String(this.lineBuffer.array(), startIndex, this.lineBuffer.limit() - startIndex);
        } else {
            return null;
        }
    }

    @Override
    public synchronized void writeNextQuery(String prefix, String query, String suffix) throws IOException {
        this.socket.writeNextLine(prefix, query, suffix);
        this.currentServerResponseHeader = ServerResponses.UNKNOWN; //reset reader state
    }
}