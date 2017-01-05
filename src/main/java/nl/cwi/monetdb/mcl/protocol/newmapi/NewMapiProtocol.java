/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.newmapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.protocol.*;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;

import java.io.IOException;
import java.util.Map;

public class NewMapiProtocol extends AbstractProtocol {

    @Override
    public ServerResponses getCurrentServerResponseHeader() {
        return null;
    }

    @Override
    public void waitUntilPrompt() throws IOException {

    }

    @Override
    public void fetchNextResponseData() throws IOException {

    }

    @Override
    public StarterHeaders getNextStarterHeader() {
        return null;
    }

    @Override
    public ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list, int seqnr) throws ProtocolException {
        return null;
    }

    @Override
    public UpdateResponse getNextUpdateResponse() throws ProtocolException {
        return null;
    }

    @Override
    public AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException {
        return null;
    }

    @Override
    public DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses) throws ProtocolException {
        return null;
    }

    @Override
    public TableResultHeaders getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types, String[] tableNames) throws ProtocolException {
        return null;
    }

    @Override
    public int parseTupleLines(int lineNumber, int[] typesMap, Object[] values, boolean[][] nulls) throws ProtocolException {
        return 0;
    }

    @Override
    public String getRemainingStringLine(int startIndex) {
        return null;
    }

    @Override
    public synchronized void writeNextQuery(String prefix, String query, String suffix) throws IOException {

    }
}
