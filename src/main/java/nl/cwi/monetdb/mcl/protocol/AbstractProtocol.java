/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.responses.AutoCommitResponse;
import nl.cwi.monetdb.mcl.responses.SchemaResponse;
import nl.cwi.monetdb.mcl.responses.UpdateResponse;
import nl.cwi.monetdb.mcl.responses.DataBlockResponse;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractProtocol {

    public abstract void waitUntilPrompt() throws IOException;

    public abstract void fetchNextResponseData() throws IOException;

    public abstract int getCurrentServerResponseHeader();

    public abstract int getNextStarterHeader();

    public abstract ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list,
                                                               int seqnr) throws ProtocolException;

    public abstract UpdateResponse getNextUpdateResponse() throws ProtocolException;

    public SchemaResponse getNextSchemaResponse() {
        return new SchemaResponse();
    }

    public abstract AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException;

    public abstract DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
            throws ProtocolException;

    public abstract int getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types,
                                           String[] tableNames) throws ProtocolException;

    public abstract int parseTupleLines(int firstLineNumber, int[] typesMap, Object[] values,
                                        boolean[][] nulls) throws ProtocolException;

    public abstract String getRemainingStringLine(int startIndex);

    public abstract void writeNextQuery(String prefix, String query, String suffix) throws IOException;
}
