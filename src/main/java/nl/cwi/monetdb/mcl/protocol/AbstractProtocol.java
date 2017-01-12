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

/**
 * A generic protocol for the underlying connection, either as a socket or an embedded connection. All the server
 * responses are retrieved from this class. At the same time, the user queries are submitted through this class.
 *
 * @author Pedro Ferreira
 */
public abstract class AbstractProtocol {

    /**
     * Waits until the server sends the PROMPT message, meaning that the next response is ready for retrieval.
     *
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void waitUntilPrompt() throws IOException;

    /**
     * Fetch the server's next response data.
     *
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void fetchNextResponseData() throws IOException;

    /**
     * Get the current server response, obtained through the fetchNextResponseData method.
     *
     * @return The integer representation of the server response
     */
    public abstract int getCurrentServerResponse();

    /**
     * Get the next starter header of a server response.
     *
     * @return The integer representation of the starter header
     */
    public abstract int getNextStarterHeader();

    /**
     * Get the next ResultSet response from the server, belonging to a ResponseList.
     *
     * @param con The current MonetDB's JDBC connection
     * @param list The Response List this result set will belong to
     * @param seqnr The sequence number of this result set on the Response List
     * @return The ResultSet instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list,
                                                               int seqnr) throws ProtocolException;

    /**
     * Get the next UpdateResponse response from the server.
     *
     * @return The UpdateResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract UpdateResponse getNextUpdateResponse() throws ProtocolException;

    /**
     * Get the next SchemaResponse response from the server.
     *
     * @return The SchemaResponse instance
     */
    public SchemaResponse getNextSchemaResponse() {
        return new SchemaResponse();
    }

    /**
     * Get the next AutoCommitResponse response from the server.
     *
     * @return The AutoCommitResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException;

    /**
     * Get the next DataBlockResponse response from the server, belonging to a ResultSetResponse
     *
     * @param rsresponses A map of ResultSetResponse, in which this Block will belong to one of them, by checking its id
     *                    against the keys of the Map.
     * @return The DataBlockResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract DataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
            throws ProtocolException;

    /**
     * Get the next Table Header for a ResultSetResponse. More than one of the parameter arrays can be filled at once.
     *
     * @param columnNames The column names array
     * @param columnLengths The column lengths array
     * @param types The columns SQL names array
     * @param tableNames The columns schemas and names in format schema.table
     * @return A TableResultHeaders integer representation, representing which of the fields was filled
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract int getNextTableHeader(String[] columnNames, int[] columnLengths, String[] types,
                                           String[] tableNames) throws ProtocolException;

    /**
     * Retrieves the next values in a DataBlockResponse from the underlying connection, starting at a specific line
     * number.
     *
     * @param firstLineNumber The first line number in the response to retrieve
     * @param typesMap The JDBC types mapping array for every column in the ResultSetResponse of the DataBlock
     * @param values An array of columns to fill the values
     * @return The number of lines parsed from the underlying connection
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract int parseTupleLines(int firstLineNumber, int[] typesMap, Object[] values) throws ProtocolException;

    /**
     * Gets the remaining response line from the underlying connection as a Java String. This method is mostly used to
     * retrieve error Strings, when they are detected while parsing a response line.
     *
     * @param startIndex The first index in the response line to retrieve the String
     * @return The String representation of the line starting at the provided index
     */
    public abstract String getRemainingStringLine(int startIndex);

    /**
     * Writes a user query to the server, while providing the respective prefixes and suffixes depending on the current
     * language and connection used.
     *
     * @param prefix The prefix to append at the beginning of the query string
     * @param query The user query to submit at the server
     * @param suffix The suffix to append at the end of the query string
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void writeNextQuery(String prefix, String query, String suffix) throws IOException;
}
