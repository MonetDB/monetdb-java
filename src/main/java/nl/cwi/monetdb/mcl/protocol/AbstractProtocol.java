/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.responses.*;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * A generic protocol for the underlying connection, either as a socket or an embedded connection. All the server
 * responses are retrieved from this class. At the same time, the user queries are submitted through this class.
 *
 * @author Pedro Ferreira
 */
public abstract class AbstractProtocol {

    /* only parse the date patterns once, use multiple times */
    /** Format of a date used by mserver */
    private final SimpleDateFormat monetDate = new SimpleDateFormat("yyyy-MM-dd");

    /** Format of a time */
    private final SimpleDateFormat monetTime = new SimpleDateFormat("HH:mm:ss.SSS");
    /** Format of a time with RFC822 time zone */
    private final SimpleDateFormat monetTimeTz = new SimpleDateFormat("HH:mm:ss.SSSZ");
    /** Format to print a Time String */
    private final SimpleDateFormat monetTimePrinter = new SimpleDateFormat("HH:mm:ss");
    /** Format to print a TimeTz String */
    private final SimpleDateFormat monetTimeTzPrinter = new SimpleDateFormat("HH:mm:ssXXX");

    /** Format of a timestamp */
    private final SimpleDateFormat monetTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    /** Format of a timestamp with RFC822 time zone */
    private final SimpleDateFormat monetTimestampTz = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    /** Format to print a TimeStamp String */
    private final SimpleDateFormat monetTimestampPrinter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    /** Format to print a TimeStampTz String */
    private final SimpleDateFormat monetTimestampTzPrinter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");

    /** A helper to parse Dates, Times and Timestamps, to be reused during the connection to save memory allocations. */
    private final ParsePosition monetParserPosition = new ParsePosition(0);

    /**
     * Gets the MonetDB Date formatter.
     *
     * @return The MonetDB Date formatter
     */
    public SimpleDateFormat getMonetDate() {
        return monetDate;
    }

    /**
     * Gets the MonetDB Time formatter.
     *
     * @return The MonetDB Time formatter
     */
    public SimpleDateFormat getMonetTime() {
        return monetTime;
    }

    /**
     * Gets the MonetDB Time with RFC822 time zone formatter.
     *
     * @return The MonetDB Time with RFC822 time zone formatter
     */
    public SimpleDateFormat getMonetTimeTz() {
        return monetTimeTz;
    }

    /**
     * Gets the MonetDB Time printer.
     *
     * @return The MonetDB Time printer
     */
    public SimpleDateFormat getMonetTimePrinter() {
        return monetTimePrinter;
    }

    /**
     * Gets the MonetDB Time with timezone printer.
     *
     * @return The MonetDB Time with timezone printer.
     */
    public SimpleDateFormat getMonetTimeTzPrinter() {
        return monetTimeTzPrinter;
    }

    /**
     * Gets the MonetDB Timestamp formatter.
     *
     * @return The MonetDB Timestamp formatter
     */
    public SimpleDateFormat getMonetTimestamp() {
        return monetTimestamp;
    }

    /**
     * Gets the MonetDB Timestamp with RFC822 time zone formatter.
     *
     * @return The MonetDB Timestamp with RFC822 time zone formatter
     */
    public SimpleDateFormat getMonetTimestampTz() {
        return monetTimestampTz;
    }

    /**
     * Gets the MonetDB Timestamp printer.
     *
     * @return The MonetDB Timestamp printer
     */
    public SimpleDateFormat getMonetTimestampPrinter() {
        return monetTimestampPrinter;
    }

    /**
     * Gets the MonetDB Timestamp with timezone printer.
     *
     * @return The MonetDB Timestamp with timezone printer.
     */
    public SimpleDateFormat getMonetTimestampTzPrinter() {
        return monetTimestampTzPrinter;
    }

    /**
     * Gets the Protocol parser position.
     *
     * @return The Protocol parser position
     */
    public ParsePosition getMonetParserPosition() {
        return monetParserPosition;
    }

    /**
     * Waits until the server sends the PROMPT message, meaning that the next response is ready for retrieval.
     *
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void waitUntilPrompt() throws IOException;

    /**
     * Fetches the server's next response data.
     *
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void fetchNextResponseData() throws IOException;

    /**
     * Gets the current server response.
     *
     * @return The integer representation of the server response
     */
    public abstract int getCurrentServerResponse();

    /**
     * Gets the next starter header of a server response.
     *
     * @return The integer representation of {@link StarterHeaders}
     */
    public abstract int getNextStarterHeader();

    /**
     * Gets the next ResultSet response from the server, belonging to a ResponseList.
     *
     * @param con The current MonetDB's JDBC connection
     * @param list The Response List this result set will belong to
     * @param seqnr The sequence number of this result set on the Response List
     * @param maxrows A maxrows to set if so
     * @return The ResultSet instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract ResultSetResponse getNextResultSetResponse(MonetConnection con, MonetConnection.ResponseList list,
                                                               int seqnr, int maxrows) throws ProtocolException;

    /**
     * Gets the next UpdateResponse response from the server.
     *
     * @return An UpdateResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract UpdateResponse getNextUpdateResponse() throws ProtocolException;

    /**
     * Gets the next SchemaResponse response from the server.
     *
     * @return A SchemaResponse instance
     */
    public SchemaResponse getNextSchemaResponse() {
        return new SchemaResponse();
    }

    /**
     * Gets the next AutoCommitResponse response from the server.
     *
     * @return An AutoCommitResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract AutoCommitResponse getNextAutoCommitResponse() throws ProtocolException;

    /**
     * Get an empty DataBlockResponse from the server.
     *
     * @param rowcount - Number of tuples
     * @param columncount - Number of tuples
     * @param protocol - This protocol
     * @param JdbcSQLTypes - the types array
     * @return An AbstractDataBlockResponse instance
     */
    public abstract AbstractDataBlockResponse getAnEmptyDataBlockResponse(int rowcount, int columncount,
                                                                          AbstractProtocol protocol,
                                                                          int[] JdbcSQLTypes);

    /**
     * Gets the next DataBlockResponse response from the server, belonging to a ResultSetResponse
     *
     * @param rsresponses A map of ResultSetResponse, in which this Block will belong to one of them, by checking its id
     *                    against the keys of the Map.
     * @return The DataBlockResponse instance
     * @throws ProtocolException If an error in the underlying connection happened.
     */
    public abstract AbstractDataBlockResponse getNextDatablockResponse(Map<Integer, ResultSetResponse> rsresponses)
            throws ProtocolException;

    /**
     * Gets the next Table Header for a ResultSetResponse. More than one of the parameter arrays can be filled at once.
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
     * @param query The user query to submit to the server
     * @param suffix The suffix to append at the end of the query string
     * @throws IOException If an error in the underlying connection happened.
     */
    public abstract void writeNextQuery(String prefix, String query, String suffix) throws IOException;
}
