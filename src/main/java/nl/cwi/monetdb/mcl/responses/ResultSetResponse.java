/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.jdbc.MonetDriver;
import nl.cwi.monetdb.mcl.connection.ControlCommands;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The ResultSetResponse represents a tabular result sent by the server. This is typically an SQL table. The MAPI
 * headers of the Response look like:
 * <pre>
 * &amp;1 1 28 2 10
 * # name,     value # name
 * # varchar,  varchar # type
 * </pre>
 * there the first line consists out of<br />
 * <tt>&amp;"qt" "id" "tc" "cc" "rc"</tt>.
 * Meanwhile on an Embedded connection the data is fetched with no parsing.
 */
public class ResultSetResponse implements IIncompleteResponse {

    /**
     * The expected final value after the table headers are set.
     */
    private static final byte IS_SET_FINAL_VALUE = 15;

    /** The number of rows in the current block */
    private final int rowcount;
    /** The total number of rows this result set has */
    private final int tuplecount;
    /** The numbers of rows to retrieve per DataBlockResponse */
    private int cacheSize;
    /** The table ID of this result */
    private final int id;
    /** The names of the columns in this result */
    private final String[] name;
    /** The types of the columns in this result */
    private final String[] type;
    /** The JDBC SQL types of the columns in this ResultSet. The content will be derived from the MonetDB types[] */
    private final int[] JdbcSQLTypes;
    /** The max string length for each column in this result */
    private final int[] columnLengths;
    /** The table for each column in this result */
    private final String[] tableNames;
    /** The query sequence number */
    private final int seqnr;
    /** A bitmap telling whether the headers are set or not */
    private byte isSet;
    /** Whether this Response is closed */
    private boolean closed;
    /** The connection belonging for this ResultSetResponse */
    private MonetConnection con;
    /** The Connection that we should use when requesting a new block */
    private MonetConnection.ResponseList parent;
    /** Whether the fetchSize was explicitly set by the user */
    private boolean cacheSizeSetExplicitly = false;
    /** Whether we should send an Xclose command to the server if we close this Response */
    private boolean destroyOnClose;
    /** the offset to be used on Xexport queries */
    private int blockOffset = 0;
    /** A List of result blocks (chunks of size fetchSize/cacheSize) */
    private final DataBlockResponse[] resultBlocks;

    /**
     * Sole constructor, which requires a MonetConnection parent to be given.
     *
     * @param con The connection of this ResultSet
     * @param parent the parent that created this Response and will supply new result blocks when necessary
     * @param id the ID of the result set
     * @param seq the query sequence number
     * @param rowcount the number of rows in the current block
     * @param tuplecount the total number of tuples in the result set
     * @param columncount the number of columns in the result set
     */
    public ResultSetResponse(MonetConnection con, MonetConnection.ResponseList parent, int id, int seq, int rowcount,
                             int tuplecount, int columncount) {
        this.con = con;
        this.parent = parent;
        if (parent.getCachesize() == 0) {
            /* Below we have to calculate how many "chunks" we need to allocate to store the entire result. However, if
               the user didn't set a cache size, as in this case, we need to stick to our defaults. */
            cacheSize = con.getDefFetchsize();
            cacheSizeSetExplicitly = false;
        } else {
            cacheSize = parent.getCachesize();
            cacheSizeSetExplicitly = true;
        }

        /* So far, so good.  Now the problem with EXPLAIN, DOT, etc queries is, that they don't support any block
           fetching, so we need to always fetch everything at once.  For that reason, the cache size is here set to the
           rowcount if it's larger, such that we do a full fetch at once. (Because we always set a reply_size, we can
           only get a larger rowcount from the server if it doesn't paginate, because it's a pseudo SQL result.) */
        if (rowcount > cacheSize) {
            cacheSize = rowcount;
        }
        this.seqnr = seq;
        this.destroyOnClose = id > 0 && tuplecount > rowcount;
        this.id = id;
        this.rowcount = rowcount;

        int maxrows = parent.getMaxrows();
        this.tuplecount = (maxrows != 0 && tuplecount > maxrows) ? maxrows : tuplecount;

        this.name = new String[columncount];
        this.type = new String[columncount];
        this.tableNames = new String[columncount];
        this.columnLengths = new int[columncount];
        this.JdbcSQLTypes = new int[columncount];

        this.resultBlocks = new DataBlockResponse[(tuplecount / cacheSize) + 1];
        this.resultBlocks[0] = new DataBlockResponse(rowcount, columncount, con.getProtocol(), this.JdbcSQLTypes);
    }

    /**
     * Internal utility method to fill the JdbcSQLTypes array with derivable values. By doing it once (in the
     * constructor) we can avoid doing this in many getXyz() methods again and again thereby improving getXyz() method
     * performance.
     */
    private void populateJdbcSQLTypesArray() {
        for (int i = 0; i < this.type.length; i++) {
            int javaSQLtype = MonetDriver.getJavaType(this.type[i]);
            if (javaSQLtype == Types.BLOB && con.getBlobAsBinary()) {
                javaSQLtype = Types.LONGVARBINARY;
            }
            if (javaSQLtype == Types.CLOB && con.getClobAsLongChar()) {
                javaSQLtype = Types.LONGVARCHAR;
            }
            this.JdbcSQLTypes[i] = javaSQLtype;
        }
    }

    /**
     * Returns whether this ResultSetResponse needs more lines. This method returns true if not all headers are set,
     * or the first DataBlockResponse reports to want more.
     */
    @Override
    public boolean wantsMore() {
        return this.isSet < IS_SET_FINAL_VALUE || resultBlocks[0].wantsMore();
    }

    /**
     * Adds the given DataBlockResponse to this ResultSetResponse at the given block position.
     *
     * @param offset the offset number of rows for this block
     * @param rowcount the number of rows for this block
     * @param proto The connection's protocol
     */
    public DataBlockResponse addDataBlockResponse(int offset, int rowcount, AbstractProtocol proto) {
        int block = (offset - blockOffset) / cacheSize;
        DataBlockResponse res = new DataBlockResponse(rowcount, this.name.length, proto, JdbcSQLTypes);
        resultBlocks[block] = res;
        return res;
    }

    /**
     * Returns this ResultSet id
     *
     * @return The resultSet id
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the number of columns on this ResultSet
     *
     * @return The number of columns on this ResultSet
     */
    public int getColumncount() {
        return name.length;
    }

    /**
     * Returns the number of rows on this ResultSet
     *
     * @return The number of rows on this ResultSet
     */
    public int getTuplecount() {
        return tuplecount;
    }

    /**
     * Returns the number of rows on the current block
     *
     * @return The number of rows on the current block
     */
    public int getRowcount() {
        return rowcount;
    }

    /**
     * Returns the names of the columns
     *
     * @return The names of the columns
     */
    public String[] getNames() {
        return name;
    }

    /**
     * Returns the types of the columns
     *
     * @return The types of the columns
     */
    public String[] getTypes() {
        return type;
    }

    /**
     * Returns the JDBC types of the columns
     *
     * @return The JDBC types of the columns
     */
    public int[] getJdbcSQLTypes() {
        return JdbcSQLTypes;
    }

    /**
     * Returns the tables of the columns
     *
     * @return The tables of the columns
     */
    public String[] getTableNames() {
        return tableNames;
    }

    /**
     * Returns the lengths of the columns
     *
     * @return The lengths of the columns
     */
    public int[] getColumnLengths() {
        return columnLengths;
    }

    /**
     * Returns the cache size used within this Response
     *
     * @return The cache size
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * Returns the current block offset
     *
     * @return The current block offset
     */
    public int getBlockOffset() {
        return blockOffset;
    }

    /**
     * Returns the ResultSet type, FORWARD_ONLY or not.
     *
     * @return The ResultSet type
     */
    public int getRSType() {
        return parent.getRstype();
    }

    /**
     * Returns the concurrency of the ResultSet.
     *
     * @return The ResultSet concurrency
     */
    public int getRSConcur() {
        return parent.getRsconcur();
    }

    /**
     * Gets the next table headers from the underlying protocol, or gets the next rows on to the underlying
     * DataResponse if the headers are already retrieved.
     *
     * @param protocol the connection's protocol
     * @throws ProtocolException if has a wrong header
     */
    @Override
    public void addLines(AbstractProtocol protocol) throws ProtocolException {
        if (this.isSet >= IS_SET_FINAL_VALUE) {
            this.resultBlocks[0].addLines(protocol);
        } else {
            int csrh = protocol.getCurrentServerResponse();
            if (csrh != ServerResponses.HEADER) {
                throw new ProtocolException("header expected, got: " + protocol.getRemainingStringLine(0));
            } else {
                int next = con.getProtocol().getNextTableHeader(this.name, this.columnLengths, this.type, this.tableNames);
                this.isSet |= next;
                if (this.isSet >= IS_SET_FINAL_VALUE) {
                    this.populateJdbcSQLTypesArray(); //VERY IMPORTANT to populate the JDBC types array
                }
            }
        }
    }

    /**
     * Returns a line from the cache. If the line is already present in the cache, it is returned, if not appropriate
     * actions are taken to make sure the right block is being fetched and as soon as the requested line is fetched it
     * is returned.
     *
     * @param row the row in the result set to return
     * @return the exact row read as requested or null if the requested row is out of the scope of the result set
     * @throws SQLException if an database error occurs
     */
    public DataBlockResponse getDataBlockCorrespondingToLine(int row) throws SQLException {
        if (row >= tuplecount || row < 0)
            return null;

        int block = (row - blockOffset) / cacheSize;
        int blockLine = (row - blockOffset) % cacheSize;

        // do we have the right block loaded? (optimistic try)
        DataBlockResponse rawr;
        // load block if appropriate
        if ((rawr = resultBlocks[block]) == null) {
            // TODO: ponder about a maximum number of blocks to keep in memory when dealing with random access to
            // reduce memory blow-up

            // if we're running forward only, we can discard the resultset block loaded
            if (parent.getRstype() == ResultSet.TYPE_FORWARD_ONLY) {
                for (int i = 0; i < block; i++)
                    resultBlocks[i] = null;

                if (MonetConnection.GetSeqCounter() - 1 == seqnr && !cacheSizeSetExplicitly &&
                        tuplecount - row > cacheSize && cacheSize < con.getDefFetchsize() * 10) {
                    // there has no query been issued after this one, so we can consider this an uninterrupted
                    // continuation request.  Let's once increase the cacheSize as it was not explicitly set,
                    // since the chances are high that we won't bother anyone else by doing so, and just gaining
                    // some performance.

                    // store the previous position in the blockOffset variable
                    blockOffset += cacheSize;

                    // increase the cache size (a lot)
                    cacheSize *= 10;

                    // by changing the cacheSize, we also change the block measures. Luckily we don't care about
                    // previous blocks because we have a forward running pointer only. However, we do have to
                    // recalculate the block number, to ensure the next call to find this new block.
                    block = (row - blockOffset) / cacheSize;
                    blockLine = (row - blockOffset) % cacheSize;
                }
            }

            // ok, need to fetch cache block first
            parent.executeQuery(con.getLanguage().getCommandTemplates(), "export " + id + " "
                    + ((block * cacheSize) + blockOffset) + " " + cacheSize);
            rawr = resultBlocks[block];
            if (rawr == null) {
                throw new AssertionError("block " + block + " should have been fetched by now :(");
            }
        }
        rawr.setBlockLine(blockLine);
        return rawr;
    }

    /**
     * Closes this Response by sending an Xclose to the server indicating that the result can be closed at the server
     * side as well.
     */
    @Override
    public void close() {
        if (closed) return;
        // send command to server indicating we're done with this result only if we had an ID in the header and this
        // result was larger than the reply size
        try {
            if (destroyOnClose) {
                con.sendControlCommand(ControlCommands.CLOSE, id);
            }
        } catch (SQLException e) {
            // probably a connection error...
        }

        // close the data block associated with us
        for (int i = 1; i < resultBlocks.length; i++) {
            DataBlockResponse r = resultBlocks[i];
            if (r != null) r.close();
        }

        closed = true;
    }

    /**
     * Returns whether this Response is closed
     *
     * @return whether this Response is closed
     */
    public boolean isClosed() {
        return closed;
    }
}
