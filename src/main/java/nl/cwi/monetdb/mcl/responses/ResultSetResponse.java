package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The ResultSetResponse represents a tabular result sent by the
 * server.  This is typically an SQL table.  The MAPI headers of the
 * Response look like:
 * <pre>
 * &amp;1 1 28 2 10
 * # name,     value # name
 * # varchar,  varchar # type
 * </pre>
 * there the first line consists out of<br />
 * <tt>&amp;"qt" "id" "tc" "cc" "rc"</tt>.
 */
public class ResultSetResponse<T> implements IIncompleteResponse {

    private static boolean CheckBooleanValuesAllTrue(boolean[] array) {
        for (boolean anArray : array) {
            if (!anArray) {
                return false;
            }
        }
        return true;
    }

    /** The number of columns in this result */
    private final int columncount;
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
    /** The max string length for each column in this result */
    private final int[] columnLengths;
    /** The table for each column in this result */
    private final String[] tableNames;
    /** The query sequence number */
    private final int seqnr;
    /** A bitmap telling whether the headers are set or not */
    private boolean[] isSet = new boolean[4];
    /** Whether this Response is closed */
    private boolean closed;

    /** The connection belonging for this ResultSetResponse */
    private MonetConnection con;
    /** The Connection that we should use when requesting a new block */
    private MonetConnection.ResponseList parent;
    /** Whether the fetchSize was explitly set by the user */
    private boolean cacheSizeSetExplicitly = false;
    /** Whether we should send an Xclose command to the server if we close this Response */
    private boolean destroyOnClose;
    /** the offset to be used on Xexport queries */
    private int blockOffset = 0;
    /** A List of result blocks (chunks of size fetchSize/cacheSize) */
    private DataBlockResponse<T>[] resultBlocks;

    /**
     * Sole constructor, which requires a MonetConnection parent to
     * be given.
     *
     * @param id the ID of the result set
     * @param tuplecount the total number of tuples in the result set
     * @param columncount the number of columns in the result set
     * @param rowcount the number of rows in the current block
     * @param parent the parent that created this Response and will
     *               supply new result blocks when necessary
     * @param seq the query sequence number
     */
    @SuppressWarnings("unchecked")
    public ResultSetResponse(MonetConnection con, MonetConnection.ResponseList parent, int id, int seq, int rowcount, int tuplecount, int columncount) {
        this.con = con;
        this.parent = parent;
        if (parent.getCachesize() == 0) {
            /* Below we have to calculate how many "chunks" we need
             * to allocate to store the entire result.  However, if
             * the user didn't set a cache size, as in this case, we
             * need to stick to our defaults. */
            cacheSize = MonetConnection.GetDefFetchsize();
            cacheSizeSetExplicitly = false;
        } else {
            cacheSize = parent.getCachesize();
            cacheSizeSetExplicitly = true;
        }

        /* So far, so good.  Now the problem with EXPLAIN, DOT, etc
         * queries is, that they don't support any block fetching,
         * so we need to always fetch everything at once.  For that
         * reason, the cache size is here set to the rowcount if
         * it's larger, such that we do a full fetch at once.
         * (Because we always set a reply_size, we can only get a
         * larger rowcount from the server if it doesn't paginate,
         * because it's a pseudo SQL result.) */
        if (rowcount > cacheSize) {
            cacheSize = rowcount;
        }
        this.seqnr = seq;
        this.destroyOnClose = id > 0 && tuplecount > rowcount;
        this.id = id;
        this.rowcount = rowcount;

        int maxrows = parent.getMaxrows();
        this.tuplecount = (maxrows != 0 && tuplecount > maxrows) ? maxrows : tuplecount;
        this.columncount = columncount;

        this.name = new String[this.columncount];
        this.type = new String[this.columncount];
        this.tableNames = new String[this.columncount];
        this.columnLengths = new int[this.columncount];

        Class<T> persistentClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.resultBlocks = (DataBlockResponse<T>[]) Array.newInstance(DataBlockResponse.class, (tuplecount / cacheSize) + 1);
        this.resultBlocks[0] = new DataBlockResponse<>(rowcount, parent.getRstype() == ResultSet.TYPE_FORWARD_ONLY, persistentClass);
    }

    /**
     * Returns whether this ResultSetResponse needs more lines.
     * This method returns true if not all headers are set, or the
     * first DataBlockResponse reports to want more.
     */
    @Override
    public boolean wantsMore() {
        return !CheckBooleanValuesAllTrue(isSet) || resultBlocks[0].wantsMore();
    }

    /**
     * Adds the given DataBlockResponse to this ResultSetResponse at
     * the given block position.
     *
     * @param offset the offset number of rows for this block
     * @param rr the DataBlockResponse to add
     */
    public void addDataBlockResponse(int offset, DataBlockResponse<T> rr) {
        int block = (offset - blockOffset) / cacheSize;
        resultBlocks[block] = rr;
    }

    /**
     * Marks this Response as being completed.  A complete Response
     * needs to be consistent with regard to its internal data.
     *
     * @throws SQLException if the data currently in this Response is not
     *                      sufficient to be consistent
     */
    @Override
    public void complete() throws SQLException {
        String error = "";
        if (!isSet[0]) error += "name header missing\n";
        if (!isSet[1]) error += "column width header missing\n";
        if (!isSet[2]) error += "table name header missing\n";
        if (!isSet[3]) error += "type header missing\n";
        if (!error.equals("")) throw new SQLException(error, "M0M10");
    }

    public int getId() {
        return id;
    }

    public int getColumncount() {
        return columncount;
    }

    public int getTuplecount() {
        return tuplecount;
    }

    public int getRowcount() {
        return rowcount;
    }

    /**
     * Returns the names of the columns
     *
     * @return the names of the columns
     */
    public String[] getNames() {
        return name;
    }

    /**
     * Returns the types of the columns
     *
     * @return the types of the columns
     */
    public String[] getTypes() {
        return type;
    }

    /**
     * Returns the tables of the columns
     *
     * @return the tables of the columns
     */
    public String[] getTableNames() {
        return tableNames;
    }

    /**
     * Returns the lengths of the columns
     *
     * @return the lengths of the columns
     */
    public int[] getColumnLengths() {
        return columnLengths;
    }

    /**
     * Returns the cache size used within this Response
     *
     * @return the cache size
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * Returns the current block offset
     *
     * @return the current block offset
     */
    public int getBlockOffset() {
        return blockOffset;
    }

    /**
     * Returns the ResultSet type, FORWARD_ONLY or not.
     *
     * @return the ResultSet type
     */
    public int getRSType() {
        return parent.getRstype();
    }

    /**
     * Returns the concurrency of the ResultSet.
     *
     * @return the ResultSet concurrency
     */
    public int getRSConcur() {
        return parent.getRsconcur();
    }

    /**
     * Parses the given string and changes the value of the matching
     * header appropriately, or passes it on to the underlying
     * DataResponse.
     *
     * @param line the string that contains the header
     * @throws MCLParseException if has a wrong header
     */
    @SuppressWarnings("unchecked")
    public void addLine(ServerResponses response, Object line) throws MCLParseException {
        if (CheckBooleanValuesAllTrue(isSet)) {
            resultBlocks[0].addLine(response, line);
        }
        if (response != ServerResponses.HEADER) {
            throw new MCLParseException("header expected, got: " + response.toString());
        } else {
            //we will always pass the tableNames pointer
            switch (con.getProtocol().getNextTableHeader(line, this.tableNames, this.columnLengths)) {
                case NAME:
                    System.arraycopy(this.tableNames, 0, this.name, 0, this.columncount);
                    isSet[0] = true;
                    break;
                case LENGTH:
                    isSet[1] = true;
                    break;
                case TYPE:
                    System.arraycopy(this.tableNames, 0, this.type, 0, this.columncount);
                    isSet[2] = true;
                    break;
                case TABLE:
                    isSet[3] = true;
                    break;
            }
        }
    }

    /**
     * Returns a line from the cache. If the line is already present in the
     * cache, it is returned, if not appropriate actions are taken to make
     * sure the right block is being fetched and as soon as the requested
     * line is fetched it is returned.
     *
     * @param row the row in the result set to return
     * @return the exact row read as requested or null if the requested row
     *         is out of the scope of the result set
     * @throws SQLException if an database error occurs
     */
    @SuppressWarnings("unchecked")
    public T getLine(int row) throws SQLException {
        if (row >= tuplecount || row < 0)
            return null;

        int block = (row - blockOffset) / cacheSize;
        int blockLine = (row - blockOffset) % cacheSize;

        // do we have the right block loaded? (optimistic try)
        DataBlockResponse<T> rawr;
        // load block if appropriate
        if ((rawr = resultBlocks[block]) == null) {
            /// TODO: ponder about a maximum number of blocks to keep
            ///       in memory when dealing with random access to
            ///       reduce memory blow-up

            // if we're running forward only, we can discard the resultset
            // block loaded
            if (parent.getRstype() == ResultSet.TYPE_FORWARD_ONLY) {
                for (int i = 0; i < block; i++)
                    resultBlocks[i] = null;

                if (MonetConnection.GetSeqCounter() - 1 == seqnr && !cacheSizeSetExplicitly &&
                        tuplecount - row > cacheSize && cacheSize < MonetConnection.GetDefFetchsize() * 10) {
                    // there has no query been issued after this
                    // one, so we can consider this an uninterrupted
                    // continuation request.  Let's once increase
                    // the cacheSize as it was not explicitly set,
                    // since the chances are high that we won't
                    // bother anyone else by doing so, and just
                    // gaining some performance.

                    // store the previous position in the
                    // blockOffset variable
                    blockOffset += cacheSize;

                    // increase the cache size (a lot)
                    cacheSize *= 10;

                    // by changing the cacheSize, we also
                    // change the block measures.  Luckily
                    // we don't care about previous blocks
                    // because we have a forward running
                    // pointer only.  However, we do have
                    // to recalculate the block number, to
                    // ensure the next call to find this
                    // new block.
                    block = (row - blockOffset) / cacheSize;
                    blockLine = (row - blockOffset) % cacheSize;
                }
            }

            // ok, need to fetch cache block first
            parent.executeQuery(con.getLanguage().getCommandTemplates(), "export " + id + " " + ((block * cacheSize) + blockOffset) + " " + cacheSize);
            rawr = resultBlocks[block];
            if (rawr == null) {
                throw new AssertionError("block " + block + " should have been fetched by now :(");
            }
        }

        return rawr.getRow(blockLine);
    }

    /**
     * Closes this Response by sending an Xclose to the server indicating
     * that the result can be closed at the server side as well.
     */
    @Override
    public void close() {
        if (closed) return;
        // send command to server indicating we're done with this
        // result only if we had an ID in the header and this result
        // was larger than the reply size
        try {
            if (destroyOnClose) {
                con.sendControlCommand("close " + id);
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
