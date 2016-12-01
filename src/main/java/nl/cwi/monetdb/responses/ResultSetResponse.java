package nl.cwi.monetdb.responses;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.io.AbstractMCLReader;
import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;

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
public class ResultSetResponse implements IResponse {
    /** The number of columns in this result */
    public final int columncount;
    /** The total number of rows this result set has */
    public final int tuplecount;
    /** The numbers of rows to retrieve per DataBlockResponse */
    private int cacheSize;
    /** The table ID of this result */
    public final int id;
    /** The names of the columns in this result */
    private String[] name;
    /** The types of the columns in this result */
    private String[] type;
    /** The max string length for each column in this result */
    private int[] columnLengths;
    /** The table for each column in this result */
    private String[] tableNames;
    /** The query sequence number */
    private final int seqnr;
    /** A List of result blocks (chunks of size fetchSize/cacheSize) */
    private DataBlockResponse[] resultBlocks;

    /** A bitmap telling whether the headers are set or not */
    private boolean[] isSet;
    /** Whether this Response is closed */
    private boolean closed;

    /** The Connection that we should use when requesting a new block */
    private ResponseList parent;
    /** Whether the fetchSize was explitly set by the user */
    private boolean cacheSizeSetExplicitly = false;
    /** Whether we should send an Xclose command to the server
     *  if we close this Response */
    private boolean destroyOnClose;
    /** the offset to be used on Xexport queries */
    private int blockOffset = 0;

    /** A parser for header lines */
    HeaderLineParser hlp;

    private final static int NAMES	= 0;
    private final static int TYPES	= 1;
    private final static int TABLES	= 2;
    private final static int LENS	= 3;

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
    ResultSetResponse(int id, int tuplecount, int columncount, int rowcount, ResponseList parent, int seq) throws SQLException {
        isSet = new boolean[7];
        this.parent = parent;
        if (parent.cachesize == 0) {
				/* Below we have to calculate how many "chunks" we need
				 * to allocate to store the entire result.  However, if
				 * the user didn't set a cache size, as in this case, we
				 * need to stick to our defaults. */
            cacheSize = ResponseList.DEF_FETCHSIZE;
            cacheSizeSetExplicitly = false;
        } else {
            cacheSize = parent.cachesize;
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
        if (rowcount > cacheSize)
            cacheSize = rowcount;
        seqnr = seq;
        closed = false;
        destroyOnClose = id > 0 && tuplecount > rowcount;

        this.id = id;
        this.tuplecount = tuplecount;
        this.columncount = columncount;
        this.resultBlocks = new DataBlockResponse[(tuplecount / cacheSize) + 1];

        hlp = server.getHeaderLineParser(columncount);

        resultBlocks[0] = new DataBlockResponse(rowcount, parent.rstype == ResultSet.TYPE_FORWARD_ONLY);
    }

    /**
     * Parses the given string and changes the value of the matching
     * header appropriately, or passes it on to the underlying
     * DataResponse.
     *
     * @param tmpLine the string that contains the header
     * @return a non-null String if the header cannot be parsed or
     *         is unknown
     */
    // {{{ addLine
    @Override
    public String addLine(String tmpLine, int linetype) {
        if (isSet[LENS] && isSet[TYPES] && isSet[TABLES] && isSet[NAMES]) {
            return resultBlocks[0].addLine(tmpLine, linetype);
        }

        if (linetype != AbstractMCLReader.HEADER)
            return "header expected, got: " + tmpLine;

        // depending on the name of the header, we continue
        try {
            switch (hlp.parse(tmpLine)) {
                case HeaderLineParser.NAME:
                    name = hlp.values.clone();
                    isSet[NAMES] = true;
                    break;
                case HeaderLineParser.LENGTH:
                    columnLengths = hlp.intValues.clone();
                    isSet[LENS] = true;
                    break;
                case HeaderLineParser.TYPE:
                    type = hlp.values.clone();
                    isSet[TYPES] = true;
                    break;
                case HeaderLineParser.TABLE:
                    tableNames = hlp.values.clone();
                    isSet[TABLES] = true;
                    break;
            }
        } catch (MCLParseException e) {
            return e.getMessage();
        }

        // all is well
        return null;
    }
    // }}}

    /**
     * Returns whether this ResultSetResponse needs more lines.
     * This method returns true if not all headers are set, or the
     * first DataBlockResponse reports to want more.
     */
    @Override
    public boolean wantsMore() {
        return !(isSet[LENS] && isSet[TYPES] && isSet[TABLES] && isSet[NAMES]) || resultBlocks[0].wantsMore();
    }

    /**
     * Returns an array of Strings containing the values between
     * ',\t' separators.
     *
     * @param chrLine a character array holding the input data
     * @param start where the relevant data starts
     * @param stop where the relevant data stops
     * @return an array of Strings
     */
    final private String[] getValues(char[] chrLine, int start, int stop) {
        int elem = 0;
        String[] values = new String[columncount];

        for (int i = start; i < stop; i++) {
            if (chrLine[i] == '\t' && chrLine[i - 1] == ',') {
                values[elem++] =
                        new String(chrLine, start, i - 1 - start);
                start = i + 1;
            }
        }
        // at the left over part
        values[elem++] = new String(chrLine, start, stop - start);

        return values;
    }

    /**
     * Adds the given DataBlockResponse to this ResultSetResponse at
     * the given block position.
     *
     * @param offset the offset number of rows for this block
     * @param rr the DataBlockResponse to add
     */
    public void addDataBlockResponse(int offset, DataBlockResponse rr) {
        int block = (offset - blockOffset) / cacheSize;
        resultBlocks[block] = rr;
    }

    /**
     * Marks this Response as being completed.  A complete Response
     * needs to be consistent with regard to its internal data.
     *
     * @throws SQLException if the data currently in this Response is not
     *                      sufficient to be consistant
     */
    @Override
    public void complete() throws SQLException {
        String error = "";
        if (!isSet[NAMES]) error += "name header missing\n";
        if (!isSet[TYPES]) error += "type header missing\n";
        if (!isSet[TABLES]) error += "table name header missing\n";
        if (!isSet[LENS]) error += "column width header missing\n";
        if (!error.equals("")) throw new SQLException(error, "M0M10");
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
        return parent.rstype;
    }

    /**
     * Returns the concurrency of the ResultSet.
     *
     * @return the ResultSet concurrency
     */
    public int getRSConcur() {
        return parent.rsconcur;
    }

    /**
     * Returns a line from the cache. If the line is already present in the
     * cache, it is returned, if not apropriate actions are taken to make
     * sure the right block is being fetched and as soon as the requested
     * line is fetched it is returned.
     *
     * @param row the row in the result set to return
     * @return the exact row read as requested or null if the requested row
     *         is out of the scope of the result set
     * @throws SQLException if an database error occurs
     */
    public String getLine(int row) throws SQLException {
        if (row >= tuplecount || row < 0)
            return null;

        int block = (row - blockOffset) / cacheSize;
        int blockLine = (row - blockOffset) % cacheSize;

        // do we have the right block loaded? (optimistic try)
        DataBlockResponse rawr;
        // load block if appropriate
        if ((rawr = resultBlocks[block]) == null) {
            /// TODO: ponder about a maximum number of blocks to keep
            ///       in memory when dealing with random access to
            ///       reduce memory blow-up

            // if we're running forward only, we can discard the oldmapi
            // block loaded
            if (parent.rstype == ResultSet.TYPE_FORWARD_ONLY) {
                for (int i = 0; i < block; i++)
                    resultBlocks[i] = null;

                if (ResponseList.SeqCounter - 1 == seqnr && !cacheSizeSetExplicitly &&
                        tuplecount - row > cacheSize && cacheSize < ResponseList.DEF_FETCHSIZE * 10) {
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
            parent.executeQuery(server.getCommandHeaderTemplates(),
                    "export " + id + " " + ((block * cacheSize) + blockOffset) + " " + cacheSize);
            rawr = resultBlocks[block];
            if (rawr == null) throw
                    new AssertionError("block " + block + " should have been fetched by now :(");
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
            if (destroyOnClose) sendControlCommand("close " + id);
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
