package nl.cwi.monetdb.responses;

import nl.cwi.monetdb.mcl.io.AbstractMCLReader;

import java.sql.SQLException;

/**
 * The DataBlockResponse is tabular data belonging to a
 * ResultSetResponse.  Tabular data from the server typically looks
 * like:
 * <pre>
 * [ "value",	56	]
 * </pre>
 * where each column is separated by ",\t" and each tuple surrounded
 * by brackets ("[" and "]").  A DataBlockResponse object holds the
 * raw data as read from the server, in a parsed manner, ready for
 * easy retrieval.
 *
 * This object is not intended to be queried by multiple threads
 * synchronously. It is designed to work for one thread retrieving
 * rows from it.  When multiple threads will retrieve rows from this
 * object, it is possible for threads to get the same data.
 */
public class DataBlockResponse implements IResponse {

    /** The String array to keep the data in */
    private final String[] data;
    /** The counter which keeps the current position in the data array */
    private int pos;
    /** Whether we can discard lines as soon as we have read them */
    private boolean forwardOnly;

    /**
     * Constructs a DataBlockResponse object
     * @param size the size of the data array to create
     * @param forward whether this is a forward only result
     */
    DataBlockResponse(int size, boolean forward) {
        this.pos = -1;
        this.data = new String[size];
        this.forwardOnly = forward;
    }

    /**
     * addLine adds a String of data to this object's data array.
     * Note that an IndexOutOfBoundsException can be thrown when an
     * attempt is made to add more than the original construction size
     * specified.
     *
     * @param line the header line as String
     * @param linetype the line type according to the MAPI protocol
     * @return a non-null String if the line is invalid,
     *         or additional lines are not allowed.
     */
    @Override
    public String addLine(String line, int linetype) {
        if (linetype != AbstractMCLReader.RESULT)
            return "protocol violation: unexpected line in data block: " + line;
        // add to the backing array
        data[++pos] = line;

        // all is well
        return null;
    }

    /**
     * Returns whether this Reponse expects more lines to be added
     * to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    @Override
    public boolean wantsMore() {
        // remember: pos is the value already stored
        return pos + 1 < data.length;
    }

    /**
     * Indicates that no more header lines will be added to this
     * Response implementation.  In most cases this is a redundant
     * operation because the data array is full.  However... it can
     * happen that this is NOT the case!
     *
     * @throws SQLException if not all rows are filled
     */
    @Override
    public void complete() throws SQLException {
        if ((pos + 1) != data.length)
            throw new SQLException("Inconsistent state detected!  Current block capacity: " + data.length +
                    ", block usage: " + (pos + 1) + ".  Did MonetDB send what it promised to?", "M0M10");
    }

    /**
     * Instructs the Response implementation to close and do the
     * necessary clean up procedures.
     *
     */
    @Override
    public void close() {
        // feed all rows to the garbage collector
        for (int i = 0; i < data.length; i++) {
            data[i] = null;
        }
    }

    /**
     * Retrieves the required row.  Warning: if the requested rows
     * is out of bounds, an IndexOutOfBoundsException will be
     * thrown.
     *
     * @param line the row to retrieve
     * @return the requested row as String
     */
    String getRow(int line) {
        if (forwardOnly) {
            String ret = data[line];
            data[line] = null;
            return ret;
        } else {
            return data[line];
        }
    }
}
