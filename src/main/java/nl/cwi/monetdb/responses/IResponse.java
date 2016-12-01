package nl.cwi.monetdb.responses;

import java.sql.SQLException;

/**
 * A Response is a message sent by the server to indicate some
 * action has taken place, and possible results of that action.
 */
public interface IResponse {

    /**
     * Adds a line to the underlying Response implementation.
     *
     * @param line the header line as String
     * @param linetype the line type according to the MAPI protocol
     * @return a non-null String if the line is invalid,
     *         or additional lines are not allowed.
     */
    String addLine(String line, int linetype);

    /**
     * Returns whether this Response expects more lines to be added
     * to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    boolean wantsMore();

    /**
     * Indicates that no more header lines will be added to this
     * Response implementation.
     *
     * @throws SQLException if the contents of the Response is not
     *         consistent or sufficient.
     */
    void complete() throws SQLException;

    /**
     * Instructs the Response implementation to close and do the
     * necessary clean up procedures.
     */
    void close();
}
