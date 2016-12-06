package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;

import java.sql.SQLException;

/**
 * Created by ferreira on 12/5/16.
 */
public interface IIncompleteResponse extends IResponse {
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

    void addLine(ServerResponses response, Object line) throws MCLParseException;
}
