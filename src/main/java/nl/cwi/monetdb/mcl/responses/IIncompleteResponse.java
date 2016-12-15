package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

/**
 * Created by ferreira on 12/15/16.
 */
public interface IIncompleteResponse extends IResponse {
    /**
     * Returns whether this Response expects more lines to be added to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    boolean wantsMore();

    void addLines(AbstractProtocol protocol) throws ProtocolException;
}
