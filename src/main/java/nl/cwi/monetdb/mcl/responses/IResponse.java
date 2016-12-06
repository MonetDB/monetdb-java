package nl.cwi.monetdb.mcl.responses;

/**
 * A Response is a message sent by the server to indicate some
 * action has taken place, and possible results of that action.
 */
public interface IResponse {
    /**
     * Instructs the Response implementation to close and do the
     * necessary clean up procedures.
     */
    void close();
}
