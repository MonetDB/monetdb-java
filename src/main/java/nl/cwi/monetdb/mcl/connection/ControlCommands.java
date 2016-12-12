package nl.cwi.monetdb.mcl.connection;

/**
 * Created by ferreira on 12/9/16.
 */
public enum ControlCommands {

    /** Send autocommit statement */
    AUTO_COMMIT,
    /** Set reply size for the server */
    REPLY_SIZE,
    /** Release a prepared statement data */
    RELEASE,
    /** Close a query */
    CLOSE
}
