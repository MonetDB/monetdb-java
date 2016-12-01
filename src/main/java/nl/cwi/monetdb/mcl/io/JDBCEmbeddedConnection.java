package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;

/**
 * Created by ferreira on 12/1/16.
 */
public class JDBCEmbeddedConnection extends MonetDBEmbeddedConnection {

    protected JDBCEmbeddedConnection(long connectionPointer) {
        super(connectionPointer);
    }
}
