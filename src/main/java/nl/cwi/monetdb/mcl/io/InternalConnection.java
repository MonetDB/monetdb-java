package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.embedded.env.IEmbeddedConnection;

/**
 * Created by ferreira on 11/30/16.
 */
public class InternalConnection implements IEmbeddedConnection {

    private long connectionPointer;

    public InternalConnection(long connectionPointer) {
        this.connectionPointer = connectionPointer;
    }

    @Override
    public long getConnectionPointer() {
        return this.connectionPointer;
    }

    @Override
    public void closeConnectionImplementation() {
        this.closeConnectionInternal(this.connectionPointer);
    }

    private native void closeConnectionInternal(long connectionPointer);
}
