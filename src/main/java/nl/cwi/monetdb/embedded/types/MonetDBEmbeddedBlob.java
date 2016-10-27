package nl.cwi.monetdb.embedded.types;

/**
 * Created by ferreira on 10/27/16.
 */
public class MonetDBEmbeddedBlob {
    protected final byte[] blob;

    public MonetDBEmbeddedBlob(byte[] blob) {
        this.blob = blob;
    }

    public byte[] getBlob() { return blob; }
}
