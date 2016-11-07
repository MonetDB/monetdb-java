package nl.cwi.monetdb.embedded.mapping;

/**
 * A Java representation for Blob data type. Added for more efficient data mapping when fetching from the database.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedBlob {

    private final byte[] blob;

    public MonetDBEmbeddedBlob(byte[] blob) {
        this.blob = blob;
    }

    public byte[] getBlob() {
        return blob;
    }

    @Override
    public String toString() {
        return new String(this.blob);
    }
}
