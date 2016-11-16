package nl.cwi.monetdb.embedded.mapping;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A Java representation for the BLOB data type. Added for more efficient data mapping when fetching from the database.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedBlob implements Serializable {

    /**
     * The BLOB's content as a Java byte array.
     */
    private final byte[] blob;

    public MonetDBEmbeddedBlob(byte[] blob) { this.blob = blob; }

    /**
     * Get the BLOB content itself,
     *
     * @return A Java byte array containing the BLOB itself
     */
    public byte[] getBlob() { return this.blob; }

    /**
     * Overriding the equals method for the byte array.
     */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof MonetDBEmbeddedBlob && Arrays.equals(this.blob, ((MonetDBEmbeddedBlob) obj).getBlob());
    }

    /**
     * Overriding the hashCode method for the byte array.
     */
    @Override
    public int hashCode() { return Arrays.hashCode(this.blob); }

    /**
     * Overriding the toString method for the byte array.
     */
    @Override
    public String toString() { return Arrays.toString(blob); }
}
