/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.mapping;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/**
 * A Java representation for the BLOB data type. Added for more efficient data mapping when fetching from the database.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedBlob implements Serializable, Blob {

    /**
     * The BLOB's content as a Java byte array.
     */
    private byte[] blob;

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

    private void checkFreed() throws SQLException {
        if(this.blob == null) {
            throw new SQLException("Thsi blob was freed!");
        }
    }

    @Override
    public long length() throws SQLException {
        this.checkFreed();
        return this.blob.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        this.checkFreed();
        return Arrays.copyOfRange(this.blob, (int) pos, length);
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        this.checkFreed();
        return new ByteArrayInputStream(this.blob);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        this.checkFreed();
        byte[] subArray = Arrays.copyOfRange(this.blob, (int)start, pattern.length);
        return Collections.indexOfSubList(Arrays.asList(subArray), Arrays.asList(pattern));
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        this.checkFreed();
        byte[] subArray = Arrays.copyOfRange(this.blob, (int)start, (int) pattern.length());
        return Collections.indexOfSubList(Arrays.asList(subArray), Arrays.asList(pattern.getBytes(0, (int)pattern.length())));
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        this.checkFreed();
        int newFinalLength = (int) pos + bytes.length;
        byte[] newblob;

        if(newFinalLength > this.blob.length) {
            newblob = new byte[newFinalLength];
        } else {
            newblob = this.blob;
        }
        System.arraycopy(bytes, 0, newblob, (int) pos, bytes.length);
        this.blob = newblob;
        return bytes.length;
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        this.checkFreed();
        int newFinalLength = (int) pos + len;
        byte[] newblob;

        if(newFinalLength > this.blob.length) {
            newblob = new byte[newFinalLength];
        } else {
            newblob = this.blob;
        }
        System.arraycopy(bytes, offset, newblob, (int) pos, len);
        this.blob = newblob;
        return bytes.length;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        this.checkFreed();
        ByteArrayOutputStream res = null;
        try {
            res = new ByteArrayOutputStream();
            res.write(this.blob);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return res;
    }

    @Override
    public void truncate(long len) throws SQLException {
        this.checkFreed();
        byte[] newblob = new byte[(int)len];
        System.arraycopy(this.blob, 0, newblob, 0, (int)len);
        this.blob = newblob;
    }

    @Override
    public void free() throws SQLException {
        this.blob = null;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        this.checkFreed();
        return new ByteArrayInputStream(Arrays.copyOfRange(this.blob, (int) pos, (int) length));
    }
}
