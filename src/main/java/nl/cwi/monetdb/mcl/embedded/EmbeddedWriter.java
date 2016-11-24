package nl.cwi.monetdb.mcl.embedded;

import nl.cwi.monetdb.mcl.connection.AbstractBufferedWriter;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public class EmbeddedWriter extends AbstractBufferedWriter {

    public EmbeddedWriter() {
        super(null);
    }

    @Override
    public void writeLine(String line) throws IOException {
        this.writeInternal(line);
        this.reader.notify();
    }

    private native void writeInternal(String str);
}
