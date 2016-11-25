package nl.cwi.monetdb.mcl.io;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public class EmbeddedMCLWriter extends AbstractMCLWriter {

    public EmbeddedMCLWriter(EmbeddedMCLReader reader) {
        super(null);
        this.reader = reader;
    }

    @Override
    public void writeLine(String line) throws IOException {
        this.writeInternal(line);
        this.reader.notify(); //wake up the embedded reader.
    }

    private native void writeInternal(String str);
}
