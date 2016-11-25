package nl.cwi.monetdb.mcl.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by ferreira on 11/24/16.
 */
public abstract class AbstractMCLWriter extends BufferedWriter {

    protected AbstractMCLReader reader;

    public AbstractMCLWriter(Writer out) {
        super(out);
    }

    /**
     * Registers the given reader in this writer.  A registered reader
     * receives a linetype reset when a line is written from this
     * writer.
     *
     * @param r an AbstractMCLReader
     */
    public void registerReader(AbstractMCLReader r) {
        this.reader = r;
    }

    public abstract void writeLine(String line) throws IOException;
}
