package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.mcl.connection.EmbeddedMonetDB;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedHeaderLineParser;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedStartOfHeaderParser;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedTupleLineParser;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public final class EmbeddedMCLWriter extends AbstractMCLWriter {

    private final EmbeddedMonetDB connection;

    private final EmbeddedStartOfHeaderParser sohp;

    private EmbeddedHeaderLineParser nexthlp;

    private EmbeddedTupleLineParser nexttlp;

    public void setNexthlp(EmbeddedHeaderLineParser nexthlp) {
        this.nexthlp = nexthlp;
    }

    public void setNexttlp(EmbeddedTupleLineParser nexttlp) {
        this.nexttlp = nexttlp;
    }

    public EmbeddedMCLWriter(EmbeddedMonetDB con, EmbeddedMCLReader reader, EmbeddedStartOfHeaderParser sohp) {
        super(null);
        this.connection = con;
        this.reader = reader;
        this.sohp = sohp;
    }

    @Override
    public void writeLine(String line) throws IOException {
        this.reader.notify(); //wake up the embedded reader.
    }

}
