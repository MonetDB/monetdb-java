package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.mcl.connection.EmbeddedMonetDB;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public final class EmbeddedMCLReader extends AbstractMCLReader {

    private final EmbeddedMonetDB connection;

    private int readerCurrentPos;

    private final int[] responseHeaderValues = new int[4];

    private String nextLine = "";

    public EmbeddedMCLReader(EmbeddedMonetDB connection) {
        super(null);
        this.connection = connection;
    }

    @Override
    public String readLine() throws IOException {
        this.lineType = this.responseHeaderValues[this.readerCurrentPos];
        this.readerCurrentPos++;

        String res = this.nextLine; //this readline will never wait!!

        if (this.lineType == ERROR && !res.matches("^![0-9A-Z]{5}!.+"))
            res = "!22000!" + res.substring(1);
        return res;
    }

    @Override
    public synchronized String waitForPrompt() throws IOException {
        try {
            this.wait(); //must mimic the socket readline with the wait/notify methods
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }
}
