package nl.cwi.monetdb.mcl.embedded;

import nl.cwi.monetdb.mcl.connection.AbstractBufferedReader;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public class EmbeddedReader extends AbstractBufferedReader {

    protected EmbeddedReader() {
        super(null);
    }

    @Override
    public String readLine() throws IOException {
        String res = this.readLineInternal();
        setLineType(res);
        if (lineType == ERROR && !res.matches("^![0-9A-Z]{5}!.+"))
            res = "!22000!" + res.substring(1);
        return res;
    }

    @Override
    public synchronized String waitForPrompt() throws IOException {
        try {
            this.wait();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        String res = this.readLine();
        if (res == null) {
            throw new IOException("Connection to server lost!");
        }
        if (lineType == ERROR) {
            return "\n" + res.substring(1);
        }
        return res.trim();
    }

    private native String readLineInternal();
}
