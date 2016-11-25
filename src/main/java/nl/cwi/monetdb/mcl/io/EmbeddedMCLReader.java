package nl.cwi.monetdb.mcl.io;

import java.io.*;

/**
 * Created by ferreira on 11/24/16.
 */
public class EmbeddedMCLReader extends AbstractMCLReader {

    public EmbeddedMCLReader() {
        super(null);
    }

    @Override
    public String readLine() throws IOException {
        String res = this.readLineInternal(); //this readline will never wait!!
        setLineType(res);
        if (lineType == ERROR && !res.matches("^![0-9A-Z]{5}!.+"))
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

    private native String readLineInternal();
}
