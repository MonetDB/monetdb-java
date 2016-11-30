package nl.cwi.monetdb.mcl.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Created by ferreira on 11/24/16.
 */
public abstract class AbstractMCLReader extends BufferedReader {

    /** "there is currently no line", or the the type is unknown is
     represented by UNKNOWN */
    public final static int UNKNOWN   = 0;
    /** a line starting with ! indicates ERROR */
    public final static int ERROR     = '!';
    /** a line starting with % indicates HEADER */
    public final static int HEADER    = '%';
    /** a line starting with [ indicates RESULT */
    public final static int RESULT    = '[';
    /** a line which matches the pattern of prompt1 is a PROMPT */
    public final static int PROMPT    = '.';
    /** a line which matches the pattern of prompt2 is a MORE */
    public final static int MORE      = ',';
    /** a line starting with &amp; indicates the start of a header block */
    public final static int SOHEADER  = '&';
    /** a line starting with ^ indicates REDIRECT */
    public final static int REDIRECT  = '^';
    /** a line starting with # indicates INFO */
    public final static int INFO      = '#';

    /** The type of the last line read */
    protected int lineType;

    public AbstractMCLReader(Reader in) {
        super(in);
    }

    /**
     * getLineType returns the type of the last line read.
     *
     * @return an integer representing the kind of line this is, one of the
     *         following constants: UNKNOWN, HEADER, ERROR, PROMPT,
     *         RESULT, REDIRECT, INFO
     */
    public int getLineType() {
        return lineType;
    }

    public abstract String waitForPrompt() throws IOException;
}
