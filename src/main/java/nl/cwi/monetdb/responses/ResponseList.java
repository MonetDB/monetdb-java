package nl.cwi.monetdb.responses;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.SendThread;
import nl.cwi.monetdb.mcl.io.AbstractMCLReader;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A list of Response objects.  Responses are added to this list.
 * Methods of this class are not synchronized.  This is left as
 * responsibility to the caller to prevent concurrent access.
 */
public class ResponseList {

    /** the default number of rows that are (attempted to) read at once */
    protected final static int DEF_FETCHSIZE = 250;
    /** The sequence counter */
    protected static int SeqCounter = 0;

    /** The cache size (number of rows in a DataBlockResponse object) */
    private final int cachesize;
    /** The maximum number of results for this query */
    private final int maxrows;
    /** The ResultSet type to produce */
    final int rstype;
    /** The ResultSet concurrency to produce */
    final int rsconcur;
    /** The sequence number of this ResponseList */
    private final int seqnr;
    /** A list of the Responses associated with the query,
     *  in the right order */
    private List<IResponse> responses = new ArrayList<>();
    /** A map of ResultSetResponses, used for additional
     *  DataBlockResponse mapping */
    private Map<Integer, ResultSetResponse> rsresponses;
    /** The current header returned by getNextResponse() */
    private int curResponse = -1;

    /**
     * Main constructor.  The query argument can either be a String
     * or List.  An SQLException is thrown if another object
     * instance is supplied.
     *
     * @param cachesize overall cachesize to use
     * @param maxrows maximum number of rows to allow in the set
     * @param rstype the type of result sets to produce
     * @param rsconcur the concurrency of result sets to produce
     */
    public ResponseList(int cachesize, int maxrows, int rstype, int rsconcur) throws SQLException {
        this.cachesize = cachesize;
        this.maxrows = maxrows;
        this.rstype = rstype;
        this.rsconcur = rsconcur;
        this.seqnr = SeqCounter++;
    }

    /**
     * Retrieves the next available response, or null if there are
     * no more responses.
     *
     * @return the next Response available or null
     */
    public IResponse getNextResponse() throws SQLException {
        if (rstype == ResultSet.TYPE_FORWARD_ONLY) {
            // free resources if we're running forward only
            if (curResponse >= 0 && curResponse < responses.size()) {
                IResponse tmp = responses.get(curResponse);
                if (tmp != null) tmp.close();
                responses.set(curResponse, null);
            }
        }
        curResponse++;
        if (curResponse >= responses.size()) {
            // ResponseList is obviously completed so, there are no
            // more responses
            return null;
        } else {
            // return this response
            return responses.get(curResponse);
        }
    }

    /**
     * Closes the Response at index i, if not null.
     *
     * @param i the index position of the header to close
     */
    public void closeResponse(int i) {
        if (i < 0 || i >= responses.size()) return;
        IResponse tmp = responses.set(i, null);
        if (tmp != null)
            tmp.close();
    }

    /**
     * Closes the current response.
     */
    public void closeCurrentResponse() {
        closeResponse(curResponse);
    }

    /**
     * Closes the current and previous responses.
     */
    public void closeCurOldResponses() {
        for (int i = curResponse; i >= 0; i--) {
            closeResponse(i);
        }
    }

    /**
     * Closes this ResponseList by closing all the Responses in this
     * ResponseList.
     */
    public void close() {
        for (int i = 0; i < responses.size(); i++) {
            closeResponse(i);
        }
    }

    /**
     * Returns whether this ResponseList has still unclosed
     * Responses.
     */
    public boolean hasUnclosedResponses() {
        for (IResponse r : responses) {
            if (r != null)
                return true;
        }
        return false;
    }

    /**
     * Executes the query contained in this ResponseList, and
     * stores the Responses resulting from this query in this
     * ResponseList.
     *
     * @throws SQLException if a database error occurs
     */
    public void processQuery(String query) throws SQLException {
        executeQuery(server.getQueryHeaderTemplates(), query);
    }

    /**
     * Internal executor of queries.
     *
     * @param templ the template to fill in
     * @param query the query to execute
     * @throws SQLException if a database error occurs
     */
    @SuppressWarnings("fallthrough")
    public void executeQuery(String[] templ, String query) throws SQLException {
        boolean sendThreadInUse = false;
        String error = null;

        try {
            synchronized (server) {
                // make sure we're ready to send query; read data till we
                // have the prompt it is possible (and most likely) that we
                // already have the prompt and do not have to skip any
                // lines.  Ignore errors from previous result sets.
                in.waitForPrompt();

                // {{{ set reply size
                /**
                 * Change the reply size of the server.  If the given
                 * value is the same as the current value known to use,
                 * then ignore this call.  If it is set to 0 we get a
                 * prompt after the server sent it's header.
                 */
                int size = cachesize == 0 ? DEF_FETCHSIZE : cachesize;
                size = maxrows != 0 ? Math.min(maxrows, size) : size;
                // don't do work if it's not needed
                if (server.getLang() == MonetConnection.LANG_SQL && size != curReplySize && templ != server.getCommandHeaderTemplates()) {
                    sendControlCommand("reply_size " + size);

                    // store the reply size after a successful change
                    curReplySize = size;
                }
                // }}} set reply size

                // If the query is larger than the TCP buffer size, use a
                // special send thread to avoid deadlock with the server due
                // to blocking behaviour when the buffer is full.  Because
                // the server will be writing back results to us, it will
                // eventually block as well when its TCP buffer gets full,
                // as we are blocking an not consuming from it.  The result
                // is a state where both client and server want to write,
                // but block.
                if (query.length() > server.getBlockSize()) {
                    // get a reference to the send thread
                    if (sendThread == null)
                        sendThread = new SendThread(out);
                    // tell it to do some work!
                    sendThread.runQuery(templ, query);
                    sendThreadInUse = true;
                } else {
                    // this is a simple call, which is a lot cheaper and will
                    // always succeed for small queries.
                    out.writeLine((templ[0] == null ? "" : templ[0] + query + templ[1] == null ? "" : templ[1]));
                }

                // go for new results
                String tmpLine = in.readLine();
                int linetype = in.getLineType();
                IResponse res = null;
                while (linetype != AbstractMCLReader.PROMPT) {
                    // each response should start with a start of header
                    // (or error)
                    switch (linetype) {
                        case AbstractMCLReader.SOHEADER:
                            // make the response object, and fill it
                            try {
                                switch (sohp.parse(tmpLine)) {
                                    case StartOfHeaderParser.Q_PARSE:
                                        throw new MCLParseException("Q_PARSE header not allowed here", 1);
                                    case StartOfHeaderParser.Q_TABLE:
                                    case StartOfHeaderParser.Q_PREPARE: {
                                        int id = sohp.getNextAsInt();
                                        int tuplecount = sohp.getNextAsInt();
                                        int columncount = sohp.getNextAsInt();
                                        int rowcount = sohp.getNextAsInt();
                                        // enforce the maxrows setting
                                        if (maxrows != 0 && tuplecount > maxrows)
                                            tuplecount = maxrows;
                                        res = new ResultSetResponse(id, tuplecount, columncount, rowcount,
                                                this, seqnr);
                                        // only add this resultset to
                                        // the hashmap if it can possibly
                                        // have an additional datablock
                                        if (rowcount < tuplecount) {
                                            if (rsresponses == null)
                                                rsresponses = new HashMap<>();
                                            rsresponses.put(id, (ResultSetResponse) res);
                                        }
                                    } break;
                                    case StartOfHeaderParser.Q_UPDATE:
                                        res = new UpdateResponse(
                                                sohp.getNextAsInt(),   // count
                                                sohp.getNextAsString() // key-id
                                        );
                                        break;
                                    case StartOfHeaderParser.Q_SCHEMA:
                                        res = new SchemaResponse();
                                        break;
                                    case StartOfHeaderParser.Q_TRANS:
                                        boolean ac = sohp.getNextAsString().equals("t");
                                        if (autoCommit && ac) {
                                            addWarning("Server enabled auto commit " +
                                                    "mode while local state " +
                                                    "already was auto commit.", "01M11"
                                            );
                                        }
                                        autoCommit = ac;
                                        res = new AutoCommitResponse(ac);
                                        break;
                                    case StartOfHeaderParser.Q_BLOCK: {
                                        // a new block of results for a
                                        // response...
                                        int id = sohp.getNextAsInt();
                                        sohp.getNextAsInt();	// columncount
                                        int rowcount = sohp.getNextAsInt();
                                        int offset = sohp.getNextAsInt();
                                        ResultSetResponse t = rsresponses.get(id);
                                        if (t == null) {
                                            error = "M0M12!no ResultSetResponse with id " + id + " found";
                                            break;
                                        }

                                        DataBlockResponse r = new DataBlockResponse(rowcount,
                                                t.getRSType() == ResultSet.TYPE_FORWARD_ONLY);

                                        t.addDataBlockResponse(offset, r);
                                        res = r;
                                    } break;
                                }
                            } catch (MCLParseException e) {
                                error = "M0M10!error while parsing start of header:\n" +
                                        e.getMessage() +
                                        " found: '" + tmpLine.charAt(e.getErrorOffset()) + "'" +
                                        " in: \"" + tmpLine + "\"" +
                                        " at pos: " + e.getErrorOffset();
                                // flush all the rest
                                in.waitForPrompt();
                                linetype = in.getLineType();
                                break;
                            }

                            // immediately handle errors after parsing
                            // the header (res may be null)
                            if (error != null) {
                                in.waitForPrompt();
                                linetype = in.getLineType();
                                break;
                            }

                            // here we have a res object, which
                            // we can start filling
                            while (res.wantsMore()) {
                                error = res.addLine(
                                        in.readLine(),
                                        in.getLineType()
                                );
                                if (error != null) {
                                    // right, some protocol violation,
                                    // skip the rest of the result
                                    error = "M0M10!" + error;
                                    in.waitForPrompt();
                                    linetype = in.getLineType();
                                    break;
                                }
                            }
                            if (error != null)
                                break;
                            // it is of no use to store
                            // DataBlockReponses, you never want to
                            // retrieve them directly anyway
                            if (!(res instanceof DataBlockResponse))
                                responses.add(res);

                            // read the next line (can be prompt, new
                            // result, error, etc.) before we start the
                            // loop over
                            tmpLine = in.readLine();
                            linetype = in.getLineType();
                            break;
                        case AbstractMCLReader.INFO:
                            addWarning(tmpLine.substring(1), "01000");

                            // read the next line (can be prompt, new
                            // result, error, etc.) before we start the
                            // loop over
                            tmpLine = in.readLine();
                            linetype = in.getLineType();
                            break;
                        default:	// Yeah... in Java this is correct!
                            // we have something we don't
                            // expect/understand, let's make it an error
                            // message
                            tmpLine = "!M0M10!protocol violation, unexpected line: " + tmpLine;
                            // don't break; fall through...
                        case AbstractMCLReader.ERROR:
                            // read everything till the prompt (should be
                            // error) we don't know if we ignore some
                            // garbage here... but the log should reveal
                            // that
                            error = in.waitForPrompt();
                            linetype = in.getLineType();
                            if (error != null) {
                                error = tmpLine.substring(1) + "\n" + error;
                            } else {
                                error = tmpLine.substring(1);
                            }
                            break;
                    }
                }
            }

            // if we used the sendThread, make sure it has finished
            if (sendThreadInUse) {
                String tmp = sendThread.getErrors();
                if (tmp != null) {
                    if (error == null) {
                        error = "08000!" + tmp;
                    } else {
                        error += "\n08000!" + tmp;
                    }
                }
            }
            if (error != null) {
                SQLException ret = null;
                String[] errors = error.split("\n");
                for (String error1 : errors) {
                    if (ret == null) {
                        ret = new SQLException(error1.substring(6),
                                error1.substring(0, 5));
                    } else {
                        ret.setNextException(new SQLException(
                                error1.substring(6),
                                error1.substring(0, 5)));
                    }
                }
                throw ret;
            }
        } catch (SocketTimeoutException e) {
            close(); // JDBC 4.1 semantics, abort()
            throw new SQLException("connection timed out", "08M33");
        } catch (IOException e) {
            closed = true;
            throw new SQLException(e.getMessage() + " (mserver still alive?)", "08000");
        }
    }
}
