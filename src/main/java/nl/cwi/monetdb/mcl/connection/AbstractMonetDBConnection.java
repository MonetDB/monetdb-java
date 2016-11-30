package nl.cwi.monetdb.mcl.connection;

import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocolParser;

import java.io.*;
import java.net.SocketException;
import java.util.List;

/**
 * Created by ferreira on 11/23/16.
 */
public abstract class AbstractMonetDBConnection {

    /** The language to connect with */
    protected MonetDBLanguage currentMonetDBLanguage = MonetDBLanguage.LANG_SQL;
    /** The database to connect to */
    protected final String database;
    /** Authentication hash method */
    protected final String hash;
    /** Whether we are debugging or not */
    protected boolean debug;
    /** The Writer for the debug log-file */
    protected Writer log;

    public AbstractMonetDBConnection(String database, String hash, boolean debug, MonetDBLanguage lang) throws IOException {
        this.database = database;
        this.hash = hash;
        this.debug = debug;
        this.currentMonetDBLanguage = lang;
    }

    public String getDatabase() {
        return database;
    }

    public String getHash() {
        return hash;
    }

    public boolean isDebug() {
        return debug;
    }

    /**
     * Enables/disables debug
     *
     * @param debug Value to set
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public MonetDBLanguage getCurrentMonetDBLanguage() {
        return currentMonetDBLanguage;
    }

    public void setCurrentMonetDBLanguage(MonetDBLanguage currentMonetDBLanguage) {
        this.currentMonetDBLanguage = currentMonetDBLanguage;
    }

    /**
     * Connects to the given host and port, logging in as the given
     * user.  If followRedirect is false, a RedirectionException is
     * thrown when a redirect is encountered.
     *
     * @return A List with informational (warning) messages. If this
     * 		list is empty; then there are no warnings.
     * @throws IOException if an I/O error occurs when creating the
     *         socket
     * @throws MCLParseException if bogus data is received
     * @throws MCLException if an MCL related error occurs
     */
    public abstract List<String> connect(String user, String pass) throws IOException, MCLParseException, MCLException;

    /**
     * Enables logging to a file what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param filename the name of the file to write to
     * @throws IOException if the file could not be opened for writing
     */
    public void debug(String filename) throws IOException {
        debug(new FileWriter(filename));
    }

    /**
     * Enables logging to a stream what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param out to write the log to
     * @throws IOException if the file could not be opened for writing
     */
    public void debug(PrintStream out) throws IOException {
        debug(new PrintWriter(out));
    }

    /**
     * Enables logging to a stream what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param out to write the log to
     * @throws IOException if the file could not be opened for writing
     */
    public void debug(Writer out) throws IOException {
        log = out;
        debug = true;
    }

    /**
     * Writes a logline tagged with a timestamp using the given string.
     * Used for debugging purposes only and represents a message that is
     * connected to writing to the socket.  A logline might look like:
     * TX 152545124: Hello MonetDB!
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logTx(String message) throws IOException {
        log.write("TX " + System.currentTimeMillis() +
                ": " + message + "\n");
    }

    /**
     * Writes a logline tagged with a timestamp using the given string.
     * Lines written using this log method are tagged as "added
     * metadata" which is not strictly part of the data sent.
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logTd(String message) throws IOException {
        log.write("TD " + System.currentTimeMillis() +
                ": " + message + "\n");
    }

    /**
     * Writes a logline tagged with a timestamp using the given string,
     * and flushes afterwards.  Used for debugging purposes only and
     * represents a message that is connected to reading from the
     * socket.  The log is flushed after writing the line.  A logline
     * might look like:
     * RX 152545124: Hi JDBC!
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logRx(String message) throws IOException {
        log.write("RX " + System.currentTimeMillis() +
                ": " + message + "\n");
        log.flush();
    }

    /**
     * Writes a logline tagged with a timestamp using the given string,
     * and flushes afterwards.  Lines written using this log method are
     * tagged as "added metadata" which is not strictly part of the data
     * received.
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logRd(String message) throws IOException {
        log.write("RD " + System.currentTimeMillis() +
                ": " + message + "\n");
        log.flush();
    }

    public synchronized void close() {
        try {
            if (debug && log instanceof FileWriter) log.close();
        } catch (IOException e) {
            // ignore it
        }
    }

    /**
     * Destructor called by garbage collector before destroying this
     * object tries to disconnect the MonetDB connection if it has not
     * been disconnected already.
     */
    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public abstract String getJDBCURL();

    public abstract int getBlockSize();

    public abstract int getSoTimeout() throws SocketException;

    public abstract void setSoTimeout(int s) throws SocketException;

    public abstract AbstractProtocolParser getUnderlyingProtocol();
}
