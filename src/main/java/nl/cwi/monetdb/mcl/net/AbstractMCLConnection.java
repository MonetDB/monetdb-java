package nl.cwi.monetdb.mcl.net;

import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.AbstractMCLReader;
import nl.cwi.monetdb.mcl.io.AbstractMCLWriter;
import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;
import nl.cwi.monetdb.mcl.parser.TupleLineParser;

import java.io.*;
import java.net.SocketException;
import java.util.List;

/**
 * Created by ferreira on 11/23/16.
 */
public abstract class AbstractMCLConnection {

    /** the SQL language */
    public final static int LANG_SQL = 0;
    /** the MAL language (officially *NOT* supported) */
    public final static int LANG_MAL = 3;
    /** an unknown language */
    public final static int LANG_UNKNOWN = -1;

    /** The hostname to connect to */
    protected String hostname;
    /** The port to connect on the host to */
    protected int port = -1;
    /** The database to connect to */
    protected String database;
    /** The username to use when authenticating */
    protected String username;
    /** Whether we are debugging or not */
    protected boolean debug;
    /** The language to connect with */
    protected String language;
    /** The hash methods to use (null = default) */
    protected String hash;
    /** The Writer for the debug log-file */
    protected Writer log;
    /** The language which is used */
    protected int lang;

    /** A template to apply to each query (like pre and post fixes) */
    protected String[] queryTempl = new String[3]; // pre, post, sep
    /** A template to apply to each command (like pre and post fixes) */
    protected String[] commandTempl = new String[3]; // pre, post, sep

    public AbstractMCLConnection(String hostname, int port, String database, String username, boolean debug, String language, String hash, String[] queryTempl, String[] commandTempl) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.debug = debug;
        this.hash = hash;
        this.queryTempl = queryTempl;
        this.commandTempl = commandTempl;
        this.setLanguage(language);
    }

    public String getHostname() {
        return hostname;
    }

    public abstract void setHostname(String hostname);

    public int getPort() {
        return port;
    }

    public abstract void setPort(int port);

    public String getDatabase() {
        return database;
    }

    /**
     * Sets the database to connect to.  If database is null, a
     * connection is made to the default database of the server.  This
     * is also the default.
     *
     * @param db the database
     */
    public abstract void setDatabase(String db);

    public String getUsername() {
        return username;
    }

    protected void setUsername(String username) {
        this.username = username;
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

    public String getLanguage() {
        return language;
    }

    /**
     * Sets the language to use for this connection.
     *
     * @param language the language
     */
    public abstract void setLanguage(String language);

    public int getLang() {
        return lang;
    }

    public String getHash() {
        return hash;
    }

    /**
     * Sets the hash method to use.  Note that this method is intended
     * for debugging purposes.  Setting a hash method can yield in
     * connection failures.  Multiple hash methods can be given by
     * separating the hashes by commas.
     * DON'T USE THIS METHOD if you don't know what you're doing.
     *
     * @param hash the hash method to use
     */
    public abstract void setHash(String hash);

    /**
     * Gets the SO_TIMEOUT from the underlying Socket.
     *
     * @return the currently in use timeout in milliseconds
     * @throws SocketException Issue with the socket
     */
    public abstract int getSoTimeout() throws SocketException;

    /**
     * Set the SO_TIMEOUT on the underlying Socket.  When for some
     * reason the connection to the database hangs, this setting can be
     * useful to break out of this indefinite wait.
     * This option must be enabled prior to entering the blocking
     * operation to have effect.
     *
     * @param s The specified timeout, in milliseconds.  A timeout
     *        of zero is interpreted as an infinite timeout.
     * @throws SocketException Issue with the socket
     */
    public abstract void setSoTimeout(int s) throws SocketException;

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
     * Returns an InputStream that reads from this open connection on
     * the MapiSocket.
     *
     * @return an input stream that reads from this open connection
     */
    public abstract InputStream getInputStream();

    /**
     * Returns an output stream for this MapiSocket.
     *
     * @return an output stream for writing bytes to this MapiSocket
     */
    public abstract OutputStream getOutputStream();

    /**
     * Returns a Reader for this MapiSocket.  The Reader is a
     * BufferedMCLReader which does protocol interpretation of the
     * BlockInputStream produced by this MapiSocket.
     *
     * @return a BufferedMCLReader connected to this MapiSocket
     */
    public abstract AbstractMCLReader getReader();

    /**
     * Returns a Writer for this MapiSocket.  The Writer is a
     * BufferedMCLWriter which produces protocol compatible data blocks
     * that the BlockOutputStream can properly translate into blocks.
     *
     * @return a BufferedMCLWriter connected to this MapiSocket
     */
    public abstract AbstractMCLWriter getWriter();

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

    public String getQueryTemplateHeader(int index) {
        return queryTempl[index] == null ? "" : queryTempl[index];
    }

    public String getCommandTemplateHeader(int index) {
        return commandTempl[index] == null ? "" : commandTempl[index];
    }

    public String[] getCommandHeaderTemplates() {
        return commandTempl;
    }

    public String[] getQueryHeaderTemplates() {
        return queryTempl;
    }

    public synchronized void close() {
        try {
            if (this.getReader() != null) this.getReader().close();
            if (this.getWriter() != null) this.getWriter().close();
            if (this.getInputStream() != null) this.getInputStream().close();
            if (this.getOutputStream() != null) this.getOutputStream().close();
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

    public abstract StartOfHeaderParser getStartOfHeaderParser();

    public abstract HeaderLineParser getHeaderLineParser(int capacity);

    public abstract TupleLineParser getTupleLineParser(int capacity);
}
