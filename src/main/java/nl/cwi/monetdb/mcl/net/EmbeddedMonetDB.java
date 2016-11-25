package nl.cwi.monetdb.mcl.net;

import nl.cwi.monetdb.embedded.env.IEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.AbstractMCLReader;
import nl.cwi.monetdb.mcl.io.AbstractMCLWriter;
import nl.cwi.monetdb.mcl.io.EmbeddedMCLReader;
import nl.cwi.monetdb.mcl.io.EmbeddedMCLWriter;
import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;
import nl.cwi.monetdb.mcl.parser.TupleLineParser;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedHeaderLineParser;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedStartOfHeaderParser;
import nl.cwi.monetdb.mcl.parser.embedded.EmbeddedTupleLineParser;

import java.io.*;
import java.net.SocketException;
import java.util.List;

/**
 * Created by ferreira on 11/23/16.
 */
public class EmbeddedMonetDB extends AbstractMCLConnection implements IEmbeddedConnection {

    private long connectionPointer;

    protected static final int BUFFER_SIZE = 102400; //100 kb to start

    private final String directory;

    private EmbeddedMCLReader reader;

    private EmbeddedMCLWriter writer;

    public EmbeddedMonetDB(String hostname, int port, String database, String username, boolean debug, String language, String hash, String directory) {
        super(hostname, port, database, username, debug, language, hash, new String[]{"", "\n;", "\n;\n"}, new String[]{"X", null, "\nX"});
        this.directory = directory;
    }

    public String getDirectory() {
        return directory;
    }

    @Override
    public void setHostname(String hostname) {
        throw new IllegalArgumentException("Cannot set a hostname on a embedded connection!");
    }

    @Override
    public void setPort(int port) {
        throw new IllegalArgumentException("Cannot set a port on a embedded connection!");
    }

    @Override
    public void setDatabase(String db) {
        throw new IllegalArgumentException("Not yet planned!");
    }

    @Override
    public void setLanguage(String language) {
        if(this.lang != LANG_SQL) {
            throw new IllegalArgumentException("The embedded connection only supports the SQL language!");
        }
    }

    @Override
    public void setHash(String hash) {
        throw new IllegalArgumentException("The embedded connection does not support user authentication yet!");
    }

    @Override
    public int getSoTimeout() throws SocketException {
        throw new IllegalArgumentException("Cannot get socket timeout on an embedded connection!");
    }

    @Override
    public void setSoTimeout(int s) throws SocketException {
        throw new IllegalArgumentException("Cannot set socket timeout on an embedded connection!");
    }

    @Override
    public List<String> connect(String user, String pass) throws IOException, MCLParseException, MCLException {
        try {
            if(MonetDBEmbeddedDatabase.IsDatabaseRunning() && !MonetDBEmbeddedDatabase.GetDatabaseDirectory().equals(this.directory)) {
                throw new MCLException("The embedded database is already running on a different directory!");
            } else {
                MonetDBEmbeddedDatabase.StartDatabase(this.directory, true, false);
            }
            this.reader = new EmbeddedMCLReader();
            this.writer = new EmbeddedMCLWriter(this.reader);
            MonetDBEmbeddedDatabase.AddJDBCEmbeddedConnection(this);
        } catch (MonetDBEmbeddedException ex) {
            throw new MCLException(ex);
        }
        return null;
    }

    @Override
    public InputStream getInputStream() {
        throw new IllegalArgumentException("Not available!");
    }

    @Override
    public OutputStream getOutputStream() {
        throw new IllegalArgumentException("Not available!");
    }

    @Override
    public AbstractMCLReader getReader() {
        return this.reader;
    }

    @Override
    public AbstractMCLWriter getWriter() {
        return this.writer;
    }

    @Override
    public synchronized void close() {
        super.close();
        try {
            MonetDBEmbeddedDatabase.StopDatabase();
        } catch (MonetDBEmbeddedException e) {
            // ignore it
        }
    }

    @Override
    public String getJDBCURL() {
        return "jdbc:monetdb://" + this.getHostname() + "@" + this.getDirectory() + "/" + this.getDatabase();
    }

    @Override
    public int getBlockSize() {
        return BUFFER_SIZE;
    }

    @Override
    public StartOfHeaderParser getStartOfHeaderParser() {
        return new EmbeddedStartOfHeaderParser();
    }

    @Override
    public HeaderLineParser getHeaderLineParser(int capacity) {
        return new EmbeddedHeaderLineParser(capacity);
    }

    @Override
    public TupleLineParser getTupleLineParser(int capacity) {
        return new EmbeddedTupleLineParser(capacity);
    }

    @Override
    public long getConnectionPointer() {
        return connectionPointer;
    }

    @Override
    public void closeConnectionImplementation() {
        this.closeConnectionInternal(this.connectionPointer);
    }

    private native void closeConnectionInternal(long connectionPointer);
}
