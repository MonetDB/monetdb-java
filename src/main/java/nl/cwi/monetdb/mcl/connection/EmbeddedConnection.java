package nl.cwi.monetdb.mcl.connection;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.io.*;
import nl.cwi.monetdb.mcl.parser.MCLParseException;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by ferreira on 11/23/16.
 */
public final class EmbeddedConnection extends MonetConnection {

    private final String directory;

    private JDBCEmbeddedConnection internalConnection;

    public EmbeddedConnection(Properties props, String database, String hash, String language, boolean blobIsBinary, boolean isDebugging, String directory) throws IOException {
        super(props, database, hash, language, blobIsBinary, isDebugging);
        this.directory = directory;
    }

    public String getDirectory() {
        return directory;
    }

    public MonetDBEmbeddedConnection getAsMonetDBEmbeddedConnection() {
        return internalConnection;
    }

    @Override
    public List<String> connect(String user, String pass) throws IOException, MCLParseException, MCLException {
        try {
            if(MonetDBEmbeddedDatabase.IsDatabaseRunning() && !MonetDBEmbeddedDatabase.GetDatabaseDirectory().equals(this.directory)) {
                throw new MCLException("The embedded database is already running on a different directory!");
            } else {
                MonetDBEmbeddedDatabase.StartDatabase(this.directory, true, false);
            }
            this.internalConnection = MonetDBEmbeddedDatabase.CreateJDBCEmbeddedConnection();
        } catch (MonetDBEmbeddedException ex) {
            throw new MCLException(ex);
        }
        return null;
    }

    @Override
    public String getJDBCURL() {
        return "jdbc:monetdb://localhost@" + this.directory + "/" + this.database;
    }

    @Override
    public int getBlockSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSoTimeout() {
        this.addWarning("Cannot get a timeout on a embedded connection!", "M1M05");
        return -1;
    }

    @Override
    public void setSoTimeout(int s) {
        this.addWarning("Cannot set a timeout on a embedded connection!", "M1M05");
    }

    @Override
    public void closeUnderlyingConnection() throws IOException {
        try {
            MonetDBEmbeddedDatabase.StopDatabase();
        } catch (MonetDBEmbeddedException e) {
            // ignore it
        }
    }
}
