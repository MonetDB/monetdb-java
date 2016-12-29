package nl.cwi.monetdb.mcl.connection;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.jdbc.MonetDriver;
import nl.cwi.monetdb.mcl.connection.embedded.EmbeddedConnection;
import nl.cwi.monetdb.mcl.connection.mapi.MapiConnection;
import nl.cwi.monetdb.mcl.connection.mapi.MapiLanguage;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

/**
 * Created by ferreira on 12/1/16.
 */
public final class MonetDBConnectionFactory {

    public static MonetConnection CreateMonetDBJDBCConnection(Properties props)
            throws SQLException, IllegalArgumentException {
        MonetConnection res;

        boolean isEmbedded = Boolean.parseBoolean(props.getProperty("embedded", "false"));
        boolean debug = Boolean.valueOf(props.getProperty("debug", "false"));
        boolean blobIsBinary = Boolean.valueOf(props.getProperty("treat_blob_as_binary", "false"));
        String language = props.getProperty("language", "sql");

        String username = props.getProperty("user", null);
        String password = props.getProperty("password", null);
        String database = props.getProperty("database");
        if (database == null || database.trim().isEmpty())
            throw new IllegalArgumentException("database should not be null or empty");
        String hash = props.getProperty("hash");
        int sockTimeout = 0;

        //instantiate the connection
        if(isEmbedded) {
            String directory = props.getProperty("directory");
            if (directory == null || directory.trim().isEmpty())
                throw new IllegalArgumentException("directory should not be null or empty");
            try {
                res = new EmbeddedConnection(props, database, hash, language, blobIsBinary, debug, directory);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else {
            String hostname = props.getProperty("host");
            if (hostname == null || hostname.trim().isEmpty())
                throw new IllegalArgumentException("hostname should not be null or empty");
            if (username == null || username.trim().isEmpty())
                throw new IllegalArgumentException("user should not be null or empty");
            if (password == null || password.trim().isEmpty())
                throw new IllegalArgumentException("password should not be null or empty");

            boolean negative1 = false, failedparse1 = false;
            int port = 0;
            try {
                port = Integer.parseInt(props.getProperty("port"));
            } catch (NumberFormatException e) {
                failedparse1 = true;
                props.setProperty("port", MonetDriver.getPORT());
            }
            if (port <= 0) {
                negative1 = true;
                port = Integer.parseInt(MonetDriver.getPORT());
                props.setProperty("port", MonetDriver.getPORT());
            }

            String timeout = props.getProperty("so_timeout", "0");
            boolean negative2 = false, failedparse2 = false;
            try {
                sockTimeout = Integer.parseInt(timeout);
            } catch (NumberFormatException e) {
                sockTimeout = 0;
                failedparse2 = true;
                props.setProperty("so_timeout", "0");
            }
            if (sockTimeout < 0) {
                negative2 = true;
                sockTimeout = 0;
                props.setProperty("so_timeout", "0");
            }
            try {
                res = new MapiConnection(props, database, hash, language, blobIsBinary, debug, hostname, port);
            } catch (IOException e) {
                throw new SQLException(e);
            }
            if(failedparse1) {
                res.addWarning("Unable to parse port number from: " + port, "M1M05");
            }
            if(negative1) {
                res.addWarning("Negative port not allowed. Value ignored", "M1M05");
            }
            if(failedparse2) {
                res.addWarning("Unable to parse socket timeout number from: " + timeout, "M1M05");
            }
            if(negative2) {
                res.addWarning("Negative socket timeout not allowed. Value ignored", "M1M05");
            }
            res.setSoTimeout(sockTimeout);
        }
        //initialize the debugging stuff if so
        if (debug) {
            try {
                String fname = props.getProperty("logfile", "monet_" +
                        System.currentTimeMillis() + ".log");
                File f = new File(fname);
                int ext = fname.lastIndexOf('.');
                if (ext < 0) ext = fname.length();
                String pre = fname.substring(0, ext);
                String suf = fname.substring(ext);
                for (int i = 1; f.exists(); i++) {
                    f = new File(pre + "-" + i + suf);
                }
                res.setDebugging(f.getAbsolutePath());
            } catch (IOException ex) {
                throw new SQLException("Opening logfile failed: " + ex.getMessage(), "08M01");
            }
        }

        try {
            List<String> warnings = res.connect(username, password);
            if(warnings != null) {
                for (String warning : warnings) {
                    res.addWarning(warning, "01M02");
                }
            }
            // apply NetworkTimeout value from legacy (pre 4.1) driver so_timeout calls
            if(!isEmbedded) {
                res.setSoTimeout(sockTimeout);
            }
        } catch (IOException e) {
            if(!isEmbedded) {
                MapiConnection con = (MapiConnection) res;
                throw new SQLException("Unable to connect (" + con.getHostname() + ":"
                        + con.getPort() + "): " + e.getMessage(), "08006");
            } else {
                EmbeddedConnection em = (EmbeddedConnection) res;
                throw new SQLException("Unable to connect the directory " + em.getDirectory() + ": "
                        + e.getMessage(), "08006");
            }
        } catch (ProtocolException e) {
            throw new SQLException(e.getMessage(), "08001");
        } catch (MCLException e) {
            String[] connex = e.getMessage().split("\n");
            SQLException sqle = new SQLException(connex[0], "08001", e);
            for (int i = 1; i < connex.length; i++) {
                sqle.setNextException(new SQLException(connex[1], "08001"));
            }
            throw sqle;
        }

        //set the timezone
        if (res.getLanguage() == MapiLanguage.LANG_SQL) {
            // enable auto commit
            res.setAutoCommit(true);

            // set our time zone on the server
            Calendar cal = Calendar.getInstance();
            int offset = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);
            offset /= (60 * 1000); // milliseconds to minutes
            String tz = offset < 0 ? "-" : "+";
            tz += (Math.abs(offset) / 60 < 10 ? "0" : "") + (Math.abs(offset) / 60) + ":";
            offset -= (offset / 60) * 60;
            tz += (offset < 10 ? "0" : "") + offset;

            res.sendIndependentCommand("SET TIME ZONE INTERVAL '" + tz + "' HOUR TO MINUTE");
        }

        return res;
    }
}
