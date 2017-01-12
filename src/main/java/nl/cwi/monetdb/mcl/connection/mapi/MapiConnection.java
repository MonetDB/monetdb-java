/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.mapi;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.connection.helpers.ChannelSecurity;
import nl.cwi.monetdb.mcl.connection.ControlCommands;
import nl.cwi.monetdb.mcl.connection.MCLException;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.oldmapi.OldMapiProtocol;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * A {@link Connection} suitable for the MonetDB database on a MAPI connection.
 *
 * @author Fabian Groffen, Martin van Dinther, Pedro Ferreira
 */
public class MapiConnection extends MonetConnection {
    /** the PROMPT ASCII char sent by the server */
    static final char PROMPT_CHAR = '.';
    /** the default number of rows that are (attempted to) read at once */
    private static final int DEF_FETCHSIZE = 250;

    /** The hostname to connect to */
    private final String hostname;
    /** The port to connect on the host to */
    private final int port;
    /** The database to connect to */
    private String database;
    /** The TCP Socket timeout in milliseconds. Default is 0 meaning the timeout is disabled (i.e., timeout of infinity) */
    private int soTimeout = 0;
    /** Whether we should follow redirects */
    private boolean followRedirects = true;
    /** How many redirections do we follow until we're fed up with it? */
    private int ttl = 10;
    /** Protocol version of the connection */
    private int version;
    /** Endianness of the server */
    private ByteOrder serverEndianness;

    public MapiConnection(Properties props, String hash, String language, boolean blobIsBinary, boolean clobIsLongChar,
                          String hostname, int port, String database) throws IOException {
        super(props, hash, MapiLanguage.GetLanguageFromString(language), blobIsBinary, clobIsLongChar);
        this.hostname = hostname;
        this.port = port;
        this.database = database;
    }

    /**
     * Gets the hostname of the server used on this connection.
     *
     * @return The hostname of the server used on this connection
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the port of the server used on this connection.
     *
     * @return The port of the server used on this connection
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the database to connect to. If database is null, a connection is made to the default database of the server.
     * This is also the default.
     *
     * @return The database name
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Gets the SO_TIMEOUT from the underlying Socket.
     *
     * @return The currently in use timeout in milliseconds
     */
    @Override
    public int getSoTimeout()  {
        try {
            if(protocol != null) {
                this.soTimeout = ((OldMapiProtocol)protocol).getSocket().getSoTimeout();
            }
            return this.soTimeout;
        } catch (SocketException e) {
            this.addWarning("The socket timeout could not be get", "M1M05");
        }
        return -1;
    }

    /**
     * Set the SO_TIMEOUT on the underlying Socket.  When for some reason the connection to the database hangs, this
     * setting can be useful to break out of this indefinite wait. This option must be enabled prior to entering the
     * blocking operation to have effect.
     *
     * @param timeout The specified timeout, in milliseconds. A timeout of zero is interpreted as an infinite timeout
     */
    @Override
    public void setSoTimeout(int timeout)  {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout can't be negative");
        }
        try {
            if(protocol != null) {
                ((OldMapiProtocol)protocol).getSocket().setSoTimeout(timeout);
            }
            this.soTimeout = timeout;
        } catch (SocketException e) {
            this.addWarning("The socket timeout could not be set", "M1M05");
        }
    }

    /**
     * Gets whether MCL redirections should be followed or not. If set to false, an MCLException will be thrown when a
     * redirect is encountered during connect. The default behaviour is to automatically follow redirects.
     *
     * @return Whether to follow redirects (true) or not (false)
     */
    public boolean isFollowRedirects() {
        return followRedirects;
    }

    /**
     * Gets the number of redirects that are followed when followRedirects is true. In order to avoid going into an
     * endless loop due to some evil server, or another error, a maximum number of redirects that may be followed can be
     * set here. Note that to disable the following of redirects you should use setFollowRedirects.
     *
     * @see #isFollowRedirects()
     * @return The number of redirects before an exception is thrown
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * Gets the mapi protocol version used by this socket. The protocol version depends on the server being used.
     *
     * @return The mapi protocol version used by this socket
     */
    public int getVersion() {
        return version;
    }

    /**
     * Gets the connection server endianness.
     *
     * @return The connection server endianness
     */
    public ByteOrder getServerEndianness() {
        return serverEndianness;
    }

    /**
     * On a MAPI connection, the block size will be the block size of the connection.
     *
     * @return The block size length
     */
    @Override
    public int getBlockSize() {
        return ((OldMapiProtocol)protocol).getSocket().getBlockSize();
    }

    /**
     * On a MAPI connection the default fetch size per DataBlock is 250 rows.
     *
     * @return The default fetch size
     */
    @Override
    public int getDefFetchsize() {
        return DEF_FETCHSIZE;
    }

    @Override
    public synchronized void closeUnderlyingConnection() throws IOException {
        ((OldMapiProtocol)protocol).getSocket().close();
    }

    @Override
    public String getJDBCURL() {
        String res = "jdbc:monetdb://" + this.hostname + ":" + this.port + "/" + this.database;
        if (this.getLanguage() == MapiLanguage.LANG_MAL)
            res += "?language=mal";
        return res;
    }

    @Override
    public void sendControlCommand(int commandID, int data) throws SQLException {
        String command = null;
        switch (commandID) {
            case ControlCommands.AUTO_COMMIT:
                command = "auto_commit " + ((data == 1) ? "1" : "0");
                break;
            case ControlCommands.REPLY_SIZE:
                command = "reply_size " + data;
                break;
            case ControlCommands.RELEASE:
                command = "release " + data;
                break;
            case ControlCommands.CLOSE:
                command = "close " + data;
        }
        try {
            protocol.writeNextQuery(language.getCommandTemplateIndex(0), command,
                    language.getCommandTemplateIndex(1));
            protocol.waitUntilPrompt();
            int csrh = protocol.getCurrentServerResponse();
            if (csrh == ServerResponses.ERROR) {
                String error = protocol.getRemainingStringLine(0);
                throw new SQLException(error.substring(6), error.substring(0, 5));
            }
        } catch (SocketTimeoutException e) {
            close(); // JDBC 4.1 semantics, abort()
            throw new SQLException("connection timed out", "08M33");
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), "08000");
        }
    }

    @Override
    public ResponseList createResponseList(int fetchSize, int maxRows, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new MonetConnection.ResponseList(fetchSize, maxRows, resultSetType, resultSetConcurrency);
    }

    /**
     * Connects to the given host and port, logging in as the given user. If followRedirect is false, a
     * RedirectionException is thrown when a redirect is encountered.
     *
     * @return A List with informational (warning) messages. If this list is empty; then there are no warnings.
     * @throws IOException if an I/O error occurs when creating the socket
     * @throws ProtocolException if bogus data is received
     * @throws MCLException if an MCL related error occurs
     */
    @Override
    public List<String> connect(String user, String pass) throws IOException, ProtocolException, MCLException {
        // Wrap around the internal connect that needs to know if it should really make a TCP connection or not.
        List<String> res = connect(this.hostname, this.port, user, pass, true);
        // apply NetworkTimeout value from legacy (pre 4.1) driver so_timeout calls
        this.setSoTimeout(this.getSoTimeout());
        return res;
    }

    private List<String> connect(String host, int port, String user, String pass, boolean makeConnection)
            throws IOException, ProtocolException, MCLException {
        if (ttl-- <= 0)
            throw new MCLException("Maximum number of redirects reached, aborting connection attempt. Sorry.");

        if (makeConnection) {
            this.protocol = new OldMapiProtocol(new OldMapiSocket(this.hostname, this.port, this));
            //set nodelay, as it greatly speeds up small messages (like we often do)
            ((OldMapiProtocol)this.protocol).getSocket().setTcpNoDelay(true);
            ((OldMapiProtocol)this.protocol).getSocket().setSoTimeout(this.soTimeout);
        }

        this.protocol.fetchNextResponseData();
        String nextLine = this.protocol.getRemainingStringLine(0);
        this.protocol.waitUntilPrompt();
        String test = this.getChallengeResponse(nextLine, user, pass, this.language.getRepresentation(),
                this.database, this.hash);
        this.protocol.writeNextQuery("", test, "");

        List<String> redirects = new ArrayList<>();
        List<String> warns = new ArrayList<>();
        String err = "";
        int next;

        do {
            this.protocol.fetchNextResponseData();
            next = this.protocol.getCurrentServerResponse();
            switch (next) {
                case ServerResponses.ERROR:
                    err += "\n" + this.protocol.getRemainingStringLine(7);
                    break;
                case ServerResponses.INFO:
                    warns.add(this.protocol.getRemainingStringLine(1));
                    break;
                case ServerResponses.REDIRECT:
                    redirects.add(this.protocol.getRemainingStringLine(1));
            }
        } while (next != ServerResponses.PROMPT);

        if (!err.equals("")) {
            this.close();
            throw new MCLException(err.trim());
        }
        if (!redirects.isEmpty()) {
            if (followRedirects) {
                // Ok, server wants us to go somewhere else. The list might have multiple clues on where to go. For now
                // we don't support anything intelligent but trying the first one. URI should be in form of:
                // "mapi:monetdb://host:port/database?arg=value&..." or "mapi:merovingian://proxy?arg=value&..." note
                // that the extra arguments must be obeyed in both cases
                String suri = redirects.get(0);
                if (!suri.startsWith("mapi:"))
                    throw new MCLException("unsupported redirect: " + suri);

                URI u;
                try {
                    u = new URI(suri.substring(5));
                } catch (URISyntaxException e) {
                    throw new ProtocolException(e.toString());
                }
                String tmp = u.getQuery();
                if (tmp != null) {
                    String args[] = tmp.split("&");
                    for (String arg : args) {
                        int pos = arg.indexOf("=");
                        if (pos > 0) {
                            tmp = arg.substring(0, pos);
                            switch (tmp) {
                                case "database":
                                    tmp = arg.substring(pos + 1);
                                    if (!tmp.equals(database)) {
                                        warns.add("redirect points to different " + "database: " + tmp);
                                        this.database = tmp;
                                    }
                                    break;
                                case "language":
                                    tmp = arg.substring(pos + 1);
                                    warns.add("redirect specifies use of different language: " + tmp);
                                     this.language = MapiLanguage.GetLanguageFromString(tmp);
                                    break;
                                case "user":
                                    tmp = arg.substring(pos + 1);
                                    if (!tmp.equals(user))
                                        warns.add("ignoring different username '" + tmp + "' set by " +
                                                "redirect, what are the security implications?");
                                    break;
                                case "password":
                                    warns.add("ignoring different password set by redirect, " +
                                            "what are the security implications?");
                                    break;
                                default:
                                    warns.add("ignoring unknown argument '" + tmp + "' from redirect");
                                    break;
                            }
                        } else {
                            warns.add("ignoring illegal argument from redirect: " + arg);
                        }
                    }
                }

                switch (u.getScheme()) {
                    case "monetdb":
                        tmp = u.getPath();
                        if (tmp != null && tmp.length() != 0) {
                            tmp = tmp.substring(1).trim();
                            if (!tmp.isEmpty() && !tmp.equals(database)) {
                                warns.add("redirect points to different " + "database: " + tmp);
                                this.database = tmp;
                            }
                        }
                        int p = u.getPort();
                        warns.addAll(connect(u.getHost(), p == -1 ? port : p, user, pass, true));
                        warns.add("Redirect by " + host + ":" + port + " to " + suri);
                        break;
                    case "merovingian":
                        // reuse this connection to inline connect to the right database that Merovingian proxies for us
                        warns.addAll(connect(host, port, user, pass, false));
                        break;
                    default:
                        throw new MCLException("unsupported scheme in redirect: " + suri);
                }
            } else {
                StringBuilder msg = new StringBuilder("The server sent a redirect for this connection:");
                for (String it : redirects) {
                    msg.append(" [").append(it).append("]");
                }
                throw new MCLException(msg.toString());
            }
        }
        return warns;
    }

    /**
     * A little helper function that processes a challenge string, and returns a response string for the server.
     * If the challenge string is null, a challengeless response is returned.
     *
     * @param chalstr the challenge string
     * @param username the username to use
     * @param password the password to use
     * @param language the language to use
     * @param database the database to connect to
     * @param hash the hash method(s) to use, or NULL for all supported hashes
     */
    private String getChallengeResponse(String chalstr, String username, String password, String language,
                                        String database, String hash) throws ProtocolException, MCLException,
            IOException {
        String response;
        String algo;

        // parse the challenge string, split it on ':'
        String[] chaltok = chalstr.split(":");
        if (chaltok.length <= 4)
            throw new ProtocolException("Server challenge string unusable! Challenge contains too few tokens: "
                    + chalstr);

        // challenge string to use as salt/key
        String challenge = chaltok[0];
        String servert = chaltok[1];
        try {
            this.version = Integer.parseInt(chaltok[2].trim()); // protocol version
        } catch (NumberFormatException e) {
            throw new ProtocolException("Protocol version unparseable: " + chaltok[2]);
        }

        switch (chaltok[4]) {
            case "BIG":
                this.serverEndianness = ByteOrder.BIG_ENDIAN;
                break;
            case "LIT":
                this.serverEndianness = ByteOrder.LITTLE_ENDIAN;
                break;
            default:
                throw new ProtocolException("Invalid byte-order: " + chaltok[4]);
        }
        ((OldMapiProtocol)protocol).getSocket().setSocketChannelEndianness(this.serverEndianness);

        // handle the challenge according to the version it is
        switch (this.version) {
            case 9:
                // proto 9 is like 8, but uses a hash instead of the plain password, the server tells us which hash in
                // the challenge after the byte-order
				/* NOTE: Java doesn't support RIPEMD160 :( */
                switch (chaltok[5]) {
                    case "SHA512":
                        algo = "SHA-512";
                        break;
                    case "SHA384":
                        algo = "SHA-384";
                        break;
                    case "SHA256":
                        algo = "SHA-256";
				/* NOTE: Java supports SHA-224 only on 8 */
                        break;
                    case "SHA1":
                        algo = "SHA-1";
                        break;
                    case "MD5":
                        algo = "MD5";
                        break;
                    default:
                        throw new MCLException("Unsupported password hash: " + chaltok[5]);
                }

                password = ChannelSecurity.DigestStrings(algo, password.getBytes("UTF-8"));

                // proto 7 (finally) used the challenge and works with a password hash. The supported implementations
                // come from the server challenge. We chose the best hash we can find, in the order SHA1, MD5, plain.
                // Also, the byte-order is reported in the challenge string. proto 8 made this obsolete, but retained
                // the byte-order report for future "binary" transports. In proto 8, the byte-order of the blocks is
                // always little endian because most machines today are.
                String hashes = (hash == null ? chaltok[3] : hash);
                Set<String> hashesSet = new HashSet<>(Arrays.asList(hashes.toUpperCase().split("[, ]")));

                // if we deal with merovingian, mask our credentials
                if (servert.equals("merovingian") && !language.equals("control")) {
                    username = "merovingian";
                    password = "merovingian";
                }
                String pwhash;

                if (hashesSet.contains("SHA512")) {
                    algo = "SHA-512";
                    pwhash = "{SHA512}";
                } else if (hashesSet.contains("SHA384")) {
                    algo = "SHA-384";
                    pwhash = "{SHA384}";
                } else if (hashesSet.contains("SHA256")) {
                    algo = "SHA-256";
                    pwhash = "{SHA256}";
                } else if (hashesSet.contains("SHA1")) {
                    algo = "SHA-1";
                    pwhash = "{SHA1}";
                } else if (hashesSet.contains("MD5")) {
                    algo = "MD5";
                    pwhash = "{MD5}";
                } else {
                    throw new MCLException("No supported password hashes in " + hashes);
                }

                pwhash += ChannelSecurity.DigestStrings(algo, password.getBytes("UTF-8"),
                        challenge.getBytes("UTF-8"));

                // generate response
                response = "BIG:";	// JVM byte-order is big-endian
                response += username + ":" + pwhash + ":" + language;
                response += ":" + (database == null ? "" : database) + ":";

                this.conn_props.setProperty("hash", hashes);
                this.conn_props.setProperty("language", language);
                this.conn_props.setProperty("database", database);

                return response;
            default:
                throw new MCLException("Unsupported protocol version: " + version);
        }
    }
}
