package nl.cwi.monetdb.mcl.connection;

import nl.cwi.monetdb.jdbc.MonetConnection;
import nl.cwi.monetdb.mcl.io.SocketConnection;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;
import nl.cwi.monetdb.mcl.protocol.oldmapi.OldMapiProtocol;
import nl.cwi.monetdb.util.ChannelSecurity;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * A Socket for communicating with the MonetDB database in MAPI block
 * mode.
 *
 * The MapiSocket implements the protocol specifics of the MAPI block
 * mode protocol, and interfaces it as a socket that delivers a
 * BufferedReader and a BufferedWriter.  Because logging in is an
 * integral part of the MAPI protocol, the MapiSocket performs the login
 * procedure.  Like the Socket class, various options can be set before
 * calling the connect() method to influence the login process.  Only
 * after a successful call to connect() the BufferedReader and
 * BufferedWriter can be retrieved.
 * <br />
 * For each line read, it is determined what type of line it is
 * according to the MonetDB MAPI protocol.  This results in a line to be
 * PROMPT, HEADER, RESULT, ERROR or UNKNOWN.  Use the getLineType()
 * method on the BufferedMCLReader to retrieve the type of the last
 * line read.
 *
 * For debugging purposes a socket level debugging is implemented where
 * each and every interaction to and from the MonetDB server is logged
 * to a file on disk.<br />
 * Incoming messages are prefixed by "RX" (received by the driver),
 * outgoing messages by "TX" (transmitted by the driver).  Special
 * decoded non-human readable messages are prefixed with "RD" and "TD"
 * instead.  Following this two char prefix, a timestamp follows as the
 * number of milliseconds since the UNIX epoch.  The rest of the line is
 * a String representation of the data sent or received.
 *
 * The general use of this Socket must be seen only in the full context
 * of a MAPI connection to a server.  It has the same ingredients as a
 * normal Socket, allowing for seamless plugging.
 * <pre>
 *    Socket   \     /  InputStream  ----&gt; (BufferedMCL)Reader
 *              &gt; o &lt;
 *  MapiSocket /     \ OutputStream  ----&gt; (BufferedMCL)Writer
 * </pre>
 * The MapiSocket allows to retrieve Streams for communicating.  They
 * are interfaced, so they can be chained in any way.  While the Socket
 * transparently deals with how data is sent over the wire, the actual
 * data read needs to be interpreted, for which a Reader/Writer
 * interface is most sufficient.  In particular the BufferedMCL*
 * implementations of those interfaces supply some extra functionality
 * geared towards the format of the data.
 *
 * @author Fabian Groffen
 * @version 4.1
 */
public class MapiConnection extends MonetConnection {

    /** The hostname to connect to */
    private final String hostname;
    /** The port to connect on the host to */
    private final int port;
    /** Whether we should follow redirects */
    private boolean followRedirects = true;
    /** How many redirections do we follow until we're fed up with it? */
    private int ttl = 10;
    /** protocol version of the connection */
    private int version;

    MapiConnection(Properties props, String database, String hash, String language, boolean blobIsBinary,
                   boolean isDebugging, String hostname, int port) throws IOException {
        super(props, database, hash, language, blobIsBinary, isDebugging);
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public boolean isFollowRedirects() {
        return followRedirects;
    }

    public int getTtl() {
        return ttl;
    }

    public int setProtocolVersion() {
        return version;
    }

    /**
     * Sets whether MCL redirections should be followed or not.  If set
     * to false, an MCLException will be thrown when a redirect is
     * encountered during connect.  The default behaviour is to
     * automatically follow redirects.
     *
     * @param r whether to follow redirects (true) or not (false)
     */
    public void setFollowRedirects(boolean r) {
        this.followRedirects = r;
    }

    /**
     * Sets the number of redirects that are followed when
     * followRedirects is true.  In order to avoid going into an endless
     * loop due to some evil server, or another error, a maximum number
     * of redirects that may be followed can be set here.  Note that to
     * disable the following of redirects you should use
     * setFollowRedirects.
     *
     * @see #setFollowRedirects(boolean r)
     * @param t the number of redirects before an exception is thrown
     */
    public void setTTL(int t) {
        this.ttl = t;
    }

    /**
     * Returns the mapi protocol version used by this socket.  The
     * protocol version depends on the server being used.  Users of the
     * MapiSocket should check this version to act appropriately.
     *
     * @return the mapi protocol version
     */
    public int getProtocolVersion() {
        return this.version;
    }

    @Override
    public int getBlockSize() {
        return ((OldMapiProtocol)protocol).getConnection().getBlockSize();
    }

    @Override
    public int getSoTimeout()  {
        try {
            return ((OldMapiProtocol)protocol).getConnection().getSoTimeout();
        } catch (SocketException e) {
            this.addWarning("The socket timeout could not be get", "M1M05");
        }
        return -1;
    }

    @Override
    public void setSoTimeout(int s)  {
        try {
            ((OldMapiProtocol)protocol).getConnection().setSoTimeout(s);
        } catch (SocketException e) {
            this.addWarning("The socket timeout could not be set", "M1M05");
        }
    }

    @Override
    public void closeUnderlyingConnection() throws IOException {
        ((OldMapiProtocol)protocol).getConnection().close();
    }

    @Override
    public String getJDBCURL() {
        String language = "";
        if (this.getLanguage() == MonetDBLanguage.LANG_MAL)
            language = "?language=mal";
        return "jdbc:monetdb://" + this.hostname + ":" + this.port + "/" + this.database + language;
    }

    @Override
    public AbstractProtocol getProtocol() {
        return this.protocol;
    }

    @Override
    public List<String> connect(String user, String pass) throws IOException, ProtocolException, MCLException {
        // Wrap around the internal connect that needs to know if it
        // should really make a TCP connection or not.
        List<String> res = connect(this.hostname, this.port, user, pass, true);
        // apply NetworkTimeout value from legacy (pre 4.1) driver
        // so_timeout calls
        this.setSoTimeout(this.getSoTimeout());
        return res;
    }

    private List<String> connect(String host, int port, String user, String pass, boolean makeConnection)
            throws IOException, ProtocolException, MCLException {
        if (ttl-- <= 0)
            throw new MCLException("Maximum number of redirects reached, aborting connection attempt. Sorry.");

        AbstractProtocol<?> pro;

        if (makeConnection) {
            pro = new OldMapiProtocol(new SocketConnection(this.hostname, this.port));
            this.protocol = pro;
            ((OldMapiProtocol)pro).getConnection().setTcpNoDelay(true);

            // set nodelay, as it greatly speeds up small messages (like we
            // often do)
            //TODO writer.registerReader(reader); ??
        } else {
            pro = this.protocol;
        }

        pro.fetchNextResponseData();
        pro.waitUntilPrompt();
        String firstLine = pro.getRemainingStringLine(0);

        String test = this.getChallengeResponse(firstLine, user, pass, this.language.getRepresentation(),
                this.database, this.hash);
        pro.writeNextCommand(MonetDBLanguage.EmptyString, test.getBytes(), MonetDBLanguage.EmptyString);

        List<String> redirects = new ArrayList<>();
        List<String> warns = new ArrayList<>();
        String err = "";
        ServerResponses next;

        do {
            pro.fetchNextResponseData();
            next = pro.getCurrentServerResponseHeader();
            switch (next) {
                case ERROR:
                    err += "\n" + pro.getRemainingStringLine(7);
                    break;
                case INFO:
                    warns.add(pro.getRemainingStringLine(1));
                case REDIRECT:
                    redirects.add(pro.getRemainingStringLine(1));
            }
        } while (next != ServerResponses.PROMPT);

        if (!err.equals("")) {
            this.close();
            throw new MCLException(err.trim());
        }
        if (!redirects.isEmpty()) {
            if (followRedirects) {
                // Ok, server wants us to go somewhere else.  The list
                // might have multiple clues on where to go.  For now we
                // don't support anything intelligent but trying the
                // first one.  URI should be in form of:
                // "mapi:monetdb://host:port/database?arg=value&..."
                // or
                // "mapi:merovingian://proxy?arg=value&..."
                // note that the extra arguments must be obeyed in both
                // cases
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
                                     this.language = MonetDBLanguage.GetLanguageFromString(tmp);
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
                        // this is a redirect to another (monetdb) server,
                        // which means a full reconnect
                        // avoid the debug log being closed
                        if (this.isDebugging) {
                            this.isDebugging = false;
                            this.close();
                            this.isDebugging = true;
                        } else {
                            this.close();
                        }
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
                        // reuse this connection to inline connect to the
                        // right database that Merovingian proxies for us
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
     * A little helper function that processes a challenge string, and
     * returns a response string for the server.  If the challenge
     * string is null, a challengeless response is returned.
     *
     * @param chalstr the challenge string
     * @param username the username to use
     * @param password the password to use
     * @param language the language to use
     * @param database the database to connect to
     * @param hash the hash method(s) to use, or NULL for all supported
     *             hashes
     */
    private String getChallengeResponse(String chalstr, String username, String password, String language,
                                        String database, String hash)
            throws ProtocolException, MCLException, IOException {
        String response;
        String algo;

        // parse the challenge string, split it on ':'
        String[] chaltok = chalstr.split(":");
        if (chaltok.length <= 4)
            throw new ProtocolException("Server challenge string unusable!  Challenge contains too few tokens: "
                    + chalstr);

        // challenge string to use as salt/key
        String challenge = chaltok[0];
        String servert = chaltok[1];
        try {
            version = Integer.parseInt(chaltok[2].trim());	// protocol version
        } catch (NumberFormatException e) {
            throw new ProtocolException("Protocol version unparseable: " + chaltok[3]);
        }

        // handle the challenge according to the version it is
        switch (version) {
            default:
                throw new MCLException("Unsupported protocol version: " + version);
            case 9:
                // proto 9 is like 8, but uses a hash instead of the
                // plain password, the server tells us which hash in the
                // challenge after the byte-order

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
				/* NOTE: Java doesn't support SHA-224 */
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

                password = ChannelSecurity.DigestStrings(algo, password);

                // proto 7 (finally) used the challenge and works with a
                // password hash.  The supported implementations come
                // from the server challenge.  We chose the best hash
                // we can find, in the order SHA1, MD5, plain.  Also,
                // the byte-order is reported in the challenge string,
                // which makes sense, since only blockmode is supported.
                // proto 8 made this obsolete, but retained the
                // byte-order report for future "binary" transports.  In
                // proto 8, the byte-order of the blocks is always little
                // endian because most machines today are.
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

                pwhash += ChannelSecurity.DigestStrings(algo, password, challenge);

                // TODO: some day when we need this, we should store
                // this
                switch (chaltok[4]) {
                    case "BIG":
                        // byte-order of server is big-endian
                        break;
                    case "LIT":
                        // byte-order of server is little-endian
                        break;
                    default:
                        throw new ProtocolException("Invalid byte-order: " + chaltok[5]);
                }

                // generate response
                response = "BIG:";	// JVM byte-order is big-endian
                response += username + ":" + pwhash + ":" + language;
                response += ":" + (database == null ? "" : database) + ":";

                this.conn_props.setProperty("hash", hashes);
                this.conn_props.setProperty("language", language);
                this.conn_props.setProperty("database", database);

                return response;
        }
    }
}
