/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.mcl.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import javax.net.ssl.SSLException;

import org.monetdb.mcl.MCLException;
import org.monetdb.mcl.io.BufferedMCLReader;
import org.monetdb.mcl.io.BufferedMCLWriter;
import org.monetdb.mcl.io.LineType;
import org.monetdb.mcl.parser.MCLParseException;

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
 *
 * For each line read, it is determined what type of line it is
 * according to the MonetDB MAPI protocol.  This results in a line to be
 * PROMPT, HEADER, RESULT, ERROR or UNKNOWN.  Use the getLineType()
 * method on the BufferedMCLReader to retrieve the type of the last
 * line read.
 *
 * For debugging purposes a socket level debugging is implemented where
 * each and every interaction to and from the MonetDB server is logged
 * to a file on disk.
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
 * @version 4.4
 * @see org.monetdb.mcl.io.BufferedMCLReader
 * @see org.monetdb.mcl.io.BufferedMCLWriter
 */
public final class MapiSocket {
	/* an even number of NUL bytes used during the handshake */
	private static final byte[] NUL_BYTES = new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0 };

	/* A mapping between hash algorithm names as used in the MAPI
	 * protocol, and the names by which the Java runtime knows them.
	 */
	private static final String[][] KNOWN_ALGORITHMS = new String[][] {
			{"SHA512", "SHA-512"},
			{"SHA384", "SHA-384"},
			{"SHA256", "SHA-256"},
			// should we deprecate this by now?
			{"SHA1", "SHA-1"},
	};

	// MUST be lowercase!
	private static final char[] HEXDIGITS = "0123456789abcdef".toCharArray();

	/** Connection parameters */
	private Target target;
	/** The TCP Socket to mserver */
	private Socket con;
	/** Stream from the Socket for reading */
	private BlockInputStream fromMonet;
	/** Stream from the Socket for writing */
	private OutputStream toMonet;
	/** MCLReader on the InputStream */
	private BufferedMCLReader reader;
	/** MCLWriter on the OutputStream */
	private BufferedMCLWriter writer;
	/** protocol version of the connection */
	private int version;
	private boolean supportsClientInfo;

	/** Whether we should follow redirects.
	 * Not sure why this needs to be separate
	 * from 'ttl' but someone someday explicitly documented setTtl
	 * with 'to disable completely, use followRedirects' so
	 * apparently there is a use case.
	 */
	private boolean followRedirects = true;
	/** How many redirections do we follow until we're fed up with it? */
	private int ttl = 10;

	/** The Writer for the debug log-file */
	private Writer log;

	/** The blocksize (hardcoded in compliance with MonetDB common/stream/stream.h) */
	public final static int BLOCK = 8190;

	/** A short in two bytes for holding the block size in bytes */
	private final byte[] blklen = new byte[2];

	/**
	 * Constructs a new MapiSocket.
	 */
	public MapiSocket() {
		target = new Target();
		con = null;
	}

	/**
	 * Sets the database to connect to.  If database is null, a
	 * connection is made to the default database of the server.  This
	 * is also the default.
	 *
	 * @param db the database
	 */
	public void setDatabase(final String db) {
		target.setDatabase(db);
	}

	/**
	 * Sets the language to use for this connection.
	 *
	 * @param lang the language
	 */
	public void setLanguage(final String lang) {
		target.setLanguage(lang);
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
	public void setHash(final String hash) {
		target.setHash(hash);
	}

	/**
	 * Sets whether MCL redirections should be followed or not.  If set
	 * to false, an MCLException will be thrown when a redirect is
	 * encountered during connect.  The default behavior is to
	 * automatically follow redirects.
	 *
	 * @param r whether to follow redirects (true) or not (false)
	 */
	public void setFollowRedirects(final boolean r) {
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
	public void setTTL(final int t) {
		this.ttl = t;
	}

	/**
	 * Set the SO_TIMEOUT on the underlying Socket.  When for some
	 * reason the connection to the database hangs, this setting can be
	 * useful to break out of this indefinite wait.
	 * This option must be enabled prior to entering the blocking
	 * operation to have effect.
	 *
	 * @param s The specified timeout, in milliseconds.  A timeout
	 *        of zero will disable timeout (i.e., timeout of infinity).
	 * @throws SocketException Issue with the socket
	 */
	public void setSoTimeout(final int s) throws SocketException {
		if (s < 0) {
			throw new IllegalArgumentException("timeout can't be negative");
		}
		target.setSoTimeout(s);
		// limit time to wait on blocking operations
		if (con != null) {
			con.setSoTimeout(s);
		}
	}

	/**
	 * Gets the SO_TIMEOUT from the underlying Socket.
	 *
	 * @return the currently in use timeout in milliseconds
	 * @throws SocketException Issue with the socket
	 */
	public int getSoTimeout() throws SocketException {
		return target.getSoTimeout();
	}

	/**
	 * Enables/disables debug mode with logging to file
	 *
	 * @param debug Value to set
	 */
	public void setDebug(final boolean debug) {
		target.setDebug(debug);
	}

	/**
	 * Connects to the given host and port, logging in as the given
	 * user.  If followRedirect is false, a RedirectionException is
	 * thrown when a redirect is encountered.
	 *
	 * @param host the hostname, or null for the loopback address
	 * @param port the port number (must be between 0 and 65535, inclusive)
	 * @param user the username
	 * @param pass the password
	 * @return A List with informational (warning) messages. If this
	 *		list is empty; then there are no warnings.
	 * @throws IOException if an I/O error occurs when creating the socket
	 * @throws SocketException - if there is an error in the underlying protocol, such as a TCP error.
	 * @throws UnknownHostException if the IP address of the host could not be determined
	 * @throws MCLParseException if bogus data is received
	 * @throws MCLException if an MCL related error occurs
	 */
	public List<String> connect(final String host, final int port, final String user, final String pass)
		throws IOException, SocketException, UnknownHostException, MCLParseException, MCLException
	{
		target.setHost(host);
		target.setPort(port);
		target.setUser(user);
		target.setPassword(pass);
		return connect(target, null);
	}

	public List<String> connect(String url, Properties props) throws URISyntaxException, ValidationError, MCLException, MCLParseException, IOException {
		return connect(new Target(url, props), null);
	}

	/**
	 * Connect according to the settings in the 'target' parameter.
	 * If followRedirect is false, a RedirectionException is
	 * thrown when a redirect is encountered.
	 *
	 * Some settings, such as the initial reply size, can already be configured
	 * during the handshake, saving a command round-trip later on.
	 * To do so, create and pass a subclass of {@link MapiSocket.OptionsCallback}.
	 *
	 * @param target the connection settings
	 * @param callback will be called if the server allows options to be set during the
	 * initial handshake
	 * @return A List with informational (warning) messages. If this
	 *		list is empty; then there are no warnings.
	 * @throws IOException if an I/O error occurs when creating the socket
	 * @throws SocketException - if there is an error in the underlying protocol, such as a TCP error.
	 * @throws UnknownHostException if the IP address of the host could not be determined
	 * @throws MCLParseException if bogus data is received
	 * @throws MCLException if an MCL related error occurs
	 */
	public List<String> connect(Target target, OptionsCallback callback) throws MCLException, MCLParseException, IOException {
		// get rid of any earlier connection state, including the existing target
		close();
		this.target = target;

		Target.Validated validated;
		try {
			validated = target.validate();
		} catch (ValidationError e) {
			throw new MCLException(e.getMessage());
		}

		if (validated.connectScan()) {
			return scanUnixSockets(callback);
		}

		ArrayList<String> warnings = new ArrayList<>();
		int attempts = 0;
		do {
			boolean ok = false;
			try {
				boolean done = tryConnect(callback, warnings);
				ok = true;
				if (done) {
					return warnings;
				}
			} finally {
				if (!ok)
					close();
			}
		} while (followRedirects && attempts++ < this.ttl);
		throw new MCLException("max redirect count exceeded");
	}

	private List<String> scanUnixSockets(OptionsCallback callback) throws MCLException, MCLParseException, IOException {
		// Because we do not support Unix Domain sockets, we just go back to connect().
		// target.connectScan() will now return false;
		target.setHost("localhost");
		return connect(target, callback);
	}

	private boolean tryConnect(OptionsCallback callback, ArrayList<String> warningBuffer) throws MCLException, IOException {
		try {
			// We need a valid target
			Target.Validated validated = target.validate();
			// con will be non-null if the previous attempt ended in a redirect to mapi:monetdb://proxy
			if (con == null)
				connectSocket(validated);
			return handshake(validated, callback, warningBuffer);
		} catch (IOException | MCLException e) {
			close();
			throw e;
		} catch (ValidationError e) {
			close();
			throw new MCLException(e.getMessage());
		}
	}

	private void connectSocket(Target.Validated validated) throws MCLException, IOException {
		// This method performs steps 2-6 of the procedure outlined in the URL spec
		String tcpHost = validated.connectTcp();
		if (tcpHost.isEmpty()) {
			throw new MCLException("Unix domain sockets are not supported, only TCP");
		}
		int port = validated.connectPort();
		Socket sock = null;
		try {
			sock = new Socket(tcpHost, port);
			sock.setSoTimeout(validated.getSoTimeout());
			sock.setTcpNoDelay(true);
			sock.setKeepAlive(true);

			sock = wrapTLS(sock, validated);

			fromMonet = new BlockInputStream(sock.getInputStream());
			toMonet = new BlockOutputStream(sock.getOutputStream());
			reader = new BufferedMCLReader(fromMonet, StandardCharsets.UTF_8);
			writer = new BufferedMCLWriter(toMonet, StandardCharsets.UTF_8);
			writer.registerReader(reader);
			reader.advance();
			if (reader.getLine() == null) {
				throw new IOException("server did not send a challenge");
			}

			// Only assign to sock when everything went ok so far
			con = sock;
			sock = null;
		} catch (SSLException e) {
			throw new MCLException("SSL error: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new MCLException("Could not connect to " + tcpHost + ":" + port + ": " + e.getMessage(), e);
		} finally {
			if (sock != null)
				try {
					sock.close();
				} catch (IOException e) {
					// ignore
				}
		}
	}

	private Socket wrapTLS(Socket sock, Target.Validated validated) throws IOException {
		if (validated.getTls())
			return SecureSocket.wrap(validated, sock);
		else {
			// Send an even number of NUL bytes to avoid a deadlock if
			// we're accidentally connecting to a TLS-protected server.
			// The cause of the deadlock is that we speak MAPI and we wait
			// for the server to send a MAPI challenge.
			// However, if the server is trying to set up TLS, it will be
			// waiting for us to send a TLS 'Client Hello' packet.
			// Hence, deadlock.
			// NUL NUL is a no-op in MAPI and will hopefully force an error
			// in the TLS server. This does not always work, some
			// TLS implementations abort on the first NUL, some need more NULs
			// than we are prepared to send here. 8 seems to be a good number.
			sock.getOutputStream().write(NUL_BYTES);
		}
		return sock;
	}

	private boolean handshake(Target.Validated validated, OptionsCallback callback, ArrayList<String> warnings) throws IOException, MCLException {
		String challenge = reader.getLine();
		reader.advance();
		if (reader.getLineType() != LineType.PROMPT)
			throw new MCLException("Garbage after server challenge: " + reader.getLine());
		String response = challengeResponse(validated, challenge, callback);
		writer.writeLine(response);
		reader.advance();

		// Process the response lines.
		String redirect = null;
		StringBuilder errors = new StringBuilder();
		while (reader.getLineType() != LineType.PROMPT) {
			switch (reader.getLineType()) {
				case REDIRECT:
					if (redirect == null)
						redirect = reader.getLine(1);
					break;
				case ERROR:
					if (errors.length() > 0)
						errors.append("\n");
					errors.append(reader.getLine(7));  // 7 not 1!
					break;
				case INFO:
					warnings.add(reader.getLine(1));
					break;
				default:
					// ignore??!!
					break;
			}
			reader.advance();
		}
		if (errors.length() > 0)
			throw new MCLException(errors.toString());

		if (redirect == null)
			return true;   // we're happy

		// process redirect
		try {
			MonetUrlParser.parse(target, redirect);
		} catch (URISyntaxException | ValidationError e) {
			throw new MCLException("While processing redirect " + redirect + ": " + e.getMessage(), e);
		}
		if (redirect.startsWith("mapi:merovingian://proxy")) {
			// The reader is stuck at LineType.PROMPT but actually the
			// next challenge is already there.
			reader.resetLineType();
			reader.advance();
		} else {
			close();
		}

		return false;   // we need another go
	}

	private String challengeResponse(Target.Validated validated, final String challengeLine, OptionsCallback callback) throws MCLException {
		// The challengeLine looks like this:
		//
		// 45IYyVyRnbgEnK92ad:merovingian:9:RIPEMD160,SHA512,SHA384,SHA256,SHA224,SHA1:LIT:SHA512:
		// WgHIibSyH:mserver:9:RIPEMD160,SHA512,SHA384,SHA256,SHA224,SHA1:LIT:SHA512:sql=6:BINARY=1:
		// 0         1       2 3                                          4   5      6     7

		String[] parts = challengeLine.split(":");
		if (parts.length < 3)
			throw new MCLException("Invalid challenge: expect at least 3 fields");
		String saltPart = parts[0];
		String serverTypePart = parts[1];
		String versionPart = parts[2];
		int version;
		if (versionPart.equals("9"))
			version = 9;
		else
			throw new MCLException("Protocol versions other than 9 are note supported: " + versionPart);
		if (parts.length < 6)
			throw new MCLException("Protocol version " + version + " requires at least 6 fields, found " + parts.length + ": " + challengeLine);
		String serverHashesPart = parts[3];
//		String endianPart = parts[4];
		String passwordHashPart = parts[5];
		String optionsPart = parts.length > 6 ? parts[6] : null;
//		String binaryPart = parts.length > 7 ? parts[7] : null;

		if (parts.length > 9)
			supportsClientInfo = true;

		String userResponse;
		String password = target.getPassword();
		if (serverTypePart.equals("merovingian") && !target.getLanguage().equals("control")) {
			userResponse = "merovingian";
			password = "merovingian";
		} else {
			userResponse = target.getUser();
		}
		String optionsResponse = handleOptions(callback, optionsPart);

		// Response looks like this:
		//
		// LIT:monetdb:{RIPEMD160}f2236256e5a9b20a5ecab4396e36c14f66c3e3c5:sql:demo
		// :FILETRANS:auto_commit=1,reply_size=1000,size_header=0,columnar_protocol=0,time_zone=3600:
		StringBuilder response = new StringBuilder(80);
		response.append("BIG:");
		response.append(userResponse).append(":");
		hashPassword(response, saltPart, password, passwordHashPart, validated.getHash(), serverHashesPart);
		response.append(":");
		response.append(validated.getLanguage()).append(":");
		response.append(validated.getDatabase()).append(":");
		response.append("FILETRANS:");
		response.append(optionsResponse).append(":");

		return response.toString();
	}

	private String hashPassword(StringBuilder responseBuffer, String salt, String password, String passwordAlgo, String configuredHashes, String serverSupportedAlgos) throws MCLException {
		// First determine which hash algorithms we can choose from for the challenge response.
		// This defaults to whatever the server offers but may be restricted by the user.
		Set<String> algoSet = new HashSet<>(Arrays.asList(serverSupportedAlgos.split(",")));
		if (!configuredHashes.isEmpty()) {
			String[] allowedList = configuredHashes.toUpperCase().split("[, ]");
			Set<String> allowedSet = new HashSet<>(Arrays.asList(allowedList));
			algoSet.retainAll(allowedSet);
			if (algoSet.isEmpty()) {
				throw new MCLException("None of the hash algorithms in <" + configuredHashes + "> are supported, server only supports <" + serverSupportedAlgos + ">");
			}
		}

		int maxHashDigits = 512 / 4;

		// We'll collect the result in the responseBuffer.
		// It will start with '{' HASHNAME '}' followed by hexdigits

		// This is where we accumulate what will eventually be hashed into the hexdigits above.
		// It consists of the hexadecimal pre-hash of the password,
		// followed by the salt from the server
		StringBuilder intermediate = new StringBuilder(maxHashDigits + salt.length());

		MessageDigest passwordDigest = pickBestAlgorithm(Collections.singleton(passwordAlgo), null);
		// Here's the password..
		hexhash(intermediate, passwordDigest, password);
		// .. and here's the salt
		intermediate.append(salt);

		responseBuffer.append('{');
		MessageDigest responseDigest = pickBestAlgorithm(algoSet, responseBuffer);
		// the call above has appended the HASHNAME, now add '}'
		responseBuffer.append('}');
		// pickBestAlgorithm has appended HASHNAME, buffer now contains '{' HASHNAME '}'
		hexhash(responseBuffer, responseDigest, intermediate.toString());
		// response buffer now contains '{' HASHNAME '}' HEX_DIGITS_OF_INTERMEDIATE_BUFFER

		return responseBuffer.toString();
	}

	/**
	 * Pick the most preferred digest algorithm and return a MessageDigest instance for that.
	 *
	 * @param algos          the MAPI names of permitted algorithms
	 * @param appendMapiName if not null, append MAPI name of chose algorithm to this buffer
	 * @return instance of the chosen digester
	 * @throws MCLException if none of the options is supported
	 */
	private MessageDigest pickBestAlgorithm(Set<String> algos, StringBuilder appendMapiName) throws MCLException {
		for (String[] choice : KNOWN_ALGORITHMS) {
			String mapiName = choice[0];
			String algoName = choice[1];
			MessageDigest digest;
			if (!algos.contains(mapiName))
				continue;
			try {
				digest = MessageDigest.getInstance(algoName);
			} catch (NoSuchAlgorithmException e) {
				continue;
			}
			// we found a match
			if (appendMapiName != null) {
				appendMapiName.append(mapiName);
			}
			return digest;
		}
		String algoNames = String.join(",", algos);
		throw new MCLException("No supported hash algorithm: " + algoNames);
	}

	/**
	 * Hash the text into the MessageDigest and append the hexadecimal form of the
	 * resulting digest to buffer.
	 *
	 * @param buffer where the hex digits are appended
	 * @param digest where the hex digits come from after the text has been digested
	 * @param text   text to digest
	 */
	private void hexhash(StringBuilder buffer, MessageDigest digest, String text) {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		digest.update(bytes);
		byte[] output = digest.digest();
		for (byte b : output) {
			int hi = (b & 0xF0) >> 4;
			int lo = b & 0x0F;
			buffer.append(HEXDIGITS[hi]);
			buffer.append(HEXDIGITS[lo]);
		}
	}

	private String handleOptions(OptionsCallback callback, String optionsPart) throws MCLException {
		if (callback == null || optionsPart == null || optionsPart.isEmpty())
			return "";

		StringBuilder buffer = new StringBuilder();
		callback.setBuffer(buffer);
		for (String optlevel : optionsPart.split(",")) {
			int eqindex = optlevel.indexOf('=');
			if (eqindex < 0)
				throw new MCLException("Invalid options part in server challenge: " + optionsPart);
			String lang = optlevel.substring(0, eqindex);
			int level;
			try {
				level = Integer.parseInt(optlevel.substring(eqindex + 1));
			} catch (NumberFormatException e) {
				throw new MCLException("Invalid option level in server challenge: " + optlevel);
			}
			callback.addOptions(lang, level);
		}

		return buffer.toString();
	}

	/**
	 * Returns an InputStream that reads from this open connection on
	 * the MapiSocket.
	 *
	 * @return an input stream that reads from this open connection
	 */
	public InputStream getInputStream() {
		return fromMonet;
	}

	/**
	 * Returns an output stream for this MapiSocket.
	 *
	 * @return an output stream for writing bytes to this MapiSocket
	 */
	public OutputStream getOutputStream() {
		return toMonet;
	}

	/**
	 * Returns a Reader for this MapiSocket.  The Reader is a
	 * BufferedMCLReader which does protocol interpretation of the
	 * BlockInputStream produced by this MapiSocket.
	 *
	 * @return a BufferedMCLReader connected to this MapiSocket
	 */
	public BufferedMCLReader getReader() {
		return reader;
	}

	/**
	 * Returns a Writer for this MapiSocket.  The Writer is a
	 * BufferedMCLWriter which produces protocol compatible data blocks
	 * that the BlockOutputStream can properly translate into blocks.
	 *
	 * @return a BufferedMCLWriter connected to this MapiSocket
	 */
	public BufferedMCLWriter getWriter() {
		return writer;
	}

	/**
	 * Returns the mapi protocol version used by this socket.  The
	 * protocol version depends on the server being used.  Users of the
	 * MapiSocket should check this version to act appropriately.
	 *
	 * @return the mapi protocol version
	 */
	public int getProtocolVersion() {
		return version;
	}

	/**
	 * Enables logging to a file what is read and written from and to
	 * the server.  Logging can be enabled at any time.  However, it is
	 * encouraged to start debugging before actually connecting the
	 * socket.
	 *
	 * @param filename the name of the file to write to
	 * @throws IOException if the file could not be opened for writing
	 */
	public void debug(final String filename) throws IOException {
		debug(new FileWriter(filename));
	}

	/**
	 * Enables logging to a stream what is read and written from and to
	 * the server.  Logging can be enabled at any time.  However, it is
	 * encouraged to start debugging before actually connecting the
	 * socket.
	 *
	 * @param out to write the log to a print stream
	 * @throws IOException if the file could not be opened for writing
	 */
// disabled as it is not used by JDBC driver code
//	public void debug(PrintStream out) throws IOException {
//		debug(new PrintWriter(out));
//	}

	/**
	 * Enables logging to a stream what is read and written from and to
	 * the server.  Logging can be enabled at any time.  However, it is
	 * encouraged to start debugging before actually connecting the
	 * socket.
	 *
	 * @param out to write the log to
	 */
	public void debug(final Writer out) {
		log = out;
		setDebug(true);
	}

	/**
	 * Get the log Writer.
	 *
	 * @return the log writer
	 */
	public Writer getLogWriter() {
		return log;
	}

	/**
	 * Writes a logline tagged with a timestamp using the given type and message
	 * and optionally flushes afterwards.
	 *
	 * Used for debugging purposes only and represents a message data that is
	 * connected to reading (RD or RX) or writing (TD or TX) to the socket.
	 * R=Receive, T=Transmit, D=Data, X=??
	 *
	 * @param type  message type: either RD, RX, TD or TX
	 * @param message  the message to log
	 * @param flush  whether we need to flush buffered data to the logfile.
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	private final void log(final String type, final String message, final boolean flush) throws IOException {
		log.write(type + System.currentTimeMillis() + ": " + message + "\n");
		if (flush)
			log.flush();
	}

	/**
	 * For internal use
	 *
	 * @param b to enable/disable insert 'fake' newline and prompt
	 * @return previous setting
	 */
	public boolean setInsertFakePrompts(boolean b) {
		return fromMonet.setInsertFakePrompts(b);
	}

	public boolean isDebug() {
		return target.isDebug();
	}

	public boolean canClientInfo() {
		return supportsClientInfo;
	}


	/**
	 * Inner class that is used to write data on a normal stream as a
	 * blocked stream.  A call to the flush() method will write a
	 * "final" block to the underlying stream.  Non-final blocks are
	 * written as soon as one or more bytes would not fit in the
	 * current block any more.  This allows to write to a block to it's
	 * full size, and then flush it explicitly to have a final block
	 * being written to the stream.
	 */
	final class BlockOutputStream extends FilterOutputStream {
		private int writePos = 0;
		private int blocksize = 0;
		private final byte[] block = new byte[BLOCK];

		/**
		 * Constructs this BlockOutputStream, backed by the given
		 * OutputStream.  A BufferedOutputStream is internally used.
		 * @param out an OutputStream
		 */
		public BlockOutputStream(final OutputStream out) {
			// always use a buffered stream, even though we know how
			// much bytes to write/read, since this is just faster for
			// some reason
			super(new BufferedOutputStream(out));
		}

		@Override
		public void flush() throws IOException {
			// write the block (as final) then flush.
			writeBlock(true);
			out.flush();

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (isDebug()) {
				log.flush();
			}
		}

		/**
		 * writeBlock puts the data in the block on the stream.  The
		 * boolean last controls whether the block is sent with an
		 * indicator to note it is the last block of a sequence or not.
		 *
		 * @param last whether this is the last block
		 * @throws IOException if writing to the stream failed
		 */
		public void writeBlock(final boolean last) throws IOException {
			if (last) {
				// always fits, because of BLOCK's size
				blocksize = (short)writePos;
				// this is the last block, so encode least
				// significant bit in the first byte (little-endian)
				blklen[0] = (byte)(blocksize << 1 & 0xFF | 1);
				blklen[1] = (byte)(blocksize >> 7);
			} else {
				// always fits, because of BLOCK's size
				blocksize = (short)BLOCK;
				// another block will follow, encode least
				// significant bit in the first byte (little-endian)
				blklen[0] = (byte)(blocksize << 1 & 0xFF);
				blklen[1] = (byte)(blocksize >> 7);
			}

			out.write(blklen);
			// write the actual block
			out.write(block, 0, writePos);

			if (isDebug()) {
				if (last) {
					log("TD ", "write final block: " + writePos + " bytes", false);
				} else {
					log("TD ", "write block: " + writePos + " bytes", false);
				}
				log("TX ", new String(block, 0, writePos, StandardCharsets.UTF_8), true);
			}

			writePos = 0;
		}

		@Override
		public void write(final int b) throws IOException {
			if (writePos == BLOCK) {
				writeBlock(false);
			}
			block[writePos++] = (byte)b;
		}

		@Override
		public void write(final byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(final byte[] b, int off, int len) throws IOException {
			while (len > 0) {
				int t = BLOCK - writePos;
				if (len > t) {
					System.arraycopy(b, off, block, writePos, t);
					off += t;
					len -= t;
					writePos += t;
					writeBlock(false);
				} else {
					System.arraycopy(b, off, block, writePos, len);
					writePos += len;
					break;
				}
			}
		}

		@Override
		public void close() throws IOException {
			// we don't want the flush() method to be called (default of
			// the FilterOutputStream), so we close manually here
			out.close();
		}
	}


	/**
	 * Inner class that is used to make the data on the blocked stream
	 * available as a normal stream.
	 */
	final class BlockInputStream extends FilterInputStream {
		private int readPos = 0;
		private int blockLen = 0;
		private boolean wasEndBlock = false;
		private final byte[] block = new byte[BLOCK + 3]; // \n.\n
		private boolean insertFakePrompts = true;

		/**
		 * Constructs this BlockInputStream, backed by the given
		 * InputStream.  A BufferedInputStream is internally used.
		 * @param in an InputStream
		 */
		public BlockInputStream(final InputStream in) {
			// always use a buffered stream, even though we know how
			// much bytes to write/read, since this is just faster for
			// some reason
			super(new BufferedInputStream(in));
		}

		public boolean setInsertFakePrompts(boolean doFake) {
			boolean old = insertFakePrompts;
			insertFakePrompts = doFake;
			return old;
		}

		@Override
		public int available() {
			return blockLen - readPos;
		}

		@Override
		public boolean markSupported() {
			return false;
		}

		@Override
		public void mark(final int readlimit) {
			throw new AssertionError("Not implemented!");
		}

		@Override
		public void reset() {
			throw new AssertionError("Not implemented!");
		}

		/**
		 * Small wrapper to get a blocking variant of the read() method
		 * on the BufferedInputStream.  We want to benefit from the
		 * Buffered pre-fetching, but not dealing with half blocks.
		 * Changing this class to be able to use the partially received
		 * data will greatly complicate matters, while a performance
		 * improvement is debatable given the relatively small size of
		 * our blocks.  Maybe it does speed up on slower links, then
		 * consider this method a quick bug fix/workaround.
		 *
		 * @param b a byte array to store read bytes
		 * @param len number of bytes to read
		 * @return false if reading the block failed due to EOF
		 * @throws IOException if an IO error occurs while reading
		 */
		private boolean _read(final byte[] b, int len) throws IOException {
			int s;
			int off = 0;
			while (len > 0) {
				s = in.read(b, off, len);
				if (s == -1) {
					// if we have read something before, we should have been
					// able to read the whole, so make this fatal
					if (off > 0) {
						if (isDebug()) {
							log("RD ", "the following incomplete block was received:", false);
							log("RX ", new String(b, 0, off, StandardCharsets.UTF_8), true);
						}
						throw new IOException("Read from " +
								con.getInetAddress().getHostName() + ":" +
								con.getPort() + ": Incomplete block read from stream");
					}
					if (isDebug())
						log("RD ", "server closed the connection (EOF)", true);
					return false;
				}
				len -= s;
				off += s;
			}

			return true;
		}

		/**
		 * Reads the next block on the stream into the internal buffer,
		 * or writes the prompt in the buffer.
		 *
		 * The blocked stream protocol consists of first a two byte
		 * integer indicating the length of the block, then the
		 * block, followed by another length + block.  The end of
		 * such sequence is put in the last bit of the length, and
		 * hence this length should be shifted to the right to
		 * obtain the real length value first.  We simply fetch
		 * blocks here as soon as they are needed for the stream's
		 * read methods.
		 *
		 * The user-flush, which is an implicit effect of the end of
		 * a block sequence, is communicated beyond the stream by
		 * inserting a prompt sequence on the stream after the last
		 * block.  This method makes sure that a final block ends with a
		 * newline, if it doesn't already, in order to facilitate a
		 * Reader that is possibly chained to this InputStream.
		 *
		 * If the stream is not positioned correctly, hell will break
		 * loose.
		 *
		 * @return blockLen
		 * @throws IOException if an IO error occurs while reading
		 */
		private int readBlock() throws IOException {
			// read next two bytes (short)
			if (!_read(blklen, 2))
				return(-1);

			// Get the short-value and store its value in blockLen.
			blockLen = (short)(
					(blklen[0] & 0xFF) >> 1 |
					(blklen[1] & 0xFF) << 7
					);
			wasEndBlock = (blklen[0] & 0x1) == 1;

			readPos = 0;

			if (isDebug()) {
				if (wasEndBlock) {
					log("RD ", "read final block: " + blockLen + " bytes", false);
				} else {
					log("RD ", "read new block: " + blockLen + " bytes", false);
				}
			}

			// sanity check to avoid bad servers make us do an ugly
			// stack trace
			if (blockLen > block.length)
				throw new IOException("Server sent a block larger than BLOCKsize: " +
						blockLen + " > " + block.length);
			if (!_read(block, blockLen))
				return -1;

			if (isDebug())
				log("RX ", new String(block, 0, blockLen, StandardCharsets.UTF_8), true);

			// if this is the last block, make it end with a newline and prompt
			if (wasEndBlock) {
				// insert 'fake' newline and prompt
				if (insertFakePrompts) {
					if (blockLen > 0 && block[blockLen - 1] != '\n') {
						// to terminate the block in a Reader
						block[blockLen++] = '\n';
					}
					for (byte b : LineType.PROMPT.bytes()) {
						block[blockLen++] = b;
					}
					block[blockLen++] = '\n';
					if (isDebug()) {
						log("RD ", "inserting prompt", true);
					}
				}
			}

			return blockLen;
		}

		@Override
		public int read() throws IOException {
			if (available() == 0) {
				if (readBlock() == -1)
					return -1;
			}

			if (isDebug())
				log("RX ", new String(block, readPos, 1, StandardCharsets.UTF_8), true);

			return block[readPos++] & 0xFF;
		}

		@Override
		public int read(final byte[] b) throws IOException {
			return read(b, 0, b.length);
		}

		@Override
		public int read(final byte[] b, int off, int len) throws IOException {
			int t;
			int size = 0;
			while (size < len) {
				t = available();
				if (t == 0) {
					if (size != 0)
						break;
					if (readBlock() == -1) {
						if (size == 0)
							size = -1;
						break;
					}
					t = available();
				}
				if (len > t) {
					System.arraycopy(block, readPos, b, off, t);
					off += t;
					len -= t;
					readPos += t;
					size += t;
				} else {
					System.arraycopy(block, readPos, b, off, len);
					readPos += len;
					size += len;
					break;
				}
			}
			return size;
		}

		@Override
		public long skip(final long n) throws IOException {
			long skip = n;
			while (skip > 0) {
				int t = available();
				if (skip > t) {
					skip -= t;
					readPos += t;
					readBlock();
				} else {
					readPos += (int)skip;
					break;
				}
			}
			return n;
		}

		/**
		 * For internal use
		 * @return new Raw object
		 */
		Raw getRaw() {
			return new Raw();
		}

		/** An alternative I/O interface that exposes the block based nature of the MAPI protocol */
		final class Raw {
			byte[] getBytes() {
				return block;
			}

			int getLength() {
				return blockLen;
			}

			int getPosition() {
				return readPos;
			}

			int consume(int delta) {
				int pos = readPos;
				readPos += delta;
				return pos;
			}

			int readBlock() throws IOException {
				boolean wasFaking = setInsertFakePrompts(false);
				try {
					return BlockInputStream.this.readBlock();
				} finally {
					setInsertFakePrompts(wasFaking);
				}
			}

			boolean wasEndBlock() {
				return wasEndBlock;
			}
		}
	}

	/**
	 * Closes the streams and socket connected to the server if possible.
	 * If an error occurs at closing a resource, it is ignored so as many
	 * resources as possible are closed.
	 */
	public synchronized void close() {
		if (writer != null) {
			try {
				writer.close();
				writer = null;
			} catch (IOException e) { /* ignore it */ }
		}
		if (reader != null) {
			try {
				reader.close();
				reader = null;
			} catch (IOException e) { /* ignore it */ }
		}
		if (toMonet != null) {
			try {
				toMonet.close();
				toMonet = null;
			} catch (IOException e) { /* ignore it */ }
		}
		if (fromMonet != null) {
			try {
				fromMonet.close();
				fromMonet = null;
			} catch (IOException e) { /* ignore it */ }
		}
		if (con != null) {
			try {
				con.close();	// close the socket
				con = null;
			} catch (IOException e) { /* ignore it */ }
		}
		if (isDebug() && log != null && log instanceof FileWriter) {
			try {
				log.close();
				log = null;
			} catch (IOException e) { /* ignore it */ }
		}
	}

	/**
	 * Return an UploadStream for use with for example COPY FROM filename ON CLIENT.
	 *
	 * Building block for {@link org.monetdb.jdbc.MonetConnection.UploadHandler}.
	 *
	 * @param chunkSize chunk size for the upload stream
	 * @return UploadStream new upload stream with the given chunk size
	 */
	public UploadStream uploadStream(int chunkSize) {
		return new UploadStream(chunkSize);
	}

	/**
	 * Return an UploadStream for use with for example COPY FROM filename ON CLIENT.
	 *
	 * Building block for {@link org.monetdb.jdbc.MonetConnection.UploadHandler}.
	 *
	 * @return UploadStream new upload stream
	 */
	public UploadStream uploadStream() {
		return new UploadStream();
	}

	/**
	 * Return a DownloadStream for use with for example COPY INTO filename ON CLIENT
	 *
	 * Building block for {@link org.monetdb.jdbc.MonetConnection.DownloadHandler}.
	 *
	 * @param prependCr convert \n to \r\n
	 * @return DownloadStream new download stream
	 */
	public DownloadStream downloadStream(boolean prependCr) {
		return new DownloadStream(fromMonet.getRaw(), toMonet, prependCr);
	}


	/**
	 * Stream of data sent to the server
	 *
	 * Building block for {@link org.monetdb.jdbc.MonetConnection.UploadHandler}.
	 *
	 * An UploadStream has a chunk size. Every chunk size bytes, the server gets
	 * the opportunity to abort the upload.
	 */
	public class UploadStream extends FilterOutputStream {
		public final static int DEFAULT_CHUNK_SIZE = 1024 * 1024;
		private final int chunkSize;
		private boolean closed = false;
		private boolean serverCancelled = false;
		private int chunkLeft;
		private byte[] promptBuffer;
		private Runnable cancellationCallback = null;

		/**
		 * Create an UploadStream with the given chunk size
		 * @param chunkSize chunk size for the upload stream
		 */
		UploadStream(final int chunkSize) {
			super(toMonet);
			if (chunkSize <= 0) {
				throw new IllegalArgumentException("chunk size must be positive");
			}
			this.chunkSize = chunkSize;
			assert LineType.MORE.bytes().length == LineType.FILETRANSFER.bytes().length;
			int promptLen = LineType.MORE.bytes().length;
			promptBuffer = new byte[promptLen + 1];
			chunkLeft = this.chunkSize;
		}

		/** Create an UploadStream with the default chunk size */
		UploadStream() {
			this(DEFAULT_CHUNK_SIZE);
		}

		/** Set a callback to be invoked if the server cancels the upload
		 *
		 * @param cancellationCallback callback to call
		 */
		public void setCancellationCallback(final Runnable cancellationCallback) {
			this.cancellationCallback = cancellationCallback;
		}

		@Override
		public void write(final int b) throws IOException {
			if (serverCancelled) {
				// We have already thrown an exception and apparently that has been ignored.
				// Probably because they're calling print methods instead of write.
				// Throw another one, maybe they'll catch this one.
				throw new IOException("Server aborted the upload");
			}
			handleChunking();
			out.write(b);
			wrote(1);
		}

		@Override
		public void write(final byte[] b) throws IOException {
			this.write(b, 0, b.length);
		}

		@Override
		public void write(final byte[] b, int off, int len) throws IOException {
			if (serverCancelled) {
				// We have already thrown an exception and apparently that has been ignored.
				// Probably because they're calling print methods instead of write.
				// Throw another one, maybe they'll catch this one.
				throw new IOException("Server aborted the upload");
			}
			while (len > 0) {
				handleChunking();
				int toWrite = Integer.min(len, chunkLeft);
				out.write(b, off, toWrite);
				off += toWrite;
				len -= toWrite;
				wrote(toWrite);
			}
		}

		@Override
		public void flush() throws IOException {
			// suppress flushes
		}

		@Override
		public void close() throws IOException {
			if (closed) {
				return;
			}
			closed = true;

			if (serverCancelled)
				closeAfterServerCancelled();
			else
				closeAfterSuccesfulUpload();
		}

		private void closeAfterSuccesfulUpload() throws IOException {
			if (chunkLeft != chunkSize) {
				// flush pending data
				flushAndReadPrompt();
			}
			// send empty block
			out.flush();
			final LineType acknowledgement = readPrompt();
			if (acknowledgement != LineType.FILETRANSFER) {
				throw new IOException("Expected server to acknowledge end of file");
			}
		}

		private void closeAfterServerCancelled() {
			// nothing to do here, we have already read the error prompt.
		}

		private void wrote(final int i) {
			chunkLeft -= i;
		}

		private void handleChunking() throws IOException {
			if (chunkLeft > 0) {
				return;
			}
			flushAndReadPrompt();
		}

		private void flushAndReadPrompt() throws IOException {
			out.flush();
			chunkLeft = chunkSize;
			final LineType lineType = readPrompt();
			switch (lineType) {
				case MORE:
					return;
				case FILETRANSFER:
					// Note, if the caller is calling print methods instead of write, the IO exception gets hidden.
					// This is unfortunate but there's nothing we can do about it.
					serverCancelled = true;
					if (cancellationCallback != null) {
						cancellationCallback.run();
					}
					throw new IOException("Server aborted the upload");
				default:
					throw new IOException("Expected MORE/DONE from server, got " + lineType);
			}
		}

		private LineType readPrompt() throws IOException {
			final int nread = fromMonet.read(promptBuffer);
			if (nread != promptBuffer.length || promptBuffer[promptBuffer.length - 1] != '\n') {
				throw new IOException("server return incomplete prompt");
			}
			return LineType.classify(promptBuffer);
		}
	}


	/**
	 * Stream of data received from the server
	 *
	 * Building block for {@link org.monetdb.jdbc.MonetConnection.DownloadHandler}.
	 */
	public static class DownloadStream extends InputStream {
		private final BlockInputStream.Raw rawIn;
		private final OutputStream out;
		private final boolean prependCr;
		private boolean endBlockSeen = false;
		private boolean closed = false;
		private boolean newlinePending = false; // used for crlf conversion

		DownloadStream(BlockInputStream.Raw rawIn, OutputStream out, boolean prependCr) {
			this.rawIn = rawIn;
			this.out = out;
			this.prependCr = prependCr;
		}

		void nextBlock() throws IOException {
			if (endBlockSeen || closed)
				return;
			final int ret = rawIn.readBlock();
			if (ret < 0 || rawIn.wasEndBlock()) {
				endBlockSeen = true;
			}
		}

		@Override
		public void close() throws IOException {
			if (closed)
				return;
			closed = true;
			while (!endBlockSeen) {
				nextBlock();
			}
			// Send acknowledgement to server
			out.write('\n');
			out.flush();
			// Do whatever super has to do
			super.close();
		}

		@Override
		public int read() throws IOException {
			final byte[] buf = { 0 };
			final int nread = read(buf, 0, 1);
			if (nread == 1)
				return buf[0] & 0xFF;
			else
				return -1;
		}

		@Override
		public int read(final byte[] dest, int off, int len) throws IOException {
			final int origOff = off;
			int end = off + len;

			while (off < end) {
				// minimum of what's requested and what we have in stock
				int chunk = Integer.min(end - off, rawIn.getLength() - rawIn.getPosition());
				assert chunk >= 0;
				if (chunk == 0) {
					// make progress by fetching more data
					if (endBlockSeen)
						break;
					nextBlock();
					continue;
				}
				// make progress copying some bytes
				if (!prependCr) {
					// no conversion needed, use arraycopy
					System.arraycopy(rawIn.getBytes(), rawIn.getPosition(), dest, off, chunk);
					off += chunk;
					rawIn.consume(chunk);
				} else {
					int chunkEnd = off + chunk;
					if (newlinePending && off < chunkEnd) {
						// we were in the middle of a line ending conversion
						dest[off++] = '\n';
						newlinePending = false;
					}
					while (off < chunkEnd) {
						byte b = rawIn.getBytes()[rawIn.consume(1)];
						if (b != '\n') {
							dest[off++] = b;
						} else if (chunkEnd - off >= 2) {
							dest[off++] = '\r';
							dest[off++] = '\n';
						} else {
							dest[off++] = '\r';
							newlinePending = true;
							break;
						}
					}
				}
			}

			if (off < end && newlinePending) {
				dest[off++] = '\n';
				newlinePending = false;
			}

			if (off == origOff && endBlockSeen)
				return -1;
			else
				return off - origOff;
		}
	}

	/**
	 * Callback used during the initial MAPI handshake.
	 *
	 * Newer MonetDB versions allow setting some options during the handshake.
	 * The options are language-specific and each has a 'level'. The server
	 * advertises up to which level options are supported for a given language.
	 * For each language/option combination, {@link #addOptions} will be invoked
	 * during the handshake. This method should call {@link #contribute} for each
	 * option it wants to set.
	 *
	 * At the time of writing, only the 'sql' language supports options,
	 * they are listed in enum mapi_handshake_options_levels in mapi.h.
	 */
	public static abstract class OptionsCallback {
		private StringBuilder buffer;

		/**
		 * Callback called for each language/level combination supported by the
		 * server. May call {@link #contribute} for options with a level STRICTLY
		 * LOWER than the level passed as a parameter.
		 * @param lang language advertised by the server
		 * @param level one higher than the maximum supported option
		 */
		public abstract void addOptions(String lang, int level);

		/**
		 * Pass option=value during the handshake
		 * @param field name
		 * @param value int value
		 */
		protected void contribute(String field, int value) {
			if (buffer.length() > 0)
				buffer.append(',');
			buffer.append(field);
			buffer.append('=');
			buffer.append(value);
		}

		/**
		 * Set the buffer
		 * @param buf a non null StringBuilder object
		 */
		void setBuffer(StringBuilder buf) {
			buffer = buf;
		}
	}
}
