/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.client;

import org.monetdb.jdbc.MonetDriver;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.util.CmdLineOpts;
import org.monetdb.util.Exporter;
import org.monetdb.util.FileTransferHandler;
import org.monetdb.util.MDBvalidator;
import org.monetdb.util.OptionsException;
import org.monetdb.util.SQLExporter;
import org.monetdb.util.XMLExporter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;	// this import is required as it will trigger loading the org.monetdb.jdbc.MonetDriver class
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This program acts like an extended client program for MonetDB. Its
 * look and feel is very much like PostgreSQL's interactive terminal
 * program.  Although it looks like this client is designed for MonetDB,
 * it demonstrates the power of the JDBC interface since it built on top
 * of JDBC only.
 *
 * @author Fabian Groffen
 * @author Martin van Dinther
 * @version 1.6
 */

public class JdbcClient {	/* cannot (yet) be final as nl.cwi.monetdb.client.JdbcClient extends this class */

	private static Connection con;
	private static DatabaseMetaData dbmd;
	private static Statement stmt;
	private static BufferedReader in;
	private static PrintWriter out;
	private static Exporter exporter;

	/**
	 * JdbcClient is a command line query tool for MonetDB, similar to mclient.
	 * It uses the JDBC API and the MonetDB JDBC driver to communicate with a
	 * MonetDB server. The MonetDB JDBC driver is included in the jdbcclient.jre8.jar
	 * for ease of use, so only 1 jar file is needed to use it.
	 *
	 * <pre>Usage java -jar jdbcclient.jre8.jar
	 *		[-h host[:port]] [-p port] [-f file] [-u user]
	 *		[-l language] [-d database] [-e] [-D [table]]
	 *		[--csvdir /path/to/csvfiles] [-X&lt;opt&gt;]
	 *		| [--help] | [--version]
	 * or using long option equivalents --host --port --file --user --language
	 * --dump --echo --database.
	 * Arguments may be written directly after the option like -p50000.
	 *
	 * If no host and port are given, localhost and 50000 are assumed.
	 * An .monetdb file may exist in the user's home directory.  This file can contain
	 * preferences to use each time JdbcClient is started.  Options given on the
	 * command line override the preferences file.  The .monetdb file syntax is
	 * &lt;option&gt;=&lt;value&gt; where option is one of the options host, port, file, mode
	 * debug, or password.  Note that the last one is perilous and therefore not
	 * available as command line option.
	 * If no input file is given using the -f flag, an interactive session is
	 * started on the terminal.
	 *
	 * OPTIONS
	 * -h --host     The hostname of the host that runs the MonetDB database.  A port
	 *               number can be supplied by use of a colon, i.e. -h somehost:12345.
	 * -p --port     The port number to connect to.
	 * -f --file     A file name to use either for reading or writing.  The file will
	 *               be used for writing when dump mode is used (-D --dump).  In read
	 *               mode, the file can also be an URL pointing to a plain text file
	 *               that is optionally gzip compressed.
	 * -u --user     The username to use when connecting to the database.
	 * -d --database Try to connect to the given database (only makes sense if
	 *               connecting to monetdbd).
	 * -l --language Use the given language, defaults to 'sql'.
	 * --help        This help screen.
	 * --version     Display driver version and exit.
	 * -e --echo     Also outputs the contents of the input file, if any.
	 * -q --quiet    Suppress printing the welcome header.
	 * -D --dump     Dumps the given table(s), or the complete database if none given.
	 * --csvdir      The directory path where csv data files wil be read from or
	 ^               written to when COPY ... ON CLIENT commands are executed.
	 * -Xoutput      The output mode when dumping.  Default is sql, xml may be used for
	 *               an experimental XML output.
	 * -Xhash        Use the given hash algorithm during challenge response.
	 *               Supported algorithm names: SHA512, SHA384, SHA256 and SHA1.
	 * -Xdebug       Writes a transmission log to disk for debugging purposes.  If a
	 *               file name is given, it is used, otherwise a file called
	 *               monet&lt;timestamp&gt;.log is created.  A given file never be
	 *               overwritten; instead a unique variation of the file is used.
	 * -Xbatching    Indicates that a batch should be used instead of direct
	 *               communication with the server for each statement.  If a number is
	 *               given, it is used as batch size.  i.e. 8000 would execute the
	 *               contents on the batch after each 8000 statements read.  Batching
	 *               can greatly speedup the process of restoring a database dump.</pre>
	 *
	 * @param args optional list of startup arguments
	 * @throws Exception if uncaught exception is thrown
	 */
	public final static void main(String[] args) throws Exception {
		final CmdLineOpts copts = new CmdLineOpts();

		// arguments which take exactly one argument
		copts.addOption("h", "host", CmdLineOpts.CAR_ONE, "localhost",
				"The hostname of the host that runs the MonetDB database.  " +
				"A port number can be supplied by use of a colon, i.e. " +
				"-h somehost:12345.");
		copts.addOption("p", "port", CmdLineOpts.CAR_ONE, "50000",
				"The port number to connect to.");
		// todo make it CAR_ONE_MANY
		copts.addOption("f", "file", CmdLineOpts.CAR_ONE, null,
				"A file name to use either for reading or writing.  The " +
				"file will be used for writing when dump mode is used " +
				"(-D --dump).  In read mode, the file can also be an URL " +
				"pointing to a plain text file that is optionally gzip " +
				"compressed.");
		copts.addOption("u", "user", CmdLineOpts.CAR_ONE, System.getProperty("user.name"),
				"The username to use when connecting to the database.");
		// this one is only here for the .monetdb file parsing, it is
		// removed before the command line arguments are parsed
		copts.addOption(null, "password", CmdLineOpts.CAR_ONE, null, null);
		copts.addOption("d", "database", CmdLineOpts.CAR_ONE, "",
				"Try to connect to the given database (only makes sense " +
				"if connecting to monetdbd).");
		copts.addOption("l", "language", CmdLineOpts.CAR_ONE, "sql",
				"Use the given language, defaults to 'sql'.");
		copts.addOption(null, "csvdir", CmdLineOpts.CAR_ONE, null,
				"The directory path where csv data files are read or " +
				"written when using ON CLIENT clause of COPY command.");

		// arguments which have no argument(s)
		copts.addOption(null, "help", CmdLineOpts.CAR_ZERO, null,
				"This help screen.");
		copts.addOption(null, "version", CmdLineOpts.CAR_ZERO, null,
				"Display driver version and exit.");
		copts.addOption("e", "echo", CmdLineOpts.CAR_ZERO, null,
				"Also outputs the contents of the input file, if any.");
		copts.addOption("q", "quiet", CmdLineOpts.CAR_ZERO, null,
				"Suppress printing the welcome header.");

		// arguments which have zero to many arguments
		copts.addOption("D", "dump", CmdLineOpts.CAR_ZERO_MANY, null,
				"Dumps the given table(s), or the complete database if " +
				"none given.");

		// extended options
		copts.addOption(null, "Xoutput", CmdLineOpts.CAR_ONE, null,
				"The output mode when dumping.  Default is sql, xml may " +
				"be used for an experimental XML output.");
		copts.addOption(null, "Xhash", CmdLineOpts.CAR_ONE, null,
				"Use the given hash algorithm during challenge response. " +
				"Supported algorithm names: SHA512, SHA384, SHA256 and SHA1.");
		// arguments which can have zero or one argument(s)
		copts.addOption(null, "Xdebug", CmdLineOpts.CAR_ZERO_ONE, null,
				"Writes a transmission log to disk for debugging purposes. " +
				"If a file name is given, it is used, otherwise a file " +
				"called monet<timestamp>.log is created.  A given file " +
				"never be overwritten; instead a unique variation of the " +
				"file is used.");
		copts.addOption(null, "Xbatching", CmdLineOpts.CAR_ZERO_ONE, null,
				"Indicates that a batch should be used instead of direct " +
				"communication with the server for each statement.  If a " +
				"number is given, it is used as batch size.  i.e. 8000 " +
				"would execute the contents on the batch after each 8000 " +
				"statements read.  Batching can greatly speedup the " +
				"process of restoring a database dump.");

		// we store user and password in separate variables in order to
		// be able to properly act on them like forgetting the password
		// from the user's file if the user supplies a username on the
		// command line arguments
		String pass = null;
		String user = null;

		// look for a file called .monetdb in the current dir or in the
		// user's homedir and read its preferences
		File pref = new File(".monetdb");
		if (!pref.exists())
			pref = new File(System.getProperty("user.home"), ".monetdb");
		if (pref.exists()) {
			try {
				copts.processFile(pref);
			} catch (OptionsException e) {
				System.err.println("Error in " + pref.getAbsolutePath() + ": " + e.getMessage());
				System.exit(1);
			}
			user = copts.getOption("user").getArgument();
			pass = copts.getOption("password").getArgument();
		}

		// process the command line arguments, remove password option
		// first, and save the user we had at this point
		copts.removeOption("password");
		try {
			copts.processArgs(args);
		} catch (OptionsException e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(1);
		}
		// we can actually compare pointers (objects) here
		if (user != copts.getOption("user").getArgument())
			pass = null;

		if (copts.getOption("help").isPresent()) {
			System.out.print(
				"Usage java -jar jdbcclient.jre8.jar\n" +
				"\t\t[-h host[:port]] [-p port] [-f file] [-u user]\n" +
				"\t\t[-l language] [-d database] [-e] [-D [table]]\n" +
				"\t\t[--csvdir /path/to/csvfiles]] [-X<opt>]\n" +
				"\t\t| [--help] | [--version]\n" +
				"or using long option equivalents --host --port --file --user --language\n" +
				"--dump --echo --database.\n" +
				"Arguments may be written directly after the option like -p50000.\n" +
				"\n" +
				"If no host and port are given, localhost and 50000 are assumed.\n" +
				"An .monetdb file may exist in the user's home directory.  This file can contain\n" +
				"preferences to use each time JdbcClient is started.  Options given on the\n" +
				"command line override the preferences file.  The .monetdb file syntax is\n" +
				"<option>=<value> where option is one of the options host, port, file, mode\n" +
				"debug, or password.  Note that the last one is perilous and therefore not\n" +
				"available as command line option.\n" +
				"If no input file is given using the -f flag, an interactive session is\n" +
				"started on the terminal.\n" +
				"\n" +
				"OPTIONS\n" +
				copts.produceHelpMessage()
				);
			System.exit(0);
		}

		if (copts.getOption("version").isPresent()) {
			// We cannot use the DatabaseMetaData here, because we
			// cannot get a Connection.  So instead, we just get the
			// values we want out of the Driver directly.
			System.out.println("JDBC Driver: v" + MonetDriver.getDriverVersion());
			System.exit(0);
		}

		// whether the semi-colon at the end of a String terminates the
		// query or not (default = yes => SQL)
		final boolean scolonterm = true;
		final boolean xmlMode = "xml".equals(copts.getOption("Xoutput").getArgument());

		// we need the password from the user, fetch it with a pseudo
		// password protector
		if (pass == null) {
			final Console syscon = System.console();
			char[] tmp = null;
			if (syscon != null) {
				tmp = syscon.readPassword("password: ");
			}
			if (tmp == null) {
				System.err.println("Invalid password!");
				System.exit(1);
			}
			pass = String.valueOf(tmp);
		}

		user = copts.getOption("user").getArgument();

		// build the hostname
		String host = copts.getOption("host").getArgument();
		if (host.indexOf(":") == -1) {
			host = host + ":" + copts.getOption("port").getArgument();
		}

		// build the extra arguments of the JDBC connect string
		String attr = "?";
		CmdLineOpts.OptionContainer oc = copts.getOption("language");
		final String lang = oc.getArgument();
		if (oc.isPresent())
			attr += "language=" + lang + "&";

/* Xquery is no longer functional or supported
		// set some behaviour based on the language XQuery
		if (lang.equals("xquery")) {
			scolonterm = false;	// no ; to end a statement
			if (!copts.getOption("Xoutput").isPresent())
				xmlMode = true; // the user will like xml results, most probably
		}
*/
		oc = copts.getOption("Xdebug");
		if (oc.isPresent()) {
			attr += "debug=true&";
			if (oc.getArgumentCount() == 1)
				attr += "logfile=" + oc.getArgument() + "&";
		}
		oc = copts.getOption("Xhash");
		if (oc.isPresent())
			attr += "hash=" + oc.getArgument() + "&";

		// request a connection suitable for MonetDB from the driver
		// manager note that the database specifier is only used when
		// connecting to a proxy-like service, since MonetDB itself
		// can't access multiple databases.
		con = null;
		final String database = copts.getOption("database").getArgument();
		try {
			// make sure the driver class is loaded (and thus register itself with the DriverManager)
			Class.forName("org.monetdb.jdbc.MonetDriver");

			con = DriverManager.getConnection(
					"jdbc:monetdb://" + host + "/" + database + attr,
					user,
					pass
			);
			SQLWarning warn = con.getWarnings();
			while (warn != null) {
				System.err.println("Connection warning: " + warn.getMessage());
				warn = warn.getNextWarning();
			}
			con.clearWarnings();
		} catch (SQLException e) {
			System.err.println("Database connect failed: " + e.getMessage());
			System.exit(1);
		}

		try {
			dbmd = con.getMetaData();
		} catch (SQLException e) {
			// we ignore this because it's probably because we don't use
			// SQL language
			dbmd = null;
		}

		oc = copts.getOption("csvdir");
		if (oc.isPresent()) {
			final String csvdir = oc.getArgument();
			if (csvdir != null) {
				// check if provided csvdir is an existing dir
				// else a download of data into file will terminate the JDBC connection!!
				if (java.nio.file.Files.isDirectory(java.nio.file.Paths.get(csvdir))) {
					final FileTransferHandler FThandler = new FileTransferHandler(csvdir, true);

					// register file data uploadHandler to allow support
					// for: COPY INTO mytable FROM 'data.csv' ON CLIENT;
					((MonetConnection) con).setUploadHandler(FThandler);

					// register file data downloadHandler to allow support
					// for: COPY select_query INTO 'data.csv' ON CLIENT;
					((MonetConnection) con).setDownloadHandler(FThandler);
				} else {
					System.err.println("Warning: provided csvdir \"" + csvdir + "\" does not exist. Ignoring csvdir setting.");
				}
			}
		}

		stmt = con.createStatement();	// is used by processInteractive(), processBatch(), doDump()

		in = new BufferedReader(new InputStreamReader(System.in));
		out = new PrintWriter(new BufferedWriter(new java.io.OutputStreamWriter(System.out)));

		// see if we will have to perform a database dump (only in SQL mode)
		if ("sql".equals(lang) && copts.getOption("dump").isPresent() && dbmd != null) {
			final int argcount = copts.getOption("dump").getArgumentCount();

			// use the given file for writing
			oc = copts.getOption("file");
			if (oc.isPresent())
				out = new PrintWriter(new BufferedWriter(new java.io.FileWriter(oc.getArgument())));

			// we only want user tables and views to be dumped (DDL and optional data), unless a specific table is requested
			final String[] types = {"TABLE","VIEW","MERGE TABLE","REMOTE TABLE","REPLICA TABLE","STREAM TABLE"};
			// Future: fetch all type names using dbmd.getTableTypes() and construct String[] with all
			// table type names excluding the SYSTEM ... ones and LOCAL TEMPORARY TABLE ones.

			// request the list of tables/views available in the current schema in the database
			ResultSet tbl = dbmd.getTables(null, con.getSchema(), null, (argcount == 0) ? types : null);
			// fetch all tables and store them in a LinkedList of Table objects
			final LinkedList<Table> tables = new LinkedList<Table>();
			while (tbl.next()) {
				tables.add(new Table(
					tbl.getString(2),	// 2 = "TABLE_SCHEM"
					tbl.getString(3),	// 3 = "TABLE_NAME"
					tbl.getString(4)));	// 4 = "TABLE_TYPE"
			}
			tbl.close();
			tbl = null;

			if (xmlMode) {
				exporter = new XMLExporter(out);
				exporter.setProperty(XMLExporter.TYPE_NIL, XMLExporter.VALUE_XSI);
			} else {
				exporter = new SQLExporter(out);
				// stick with SQL INSERT INTO commands for now
				// in the future we might do COPY INTO's here using VALUE_COPY
				exporter.setProperty(SQLExporter.TYPE_OUTPUT, SQLExporter.VALUE_INSERT);
			}
			exporter.useSchemas(true);

			// start SQL output
			if (!xmlMode)
				out.println("START TRANSACTION;\n");

			// dump specific table(s) or not?
			if (argcount > 0) { // yes we do
				final String[] dumpers = copts.getOption("dump").getArguments();
				for (int i = 0; i < tables.size(); i++) {
					Table ttmp = tables.get(i);
					for (int j = 0; j < dumpers.length; j++) {
						String dumptblnm = dumpers[j].toString();
						if (ttmp.getName().equalsIgnoreCase(dumptblnm) ||
						    ttmp.getFqname().equalsIgnoreCase(dumptblnm))
						{
							// dump the table
							doDump(out, ttmp);
						}
					}
				}
			} else {
				/* this returns everything, so including SYSTEM TABLE constraints */
				tbl = dbmd.getImportedKeys(null, null, null);
				while (tbl.next()) {
					// find FK table object  6 = "FKTABLE_SCHEM", 7 = "FKTABLE_NAME"
					Table fk = Table.findTable(tbl.getString(6), tbl.getString(7), tables);

					// find PK table object  2 = "PKTABLE_SCHEM", 3 = "PKTABLE_NAME"
					Table pk = Table.findTable(tbl.getString(2), tbl.getString(3), tables);

					// this happens when a system table has referential constraints
					if (fk == null || pk == null)
						continue;

					// add PK table dependency to FK table
					fk.addDependency(pk);
				}
				tbl.close();
				tbl = null;

				// search for cycles of type a -> (x ->)+ b probably not
				// the most optimal way, but it works by just scanning
				// every table for loops in a recursive manor
				for (Table t : tables) {
					Table.checkForLoop(t, new ArrayList<Table>());
				}

				// find the graph, at this point we know there are no
				// cycles, thus a solution exists
				for (int i = 0; i < tables.size(); i++) {
					final List<Table> needs = tables.get(i).requires(tables.subList(0, i + 1));
					if (needs.size() > 0) {
						tables.removeAll(needs);
						tables.addAll(i, needs);

						// re-evaluate this position, for there is a new
						// table now
						i--;
					}
				}

				// we now have the right order to dump tables
				for (Table t : tables) {
					// dump the table
					doDump(out, t);
				}
			}

			if (!xmlMode)
				out.println("COMMIT;");
			out.flush();

			// free resources, close the statement
			stmt.close();
			// close the connection with the database
			con.close();
			// completed database dump
			System.exit(0);
		}

		if (xmlMode) {
			exporter = new XMLExporter(out);
			exporter.setProperty(XMLExporter.TYPE_NIL, XMLExporter.VALUE_XSI);
		} else {
			exporter = new SQLExporter(out);
			// we want nice table formatted output
			exporter.setProperty(SQLExporter.TYPE_OUTPUT, SQLExporter.VALUE_TABLE);
		}
		exporter.useSchemas(false);

		try {
			// use the given file for reading
			final boolean hasFile = copts.getOption("file").isPresent();
			final boolean doEcho = hasFile && copts.getOption("echo").isPresent();
			if (hasFile) {
				final String tmp = copts.getOption("file").getArgument();
				try {
					in = getReader(tmp);
				} catch (Exception e) {
					System.err.println("Error: " + e.getMessage());
					System.exit(1);
				}

				// check for batch mode
				int batchSize = 0;
				oc = copts.getOption("Xbatching");
				if (oc.isPresent()) {
					if (oc.getArgumentCount() == 1) {
						// parse the number
						try {
							batchSize = Integer.parseInt(oc.getArgument());
						} catch (NumberFormatException ex) {
							// complain to the user
							throw new IllegalArgumentException("Illegal argument for Xbatching: " + oc.getArgument() + " is not a parseable number!");
						}
					}
					processBatch(batchSize);
				} else {
					processInteractive(true, doEcho, scolonterm, user);
				}
			} else {
				if (!copts.getOption("quiet").isPresent()) {
					// print welcome message
					out.println("Welcome to the MonetDB interactive JDBC terminal!");
					if (dbmd != null) {
						out.println("JDBC Driver: " + dbmd.getDriverName() +
							" v" + dbmd.getDriverVersion());
						out.println("Database Server: " + dbmd.getDatabaseProductName() +
							" v" + dbmd.getDatabaseProductVersion());
					}
					out.println("Current Schema: " + con.getSchema());
					out.println("Type \\q to quit (you can also use: quit or exit), \\? or \\h for a list of available commands");
					out.flush();
				}
				processInteractive(false, doEcho, scolonterm, user);
			}

			// free resources, close the statement
			stmt.close();
			// close the connection with the database
			con.close();
			// close the file (if we used a file)
			in.close();
		} catch (Exception e) {
			System.err.println("A fatal exception occurred: " + e.toString());
			e.printStackTrace(System.err);
			// at least try to close the connection properly, since it will
			// close all statements associated with it
			try {
				con.close();
			} catch (SQLException ex) {
				// ok... nice try
			}
			System.exit(1);
		}
	}

	/**
	 * Tries to interpret the given String as URL or file.  Returns the
	 * assigned BufferedReader, or throws an Exception if the given
	 * string couldn't be identified as a valid URL or file.
	 *
	 * @param uri URL or filename as String
	 * @return a BufferedReader for the uri
	 * @throws Exception if uri cannot be identified as a valid URL or file
	 */
	static BufferedReader getReader(final String uri) throws Exception {
		BufferedReader ret = null;
		URL u = null;

		// Try and parse as URL first
		try {
			u = new URL(uri);
		} catch (java.net.MalformedURLException e) {
			// no URL, try as file
			try {
				ret = new BufferedReader(new java.io.FileReader(uri));
			} catch (java.io.FileNotFoundException fnfe) {
				// the message is descriptive enough, adds "(No such file
				// or directory)" itself.
				throw new Exception(fnfe.getMessage());
			}
		}

		if (ret == null) {
			try {
				HttpURLConnection.setFollowRedirects(true);
				final HttpURLConnection con = (HttpURLConnection)u.openConnection();
				con.setRequestMethod("GET");
				final String ct = con.getContentType();
				if ("application/x-gzip".equals(ct)) {
					// open gzip stream
					ret = new BufferedReader(new InputStreamReader(
							new java.util.zip.GZIPInputStream(con.getInputStream())));
				} else {
					// text/plain otherwise just attempt to read as is
					ret = new BufferedReader(new InputStreamReader(con.getInputStream()));
				}
			} catch (IOException e) {
				// failed to open the url
				throw new Exception("No such host/file: " + e.getMessage());
			} catch (Exception e) {
				// this is an exception that comes from deep ...
				throw new Exception("Invalid URL: " + e.getMessage());
			}
		}

		return ret;
	}

	/**
	 * Starts an interactive processing loop, where output is adjusted to an
	 * user session.  This processing loop is not suitable for bulk processing
	 * as in executing the contents of a file, since processing on the given
	 * input is done after each row that has been entered.
	 *
	 * @param hasFile a boolean indicating whether a file is used as input
	 * @param doEcho a boolean indicating whether to echo the given input
	 * @param scolonterm whether a ';' makes this query part complete
	 * @param user a String representing the username of the current user
	 * @throws IOException if an IO exception occurs
	 * @throws SQLException if a database related error occurs
	 */
	private static void processInteractive(
		final boolean hasFile,
		final boolean doEcho,
		final boolean scolonterm,
		final String user)
		throws IOException, SQLException
	{
		// an SQL stack keeps track of ( " and '
		final SQLStack stack = new SQLStack();
		boolean lastac = false;

		if (!hasFile) {
			lastac = con.getAutoCommit();
			out.println("auto commit mode: " + (lastac ? "on" : "off"));
			out.print(getPrompt(stack, true));
			out.flush();
		}

		String curLine;
		String query = "";
		boolean doProcess;
		boolean wasComplete = true;

		// the main (interactive) process loop
		for (long i = 1; true; i++) {
			// Manually read a line, because we want to detect an EOF
			// (ctrl-D).  Doing so allows to have a terminator for query
			// which is not based on a special character, as is the
			// problem for XQuery
			curLine = in.readLine();
			if (curLine == null) {
				out.println("");
				if (!query.isEmpty()) {
					try {
						executeQuery(query, stmt, out, !hasFile);
					} catch (SQLException e) {
						out.flush();
						do {
							if (hasFile) {
								System.err.println("Error on line " + i + ": [" + e.getSQLState() + "] " + e.getMessage());
							} else {
								System.err.println("Error [" + e.getSQLState() + "]: " + e.getMessage());
							}
							// print all error messages in the chain (if any)
						} while ((e = e.getNextException()) != null);
					}
					query = "";
					wasComplete = true;
					if (!hasFile) {
						final boolean ac = con.getAutoCommit();
						if (ac != lastac) {
							out.println("auto commit mode: " + (ac ? "on" : "off"));
							lastac = ac;
						}
						out.print(getPrompt(stack, wasComplete));
					}
					out.flush();
					// try to read again
					continue;
				} else {
					// user did ctrl-D without something in the buffer,
					// so terminate
					break;
				}
			}

			if (doEcho) {
				out.println(curLine);
				out.flush();
			}

			// a query part is a line of an SQL query
			QueryPart qp = scanQuery(curLine, stack, scolonterm);
			if (!qp.isEmpty()) {
				final String command = qp.getQuery();
				doProcess = true;
				if (wasComplete) {
					doProcess = false;
					// check for commands only when the previous row was complete
					if (command.equals("\\q") || command.equals("quit") || command.equals("exit")) {
						// quit
						break;
					} else if (command.equals("\\?") || command.equals("\\h")) {
						out.println("Available commands:");
						out.println("\\q       quits this program (you can also use: quit or exit)");
						if (dbmd != null) {
							out.println("\\d       list available user tables and views in current schema");
							out.println("\\dS      list available system tables and views in sys schema");
							out.println("\\d <obj> describes the given table or view");
						}
						out.println("\\l<uri>  executes the contents of the given file or URL");
						out.println("\\i<uri>  batch executes the inserts from the given file or URL");
						out.println("\\vsci    validate sql system catalog integrity");
					//	out.println("\\vsni    validate sql system netcdf tables integrity");	// do not list as it depends on availability of netcdf library on server
					//	out.println("\\vsgi    validate sql system geom tables integrity");	// do not list as it depends on availability of geom library on server
						out.println("\\vsi <schema>  validate integrity of data in the given schema");
						out.println("\\vdbi    validate integrity of data in all user schemas in the database");
						out.println("\\? or \\h this help screen");
					} else if (dbmd != null && command.startsWith("\\d")) {
						ResultSet tbl = null;
						try {
							if (command.equals("\\dS")) {
								String curSchema = con.getSchema();
								if (!("sys".equals(curSchema) || "tmp".equals(curSchema) || "logging".equals(curSchema)))
									curSchema = "sys";
								// list available system tables and views in sys/tmp/logging schema
								tbl = dbmd.getTables(null, curSchema, null, null);

								// give us a list of all non-system tables and views (including temp ones)
								while (tbl.next()) {
									final String tableType = tbl.getString(4);	// 4 = "TABLE_TYPE"
									if (tableType != null && tableType.startsWith("SYSTEM ")) {
										String tableNm = tbl.getString(3);	// 3 = "TABLE_NAME"
										if (tableNm.contains(" ") || tableNm.contains("\t"))
											tableNm = Exporter.dq(tableNm);
										out.println(tableType + "\t" +
											tbl.getString(2) + "." +	// 2 = "TABLE_SCHEM"
											tableNm);
									}
								}
							} else {
								String object = command.substring(2).trim();
								if (scolonterm && object.endsWith(";"))
									object = object.substring(0, object.length() - 1);
								if (object.isEmpty()) {
									// list available user tables and views in current schema
									tbl = dbmd.getTables(null, con.getSchema(), null, null);

									// give us a list of all non-system tables and views (including temp ones)
									while (tbl.next()) {
										final String tableType = tbl.getString(4);	// 4 = "TABLE_TYPE"
										if (tableType != null && !tableType.startsWith("SYSTEM ")) {
											String tableNm = tbl.getString(3);	// 3 = "TABLE_NAME"
											if (tableNm.contains(" ") || tableNm.contains("\t"))
												tableNm = Exporter.dq(tableNm);
											out.println(tableType + "\t" +
												tbl.getString(2) + "." +	// 2 = "TABLE_SCHEM"
												tableNm);
										}
									}
								} else {
									// describes the given table or view
									String schema;
									String obj_nm = object;
									int len;
									boolean found = false;
									final int dot = object.indexOf(".");
									if (dot > 0) {
										// use specified schema
										schema = object.substring(0, dot);
										obj_nm = object.substring(dot + 1);
										// remove potential surrounding double quotes around schema name
										len = schema.length();
										if (len > 2 && schema.charAt(0) == '"' && schema.charAt(len -1) == '"')
											schema = schema.substring(1, len -1);
									} else {
										// use current schema
										schema = con.getSchema();
									}
									// remove potential surrounding double quotes around table or view name
									len = obj_nm.length();
									if (len > 2 && obj_nm.charAt(0) == '"' && obj_nm.charAt(len -1) == '"')
										obj_nm = obj_nm.substring(1, len -1);

									// System.err.println("calling dbmd.getTables(" + schema + ", " + obj_nm + ")");
									tbl = dbmd.getTables(null, schema, obj_nm, null);
									while (tbl.next() && !found) {
										final String schemaName = tbl.getString(2);	// 2 = "TABLE_SCHEM"
										final String tableName = tbl.getString(3);	// 3 = "TABLE_NAME"
										if (obj_nm.equals(tableName) && schema.equals(schemaName)) {
											// we found it, describe it
											exporter.dumpSchema(dbmd,
												tbl.getString(4),	// 4 = "TABLE_TYPE"
												schemaName,
												tableName);

											found = true;
											break;
										}
									}
									if (!found)
										System.err.println("No match found for table or view: " + object);
								}
							}
						} catch (SQLException e) {
							out.flush();
							do {
								System.err.println("Error [" + e.getSQLState() + "]: " + e.getMessage());
								// print all error messages in the chain (if any)
							} while ((e = e.getNextException()) != null);
						} finally {
							if (tbl != null)
								tbl.close();
						}
					} else if (command.startsWith("\\v")) {
						if (command.equals("\\vsci")) {
							MDBvalidator.validateSqlCatalogIntegrity(con, true);
						} else if (command.equals("\\vsci_noheader")) {	// used only for internal automated testing
							MDBvalidator.validateSqlCatalogIntegrity(con, false);
						} else if (command.equals("\\vsni")) {
							MDBvalidator.validateSqlNetcdfTablesIntegrity(con, true);
						} else if (command.equals("\\vsni_noheader")) {	// used only for internal automated testing
							MDBvalidator.validateSqlNetcdfTablesIntegrity(con, false);
						} else if (command.equals("\\vsgi")) {
							MDBvalidator.validateSqlGeomTablesIntegrity(con, true);
						} else if (command.equals("\\vsgi_noheader")) {	// used only for internal automated testing
							MDBvalidator.validateSqlGeomTablesIntegrity(con, false);
						} else if (command.startsWith("\\vsi ")) {
							String schema_nm = command.substring(5);
							MDBvalidator.validateSchemaIntegrity(con, schema_nm, true);
						} else if (command.startsWith("\\vsi_noheader ")) {	// used only for internal automated testing
							String schema_nm = command.substring(14);
							MDBvalidator.validateSchemaIntegrity(con, schema_nm, false);
						} else if (command.equals("\\vdbi")) {
							MDBvalidator.validateDBIntegrity(con, true);
						} else if (command.equals("\\vdbi_noheader")) {	// used only for internal automated testing
							MDBvalidator.validateDBIntegrity(con, false);
						} else {
							doProcess = true;
						}
					} else if (command.startsWith("\\l") || command.startsWith("\\i")) {
						String object = command.substring(2).trim();
						if (scolonterm && object.endsWith(";"))
							object = object.substring(0, object.length() - 1);
						if (object.isEmpty()) {
							System.err.println("Usage: '" + command.substring(0, 2) + "<uri>' where <uri> is a file or URL");
						} else {
							// temporarily redirect input from in
							final BufferedReader console = in;
							try {
								in = getReader(object);
								if (command.startsWith("\\l")) {
									processInteractive(true, doEcho, scolonterm, user);
								} else {
									processBatch(0);
								}
							} catch (Exception e) {
								out.flush();
								System.err.println("Error: " + e.getMessage());
							} finally {
								// put back in redirection
								in = console;
							}
						}
					} else {
						doProcess = true;
					}
				}

				if (doProcess) {
					query += command + (qp.hasOpenQuote() ? "\\n" : " ");
					if (qp.isComplete()) {
						// strip off trailing ';'
						query = query.substring(0, query.length() - 2);
						// execute query
						try {
							executeQuery(query, stmt, out, !hasFile);
						} catch (SQLException e) {
							out.flush();
							final String startmsg = (hasFile ? ("Error on line " + i + ": [") : "Error [");
							do {
								System.err.println(startmsg + e.getSQLState() + "] " + e.getMessage());
								// print all error messages in the chain (if any)
							} while ((e = e.getNextException()) != null);
						}
						query = "";
						wasComplete = true;
					} else {
						wasComplete = false;
					}
				}
			}

			if (!hasFile) {
				final boolean ac = con.getAutoCommit();
				if (ac != lastac) {
					out.println("auto commit mode: " + (ac ? "on" : "off"));
					lastac = ac;
				}
				out.print(getPrompt(stack, wasComplete));
			}
			out.flush();
		}
	}

	/**
	 * Executes the given query and prints the result tabularised to the
	 * given PrintWriter stream.  The result of this method is the
	 * default output of a query: tabular data.
	 *
	 * @param query the query to execute
	 * @param stmt the Statement to execute the query on
	 * @param out the PrintWriter to write to
	 * @param showTiming flag to specify if timing information nees to be printed
	 * @throws SQLException if a database related error occurs
	 */
	private static void executeQuery(final String query,
			final Statement stmt,
			final PrintWriter out,
			final boolean showTiming)
		throws SQLException
	{
		// warnings generated during querying
		SQLWarning warn;
		long startTime = (showTiming ? System.currentTimeMillis() : 0);
		long finishTime = 0;

		// execute the query, let the driver decide what type it is
		int aff = -1;
		boolean nextRslt = stmt.execute(query, Statement.RETURN_GENERATED_KEYS);
		if (!nextRslt)
			aff = stmt.getUpdateCount();
		do {
			if (nextRslt) {
				// we have a ResultSet, print it
				final ResultSet rs = stmt.getResultSet();

				exporter.dumpResultSet(rs);
				if (showTiming) {
					finishTime = System.currentTimeMillis();
					out.println("Elapsed Time: " + (finishTime - startTime) + " ms");
					startTime = finishTime;
				}

				// if there were warnings for this result,
				// show them!
				warn = rs.getWarnings();
				if (warn != null) {
					// force stdout to be written so the
					// warning appears below it
					out.flush();
					do {
						System.err.println("ResultSet warning: " +
							warn.getMessage());
						warn = warn.getNextWarning();
					} while (warn != null);
					rs.clearWarnings();
				}
				rs.close();
			} else if (aff != -1) {
				String timingoutput = "";
				if (showTiming) {
					finishTime = System.currentTimeMillis();
					timingoutput = ". Elapsed Time: " + (finishTime - startTime) + " ms";
					startTime = finishTime;
				}

				if (aff == Statement.SUCCESS_NO_INFO) {
					out.println("Operation successful" + timingoutput);
				} else {
					// we have an update count
					// see if a key was generated
					final ResultSet rs = stmt.getGeneratedKeys();
					final boolean hasGeneratedKeyData = rs.next();
					out.println(aff + " affected row" + (aff != 1 ? "s" : "") +
						(hasGeneratedKeyData ? ", last generated key: " + rs.getString(1) : "") +
						timingoutput);
					rs.close();
				}
			}

			out.flush();
		} while ((nextRslt = stmt.getMoreResults()) ||
			 (aff = stmt.getUpdateCount()) != -1);

		// if there were warnings for this statement show them!
		warn = stmt.getWarnings();
		while (warn != null) {
			System.err.println("Statement warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
		stmt.clearWarnings();

		// if there were warnings for this connection show them!
		warn = con.getWarnings();
		while (warn != null) {
			// suppress warning when issueing a "set schema xyz;" command
//			if ( !(warn.getMessage()).equals("Server enabled auto commit mode while local state already was auto commit.") )
				System.err.println("Connection warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
		con.clearWarnings();
	}

	/**
	 * Starts a processing loop optimized for processing (large) chunks of
	 * continuous data, such as input from a file.  Unlike in the interactive
	 * loop above, queries are sent only to the database if a certain batch
	 * amount is reached.  No client side query checks are made, but everything
	 * is sent to the server as-is.
	 *
	 * @param batchSize the number of items to store in the batch before
	 *		sending them to the database for execution.
	 * @throws IOException if an IO exception occurs.
	 */
	private static void processBatch(final int batchSize) throws IOException {
		final StringBuilder query = new StringBuilder(2048);
		int i = 0;
		try {
			String curLine;
			// the main loop
			for (i = 1; (curLine = in.readLine()) != null; i++) {
				query.append(curLine);
				if (curLine.endsWith(";")) {
					// lousy check for end of statement, but in batch mode it
					// is not very important to catch all end of statements...
					stmt.addBatch(query.toString());
					query.setLength(0);	// clear the buffer
				} else {
					query.append('\n');
				}
				if (batchSize > 0 && i % batchSize == 0) {
					stmt.executeBatch();
					// stmt.clearBatch();	// this is no longer needed after call executeBatch(), see https://www.monetdb.org/bugzilla/show_bug.cgi?id=6953
				}
			}
			stmt.addBatch(query.toString());
			stmt.executeBatch();
			// stmt.clearBatch();	// this is no longer needed after call executeBatch(), see https://www.monetdb.org/bugzilla/show_bug.cgi?id=6953
		} catch (SQLException e) {
			do {
				System.err.println("Error at line " + i + ": [" + e.getSQLState() + "] " + e.getMessage());
				// print all error messages in the chain (if any)
			} while ((e = e.getNextException()) != null);
		}
	}

	/**
	 * Wrapper method that decides to dump SQL or XML.  In the latter case,
	 * this method does the XML data generation.
	 *
	 * @param out a Writer to write the data to
	 * @param table the table to dump
	 * @throws SQLException if a database related error occurs
	 */
	private static void doDump(final PrintWriter out, final Table table) throws SQLException {
		final String tableType = table.getType();

		// dump CREATE definition of this table/view
		exporter.dumpSchema(dbmd, tableType, table.getSchem(), table.getName());
		out.println();

		// only dump data from real tables, not from VIEWs / MERGE / REMOTE / REPLICA / STREAM tables
		if (tableType.contains("TABLE")
		&& !tableType.equals("MERGE TABLE")
		&& !tableType.equals("REMOTE TABLE")
		&& !tableType.equals("REPLICA TABLE")
		&& !tableType.equals("STREAM TABLE")) {
			final ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getFqnameQ());
			if (rs != null) {
				exporter.dumpResultSet(rs);
				rs.close();
				out.println();
			}
		}
	}

	/**
	 * Simple helper method that generates a prompt.
	 *
	 * @param stack the current SQLStack
	 * @param compl whether the statement is complete
	 * @return a prompt which consist of "sql" plus the top of the stack
	 */
	private static String getPrompt(final SQLStack stack, final boolean compl) {
		return (compl ? "sql" : "more") + (stack.empty() ? ">" : stack.peek()) + " ";
	}

	/**
	 * Scans the given string and tries to discover if it is a complete query
	 * or that there needs something to be added.  If a string doesn't end with
	 * a ; it is considered not to be complete.  SQL string quotation using ' and
	 * SQL identifier quotation using " is taken into account when scanning a
	 * string this way.
	 * Additionally, this method removes comments from the SQL statements,
	 * identified by -- and removes white space where appropriate.
	 *
	 * @param query the query to parse
	 * @param stack query stack to work with
	 * @param scolonterm whether a ';' makes this query part complete
	 * @return a QueryPart object containing the results of this parse
	 */
	private static QueryPart scanQuery(
			final String query,
			final SQLStack stack,
			final boolean scolonterm)
	{
		// examine string, char for char
		final boolean wasInString = (stack.peek() == '\'');
		final boolean wasInIdentifier = (stack.peek() == '"');
		boolean escaped = false;
		int len = query.length();
		for (int i = 0; i < len; i++) {
			switch(query.charAt(i)) {
				case '\\':
					escaped = !escaped;
				break;
				default:
					escaped = false;
				break;
				case '\'':
					/**
					 * We can not be in a string if we are in an identifier. So
					 * If we find a ' and are not in an identifier, and not in
					 * a string we can safely assume we will be now in a string.
					 * If we are in a string already, we should stop being in a
					 * string if we find a quote which is not prefixed by a \,
					 * for that would be an escaped quote. However, a nasty
					 * situation can occur where the string is like 'test \\'.
					 * As obvious, a test for a \ in front of a ' doesn't hold
					 * here. Because 'test \\\'' can exist as well, we need to
					 * know if a quote is prefixed by an escaping slash or not.
					 */
					if (!escaped && stack.peek() != '"') {
						if (stack.peek() != '\'') {
							// although it makes no sense to escape a quote
							// outside a string, it is escaped, thus not meant
							// as quote for us, apparently
							stack.push('\'');
						} else {
							stack.pop();
						}
					}
					// reset escaped flag
					escaped = false;
				break;
				case '"':
					if (!escaped && stack.peek() != '\'') {
						if (stack.peek() != '"') {
							stack.push('"');
						} else {
							stack.pop();
						}
					}
					// reset escaped flag
					escaped = false;
				break;
				case '-':
					if (!escaped && stack.peek() != '\'' && stack.peek() != '"' && i + 1 < len && query.charAt(i + 1) == '-') {
						len = i;
					}
					escaped = false;
				break;
				case '(':
					if (!escaped && stack.peek() != '\'' && stack.peek() != '"') {
						stack.push('(');
					}
					escaped = false;
				break;
				case ')':
					if (!escaped && stack.peek() == '(') {
						stack.pop();
					}
					escaped = false;
				break;
			}
		}

		int start = 0;
		if (!wasInString && !wasInIdentifier && len > 0) {
			// trim spaces at the start of the string
			for (; start < len && Character.isWhitespace(query.charAt(start)); start++);
		}
		int stop = len - 1;
		if (stack.peek() !=  '\'' && !wasInIdentifier && stop > start) {
			// trim spaces at the end of the string
			for (; stop >= start && Character.isWhitespace(query.charAt(stop)); stop--);
		}
		stop++;

		if (start == stop) {
			// we have an empty string
			return new QueryPart(false, null, stack.peek() ==  '\'' || stack.peek() == '"');
		} else if (stack.peek() ==  '\'' || stack.peek() == '"') {
			// we have an open quote
			return new QueryPart(false, query.substring(start, stop), true);
		} else {
			// see if the string is complete
			if (scolonterm && query.charAt(stop - 1) == ';') {
				return new QueryPart(true, query.substring(start, stop), false);
			} else {
				return new QueryPart(false, query.substring(start, stop), false);
			}
		}
	}
}

/**
 * A QueryPart is (a part of) a SQL query.  In the QueryPart object information
 * like the actual SQL query string, whether it has an open quote and the like
 * is stored.
 */
final class QueryPart {
	private final boolean complete;
	private final String query;
	private final boolean open;

	QueryPart(final boolean complete, final String query, final boolean open) {
		this.complete = complete;
		this.query = query;
		this.open = open;
	}

	boolean isEmpty() {
		return query == null;
	}

	boolean isComplete() {
		return complete;
	}

	String getQuery() {
		return query;
	}

	boolean hasOpenQuote() {
		return open;
	}
}

/**
 * An SQLStack is a simple stack that keeps track of open brackets and
 * (single and double) quotes in an SQL query.
 */
final class SQLStack {
	final StringBuilder stack = new StringBuilder();

	char peek() {
		if (empty()) {
			return '\0';
		} else {
			return stack.charAt(stack.length() - 1);
		}
	}

	char pop() {
		final char tmp = peek();
		if (tmp != '\0') {
			stack.setLength(stack.length() - 1);
		}
		return tmp;
	}

	char push(char item) {
		stack.append(item);
		return item;
	}

	boolean empty() {
		return stack.length() == 0;
	}
}

/**
 * A Table represents an SQL table.  All data required to
 * generate a fully qualified name is stored, as well as dependency
 * data.
 */
final class Table {
	final String schem;
	final String name;
	final String type;
	final String fqname;
	final ArrayList<Table> needs = new ArrayList<Table>();

	Table(final String schem, final String name, final String type) {
		this.schem = schem;
		this.name = name;
		this.type = type;
		this.fqname = schem + "." + name;
	}

	void addDependency(final Table dependsOn) throws Exception {
		if (this.fqname.equals(dependsOn.fqname))
			throw new Exception("Cyclic dependency graphs are not supported (foreign key relation references self)");

		if (dependsOn.needs.contains(this))
			throw new Exception("Cyclic dependency graphs are not supported (foreign key relation a->b and b->a)");

		if (!needs.contains(dependsOn))
			needs.add(dependsOn);
	}

	List<Table> requires(final List<Table> existingTables) {
		if (existingTables == null || existingTables.isEmpty())
			return new ArrayList<Table>(needs);

		final ArrayList<Table> req = new ArrayList<Table>();
		for (Table n : needs) {
			if (!existingTables.contains(n))
				req.add(n);
		}

		return req;
	}

	final String getSchem() {
		return schem;
	}

	final String getName() {
		return name;
	}

	final String getType() {
		return type;
	}

	final String getFqname() {
		return fqname;
	}

	final String getFqnameQ() {
		return Exporter.dq(schem) + "." + Exporter.dq(name);
	}

	public final String toString() {
		return fqname;
	}

	static final Table findTable(final String schname, final String tblname, final List<Table> list) {
		for (Table t : list) {
			if (t.schem.equals(schname) && t.name.equals(tblname))
				return t;
		}
		// not found
		return null;
	}

	static final void checkForLoop(final Table table, final List<Table> parents) throws Exception {
		parents.add(table);
		for (int i = 0; i < table.needs.size(); i++) {
			Table child = table.needs.get(i);
			if (parents.contains(child))
				throw new Exception("Cyclic dependency graphs are not supported (cycle detected for " + child.fqname + ")");
			checkForLoop(child, parents);
		}
	}
}

