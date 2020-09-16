/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

package nl.cwi.monetdb.client;

import nl.cwi.monetdb.util.CmdLineOpts;
import nl.cwi.monetdb.util.Exporter;
import nl.cwi.monetdb.util.OptionsException;
import nl.cwi.monetdb.util.SQLExporter;
import nl.cwi.monetdb.util.XMLExporter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;	// required as it will load the nl.cwi.monetdb.jdbc.MonetDriver class
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
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
 * @author Fabian Groffen, Martin van Dinther
 * @version 1.5
 */

public final class JdbcClient {

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
	 *		[-l language] [-d database] [-e] [-D [table]] [-X&lt;opt&gt;]
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
	 * -Xoutput      The output mode when dumping.  Default is sql, xml may be used for
	 *               an experimental XML output.
	 * -Xhash        Use the given hash algorithm during challenge response.  Supported
	 *               algorithm names: SHA512, SHA384, SHA256 and SHA1.
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
				"Use the given hash algorithm during challenge response.  " +
				"Supported algorithm names: SHA512, SHA384, SHA256 and SHA1.");
		// arguments which can have zero or one argument(s)
		copts.addOption(null, "Xdebug", CmdLineOpts.CAR_ZERO_ONE, null,
				"Writes a transmission log to disk for debugging purposes.  " +
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
				"\t\t[-l language] [-d database] [-e] [-D [table]] [-X<opt>]\n" +
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
			System.out.println("JDBC Driver: v" + nl.cwi.monetdb.jdbc.MonetDriver.getDriverVersion());
			System.exit(0);
		}

		// whether the semi-colon at the end of a String terminates the
		// query or not (default = yes => SQL)
		final boolean scolonterm = true;
		final boolean xmlMode = "xml".equals(copts.getOption("Xoutput").getArgument());

		// we need the password from the user, fetch it with a pseudo
		// password protector
		if (pass == null) {
			final char[] tmp = System.console().readPassword("password: ");
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

		// make sure the driver is loaded
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers

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

		in = new BufferedReader(new InputStreamReader(System.in));
		out = new PrintWriter(new BufferedWriter(new java.io.OutputStreamWriter(System.out)));

		stmt = con.createStatement();	// is used by doDump

		// see if we will have to perform a database dump (only in SQL mode)
		if ("sql".equals(lang) && copts.getOption("dump").isPresent() && dbmd != null) {
			// use the given file for writing
			oc = copts.getOption("file");
			if (oc.isPresent())
				out = new PrintWriter(new BufferedWriter(new java.io.FileWriter(oc.getArgument())));

			// we only want user tables and views to be dumped, unless a specific table is requested
			final String[] types = {"TABLE","VIEW","MERGE TABLE","REMOTE TABLE","REPLICA TABLE","STREAM TABLE"};
			// Future: fetch all type names using dbmd.getTableTypes() and construct String[] with all
			// table type names excluding the SYSTEM ... ones and LOCAL TEMPORARY TABLE ones.

			// request the list of tables available in the current schema in the database
			ResultSet tbl = dbmd.getTables(null, con.getSchema(), null,
							(copts.getOption("dump").getArgumentCount() == 0) ? types : null);
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
			if (copts.getOption("dump").getArgumentCount() > 0) { // yes we do
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
						out.println("\\? or \\h this help screen");
					} else if (dbmd != null && command.startsWith("\\d")) {
						ResultSet tbl = null;
						try {
							if (command.equals("\\dS")) {
								// list available system tables and views in sys schema
								tbl = dbmd.getTables(null, "sys", null, null);

								// give us a list of all non-system tables and views (including temp ones)
								while (tbl.next()) {
									final String tableType = tbl.getString(4);	// 4 = "TABLE_TYPE"
									if (tableType != null && tableType.startsWith("SYSTEM ")) {
										String tableNm = tbl.getString(3);	// 3 = "TABLE_NAME"
										if (tableNm.contains(" ") || tableNm.contains("\t"))
											tableNm = dq(tableNm);
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
												tableNm = dq(tableNm);
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
					} else if (command.equals("\\check_system_catalog_integrity")) {
						MDBvalidator.check_system_catalog_integrity(con);
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
		boolean	nextRslt = stmt.execute(query, Statement.RETURN_GENERATED_KEYS);
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

		// if there were warnings for this statement,
		// and/or connection show them!
		warn = stmt.getWarnings();
		while (warn != null) {
			System.err.println("Statement warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
		stmt.clearWarnings();

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
					stmt.clearBatch();
				}
			}
			stmt.addBatch(query.toString());
			stmt.executeBatch();
			stmt.clearBatch();
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

		// only dump data from real tables, not from views / MERGE / REMOTE / REPLICA tables
		if (tableType.contains("TABLE")
		&& !tableType.equals("MERGE TABLE")
		&& !tableType.equals("REMOTE TABLE")
		&& !tableType.equals("REPLICA TABLE")) {
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
		boolean wasInString = (stack.peek() == '\'');
		boolean wasInIdentifier = (stack.peek() == '"');
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

	static final String dq(final String in) {
		return "\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"";
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
		return JdbcClient.dq(schem) + "." + JdbcClient.dq(name);
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

/**
 * MonetDB Data Integrity Validator program (MDBvalidator) can
 * a) check system tables data integrity (in system schemas: sys, tmp, json, profiler, and possibly more depending on MonetDB version)
 *    this includes violations of:
 *		primary key uniqueness
 *		primary key column(s) not null
 *		unique constraint uniqueness (alternate keys)
 *		foreign key referential integrity (match / partial)
 *		column not null
 *		column maximum length for char/varchar/clob/blob/json/url
 * b) check user schema tables & columns data integrity based on available meta data from system tables/views
 *		column not null
 *		column maximum length for char/varchar/clob/blob/json/url
 *	TODO primary key uniqueness
 *	TODO primary key column(s) not null
 *	TODO unique constraint uniqueness (alternate keys)
 *	TODO foreign key referential integrity (match / partial)
 *
 * designed and created by Martin van Dinther
 */

final class MDBvalidator {
	private static final String prg = "MDBvalidator";
	private static Connection con;
	private static int majorversion;
	private static int minorversion;

	private static boolean verbose = false;		// set it to true for tracing all generated SQL queries
//	private static boolean use_log_table = false;	// Not Yet Supported (and maybe not needed)

// MDBvalidator() {}

	public static void main(String[] args) throws Exception {
		System.out.println(prg + " started with " + args.length + " arguments." + (args.length == 0 ? " Using default JDBC URL !" : ""));
		// parse input args: connection (JDBC_URL), check systbls (default) or user schema or user db

		String JDBC_URL = (args.length > 0) ? args[0]
						: "jdbc:monetdb://localhost:50000/demo?user=monetdb&password=monetdb&so_timeout=14000"; // &treat_clob_as_varchar=true";
		if (!JDBC_URL.startsWith("jdbc:monetdb://")) {
			System.out.println("ERROR: Invalid JDBC URL. It does not start with jdbc:monetdb:");
			return;
		}
		try {
			// make connection to target server
			con = java.sql.DriverManager.getConnection(JDBC_URL);
			System.out.println(prg + " connected to MonetDB server");
			printExceptions(con.getWarnings());

			check_system_catalog_integrity(con);
		} catch (SQLException e) {
			printExceptions(e);
		}

		// free resources
		if (con != null) {
			try { con.close(); } catch (SQLException e) { /* ignore */ }
		}
	}

	static void check_system_catalog_integrity(Connection conn) {
		long start_time = System.currentTimeMillis();
		try {
			con = conn;
			// retrieve server version numbers (major and minor). These are needed to filter out version specific validations
			DatabaseMetaData dbmd = con.getMetaData();
			if (dbmd != null) {
				majorversion = dbmd.getDatabaseMajorVersion();
				minorversion = dbmd.getDatabaseMinorVersion();
				System.out.println("MonetDB server version " + dbmd.getDatabaseProductVersion());
				// validate majorversion (should be 11) and minorversion (should be >= 19) (from Jul2015 (11.19.15))
				if (majorversion < 11 || (majorversion == 11 && minorversion < 19)) {
					System.out.println("ERROR: this MonetDB server is too old for " + prg + ". Please upgrade server.");
					con.close();
					return;
				}
			}
			String cur_schema = con.getSchema();
			if (!"sys".equals(cur_schema))
				con.setSchema("sys");

			verify("sys", null, sys_pkeys, sys_akeys, sys_fkeys, sys_notnull, true);
			verify("tmp", null, tmp_pkeys, tmp_akeys, tmp_fkeys, tmp_notnull, true);

			// first determine if netcdf tables (sys.netcdf_files, sys.netcdf_dims, sys.netcdf_vars, sys.netcdf_vardim) exist in the db
			if (false) {	// ToDo built check for existance of the 5 netcdf tables in sys
				verify("sys", "netcdf", netcdf_pkeys, netcdf_akeys, netcdf_fkeys, netcdf_notnull, false);
			}

			// first determine if geom tables (sys.spatial_ref_sys) exist in the db
			if (true) {	// ToDo built check for existance of the 2 geom tables in sys
				verify("sys", "geom", geom_pkeys, geom_akeys, geom_fkeys, geom_notnull, false);
			}
		} catch (SQLException e) {
			printExceptions(e);
		}

		long elapsed = System.currentTimeMillis() - start_time;
		long secs = elapsed /1000;
		System.out.println("Validation completed in " + secs + "s and " + (elapsed - (secs *1000)) + "ms");
	}

	private static void verify(String schema, String group, String[][] pkeys, String[][] akeys, String[][] fkeys, String[][] colnotnull, boolean checkmaxstr) {
		boolean is_system_schema = ("sys".equals(schema) || "tmp".equals(schema));
		if (pkeys != null)
			verifyUniqueness(schema, group, pkeys, "Primary Key uniqueness");
		if (pkeys != null)
			verifyNotNull(schema, group, pkeys, "Primary Key Not Null");
		if (akeys != null)
			verifyUniqueness(schema, group, akeys, "Alternate Key uniqueness");
		if (fkeys != null)
			verifyFKs(schema, group, fkeys, "Foreign Key referential integrity");
		if (colnotnull != null)
			verifyNotNull(schema, group, colnotnull, "Not Null");
		else
			verifyNotNull(schema, is_system_schema, "Not Null");
		if (checkmaxstr)
			verifyMaxCharStrLength(schema, is_system_schema, "Max Character Length");
/* TODO
 *		col char/varchar/clob/blob/json/url minimum length (some columns may not be empty, so length >= 1)
 *		col with sequence (serial/bigserial/autoincrement) in range (0/1/min_value .. max_value)
 *		col domain is valid (date/time/timestamp/json/inet/url/uuid/...)
 *		col in list checks (some columns may have only certain values which are not recorded somewhere (eg as fk))
 *		col conditional checks (column is not null when other column is (not) null)
		-- either column_id or expression in sys.table_partitions must be populated
		SELECT "column_id", "expression", 'Missing either column_id or expression' AS violation, * FROM "sys"."table_partitions" WHERE "column_id" IS NULL AND "expression" IS NULL;
		SELECT "column_id", "expression", 'column_id and expression may not both be populated. One of them must be NULL' AS violation, * FROM "sys"."table_partitions" WHERE "column_id" IS NOT NULL AND "expression" IS NOT NULL;
 */
	}

	private static void verifyUniqueness(String schema, String group, String[][] data, String check_type) {
		final int len = data.length;
		System.out.println("Checking " + len + (group != null ? " " + group : "") + " tables in schema " + schema + " for " + check_type + " violations.");

		StringBuilder sb = new StringBuilder(256);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT COUNT(*) AS duplicates, ");
		final int qry_len = sb.length();
		String tbl;
		String keycols;
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][2])) {
				tbl = data[i][0];
				keycols = data[i][1];
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(keycols).append(" FROM ");
				if (!tbl.startsWith("(")) {	// when tbl starts with ( it is a unioned table set which we cannot prefix with a schema name qualifier
					sb.append(schema).append('.');
				}
				sb.append(tbl)
				.append(" GROUP BY ").append(keycols)
				.append(" HAVING COUNT(*) > 1;");
				validate(sb.toString(), schema, tbl, keycols, check_type);
			}
		}
	}

	private static void verifyNotNull(String schema, String group, String[][] data, String check_type) {
		final int len = data.length;
		System.out.println("Checking " + len + (group != null ? " " + group : "") + " columns in schema " + schema + " for " + check_type + " violations.");

		StringBuilder sb = new StringBuilder(256);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT ");
		final int qry_len = sb.length();
		String tbl;
		String col;
		boolean multicolumn = false;
		StringBuilder isNullCond = new StringBuilder(80);
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][2])) {
				tbl = data[i][0];
				col = data[i][1];
				multicolumn = col.contains(", ");	// some pkeys consist of multiple columns
				isNullCond.setLength(0);	// empty previous content
				if (multicolumn) {
					String[] cols = col.split(", ");
					for (int c = 0; c < cols.length; c++) {
						if (c > 0) {
							isNullCond.append(" OR ");
						}
						isNullCond.append(cols[c]).append(" IS NULL");
					}
				} else {
					isNullCond.append(col).append(" IS NULL");
				}
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(col)
				.append(", * FROM ").append(schema).append('.').append(tbl)
				.append(" WHERE ").append(isNullCond).append(';');
				validate(sb.toString(), schema, tbl, col, check_type);
			}
		}
	}

	private static void verifyFKs(String schema, String group, String[][] data, String check_type) {
		final int len = data.length;
		System.out.println("Checking " + len + (group != null ? " " + group : "") + " foreign keys in schema " + schema + " for " + check_type + " violations.");

		StringBuilder sb = new StringBuilder(400);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT ");
		final int qry_len = sb.length();
		String tbl;
		String cols;
		String ref_tbl;
		String ref_cols;
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][4])) {
				tbl = data[i][0];
				cols = data[i][1];
				ref_cols = data[i][2];
				ref_tbl = data[i][3];
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(cols).append(", * FROM ").append(schema).append('.').append(tbl);
				if (!tbl.contains(" WHERE "))
					sb.append(" WHERE ");
				sb.append('(').append(cols).append(") NOT IN (SELECT ").append(ref_cols).append(" FROM ");
				if (!ref_tbl.contains("."))
					sb.append(schema).append('.');
				sb.append(ref_tbl).append(");");
				validate(sb.toString(), schema, tbl, cols, check_type);
			}
		}
	}

	private static void verifyNotNull(String schema, boolean system, String check_type) {
		// fetch the NOT NULL info from the MonetDB system tables as those are leading for user tables (but not system tables)
		StringBuilder sb = new StringBuilder(400);
		sb.append(" from sys.columns c join sys.tables t on c.table_id = t.id join sys.schemas s on t.schema_id = s.id"
				+ " where t.type in (0, 10, 1, 11) and c.\"null\" = false"	// t.type 0 = TABLE, 10 = SYSTEM TABLE, 1 = VIEW, 11 = SYSTEM VIEW
				+ " and t.system = ").append(system)
			.append(" and s.name = '").append(schema).append("'");
		String qry = sb.toString();
		long count = runCountQuery(qry);
		System.out.println("Checking " + count + " columns in schema " + schema + " for " + check_type + " violations.");

		Statement stmt = createStatement("verifyMaxCharStrLength()");
		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			sb.append("SELECT s.name as sch_nm, t.name as tbl_nm, c.name as col_nm")	// , t.type, t.system, c.type, c.type_digits
			.append(qry).append(" ORDER BY s.name, t.name, c.name;");
			qry = sb.toString();
			if (stmt != null)
				rs = stmt.executeQuery(qry);
			if (rs != null) {
				String sch, tbl, col;
				while (rs.next()) {
					// retrieve meta data
					sch = rs.getString(1);
					tbl = rs.getString(2);
					col = rs.getString(3);
					// compose validation query for this specific column
					sb.setLength(0);	// empty previous usage of sb
					sb.append("SELECT '").append(sch).append('.').append(tbl).append('.').append(col).append("' as full_col_nm, *")
					.append(" FROM \"").append(sch).append("\".\"").append(tbl).append("\"")
					.append(" WHERE \"").append(col).append("\" IS NULL;");
					validate(sb.toString(), sch, tbl, col, check_type);
				}
			}
		} catch (SQLException e) {
			System.out.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	private static void verifyMaxCharStrLength(String schema, boolean system, String check_type) {
		// fetch the max char str len info from the MonetDB system tables as those are leading
		StringBuilder sb = new StringBuilder(400);
		sb.append(" from sys.columns c join sys.tables t on c.table_id = t.id join sys.schemas s on t.schema_id = s.id"
				+ " where t.type in (0, 10, 1, 11) and c.type_digits >= 1"	// t.type 0 = TABLE, 10 = SYSTEM TABLE, 1 = VIEW, 11 = SYSTEM VIEW
				+ " and t.system = ").append(system)
			.append(" and s.name = '").append(schema).append("'")
			.append(" and c.type in ('varchar', 'char', 'clob', 'json', 'url', 'blob')");	// only for variable character/bytes data type columns
		String qry = sb.toString();
		long count = runCountQuery(qry);
		System.out.println("Checking " + count + " columns in schema " + schema + " for " + check_type + " violations.");

		Statement stmt = createStatement("verifyMaxCharStrLength()");
		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			sb.append("SELECT s.name as sch_nm, t.name as tbl_nm, c.name as col_nm, c.type_digits")	// , t.type, t.system, c.type
			.append(qry).append(" ORDER BY s.name, t.name, c.name, c.type_digits;");
			qry = sb.toString();
			if (stmt != null)
				rs = stmt.executeQuery(qry);
			if (rs != null) {
				long max_len = 0;
				String sch, tbl, col;
				while (rs.next()) {
					// retrieve meta data
					sch = rs.getString(1);
					tbl = rs.getString(2);
					col = rs.getString(3);
					max_len = rs.getLong(4);
					// compose validation query for this specific column
					sb.setLength(0);	// empty previous usage of sb
					sb.append("SELECT '").append(sch).append('.').append(tbl).append('.').append(col).append("' as full_col_nm, ")
					.append(max_len).append(" as max_allowed_length, ")
					.append("length(\"").append(col).append("\") as data_length, ")
					.append("\"").append(col).append("\" as data_value")
					.append(" FROM \"").append(sch).append("\".\"").append(tbl).append("\"")
					.append(" WHERE \"").append(col).append("\" IS NOT NULL AND length(\"").append(col).append("\") > ").append(max_len);
					validate(sb.toString(), sch, tbl, col, check_type);
				}
			}
		} catch (SQLException e) {
			System.out.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}


	/* Run a validation query.
	 * It should result in no rows returned.
	 * When rows are returned those are the ones that contain violations.
	 * Retrieve them and convert the results into a (large) violation string
	 * Log/Print the violation.
	 */
	private static void validate(String qry, String sch, String tbl, String cols, String checktype) {
		Statement stmt = createStatement("validate()");
		ResultSet rs = null;
		ResultSetMetaData rsmd;
		StringBuilder sb = new StringBuilder(1024);
		int nr_cols;
		int row;
		String val;
		int tp;
		try {
			if (verbose) {
				System.out.println(qry);
			}
			if (stmt != null)
				rs = stmt.executeQuery(qry);
			if (rs != null) {
				rsmd = rs.getMetaData();
				nr_cols = rsmd.getColumnCount();
				sb.setLength(0);	// empty previous usage of sb
				row = 0;
				while (rs.next()) {
					// query returns found violations
					row++;
					if (row == 1) {
						// print result header once
						for (int i = 1; i <= nr_cols; i++) {
							sb.append((i > 1) ? ", " : "\t");
							sb.append(rsmd.getColumnLabel(i));
						}
						sb.append('\n');
					}
					// retrieve row data
					for (int i = 1; i <= nr_cols; i++) {
						sb.append((i > 1) ? ", " : "\t");
						val = rs.getString(i);
						if (val == null || rs.wasNull()) {
							sb.append("null");
						} else {
							tp = rsmd.getColumnType(i);	// this method is very fast, so no need to cache it outside the loop
							if (tp == Types.VARCHAR || tp == Types.CHAR || tp == Types.CLOB || tp == Types.BLOB) {
								sb.append('"').append(val).append('"');
							} else {
								sb.append(val);
							}
						}
					}
					sb.append('\n');
				}
				if (row > 0)
					logViolations(checktype, sch, tbl, cols, qry, sb.toString());
			}
		} catch (SQLException e) {
			System.out.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	private static long runCountQuery(String from_qry) {
		Statement stmt = createStatement("runCountQuery()");
		ResultSet rs = null;
		long count = 0;
		try {
			if (stmt != null)
				rs = stmt.executeQuery("SELECT COUNT(*) " + from_qry);
			if (rs != null) {
				if (rs.next()) {
					// retrieve count data
					count = rs.getLong(1);
				}
			}
		} catch (SQLException e) {
			System.out.println("Failed to execute select count(*) " + from_qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
		return count;
	}

	private static Statement createStatement(String method) {
		try {
			return con.createStatement();
		} catch (SQLException e) {
			System.out.print("Failed to create Statement in " + method);
			printExceptions(e);
		}
		return null;
	}

	//	private static java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS");
	private static void logViolations(String checktype, String schema, String table, String columns, String query, String violations) {
//		String dts = df.format(new java.util.Date());
		StringBuilder sb = new StringBuilder(8192);
//		sb.append(dts).append('\t')
		sb.append(checktype).append(" violation(s) found in \"")
		  .append(schema).append("\".\"").append(table).append("\".\"").append(columns).append("\":\n")
		  .append(violations)
		  .append("Found using query: ").append(query).append("\n");
		System.out.println(sb.toString());
	}

	private static boolean isValidVersion(String version) {
		if (version != null) {
			try {
				int v = Integer.parseInt(version);
				return minorversion >= v;
			} catch (NumberFormatException e) {
				/* ignore */
			}
			return false;
		}
		return true;	// when no version string is supplied it is valid by default
	}

	/* test if the standard log table already exists in target DB.
	 * if not try to create it.
	 */
/*	private static boolean create_log_table(Connection con) {
		private static final String log_tbl_name = "sys.\"" + prg + "_log\"";
		boolean has_log_table = false;
		Statement stmt = createStatement("create_log_table()");
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery("SELECT * FROM " + log_tbl_name + " LIMIT 1;");
			if (rs != null) {
				if (rs.next()) {
					has_log_table = true;
					// ToDo check that the columns are the correct signature
				}
			}
		} catch (SQLException e0) {
			// we will get here when the log table does not exist yet
			try {
				// create it ourselves
				int upd = stmt.executeUpdate("CREATE TABLE " + log_tbl_name + " IF NOT EXISTS "
					+ "(datetime   TIMESTAMP(6) NOT NULL,"
					+ " schema_nm  VARCHAR(1024) NOT NULL,"
					+ " table_nm   VARCHAR(1024) NOT NULL,"
					+ " column_nm  VARCHAR(10240) NOT NULL,"
					+ " validation VARCHAR(99999) NOT NULL,"
					+ " violations VARCHAR(99999) NOT NULL);");
				if (upd >= 0)
					has_log_table = true;
			} catch (SQLException e) {
				System.out.print("Failed to create table " + log_tbl_name);
				printExceptions(e);
			}
		}
		freeStmtRs(stmt, rs);
		return has_log_table;
	}
*/

	private static void printExceptions(SQLException se) {
		while (se != null) {
			System.out.println(se.getSQLState() + " " + se.getMessage());
			se = se.getNextException();
		}
	}

	private static void freeStmtRs(Statement stmt, ResultSet rs) {
		// free resources
		if (rs != null) {
			try { rs.close(); } catch (SQLException e) { /* ignore */ }
		}
		if (stmt != null) {
			try { stmt.close(); } catch (SQLException e) { /* ignore */ }
		}
	}


// ********* below are multiple 2-dimensional String arrays containing the data for constructing the validation queries *********
	// based on data from: https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests

	// static list of all sys tables with its pkey columns
	// each entry contains: table_nm, pk_col_nms, from_minor_version
	// data pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_PrimaryKey_uniqueness.sql
	private static final String[][] sys_pkeys = {
		{"schemas", "id", null},
		{"table_types", "table_type_id", null},
		{"_tables", "id", null},
		{"tables", "id", null},	// is a view
		{"_columns", "id", null},
		{"columns", "id", null},	// is a view
		{"function_types", "function_type_id", null},
		{"function_languages", "language_id", null},
		{"functions", "id", null},
		{"systemfunctions", "function_id", null},	// has become a view in Mar2018 and maybe removed in the future as is deprecated
		{"args", "id", null},
		{"types", "id", null},
		{"objects", "id, nr", null},
		{"key_types", "key_type_id", null},
		{"keys", "id", null},
		{"index_types", "index_type_id", null},
		{"idxs", "id", null},
		{"triggers", "id", null},
		{"sequences", "id", null},
		{"comments", "id", null},
		{"dependency_types", "dependency_type_id", null},
		{"dependencies", "id, depend_id", null},
		{"ids", "id", null},	// is a view
		{"auths", "id", null},
		{"users", "name", null},
		{"user_role", "login_id, role_id", null},
		{"privilege_codes", "privilege_code_id", null},
		{"privileges", "obj_id, auth_id, privileges", null},
		{"querylog_catalog", "id", null},
		{"querylog_calls", "id", null},
		{"querylog_history", "id", null},
// old	{"queue", "qtag", null},	// queue has changed in Jun2020 (11.37.7), pkey was previously qtag
		{"queue", "tag", "37"},		// queue has changed in Jun2020 (11.37.7), pkey is now called tag
		{"optimizers", "name", null},
		{"environment", "name", null},
		{"keywords", "keyword", null},
		{"db_user_info", "name", null},
// old	{"sessions", "\"user\", login, active", null},	// sessions has changed in Jun2020 (11.37.7), pkey was previously "user", login, active
		{"sessions", "sessionid", "37"},	// sessions has changed in Jun2020 (11.37.7), pkey is now called sessionid
		{"statistics", "column_id", null},
		{"rejects", "rowid", "19"},	// querying this view caused problems in versions pre Jul2015, see https://www.monetdb.org/bugzilla/show_bug.cgi?id=3794
// old	{"tracelog", "event", null},		-- Error: Profiler not started. This table now (Jun2020) contains only: ticks, stmt
//		{"storage", "schema, table, column", null},	// is a view on storage()
		{"\"storage\"()", "schema, table, column", null},	// the function "storage"() also lists the storage for system tables
	// new views introduced in Apr 2019 feature release (11.33.3)
//		{"tablestorage", "schema, table", "33"},	// is a view on view storage
//		{"schemastorage", "schema", "33"},	// is a view on view storage
		{"storagemodelinput", "schema, table, column", null},
//		{"storagemodel", "schema, table, column", null},	// is a view on storagemodelinput
//		{"tablestoragemodel", "schema, table", null},	// is a view on storagemodelinput
	// new tables introduced in Apr 2019 feature release (11.33.3)
		{"table_partitions", "id", "33"},
		{"range_partitions", "table_id, partition_id, minimum", "33"},
		{"value_partitions", "table_id, partition_id, \"value\"", "33"}
	};

	private static final String[][] tmp_pkeys = {
		{"_tables", "id", null},
		{"_columns", "id", null},
		{"objects", "id, nr", null},
		{"keys", "id", null},
		{"idxs", "id", null},
		{"triggers", "id", null}
	};

	private static final String[][] netcdf_pkeys = {
		{"netcdf_files", "file_id", null},
		{"netcdf_dims", "dim_id, file_id", null},
		{"netcdf_vars", "var_id, file_id", null},
		{"netcdf_vardim", "var_id, dim_id, file_id", null}
	};

	private static final String[][] geom_pkeys = {
		{"spatial_ref_sys", "srid", null}
	};

	
	// static list of all sys tables with its alternate key (unique constraint) columns
	// each entry contains: table_nm, ak_col_nms, from_minor_version
	// data pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_AlternateKey_uniqueness.sql
	private static final String[][] sys_akeys = {
		{"schemas", "name", null},
		{"table_types", "table_type_name", null},
		{"_tables", "schema_id, name", null},
		{"tables", "schema_id, name", null},	// is a view
		{"_columns", "table_id, name", null},
		{"columns", "table_id, name", null},	// is a view
		{"_columns", "table_id, number", null},
		{"columns", "table_id, number", null},	// is a view
		// The id values from sys.schemas, sys._tables, sys._columns and sys.functions combined must be exclusive (see FK from sys.privileges.obj_id)
		{"(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) T", "T.id", null},
		{"(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys.tables UNION ALL SELECT id FROM sys.columns UNION ALL SELECT id FROM sys.functions) T", "T.id", null},
		{"function_types", "function_type_name", null},
		{"function_languages", "language_name", null},
		// the next query used to return duplicates for overloaded functions (same function but with different arg names/types), hence it has been extended
		{"functions f join sys.args a on f.id=a.func_id", "schema_id, f.name, func, mod, language, f.type, side_effect, varres, vararg, a.id", null},
		{"args", "func_id, name, inout", null},
		{"types", "schema_id, systemname, sqlname", null},
		{"objects", "id, name", null},
		{"key_types", "key_type_name", null},
		{"keys", "table_id, name", null},
		{"index_types", "index_type_name", null},
		{"idxs", "table_id, name", null},
		{"triggers", "table_id, name", null},
		{"sequences", "schema_id, name", null},
		{"comments", "id", null},
		{"dependency_types", "dependency_type_name", null},
		{"auths", "name", null},		// is this always unique?? isn't it possible to define a user and a role with the same name?
		{"privilege_codes", "privilege_code_name", null},
		{"optimizers", "def", null},
	// new tables introduced in Apr 2019 feature release (11.33.3)
		{"table_partitions WHERE column_id IS NOT NULL", "table_id, column_id", "33"},	// requires WHERE "column_id" IS NOT NULL
		{"table_partitions WHERE \"expression\" IS NOT NULL", "table_id, \"expression\"", "33"},	// requires WHERE "expression" IS NOT NULL
		{"range_partitions", "table_id, partition_id, \"maximum\"", "33"}
	};

	private static final String[][] tmp_akeys = {
		{"_tables", "schema_id, name", null},
		{"_columns", "table_id, name", null},
		{"_columns", "table_id, number", null},
		{"objects", "id, name", null},
		{"keys", "table_id, name", null},
		{"idxs", "table_id, name", null},
		{"triggers", "table_id, name", null}
	};

	private static final String[][] netcdf_akeys = {
		{"netcdf_files", "location", null}
	};

	private static final String[][] geom_akeys = {
		{"spatial_ref_sys", "auth_name, auth_srid, srtext, proj4text", null}
	};


	// static list of all sys tables with its foreign key columns
	// each entry contains: table_nm, fk_col_nms, ref_col_nms, ref_tbl_nm, from_minor_version
	// data pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_ForeignKey_referential_integrity.sql
	private static final String[][] sys_fkeys = {
		{"schemas", "authorization", "id", "auths", null},
		{"schemas", "owner", "id", "auths", null},
		{"_tables", "schema_id", "id", "schemas", null},
		{"tables", "schema_id", "id", "schemas", null},
		{"_tables", "type", "table_type_id", "table_types", null},
		{"tables", "type", "table_type_id", "table_types", null},
		{"_columns", "table_id", "id", "_tables", null},
		{"columns", "table_id", "id", "tables", null},
		{"_columns", "type", "sqlname", "types", null},
		{"columns", "type", "sqlname", "types", null},
		{"functions", "schema_id", "id", "schemas", null},
		{"functions", "type", "function_type_id", "function_types", null},
		{"functions", "language", "language_id", "function_languages", null},
		// system functions should refer only to functions in MonetDB system schemas (on Dec2016 these are: sys, json, profiler and bam)
		{"functions WHERE system AND ", "schema_id", "id", "schemas WHERE system", null},
		{"args", "func_id", "id", "functions", null},
		{"args", "type", "sqlname", "types", null},
		{"types", "schema_id", "id", "schemas", null},
	//	{"types WHERE schema_id <> 0 AND ", "schema_id", "id", "schemas", null},
		{"objects", "id", "id", "ids", null},
		{"ids WHERE obj_type IN ('key', 'index') AND ", "id", "id", "objects", null},
		{"keys", "id", "id", "objects", null},
		{"keys", "table_id", "id", "_tables", null},
		{"keys", "table_id", "id", "tables", null},
		{"keys", "type", "key_type_id", "key_types", null},
		{"keys WHERE rkey <> -1 AND ", "rkey", "id", "keys", null},
// SELECT * FROM sys.keys WHERE action <> -1 AND action NOT IN (SELECT id FROM sys.?);  -- TODO: find out which action values are valid and what they mean.
		{"idxs", "id", "id", "objects", null},
		{"idxs", "table_id", "id", "_tables", null},
		{"idxs", "table_id", "id", "tables", null},
		{"idxs", "type", "index_type_id", "index_types", null},
		{"sequences", "schema_id", "id", "schemas", null},
		{"triggers", "table_id", "id", "_tables", null},
		{"triggers", "table_id", "id", "tables", null},
		{"comments", "id", "id", "ids", null},
		{"dependencies", "id", "id", "ids", null},
		{"dependencies", "depend_id", "id", "ids", null},
		{"dependencies", "depend_type", "dependency_type_id", "dependency_types", null},
		{"dependencies", "id, depend_id, depend_type", "v.id, v.used_by_id, v.depend_type", "dependencies_vw v", null},
		{"auths WHERE grantor > 0 AND ", "grantor", "id", "auths", null},
		{"users", "name", "name", "auths", null},
		{"users", "default_schema", "id", "schemas", null},
		{"db_user_info", "name", "name", "auths", null},
		{"db_user_info", "default_schema", "id", "schemas", null},
		{"user_role", "login_id", "id", "auths", null},
		{"user_role", "login_id", "a.id", "auths a WHERE a.name IN (SELECT u.name FROM sys.users u)", null},
		{"user_role", "role_id", "id", "auths", null},
		{"user_role", "role_id", "a.id", "auths a WHERE a.name IN (SELECT u.name FROM sys.users u)", null},
		{"user_role", "role_id", "id", "roles", null}
/* hier verder afmaken
SELECT * FROM sys.privileges WHERE obj_id NOT IN (SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions);
SELECT * FROM sys.privileges WHERE auth_id NOT IN (SELECT id FROM sys.auths);
SELECT * FROM sys.privileges WHERE grantor NOT IN (SELECT id FROM sys.auths) AND grantor > 0;
SELECT * FROM sys.privileges WHERE privileges NOT IN (SELECT privilege_code_id FROM sys.privilege_codes);
--SELECT * FROM sys.privileges WHERE privileges NOT IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,32); -- old check before table sys.privilege_codes existed

SELECT * FROM sys.querylog_catalog WHERE owner NOT IN (SELECT name FROM sys.users);
SELECT * FROM sys.querylog_catalog WHERE pipe NOT IN (SELECT name FROM sys.optimizers);
SELECT * FROM sys.querylog_calls   WHERE id NOT IN (SELECT id FROM sys.querylog_catalog);
SELECT * FROM sys.querylog_history WHERE id NOT IN (SELECT id FROM sys.querylog_catalog);
SELECT * FROM sys.querylog_history WHERE owner NOT IN (SELECT name FROM sys.users);
SELECT * FROM sys.querylog_history WHERE pipe NOT IN (SELECT name FROM sys.optimizers);

SELECT * FROM sys.queue WHERE tag > cast(0 as oid) AND tag NOT IN (SELECT tag FROM sys.queue);
SELECT * FROM sys.queue WHERE tag > cast(0 as oid) AND tag NOT IN (SELECT cast(tag as oid) FROM sys.queue);
SELECT * FROM sys.queue WHERE tag NOT IN (SELECT cast(tag as oid) FROM sys.queue);
SELECT * FROM sys.queue WHERE "username" NOT IN (SELECT name FROM sys.users);

SELECT * FROM sys.sessions WHERE "username" NOT IN (SELECT name FROM sys.users);

SELECT * FROM sys.statistics WHERE column_id NOT IN (SELECT id FROM sys._columns UNION ALL SELECT id FROM tmp._columns);
SELECT * FROM sys.statistics WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storage() WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storage() WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storage() WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);
SELECT * FROM sys.storage() WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs);
SELECT * FROM sys.storage() WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storage WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storage WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storage WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);
SELECT * FROM sys.storage WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs);
SELECT * FROM sys.storage WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.tablestorage WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.tablestorage WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.tablestorage WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);

SELECT * FROM sys.schemastorage WHERE schema NOT IN (SELECT name FROM sys.schemas);

SELECT * FROM sys.storagemodel WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storagemodel WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storagemodel WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);
SELECT * FROM sys.storagemodel WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs);
SELECT * FROM sys.storagemodel WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storagemodelinput WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storagemodelinput WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storagemodelinput WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);
SELECT * FROM sys.storagemodelinput WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs);
SELECT * FROM sys.storagemodelinput WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT schema, table, rowcount, columnsize, heapsize, hashsize, imprintsize, orderidxsize FROM sys.tablestoragemodel WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT schema, table, rowcount, columnsize, heapsize, hashsize, imprintsize, orderidxsize FROM sys.tablestoragemodel WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT schema, table, rowcount, columnsize, heapsize, hashsize, imprintsize, orderidxsize FROM sys.tablestoragemodel WHERE (schema, table) NOT IN (SELECT sch.name, tbl.name FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id);

-- new tables introduced in 2019
SELECT * FROM sys.table_partitions WHERE "table_id" NOT IN (SELECT id FROM sys._tables);
SELECT * FROM sys.table_partitions WHERE "column_id" IS NOT NULL AND "column_id" NOT IN (SELECT id FROM sys._columns);
SELECT * FROM sys.table_partitions WHERE "type" NOT IN (5,6,9,10);	-- 5=By Column Range (1+4), 6=By Expression Range (2+4), 9=By Column Value (1+8), 10=By Expression Value (2+8), see sql_catalog.h #define PARTITION_*

SELECT * FROM sys.range_partitions WHERE "table_id" NOT IN (SELECT id FROM sys._tables);
SELECT * FROM sys.range_partitions WHERE "partition_id" NOT IN (SELECT id FROM sys.table_partitions);

SELECT * FROM sys.value_partitions WHERE "table_id" NOT IN (SELECT id FROM sys._tables);
SELECT * FROM sys.value_partitions WHERE "partition_id" NOT IN (SELECT id FROM sys.table_partitions);
*/
	};

	private static final String[][] tmp_fkeys = {
		{"_tables", "schema_id", "id", "sys.schemas", null},
		{"_tables", "type", "table_type_id", "sys.table_types", null},
		{"_columns", "table_id", "id", "_tables", null},
		{"_columns", "type", "sqlname", "sys.types", null},
		{"keys", "table_id", "id", "_tables", null},
		{"keys", "type", "key_type_id", "sys.key_types", null},
		{"keys WHERE rkey <> -1 AND ", "rkey", "id", "keys", null},
// SELECT * FROM tmp.keys WHERE action <> -1 AND action NOT IN (SELECT id FROM tmp.?);  -- TODO: find out which action values are valid and what they mean.
		{"idxs", "id", "id", "objects", null},
		{"idxs", "table_id", "id", "_tables", null},
		{"idxs", "type", "index_type_id", "sys.index_types", null},
		{"triggers", "table_id", "id", "_tables", null}
		// nog verder afmaken, zie fkey data hierboven
	};

	private static final String[][] netcdf_fkeys = {
		{"netcdf_attrs", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_dims", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vars", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vardim", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vardim", "dim_id", "dim_id", "netcdf_dims", null},
		{"netcdf_vardim", "dim_id, file_id", "dim_id, file_id", "netcdf_dims", null},
		{"netcdf_vardim", "var_id", "var_id", "netcdf_vars", null},
		{"netcdf_vardim", "var_id, file_id", "var_id, file_id", "netcdf_vars", null}
	};

	private static final String[][] geom_fkeys = {
		{"spatial_ref_sys", "auth_srid", "srid", "spatial_ref_sys", null}
	};


	// static list of all sys tables with its not null constraint columns
	// each entry contains: table_nm, col_nm, from_minor_version
	// data pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_Not_Nullable_columns.sql
	private static final String[][] sys_notnull = {
		{"_columns", "id", null},
		{"_columns", "name", null},
		{"_columns", "type", null},
		{"_columns", "type_digits", null},
		{"_columns", "type_scale", null},
		{"_columns", "table_id", null},
		{"_columns", "\"null\"", null},
		{"_columns", "number", null},
		{"_tables", "id", null},
		{"_tables", "name", null},
		{"_tables", "schema_id", null},
		{"_tables", "type", null},
		{"_tables", "system", null},
		{"_tables", "commit_action", null},
		{"_tables", "access", null},
		{"args", "id", null},
		{"args", "func_id", null},
		{"args", "name", null},
		{"args", "type", null},
		{"args", "type_digits", null},
		{"args", "type_scale", null},
		{"args", "inout", null},
		{"args", "number", null},
		{"auths", "id", null},
		{"auths", "name", null},
		{"auths", "grantor", null},
		{"db_user_info", "name", null},
		{"db_user_info", "fullname", null},
		{"db_user_info", "default_schema", null},
		{"dependencies", "id", null},
		{"dependencies", "depend_id", null},
		{"dependencies", "depend_type", null},
		{"function_languages", "language_id", null},
		{"function_languages", "language_name", null},
		{"function_types", "function_type_id", null},
		{"function_types", "function_type_name", null},
		{"function_types", "function_type_keyword", null},
		{"functions", "id", null},
		{"functions", "name", null},
		{"functions", "func", null},
		{"functions", "mod", null},
		{"functions", "language", null},
		{"functions", "type", null},
		{"functions", "side_effect", null},
		{"functions", "varres", null},
		{"functions", "vararg", null},
		{"functions", "schema_id", null},
		{"functions", "system", null},
		{"idxs", "id", null},
		{"idxs", "table_id", null},
		{"idxs", "type", null},
		{"idxs", "name", null},
		{"index_types", "index_type_id", null},
		{"index_types", "index_type_name", null},
		{"key_types", "key_type_id", null},
		{"key_types", "key_type_name", null},
		{"keys", "id", null},
		{"keys", "table_id", null},
		{"keys", "type", null},
		{"keys", "name", null},
		{"keys", "rkey", null},
		{"keys", "action", null},
		{"keywords", "keyword", null},
		{"objects", "id", null},
		{"objects", "name", null},
		{"objects", "nr", null},
		{"optimizers", "name", null},
		{"optimizers", "def", null},
		{"optimizers", "status", null},
		{"privilege_codes", "privilege_code_id", null},
		{"privilege_codes", "privilege_code_name", null},
		{"privileges", "obj_id", null},
		{"privileges", "auth_id", null},
		{"privileges", "privileges", null},
		{"privileges", "grantor", null},
		{"privileges", "grantable", null},
		{"schemas", "id", null},
		{"schemas", "name", null},
		{"schemas", "authorization", null},
		{"schemas", "owner", null},
		{"schemas", "system", null},
		{"sequences", "id", null},
		{"sequences", "schema_id", null},
		{"sequences", "name", null},
		{"sequences", "start", null},
		{"sequences", "minvalue", null},
		{"sequences", "maxvalue", null},
		{"sequences", "increment", null},
		{"sequences", "cacheinc", null},
		{"sequences", "cycle", null},
		{"statistics", "column_id", null},
		{"statistics", "type", null},
		{"statistics", "width", null},
		{"statistics", "stamp", null},
		{"statistics", "\"sample\"", null},
		{"statistics", "count", null},
		{"statistics", "\"unique\"", null},
		{"statistics", "nils", null},
		{"statistics", "sorted", null},
		{"statistics", "revsorted", null},
/* temporary disabled, TODO re-enable
		{"storage", "schema", null},
		{"storage", "table", null},
		{"storage", "column", null},
		{"storage", "type", null},
		{"storage", "mode", null},
		{"storage", "location", null},
		{"storage", "count", null},
		{"storage", "typewidth", null},
		{"storage", "columnsize", null},
		{"storage", "heapsize", null},
		{"storage", "hashes", null},
		{"storage", "phash", null},
		{"storage", "imprints", null},
		{"storage", "orderidx", null},
*/		{"storagemodelinput", "schema", null},
		{"storagemodelinput", "table", null},
		{"storagemodelinput", "column", null},
		{"storagemodelinput", "type", null},
		{"storagemodelinput", "typewidth", null},
		{"storagemodelinput", "count", null},
		{"storagemodelinput", "\"distinct\"", null},
		{"storagemodelinput", "atomwidth", null},
		{"storagemodelinput", "reference", null},
		{"storagemodelinput", "sorted", null},
		{"storagemodelinput", "\"unique\"", null},
		{"storagemodelinput", "isacolumn", null},
		{"table_types", "table_type_id", null},
		{"table_types", "table_type_name", null},
		{"tables", "id", null},
		{"tables", "name", null},
		{"tables", "schema_id", null},
		{"tables", "type", null},
		{"tables", "system", null},
		{"tables", "commit_action", null},
		{"tables", "access", null},
		{"tables", "temporary", null},
		{"tracelog", "ticks", null},
		{"tracelog", "stmt", null},
		{"triggers", "id", null},
		{"triggers", "name", null},
		{"triggers", "table_id", null},
		{"triggers", "time", null},
		{"triggers", "orientation", null},
		{"triggers", "event", null},
		{"triggers", "statement", null},
		{"types", "id", null},
		{"types", "systemname", null},
		{"types", "sqlname", null},
		{"types", "digits", null},
		{"types", "scale", null},
		{"types", "radix", null},
		{"types", "eclass", null},
		{"types", "schema_id", null},
		{"user_role", "login_id", null},
		{"user_role", "role_id", null},
		{"users", "name", null},
		{"users", "fullname", null},
		{"users", "default_schema", null},
		{"var_values", "var_name", null},
		{"var_values", "value", null},
	// new tables introduced in Apr 2019 feature release (11.33.3)
		{"range_partitions", "table_id", "33"},
		{"range_partitions", "partition_id", "33"},
		{"range_partitions", "with_nulls", "33"},
		{"table_partitions", "id", "33"},
		{"table_partitions", "table_id", "33"},
		{"table_partitions", "type", "33"},
		{"value_partitions", "table_id", "33"},
		{"value_partitions", "partition_id", "33"},
		{"value_partitions", "value", "33"}		// ?? Can this be null when WITH NULL VALUES is specified.
	};

	private static final String[][] tmp_notnull = {
		{"_columns", "id", null},
		{"_columns", "name", null},
		{"_columns", "type", null},
		{"_columns", "type_digits", null},
		{"_columns", "type_scale", null},
		{"_columns", "table_id", null},
		{"_columns", "\"null\"", null},
		{"_columns", "number", null},
		{"_tables", "id", null},
		{"_tables", "name", null},
		{"_tables", "schema_id", null},
		{"_tables", "type", null},
		{"_tables", "system", null},
		{"_tables", "commit_action", null},
		{"_tables", "access", null},
		{"idxs", "id", null},
		{"idxs", "table_id", null},
		{"idxs", "type", null},
		{"idxs", "name", null},
		{"keys", "id", null},
		{"keys", "table_id", null},
		{"keys", "type", null},
		{"keys", "name", null},
		{"keys", "rkey", null},
		{"keys", "action", null},
		{"objects", "id", null},
		{"objects", "name", null},
		{"objects", "nr", null},
		{"triggers", "id", null},
		{"triggers", "name", null},
		{"triggers", "table_id", null},
		{"triggers", "time", null},
		{"triggers", "orientation", null},
		{"triggers", "event", null},
		{"triggers", "statement", null}
	};

	private static final String[][] netcdf_notnull = {
		{"netcdf_files", "file_id", null},
		{"netcdf_files", "location", null},
		{"netcdf_dims", "dim_id", null},
		{"netcdf_dims", "file_id", null},
		{"netcdf_dims", "name", null},
		{"netcdf_dims", "length", null},
		{"netcdf_vars", "var_id", null},
		{"netcdf_vars", "file_id", null},
		{"netcdf_vars", "name", null},
		{"netcdf_vars", "vartype", null},
		{"netcdf_vardim", "var_id", null},
		{"netcdf_vardim", "dim_id", null},
		{"netcdf_vardim", "file_id", null},
		{"netcdf_vardim", "dimpos", null},
		{"netcdf_attrs", "obj_name", null},
		{"netcdf_attrs", "att_name", null},
		{"netcdf_attrs", "att_type", null},
		{"netcdf_attrs", "value", null},
		{"netcdf_attrs", "file_id", null},
		{"netcdf_attrs", "gr_name", null}
	};

	private static final String[][] geom_notnull = {
		{"spatial_ref_sys", "srid", null},
		{"spatial_ref_sys", "auth_name", null},
		{"spatial_ref_sys", "auth_srid", null},
		{"spatial_ref_sys", "srtext", null},
		{"spatial_ref_sys", "proj4text", null}
	};
}
