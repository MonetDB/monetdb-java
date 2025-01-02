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

import java.sql.*;
import java.util.*;
import org.monetdb.mcl.net.MapiSocket;
import org.monetdb.mcl.io.BufferedMCLReader;
import org.monetdb.mcl.io.BufferedMCLWriter;

/**
 * This program demonstrates how the MonetDB JDBC driver can facilitate
 * in performing COPY INTO ... FROM STDIN sequences.
 * It shows how a data stream via MapiSocket to STDIN can be performed.
 *
 * @author Fabian Groffen, Martin van Dinther
 */

public final class SQLcopyinto {
	final private static String tablenm = "exampleSQLCopyInto";

	public static void main(String[] args) throws Exception {
		System.out.println("SQLcopyinto started");
		if (args.length == 0) {
			System.err.println("Error: missing startup argument: the jdbc connection url !");
			System.err.println("Usage: java -cp monetdb-jdbc-3.0.jre8.jar:. SQLcopyinto \"jdbc:monetdb://localhost:50000/demo?user=monetdb&password=monetdb\"");
			System.exit(-1);
		}
		String jdbc_url = args[0];

		// request a connection to MonetDB server via the driver manager
		Connection conn = DriverManager.getConnection(jdbc_url);
		System.out.println("Connected to MonetDB server");

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS " + tablenm + " (id int, val varchar(24))");

			fillTableUsingCopyIntoSTDIN(conn);

			// check content of the table populated via COPY INTO ... FROM STDIN
			System.out.println("Listing uploaded data:");
			rs = stmt.executeQuery("SELECT * FROM " + tablenm);
			if (rs != null) {
				while (rs.next()) {
					System.out.println("Row data: " + rs.getString(1) + ", " + rs.getString(2));
				}
				rs.close();
				rs = null;
			}

			stmt.execute("DROP TABLE " + tablenm);

			System.out.println("SQLcopyinto completed");
		} catch (SQLException e) {
			System.err.println("SQLException: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
		} finally {
			// free resources
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			// close the JDBC connection to MonetDB server
			if (conn != null)
				conn.close();
		}
	}

	public static void fillTableUsingCopyIntoSTDIN(Connection mcon) throws Exception {
		System.out.println();
		System.out.println("CopyInto STDIN begin");

		MapiSocket server = new MapiSocket();
		try {
			server.setLanguage("sql");

			// extract from MonetConnection object the used connection properties
			String host = mcon.getClientInfo("host");
			int port = Integer.parseInt(mcon.getClientInfo("port"));
			String login = mcon.getClientInfo("user");
			String passw = mcon.getClientInfo("password");
			// System.out.println("host: " + host + " port: " + port + " login: " + login + " passwd: " + passw);

			System.out.println("Before connecting to MonetDB server via MapiSocket");
			List<String> warning = server.connect(host, port, login, passw);
			if (warning != null) {
				for (Iterator<String> it = warning.iterator(); it.hasNext(); ) {
					System.out.println("Warning: " + it.next().toString());
				}
			}
			System.out.println("Connected to MonetDB server via MapiSocket");

			BufferedMCLReader mclIn = server.getReader();
			BufferedMCLWriter mclOut = server.getWriter();

			String error = mclIn.discardRemainder();
			if (error != null)
				System.err.println("Received start error: " + error);

			System.out.println("Before sending data to STDIN");

			// the leading 's' is essential, since it is a protocol marker
			// that should not be omitted, likewise the trailing semicolon
			mclOut.write('s');
			mclOut.write("COPY INTO " + tablenm + " FROM STDIN USING DELIMITERS ',',E'\\n';");
			mclOut.newLine();
			// now write the row data values as csv data lines to the STDIN stream
			for (int i = 0; i < 40; i++) {
				mclOut.write("" + i + ",val_" + i);
				mclOut.newLine();
			}

			mclOut.writeLine(""); // need this one for synchronisation over flush()
			error = mclIn.discardRemainder();
			if (error != null)
				System.err.println("Received error: " + error);

			mclOut.writeLine(""); // need this one for synchronisation over flush()
			error = mclIn.discardRemainder();
			if (error != null)
				System.err.println("Received finish error: " + error);

			System.out.println("Completed sending data via STDIN");
		} catch (Exception e) {
			System.err.println("Mapi Exception: " + e.getMessage());
		} finally {
			// close MapiSocket connection to MonetDB server
			server.close();
		}

		System.out.println("CopyInto STDIN end");
		System.out.println();
	}
}
