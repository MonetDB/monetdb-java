/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

import java.sql.*;
import java.io.*;
import java.util.*;

import nl.cwi.monetdb.mcl.connection.mapi.MapiConnection;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;

/**
 * This example demonstrates how the MonetDB JDBC driver can facilitate
 * in performing COPY INTO sequences.  This is mainly meant to show how
 * a quick load can be performed from Java.
 *
 * @author Fabian Groffen
 */

public class SQLcopyinto {
	public static void main(String[] args) throws Exception {
		// request a connection suitable for Monet from the driver manager
		// note that the database specifier is currently not implemented, for
		// Monet itself can't access multiple databases.
		// turn on debugging
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");

		// get a statement to execute on
		Statement stmt = con.createStatement();

		String query = "CREATE TABLE example (id int, val varchar(24))";
		try {
			stmt.execute(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			System.exit(1);
		}

		// now create a connection manually to perform a load, this can
		// of course also be done simultaneously with the JDBC
		// connection being kept connected

		MapiConnection server = new MapiConnection(null, null, "sql", false, true,"localhost", 50000, "database");

		try {
			List warning = server.connect("monetdb", "monetdb");
			if (warning != null) {
				for (Object aWarning : warning) {
					System.out.println(aWarning.toString());
				}
			}
			AbstractProtocol oldmMapiProtocol = server.getProtocol();

			oldmMapiProtocol.waitUntilPrompt();
			String error = oldmMapiProtocol.getRemainingStringLine(0);
			if (error != null)
				throw new Exception(error);

			query = "COPY INTO example FROM STDIN USING DELIMITERS ',','\\n';";
			// the leading 's' is essential, since it is a protocol
			// marker that should not be omitted, likewise the
			// trailing semicolon
			oldmMapiProtocol.writeNextQuery("s", query, "\n");

			for (int i = 0; i < 100; i++) {
				oldmMapiProtocol.writeNextQuery(null, "" + i + ",val_" + i, "\n");
			}
			oldmMapiProtocol.waitUntilPrompt();
			error = oldmMapiProtocol.getRemainingStringLine(0);
			if (error != null)
				throw new Exception(error);
			// disconnect from server
			server.close();
		} catch (IOException e) {
			System.err.println("unable to connect: " + e.getMessage());
			System.exit(-1);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		query = "SELECT COUNT(*) FROM example";
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			System.exit(1);
		}
		if (rs != null && rs.next())
			System.out.println(rs.getString(1));

		// free resources, close the statement
		stmt.close();
		// close the connection with the database
		con.close();

	}
}
