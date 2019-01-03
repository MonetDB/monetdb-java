/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;

public class Test_Cmanycon {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		List<Connection> cons = new ArrayList<Connection>(100);	// Connections go in here

		try {
			// spawn a lot of Connections, just for fun...
			int i;
			for (i = 0; i < 50; i++) {
				System.out.print("Establishing Connection " + i + "...");
				Connection con = DriverManager.getConnection(args[0]);
				System.out.print(" done...");

				// do something with the connection to test if it works
				con.setAutoCommit(false);
				System.out.println(" alive");

				cons.add(con);
			}

			// now try to nicely close them
			i = 0;
			for (Iterator<Connection> it = cons.iterator(); it.hasNext(); i++) {
				Connection con = it.next();

				// see if the connection still works
				System.out.print("Closing Connection " + i + "...");
				con.setAutoCommit(true);
				System.out.print(" still alive...");
				con.close();
				System.out.println(" done");
			}
		} catch (SQLException e) {
			System.out.println("FAILED! " + e.getMessage());
		}
	}
}
