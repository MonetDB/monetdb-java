/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;

public class Test_PSmanycon {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		List<PreparedStatement> pss = new ArrayList<PreparedStatement>(100);	// PreparedStatements go in here

		try {
			// spawn a lot of Connections with 1 PreparedStatement, just for fun...
			int i;
			for (i = 0; i < 50; i++) {
				System.out.print("Establishing Connection " + i + "...");
				Connection con = DriverManager.getConnection(args[0]);
				System.out.print(" done...");

				// do something with the connection to test if it works
				PreparedStatement pstmt = con.prepareStatement("select " + i);
				System.out.println(" alive");

				pss.add(pstmt);
			}

			// now try to nicely execute them
			i = 0;
			for (Iterator<PreparedStatement> it = pss.iterator(); it.hasNext(); i++) {
				PreparedStatement pstmt = it.next();

				// see if the connection still works
				System.out.print("Executing PreparedStatement " + i + "...");
				if (!pstmt.execute())
					throw new Exception("should have seen a ResultSet!");

				ResultSet rs = pstmt.getResultSet();
				if (!rs.next())
					throw new Exception("ResultSet is empty");
				System.out.print(" result: " + rs.getString(1));

				// close the connection and associated resources
				pstmt.getConnection().close();
				System.out.println(", done");

				if ((i + 1) % 23 == 0) {
					// inject a failed transaction
					Connection con = DriverManager.getConnection(args[0]);
					Statement stmt = con.createStatement();
					try {
						int affrows = stmt.executeUpdate("update foo where bar is wrong");
						System.out.println("oops, faulty statement just got through :(");
					} catch (SQLException e) {
						System.out.println("Forced transaction failure");
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("FAILED! " + e.getMessage());
		}
	}
}
