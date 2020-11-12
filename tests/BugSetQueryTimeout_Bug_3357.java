/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class BugSetQueryTimeout_Bug_3357 {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		Statement st = con.createStatement();
		try {
			System.out.println("QueryTimeout = " + st.getQueryTimeout());

			testTimeout(st, 123);
			testTimeout(st, 123);
			testTimeout(st, 2134567890);
			testTimeout(st, 0);
			testTimeout(st, 0);
			testTimeout(st, -1);	// to generate an SQLException as negative timeouts are invalid
		} catch (SQLException se) {
			System.out.println("setQueryTimeout(timeout_value) throws: " + se);
		} finally {
			st.close();
		}
		con.close();
	}

	private static void testTimeout(Statement st, int secs) throws SQLException {
		st.setQueryTimeout(secs);
		// as the call to set the timeout is delayed till a statement is executed, issue a select statment
		ResultSet rs = st.executeQuery("SELECT " + secs);
		if (rs != null)
			rs.close();
		System.out.println("QueryTimeout = " + st.getQueryTimeout());
	}
}
