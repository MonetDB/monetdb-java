/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class BugSetQueryTimeout_Bug_3357 {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement st = con.createStatement();
		try {
			System.out.println("QueryTimeout = " + st.getQueryTimeout());

			st.setQueryTimeout(123);
			System.out.println("QueryTimeout = " + st.getQueryTimeout());

			st.setQueryTimeout(2134567890);
			System.out.println("QueryTimeout = " + st.getQueryTimeout());

			st.setQueryTimeout(0);
			System.out.println("QueryTimeout = " + st.getQueryTimeout());

			st.setQueryTimeout(-1);	// to generate an SQLException as negative timeouts are invalid
			System.out.println("QueryTimeout = " + st.getQueryTimeout());
		} catch (SQLException se) {
			System.out.println("setQueryTimeout(timeout_value) throws: " + se);
		} finally {
			st.close();
		}
		con.close();
	}
}
