/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

import java.sql.*;

/**
 * This example assumes there exists tables a and b filled with some data.
 * On these tables some queries are executed and the JDBC driver is tested
 * on it's accuracy and robustness against 'users'.
 *
 * @author Fabian Groffen
 */
public class MJDBCTest {
	public static void main(String[] args) throws Exception {
		String MonetDB_JDBC_URL = "jdbc:monetdb://localhost:50000/demo";	// change host, port and databasename
		Connection con = null;
		try {
			con = DriverManager.getConnection(MonetDB_JDBC_URL, "monetdb", "monetdb");
		} catch (SQLException e) {
			System.err.println("Failed to connect to MonetDB server! Message: " + e.getMessage());
		}

		if (con == null) {
			System.err.println("Failed to create a connection object!");
			return;
		}

		Statement st = con.createStatement();
		ResultSet rs;

		String sql = "SELECT a.var1, COUNT(b.id) as total FROM a, b WHERE a.var1 = b.id AND a.var1 = 'andb' GROUP BY a.var1 ORDER BY a.var1, total;";
		rs = st.executeQuery(sql);
		// get meta data and print columns with their type
		ResultSetMetaData md = rs.getMetaData();
		for (int i = 1; i <= md.getColumnCount(); i++) {
			System.out.print(md.getColumnName(i) + ":" +
				md.getColumnTypeName(i) + "\t");
		}
		System.out.println("");
		// print the data: only the first 5 rows, while there probably are
		// a lot more. This shouldn't cause any problems afterwards since the
		// result should get properly discarded on the next query
		for (int i = 0; rs.next() && i < 5; i++) {
			for (int j = 1; j <= md.getColumnCount(); j++) {
				System.out.print(rs.getString(j) + "\t");
			}
			System.out.println("");
		}
		
		// tell the driver to only return 5 rows, it can optimize on this
		// value, and will not fetch any more than 5 rows.
		st.setMaxRows(5);
		// we ask the database for 22 rows, while we set the JDBC driver to
		// 5 rows, this shouldn't be a problem at all...
		rs = st.executeQuery("select * from a limit 22");
		int var1_cnr = rs.findColumn("var1");
		int var2_cnr = rs.findColumn("var2");
		int var3_cnr = rs.findColumn("var3");
		int var4_cnr = rs.findColumn("var4");
		// read till the driver says there are no rows left
		for (int i = 0; rs.next(); i++) {
			System.out.println(
				"[" + rs.getString(var1_cnr) + "]" +
				"[" + rs.getString(var2_cnr) + "]" +
				"[" + rs.getInt(var3_cnr) + "]" +
				"[" + rs.getString(var4_cnr) + "]" );
		}

		// this rs.close is not needed, should be done by next execute(Query) call
		// however if there can be some time between this point and the next
		// execute call, it is from a resource perspective better to close it.
		rs.close();
		
		// unset the row limit; 0 means as much as the database sends us
		st.setMaxRows(0);
		// we only ask 10 rows
		rs = st.executeQuery("select * from b limit 10;");
		int rowid_cnr = rs.findColumn("rowid");
		int id_cnr = rs.findColumn("id");
		var1_cnr = rs.findColumn("var1");
		var2_cnr = rs.findColumn("var2");
		var3_cnr = rs.findColumn("var3");
		var4_cnr = rs.findColumn("var4");
		// and simply print them
		while (rs.next()) {
			System.out.println(
				rs.getInt(rowid_cnr) + ", " +
				rs.getString(id_cnr) + ", " +
				rs.getInt(var1_cnr) + ", " +
				rs.getInt(var2_cnr) + ", " +
				rs.getString(var3_cnr) + ", " +
				rs.getString(var4_cnr) );
		}
		
		// this close is not required, as the Statement will close the last
		// ResultSet around when it's closed
		// again, if that can take some time, it's nicer to close immediately
		// the reason why these closes are commented out here, is to test if
		// the driver really cleans up it's mess like it should
		rs.close();

		// perform a ResultSet-less query (with no trailing ; since that should
		// be possible as well and is JDBC standard)
		// Note that this method should return the number of updated rows. This
		// method however always returns -1, since Monet currently doesn't
		// support returning the affected rows.
		st.executeUpdate("delete from a where var1 = 'zzzz'");

		// Don't forget to do it yourself if the connection is reused or much
		// longer alive, since the Statement object contains a lot of things
		// you probably want to reclaim if you don't need them anymore.
		st.close();
		// closing the connection should take care of closing all generated
		// statements from it...
		con.close();
	}
}
