/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Bug_PrepStmtSetObject_CLOB_6349 {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt = null;
		ParameterMetaData pmd = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;

		System.out.println("0. true\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE PrepStmtSetObject_CLOB (myint INT, myvarchar VARCHAR(15), myclob CLOB)");
			stmt.executeUpdate("INSERT INTO PrepStmtSetObject_CLOB VALUES (123, 'A string', 'A longer string')");
			stmt.executeUpdate("INSERT INTO PrepStmtSetObject_CLOB VALUES (NULL, NULL, NULL)");  // all NULLs

			pstmt = con.prepareStatement("SELECT myclob, myvarchar, myint FROM PrepStmtSetObject_CLOB WHERE myclob = ?");
			pmd = pstmt.getParameterMetaData();
			System.out.println("Prepared Query has " + pmd.getParameterCount() + " parameters. Type of first is: " + pmd.getParameterTypeName(1));
			rsmd = pstmt.getMetaData();
			System.out.println("Prepared Query has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1));

			pstmt.setObject(1, "A longer string");
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			System.out.println("Query ResultSet has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1));

			boolean has_row = rs.next();
			boolean has_rows = rs.next();
			if (has_row == false || has_rows == true)
				System.out.println("Fetching Query ResultSet failed");

			stmt.executeUpdate("DROP TABLE PrepStmtSetObject_CLOB");

		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		} finally {
			if (rs != null)    rs.close();
			if (pstmt != null) pstmt.close();
			stmt.close();
		}

		con.close();
	}
}

