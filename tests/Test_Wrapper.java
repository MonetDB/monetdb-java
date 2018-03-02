/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

import java.sql.*;

public class Test_Wrapper {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		final Connection con = DriverManager.getConnection(args[0]);
		System.out.println("Connected. Auto commit is: " + con.getAutoCommit());

		try {
			final String jdbc_pkg = "java.sql.";
			final String monetdb_jdbc_pkg = "nl.cwi.monetdb.jdbc.";

			checkIsWrapperFor("Connection", con, jdbc_pkg, "Connection");
			checkIsWrapperFor("Connection", con, monetdb_jdbc_pkg, "MonetConnection");
			checkIsWrapperFor("Connection", con, jdbc_pkg, "Statement");
			checkIsWrapperFor("Connection", con, monetdb_jdbc_pkg, "MonetStatement");

			DatabaseMetaData dbmd = con.getMetaData();
			checkIsWrapperFor("DatabaseMetaData", dbmd, jdbc_pkg, "DatabaseMetaData");
			checkIsWrapperFor("DatabaseMetaData", dbmd, monetdb_jdbc_pkg, "MonetDatabaseMetaData");
			checkIsWrapperFor("DatabaseMetaData", dbmd, jdbc_pkg, "Statement");
			checkIsWrapperFor("DatabaseMetaData", dbmd, monetdb_jdbc_pkg, "MonetStatement");

			ResultSet rs = dbmd.getSchemas();
			checkIsWrapperFor("ResultSet", rs, jdbc_pkg, "ResultSet");
			checkIsWrapperFor("ResultSet", rs, monetdb_jdbc_pkg, "MonetResultSet");
			checkIsWrapperFor("ResultSet", rs, jdbc_pkg, "Statement");
			checkIsWrapperFor("ResultSet", rs, monetdb_jdbc_pkg, "MonetStatement");

			ResultSetMetaData rsmd = rs.getMetaData();
			checkIsWrapperFor("ResultSetMetaData", rsmd, jdbc_pkg, "ResultSetMetaData");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetResultSet");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetResultSet$rsmdw");  // it is a private class of MonetResultSet
			checkIsWrapperFor("ResultSetMetaData", rsmd, jdbc_pkg, "Statement");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetStatement");

			rs.close();

			Statement stmt = con.createStatement();
			checkIsWrapperFor("Statement", stmt, jdbc_pkg, "Statement");
			checkIsWrapperFor("Statement", stmt, monetdb_jdbc_pkg, "MonetStatement");
			checkIsWrapperFor("Statement", stmt, jdbc_pkg, "Connection");
			checkIsWrapperFor("Statement", stmt, monetdb_jdbc_pkg, "MonetConnection");

			stmt.close();

			PreparedStatement pstmt = con.prepareStatement("SELECT name FROM sys.tables WHERE system AND name like ?");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "PreparedStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "Statement");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "Connection");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetConnection");

			ParameterMetaData pmd = pstmt.getParameterMetaData();
			checkIsWrapperFor("ParameterMetaData", pmd, jdbc_pkg, "ParameterMetaData");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetPreparedStatement$pmdw");  // it is a private class of MonetPreparedStatement
			checkIsWrapperFor("ParameterMetaData", pmd, jdbc_pkg, "Connection");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetConnection");

			ResultSetMetaData psrsmd = pstmt.getMetaData();
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, jdbc_pkg, "ResultSetMetaData");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetPreparedStatement$rsmdw");  // it is a private class of MonetPreparedStatement
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, jdbc_pkg, "Connection");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetConnection");

			pstmt.close();

		} catch (SQLException e) {
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
		}
		con.close();
	}

	private static void checkIsWrapperFor(String objnm, Wrapper obj, String pkgnm, String classnm) {
		try {
			Class<?> clazz = Class.forName(pkgnm + classnm);
			boolean isWrapper = obj.isWrapperFor(clazz);
			System.out.print(objnm + ". isWrapperFor(" + classnm + ") returns: " + isWrapper);
			if (isWrapper) {
				Object wobj = obj.unwrap(clazz);
				System.out.print("\tCalled unwrap(). Returned object is " + (wobj != null ? "not null, so oke" : "null !!"));
			}
			System.out.println();
		} catch (ClassNotFoundException cnfe) {
			System.out.println(cnfe.toString());
		} catch (SQLException se) {
			System.out.println(se.getMessage());
		}
	}
}
