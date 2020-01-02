/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Test_CallableStmt {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = null;
		CallableStatement cstmt = null;
		try {
			String tbl_nm = "tbl6402";
			String proc_nm = "proc6402";

			stmt = con.createStatement();

			// create a test table.
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tbl_nm + " (tint int, tdouble double, tbool boolean, tvarchar varchar(15), tclob clob, turl url, tclen int);");
			System.out.println("Created table: " + tbl_nm);

			// create a procedure with multiple different IN parameters which inserts a row into a table of which one column is computed.
			stmt.executeUpdate("CREATE PROCEDURE " + proc_nm + " (myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob, myurl url) BEGIN" +
				" INSERT INTO " + tbl_nm + " (tint, tdouble, tbool, tvarchar, tclob, turl, tclen) VALUES (myint, mydouble, mybool, myvarchar, myclob, myurl, LENGTH(myvarchar) + LENGTH(myclob)); " +
				"END;");
			System.out.println("Created procedure: " + proc_nm);

			// make sure we can call the procedure the old way (as string)
			stmt.executeUpdate("call " + proc_nm + "(1, 1.1, true,'one','ONE', 'www.monetdb.org');");
			System.out.println("Called procedure (1): " + proc_nm);
			showContents(con, tbl_nm);


			// now use a CallableStament object
			cstmt = con.prepareCall(" { call " + proc_nm + " (?,?, ?, ? , ?,?) } ;");
			System.out.println("Prepared Callable procedure: " + proc_nm);

			// specify first set of params
			cstmt.setInt(1, 2);
			cstmt.setDouble(2, 2.02);
			cstmt.setBoolean(3, true);
			cstmt.setString(4, "Two");
			Clob myclob = con.createClob();
			myclob.setString(1, "TWOs");
			cstmt.setClob(5, myclob);
			cstmt.setString(6, "http://www.monetdb.org/");
			cstmt.execute();
			System.out.println("Called Prepared procedure (1): " + proc_nm);
			showParams(cstmt);
			showContents(con, tbl_nm);

			myclob.setString(1, "TREEs");
			// specify second set of params (some (1 and 3 and 5) are left the same)
			cstmt.setDouble(2, 3.02);
			cstmt.setString(4, "Tree");
			cstmt.setURL(6, new java.net.URL("https://www.monetdb.org/"));
			cstmt.execute();
			System.out.println("Called Prepared procedure (2): " + proc_nm);
			// showParams(cstmt);
			showContents(con, tbl_nm);

			// specify third set of params (some (1 and 2) are left the same)
			cstmt.setInt(1, 4);
			cstmt.setBoolean(3, false);
			cstmt.setString(4, "Four");
			cstmt.executeUpdate();
			System.out.println("Called Prepared procedure (3): " + proc_nm);
			showContents(con, tbl_nm);

			// test setNull() also
			cstmt.setNull(3, Types.BOOLEAN);
			cstmt.setNull(5, Types.CLOB);
			cstmt.setNull(2, Types.DOUBLE);
			cstmt.setNull(4, Types.VARCHAR);
			cstmt.setNull(1, Types.INTEGER);
			cstmt.executeUpdate();
			System.out.println("Called Prepared procedure (with NULLs): " + proc_nm);
			showContents(con, tbl_nm);


			System.out.println("Test completed. Cleanup procedure and table.");
			stmt.execute("DROP PROCEDURE IF EXISTS " + proc_nm + ";");
			stmt.execute("DROP TABLE     IF EXISTS " + tbl_nm + ";");

		} catch (SQLException e) {
			System.out.println("main failed: " + e.getMessage());
			System.out.println("ABORTING TEST");
		} finally {
			try {
				if (cstmt != null)
					cstmt.close();
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) { /* ignore */ }
		}

		con.close();
	}


	// some utility methods for showing table content and params meta data
	static void showContents(Connection con, String tblnm) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT * FROM " + tblnm);
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				System.out.println("Table " + tblnm + " has " + rsmd.getColumnCount() + " columns:");
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					System.out.print("\t" + rsmd.getColumnLabel(col));
				}
				System.out.println();
				while (rs.next()) {
					for (int col = 1; col <= rsmd.getColumnCount(); col++) {
						System.out.print("\t" + rs.getString(col));
					}
					System.out.println();
				}
			} else
				System.out.println("failed to execute query: SELECT * FROM " + tblnm);
		} catch (SQLException e) {
			System.out.println("showContents failed: " + e.getMessage());
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) { /* ignore */ }
		}
	}

	static void showParams(PreparedStatement stmt) {
		try {
			ParameterMetaData pmd = stmt.getParameterMetaData();
			System.out.println(pmd.getParameterCount() + " parameters reported:");
			for (int parm = 1; parm <= pmd.getParameterCount(); parm++) {
				System.out.print(parm + ".");
				int nullable = pmd.isNullable(parm);
				System.out.println("\tnullable  " + nullable + " (" + paramNullableName(nullable) + ")");
				System.out.println("\tsigned    " + pmd.isSigned(parm));
				System.out.println("\tprecision " + pmd.getPrecision(parm));
				System.out.println("\tscale     " + pmd.getScale(parm));
				System.out.println("\ttype      " + pmd.getParameterType(parm));
				System.out.println("\ttypename  " + pmd.getParameterTypeName(parm));
				System.out.println("\tclassname " + pmd.getParameterClassName(parm));
				int mode = pmd.getParameterMode(parm);
				System.out.println("\tmode      " + mode + " (" + paramModeName(mode) + ")");
			}
		} catch (SQLException e) {
			System.out.println("showParams failed: " + e.getMessage());
		}
	}

	static String paramNullableName(int nullable) {
		if (nullable == ParameterMetaData.parameterNoNulls)
			return "NO";
		if (nullable == ParameterMetaData.parameterNullable)
			return "YA";
		if (nullable == ParameterMetaData.parameterNullableUnknown)
			return "UNKNOWN";
		return "INVALID" + nullable;
	}

	static String paramModeName(int mode) {
		if (mode == ParameterMetaData.parameterModeIn)
			return "IN";
		if (mode == ParameterMetaData.parameterModeInOut)
			return "INOUT";
		if (mode == ParameterMetaData.parameterModeOut)
			return "OUT";
		if (mode == ParameterMetaData.parameterModeUnknown)
			return "UNKNOWN";
		return "INVALID" + mode;
	}
}
