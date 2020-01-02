/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Test_PSmetadata {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		ParameterMetaData pmd = null;

		con.setAutoCommit(false);
		// >> false: auto commit should be off now
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE table_Test_PSmetadata ( myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob )");

			// all NULLs
			stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (NULL, NULL,            NULL,           NULL,                  NULL)");
			// all filled in
			stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (2   , 3.0,             true,           'A string',            'bla bla bla')");

			pstmt = con.prepareStatement("SELECT CASE WHEN myint IS NULL THEN 0 ELSE 1 END AS intnull, * FROM table_Test_PSmetadata WHERE myint = ?");

			rsmd = pstmt.getMetaData();

			System.out.println("0. 6 columns:\t" + rsmd.getColumnCount());
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				System.out.println("" + col + ".\t" + rsmd.getCatalogName(col));
				System.out.println("\tclassname     " + rsmd.getColumnClassName(col));
				System.out.println("\tdisplaysize   " + rsmd.getColumnDisplaySize(col));
				System.out.println("\tlabel         " + rsmd.getColumnLabel(col));
				System.out.println("\tname          " + rsmd.getColumnName(col));
				System.out.println("\ttype          " + rsmd.getColumnType(col));
				System.out.println("\ttypename      " + rsmd.getColumnTypeName(col));
				System.out.println("\tprecision     " + rsmd.getPrecision(col));
				System.out.println("\tscale         " + rsmd.getScale(col));
				System.out.println("\tschemaname    " + rsmd.getSchemaName(col));
				System.out.println("\ttablename     " + rsmd.getTableName(col));
				System.out.println("\tautoincrement " + rsmd.isAutoIncrement(col));
				System.out.println("\tcasesensitive " + rsmd.isCaseSensitive(col));
				System.out.println("\tcurrency      " + rsmd.isCurrency(col));
				System.out.println("\tdefwritable   " + rsmd.isDefinitelyWritable(col));
				System.out.println("\tnullable      " + rsmd.isNullable(col));
				System.out.println("\treadonly      " + rsmd.isReadOnly(col));
				System.out.println("\tsearchable    " + rsmd.isSearchable(col));
				System.out.println("\tsigned        " + rsmd.isSigned(col));
				System.out.println("\twritable      " + rsmd.isWritable(col));
			}

			showParams(pstmt);
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}

	// some utility methods for showing table content and params meta data
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
