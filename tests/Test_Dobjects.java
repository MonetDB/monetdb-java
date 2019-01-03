/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

import java.sql.*;

public class Test_Dobjects {
	private static void dumpResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		System.out.println("Resultset with " + columnCount + " columns");
		for (int col = 1; col <= columnCount; col++) {
			System.out.print(rsmd.getColumnName(col) + "\t");
		}
		System.out.println();
		while (rs.next()) {
			for (int col = 1; col <= columnCount; col++) {
				System.out.print(rs.getString(col) + "\t");
			}
			System.out.println();
		}
		rs.close();
	}

	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		DatabaseMetaData dbmd = con.getMetaData();
		try {
			// inspect the catalog by use of dbmd functions
			dumpResultSet(dbmd.getCatalogs());
//			dumpResultSet(dbmd.getSchemas());	// this produces different outputs on different platforms due to dependency on SAMTOOLS and NETCDF. so exclude it
			dumpResultSet(dbmd.getSchemas(null, "sys"));
//			dumpResultSet(dbmd.getTables(null, "sys", null, null));	// this produces different outputs on different platforms due to dependency on Geom and NETCDF.
			dumpResultSet(dbmd.getTables(null, "tmp", null, null));	// schema tmp has 6 tables
			dumpResultSet(dbmd.getUDTs(null, "sys", null, null));
			int[] UDTtypes = { Types.STRUCT, Types.DISTINCT };
			dumpResultSet(dbmd.getUDTs(null, "sys", null, UDTtypes));
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}
		con.close();
	}
}
