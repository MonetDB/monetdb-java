/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Bug_Connect_as_voc_getMetaData_Failure_Bug_6388 {
	public static void main(String[] args) throws SQLException
	{
		Connection con1 = null;
		Statement stmt1 = null;

		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		con1 = DriverManager.getConnection(args[0]);
		stmt1 = con1.createStatement();

		// test the creation of a table with concurrent clients
		try {
			System.out.println("1. CREATE USER voc");
			stmt1.executeUpdate("CREATE USER \"voc\" WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA \"sys\"");
			System.out.println("2. CREATE SCHEMA voc");
			stmt1.executeUpdate("CREATE SCHEMA \"voc\" AUTHORIZATION \"voc\"");
			System.out.println("3. ALTER USER voc");
			stmt1.executeUpdate("ALTER USER \"voc\" SET SCHEMA \"voc\"");
			System.out.println("creation succeeded :)");
			System.out.println();

			login_as_voc_and_get_MetaData(args[0].replace("=monetdb", "=voc"));

			System.out.println();
			System.out.println("Cleanup created objects");
			System.out.println("5. ALTER USER voc");
			stmt1.executeUpdate("ALTER USER \"voc\" SET SCHEMA \"sys\"");
			System.out.println("6. DROP SCHEMA voc");
			stmt1.executeUpdate("DROP SCHEMA \"voc\"");
			System.out.println("7. DROP USER voc");
			stmt1.executeUpdate("DROP USER \"voc\"");
			System.out.println("cleanup succeeded :)");
		} catch (SQLException e) {
			System.out.println("FAILED creating user and schema voc. " + e.getMessage());
		} finally {
			stmt1.close();
			con1.close();
		}
	}

	private static void login_as_voc_and_get_MetaData(String connectString) {
		Connection con2 = null;

		try {
			System.out.println("4.1. connect as user: voc");
			con2 = DriverManager.getConnection(connectString);
			System.out.println("connected :)");
		} catch (SQLException e) {
			System.out.println("FAILED to connect as user voc. " + e.getMessage());
			return;
		}

		try {
			DatabaseMetaData dbmd = con2.getMetaData();

			System.out.println("4.2. getUserName()");
			System.out.println("UserName = " + dbmd.getUserName());

			System.out.println("4.3. getMaxConnections()");
			System.out.println("MaxConnections = " + dbmd.getMaxConnections());

			System.out.println("4.4. getDatabaseProductVersion()");
			String dbmsVersion = dbmd.getDatabaseProductVersion();	// should be 11.27.1 or higher
			boolean postJul2017 = ("11.27.1".compareTo(dbmsVersion) <= 0);
			System.out.println("DatabaseProductVersion = " + (postJul2017 ? "11.27.+" : dbmsVersion));

			System.out.println("4.5. getDatabaseMajorVersion()");
			System.out.println("DatabaseMajorVersion = " + dbmd.getDatabaseMajorVersion());	// should be 11

			System.out.println("4.6. getDatabaseMinorVersion()");
			int dbmsMinorVersion = dbmd.getDatabaseMinorVersion();	// should be 27 or higher
			System.out.println("DatabaseMinorVersion = " + (dbmsMinorVersion >= 27 ? "27+" : dbmsMinorVersion));

			System.out.println("4.7. getTables(null, 'tmp', null, null)");
			ResultSet rs1 = dbmd.getTables(null, "tmp", null, null);
			if (rs1 != null) {
				System.out.println("List Tables in schema tmp:");
				while (rs1.next()) {
					System.out.println(rs1.getString(3));
				}
				rs1.close();
			}
			System.out.println("completed listing Tables in schema tmp");

			System.out.println("4.8. getTableTypes()");
			rs1 = dbmd.getTableTypes();
			if (rs1 != null) {
				System.out.println("List TableTypes:");
				while (rs1.next()) {
					System.out.println(rs1.getString(1));
				}
				rs1.close();
			}
			System.out.println("completed listing TableTypes");

			System.out.println("voc meta data Test completed successfully");
		} catch (SQLException e) {
			System.out.println("FAILED fetching MonetDatabaseMetaData. " + e.getMessage());
		} finally {
			try {
				con2.close();
			} catch (SQLException e) {
				System.out.println("FAILED to close voc connection. " + e.getMessage());
			}
		}
	}
}
