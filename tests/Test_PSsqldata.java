/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

import java.net.URL;
import java.sql.*;

import nl.cwi.monetdb.jdbc.MonetINET;

public class Test_PSsqldata {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;
		ResultSet rs;
		ResultSetMetaData rsmd ;
		ParameterMetaData pmd;

		con.setAutoCommit(false);
		// >> false: auto commit should be off now
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE table_Test_PSsqldata ( myinet inet, myurl url )");

			pstmt = con.prepareStatement("INSERT INTO table_Test_PSsqldata VALUES (?, ?)");

			pmd = pstmt.getParameterMetaData();
			System.out.println("1. 2 parameters:\t" + pmd.getParameterCount());
			for (int col = 1; col <= pmd.getParameterCount(); col++) {
				System.out.println("" + col + ".");
				System.out.println("\ttype          " + pmd.getParameterType(col));
				System.out.println("\ttypename      " + pmd.getParameterTypeName(col));
				System.out.println("\tclassname     " + pmd.getParameterClassName(col));
			}

			MonetINET tinet = new MonetINET("172.5.5.5/24");
			URL turl = new URL("http://www.monetdb.org/");
			pstmt.setObject(1, tinet);
			pstmt.setObject(2, turl);
			pstmt.execute();

			tinet.setNetmaskBits(16);
			pstmt.execute();

			rs = stmt.executeQuery("SELECT * FROM table_Test_PSsqldata");
			rsmd = rs.getMetaData();

			for (int i = 1; rs.next(); i++) {
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					Object x = rs.getObject(col);
					if (x == null) {
						System.out.println("" + i + ".\t<null>");
					} else {
						System.out.println("" + i + ".\t" + x.toString());
						if (x instanceof MonetINET) {
							MonetINET inet = (MonetINET)x;
							System.out.println("\t" + inet.getAddress() + "/" + inet.getNetmaskBits());
							System.out.println("\t" + inet.getInetAddress().toString());
						} else if (x instanceof URL) {
							URL url = (URL)x;
							System.out.println("\t" + url.toString());
						}
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}
}
