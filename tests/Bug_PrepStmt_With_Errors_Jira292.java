/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Bug_PrepStmt_With_Errors_Jira292 {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ParameterMetaData pmd = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;

		try {
			con = DriverManager.getConnection(args[0]);
			System.out.println("0. true\t" + con.getAutoCommit());
			con.setNetworkTimeout(null, (10 *1000));

			stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists abacus;");
			stmt.executeUpdate("create table abacus ( \"'Zeitachse'\" date,\"'Abo_ID'\" int,\"'description'\" varchar(256),\"'Klassierungs-Typ'\" clob,\"'KlassierungApplikation'\" clob,\"'EP Netto'\" decimal,\"'Nettoumsatz'\" decimal,\"'validfrom'\" date,\"'validuntil'\" date,\"'Abo_aufgeschaltet'\" int,\"'Abo_deaktiviert'\" int,\"'Differenz'\" decimal,\"'User_ID'\" int,\"'UserName'\" varchar(256),\"'client'\" varchar(256),\"'Installations_ID'\" int,\"'InstallationsName'\" varchar(256),\"'Installationsprovider_ID'\" int,\"'InstallationsproviderName'\" varchar(256),\"'INR'\" bigint,\"'NAME'\" varchar(256),\"'PLZ'\" varchar(256),\"'ORT'\" varchar(256),\"'STAAT'\" varchar(256),\"'Reseller_ID'\" int,\"'ResellerName'\" varchar(256),\"'ET_ABO'\" clob,\"'UserName_1'\" varchar(256),\"'Anzahl_Abos'\" decimal,\"'Anzahl_User'\" decimal,\"'Jahr'\" decimal,\"'Monat'\" decimal,\"'Jahr_Monat'\" clob,\"'IFJ'\" clob,\"'RECNUM$'\" int,\"'InlineCalc_Year_Zeitachse'\" int);");
			stmt.executeUpdate("insert into abacus values ('2019-10-30',2239,'description','Klassierungs-Typ','Klassierung-Applikation',73.28,68.29,'2018-01-01','2018-12-01',563,63,56.3,852,'UserName','client',134,'InstallationsName',892,'InstallationsproviderName',9348,'NAME','PLZ','ORT','STAAT',934,'ResellerName','ET_ABO','UserName_1',849.2739,1742.718,395.824,39.824,'Jahr_Monat','IFJ',395824,3789);");

			System.out.println("1. table created and inserted 1 row");

			String qry = "SELECT \"'ResellerName'\" FROM abacus WHERE  ( ( (\"'InstallationsproviderName'\"='Bienz Pius Treuhand- und Revisions AG')) AND  ( (\"'validuntil'\"='2018-01-01' AND \"'description'\"='ABEA 2' AND (EXTRACT(YEAR FROM \"'Zeitachse'\")*100 + EXTRACT(MONTH FROM \"'Zeitachse'\"))/100.0='2019.010' AND \"'UserName'\"='AL - Astrid Lincke (Delphys)' AND \"'validfrom'\"='2016-12-01')) AND  ( (\"'IFJ'\"='ohne IFJ')) AND  ( (\"'InlineCalc_Year_Zeitachse'\"='2019'))) GROUP BY \"'ResellerName'\" LIMIT 1001 OFFSET 0;";
			try {
				System.out.println("2. before select query execution");
				rs = stmt.executeQuery(qry);
				System.out.println("2a. select query executed");
				if (rs != null) {
					if (rs.next()) {
						System.out.println("2b. select query returned: " + rs.getString(1));
					}
					rs.close();
					rs = null;
					System.out.println("2c. closed select query resultset");
				}
				System.out.println("2d. normal end of select query");
			} catch (SQLException se) {
				System.out.println("select query Exception: "+ se.getMessage());
				while ((se = se.getNextException()) != null)
					System.out.println("next Exception: "+ se.getMessage());
			}

			try {
				System.out.println("3. before creating a prepared select query");
				pstmt = con.prepareStatement(qry);
				System.out.println("3a. prepared select query");

				pmd = pstmt.getParameterMetaData();
				System.out.println("3b. Prepared Query has " + pmd.getParameterCount() + " parameters."); // "Type of first is: " + pmd.getParameterTypeName(1));
				rsmd = pstmt.getMetaData();
				System.out.println("3c. Prepared Query has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1));

				System.out.println("3d. before executing the prepared select query");
				rs = pstmt.executeQuery();
				System.out.println("3e. prepared select query executed");
				if (rs != null) {
					rsmd = rs.getMetaData();
					System.out.println("3f. prepared Query ResultSet has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1));

					if (rs.next()) {
						System.out.println("3g. prepared select query returned: " + rs.getString(1));
					}
					rs.close();
					rs = null;
					System.out.println("3h. closed prepared select query resultset");
				}
				System.out.println("3i. normal end of prepared select query");
			} catch (SQLException se) {
				System.out.println("prepared select query Exception: "+ se.getMessage());
				while ((se = se.getNextException()) != null)
					System.out.println("next Exception: "+ se.getMessage());
			}

			System.out.println("4. drop table");
			stmt.executeUpdate("drop table abacus");

			System.out.println("5. normal end of test");
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		} finally {
			if (rs != null)    try { rs.close();    } catch (SQLException e) { /* ignore */ }
			if (pstmt != null) try { pstmt.close(); } catch (SQLException e) { /* ignore */ }
			if (stmt != null)  try { stmt.close();  } catch (SQLException e) { /* ignore */ }
			if (con != null)   try { con.close();   } catch (SQLException e) { /* ignore */ }
		}
	}
}

