/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

import java.sql.*;

import nl.cwi.monetdb.jdbc.MonetINET;
import nl.cwi.monetdb.jdbc.MonetURL;

public class Bug_PrepStmtSetString_6382 {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		System.out.println("0. true\t" + con.getAutoCommit());

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ParameterMetaData pmd = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		try {
			stmt = con.createStatement();
			String tableName = "PrepStmtSetString_6382";
			System.out.println("Creating table " + tableName);
			stmt.executeUpdate("CREATE TABLE " + tableName + " (myint INT, myvarchar VARCHAR(15), myjson JSON, myuuid UUID, myurl URL, myinet INET)");

			System.out.println("Inserting row 1");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'row 1', '{}', uuid '34c8deb5-e608-406b-beda-6a951f73d455', 'https://www.monetdb.org/', '128.0.0.1')");

			System.out.println("Inserting row 2");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'row 2', '[]', NULL, NULL, NULL)");


			System.out.println("Creating a prepared statement with 6 parameters and inserting rows using setInt(), setString(), setNull(), setNString(), setURL(), setObject().");
			pstmt = con.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?, ? ,?,? , ?)");
			pmd = pstmt.getParameterMetaData();
			int pcount = pmd.getParameterCount();
			System.out.println("Prepared Statement has " + pcount + " parameters:" + (pcount != 6 ? " ERROR: Expected 6 parameters!" : ""));
			for (int p = 1; p <= pcount; p++) {
				System.out.println(" Parameter " + p + " type is: " + pmd.getParameterTypeName(p) + ". JDBC SQL type: " + pmd.getParameterType(p));
			}

			int row = 3;
			pstmt.setInt(1, row);
			pstmt.setString(2, "row " + row);
			pstmt.setString(3, "{\"menu\": {\n  \"id\": \"file\",\n  \"value\": \"File\",\n  \"popup\": {\n    \"menuitem\": [\n      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n    ]\n  }\n}}");
			pstmt.setNull(4, 0);
			pstmt.setNull(5, 0);
			pstmt.setNull(6, 0);
			System.out.println("Inserting row " + row);
			int inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");

			row++;  // row 4
			pstmt.setShort(1, (short)row);
			pstmt.setNString(2, "row " + row);
			pstmt.setNull(3, 0);
			pstmt.setString(4, "4a148b7d-8d47-4e1e-a21e-09a71abf2215");
			System.out.println("Inserting row " + row);
			inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");

			row++;  // row 5
			pstmt.setLong(1, row);
			pstmt.setString(2, "row " + row);
			pstmt.setNull(4, 0);
			pstmt.setURL(5, new java.net.URL("https://www.cwi.nl/"));
			System.out.println("Inserting row " + row);
			inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");

			row++;  // row 6
			pstmt.setBigDecimal(1, new java.math.BigDecimal(row));
			pstmt.setNString(2, "row " + row);
			pstmt.setNull(5, 0);
			pstmt.setString(6, "127.255.255.255");
			System.out.println("Inserting row " + row);
			inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");

			/* also test generic setObject(int, String) */
			row++;  // row 7
			pstmt.setObject(1, new Integer(row));
			pstmt.setObject(2, "row " + row);
			pstmt.setObject(3, "{\"menu\": {\n    \"header\": \"SVG Viewer\",\n    \"items\": [\n        {\"id\": \"Open\"},\n        {\"id\": \"OpenNew\", \"label\": \"Open New\"},\n        null,\n        {\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},\n        {\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},\n        {\"id\": \"OriginalView\", \"label\": \"Original View\"},\n        null,\n        {\"id\": \"Quality\"},\n        {\"id\": \"Pause\"},\n        {\"id\": \"Mute\"},\n        null,\n        {\"id\": \"Help\"},\n        {\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}\n    ]\n}}");
			pstmt.setObject(4, "b39dc76e-4faf-4fd9-bc1e-17df48acf764");
			pstmt.setObject(5, "https://en.wikipedia.org/wiki/IP_address");
			pstmt.setObject(6, "223.255.255.255");
			System.out.println("Inserting row " + row);
			inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");

			row++;  // row 8
			pstmt.setObject(1, new java.math.BigDecimal(row));
			pstmt.setObject(2, "row " + row);
			pstmt.setObject(3, null);
			pstmt.setObject(4, java.util.UUID.fromString("ff125769-b63c-4c3c-859f-5b84a9349e24"));
			MonetURL myURL = new MonetURL("https://en.wikipedia.org/wiki/IP_address");
			pstmt.setObject(5, myURL);
			MonetINET myINET = new MonetINET("223.234.245.255");
			pstmt.setObject(6, myINET);
			System.out.println("Inserting row " + row);
			inserted = pstmt.executeUpdate();
			System.out.println("Inserted " + inserted + " row");


			System.out.println("List contents of TABLE " + tableName + " after " + row + " rows inserted");
			rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY 1");
			rsmd = rs.getMetaData();
			int colcount = rsmd.getColumnCount();
			System.out.println("Query has " + colcount + " output columns." + (colcount != 6 ? " ERROR: Expected 6 columns!" : ""));
			row = 0;
			while (rs.next()) {
				System.out.print("row " + ++row);
				for (int c = 1; c <= colcount; c++) {
					System.out.print("\t" + rs.getString(c));
				}
				System.out.println();
			}

			System.out.println();
			System.out.println("Cleanup TABLE " + tableName);
			stmt.executeUpdate("DROP TABLE " + tableName);
			System.out.println("Test completed successfully");
		} catch (SQLException e) {
			System.err.println("FAILED :( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.err.println("FAILED :( " + e.getMessage());
			System.err.println("ABORTING TEST!!!");
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
			if (stmt != null)
				stmt.close();
			con.close();
		}
	}
}

