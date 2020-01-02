/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Test_Rtimedate {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;

		con.setAutoCommit(false);
		// >> false: auto commit should be off now
		System.out.println("false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE table_Test_Rtimedate ( id int PRIMARY KEY, ts timestamp, t time, d date, vc varchar(30) )");

			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (1, timestamp '2004-04-24 11:43:53.123')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, t) VALUES (2, time '11:43:53.123')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (3, date '2004-04-24')");
			// same values but now as strings to test string to timestamp / time / date object conversions
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (4, '2004-04-24 11:43:53.654321')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (5, '11:43:53')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (6, '2004-04-24')");

			// test also with small years (< 1000) (see bug 6468)
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (11, timestamp '904-04-24 11:43:53.567')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (12, timestamp '74-04-24 11:43:53.567')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (13, timestamp '4-04-24 11:43:53.567')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (14, date '904-04-24')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (15, date '74-04-24')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (16, date '4-04-24')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (17, '904-04-24 11:43:53.567')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (18, '74-04-24 11:43:53.567')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (19, '4-04-24 11:43:53.567')");

			// test also with negative years (see bug 6468)
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (21, timestamp '-4-04-24 11:43:53.567')");	// negative year
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (22, timestamp '-2004-04-24 11:43:53.567')"); // negative year
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (23, date '-4-04-24')");	// negative year
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (24, date '-3004-04-24')");	// negative year
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (25, '-2004-04-24 11:43:53.654321')");	// negative year
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (26, '-3004-04-24')");	// negative year

			rs = stmt.executeQuery("SELECT * FROM table_Test_Rtimedate");

			readNextRow(rs, 1, "ts");
			readNextRow(rs, 2, "t");
			readNextRow(rs, 3, "d");

			readNextRow(rs, 4, "vc");
			readNextRow(rs, 5, "vc");
			readNextRow(rs, 6, "vc");

			readNextRow(rs, 11, "ts");
			readNextRow(rs, 12, "ts");
			readNextRow(rs, 13, "ts");
			readNextRow(rs, 14, "d");
			readNextRow(rs, 15, "d");
			readNextRow(rs, 16, "d");
			readNextRow(rs, 17, "vc");
			readNextRow(rs, 18, "vc");
			readNextRow(rs, 19, "vc");

			readNextRow(rs, 21, "ts");
			readNextRow(rs, 22, "ts");
			readNextRow(rs, 23, "d");
			readNextRow(rs, 24, "d");
			readNextRow(rs, 25, "vc");
			readNextRow(rs, 26, "vc");

			readWarnings(stmt.getWarnings());
			readWarnings(con.getWarnings());
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}

	private static void readNextRow(ResultSet rs, int rowseq, String colnm) throws SQLException {
		rs.next();
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// fetch the column value using multiple methods: getString(), getTimestamp(), getTime() and getDate()
		// to test proper conversion and error reporting
		String data = rs.getString("id") + ". " + colnm + " " + rs.getString(colnm) + " to ";

		// getTimestamp() may raise a conversion warning when the value is of type Time or a String which doesn't match format yyyy-mm-dd hh:mm:ss
		try {
			System.out.println(data + "ts: " + rs.getTimestamp(colnm));
		} catch (SQLException e) {
			System.out.println("rs.getTimestamp(colnm) failed with error: " + e.getMessage());
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// getTime() may raise a conversion warning when the value is of type Date or a String which doesn't match format hh:mm:ss
		try {
			System.out.println(data + "tm: " + rs.getTime(colnm));
		} catch (SQLException e) {
			System.out.println("rs.getTime(colnm) failed with error: " + e.getMessage());
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// getDate() may raise a conversion warning when the value is of type Time or a String which doesn't match format yyyy-mm-dd
		try {
			System.out.println(data + "dt: " + rs.getDate(colnm));
		} catch (SQLException e) {
			System.out.println("rs.getDate(colnm) failed with error: " + e.getMessage());
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();
	}

	private static void readWarnings(SQLWarning w) {
		while (w != null) {
			System.out.println("Warning: " + w.toString());
			w = w.getNextWarning();
		}
	}
}
