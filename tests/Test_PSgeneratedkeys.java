/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

import java.sql.*;

public class Test_PSgeneratedkeys {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		con.setAutoCommit(false);
		// >> false: auto commit was just switched off
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate(
				"CREATE TABLE psgenkey (" +
				"       id       serial," +
				"       val      varchar(20)" +
				")");
			stmt.close();
		} catch (SQLException e) {
			System.out.println(e);
			System.out.println("Creation of test table failed! :(");
			System.out.println("ABORTING TEST!!!");
			System.exit(-1);
		}

		try {
			PreparedStatement pstmt = con.prepareStatement(
				"INSERT INTO psgenkey (val) VALUES ('this is a test')",
				Statement.RETURN_GENERATED_KEYS);

			System.out.print("1. inserting a record...");
			pstmt.executeUpdate();
			System.out.println("success :)");

			// now get the generated keys
			System.out.print("2. getting generated keys...");
			ResultSet keys = pstmt.getGeneratedKeys();
			if (keys == null || !keys.next()) {
				System.out.println("there are no keys! :(");
				System.out.println("ABORTING TEST!!!");
				System.exit(-1);
			}

			System.out.println("generated key index: " + keys.getInt(1));
			while (keys.next()) {
				System.out.println("generated key index: " + keys.getInt(1));
			}

			if (keys.getStatement() == null) {
				System.out.println("ResultSet.getStatement() should never return null!");
			}

			keys.close();
			pstmt.close();
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}
}
