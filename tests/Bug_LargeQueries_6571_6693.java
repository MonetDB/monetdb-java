/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

public class Bug_LargeQueries_6571_6693 {
	final static String tbl_nm = "tbl6693";
	final static String largedata = createLargedata(9216);

	private static String createLargedata(int num) {
		String repeatValue = "*";
		StringBuilder sb = new StringBuilder(num * repeatValue.length());
		for (int i = 0; i < num; i++)
			sb.append(repeatValue);
		String largedata = sb.toString();
		if (largedata.length() <= 8192)
			System.out.println("Length (" + largedata.length() + ") of largedata value is too small! Should be larger than 8192!");
		return largedata;
	}

	// To execute this test program: start a local MonetDB server (mserver5 process) and next execute command:
	// java -cp monetdb-jdbc-2.29.jar:. Bug_LargeQueries_6571_6693 "jdbc:monetdb://localhost:50000/demo?user=monetdb&password=monetdb"

	public static void main(String[] args) throws SQLException {
		int script_iterations = 10;
		String conURL = args.length > 0 ? args[0] : "";

		if (args.length > 1) {
			try {
				script_iterations = Integer.parseInt(args[1]);
			} catch (NumberFormatException nfe) {
				System.err.println("Cannot convert 2nd argumnent to an integer. Ignoring it.");
			}
		}

		try (Connection con = DriverManager.getConnection(conURL)) {
			try (Statement stmt = con.createStatement()) {
				// create a test table.
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tbl_nm + " (attribute CLOB, value CLOB);");
				System.out.print("Created table: " + tbl_nm);
				System.out.print(" Inserting rows. ");
				String insertCmd = "INSERT INTO " + tbl_nm + " VALUES ('activeset_default_fiets', '" + largedata + "');";
				int ins = stmt.executeUpdate(insertCmd);
				ins += stmt.executeUpdate(insertCmd);
				ins += stmt.executeUpdate(insertCmd);
				System.out.println(ins + " rows inserted");
			}

			run_tests(conURL, script_iterations);

			try (Statement stmt = con.createStatement()) {
				System.out.println("Cleanup TABLE " + tbl_nm);
				stmt.executeUpdate("DROP TABLE IF EXISTS " + tbl_nm);
			}
		}

		System.out.println("Test completed without hanging");
	}

	private static void run_tests(String conURL, int iterations) throws SQLException {
		String script =
			  "delete from " + tbl_nm + " where attribute='activeset_default_fiets';\n"
			+ "insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n"
			+ "insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n"
			+ "insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n"
			+ "select value from " + tbl_nm + " where attribute='activeset_default_fiets';\n";
		System.out.println("Script size is " + script.length());

		// first try to make the execution hang after many iterations of sending large data queries within one connection
		System.out.println("First test repeat " + iterations + " times");
		try (Connection con = DriverManager.getConnection(conURL)) {
			System.out.print("Iteration: ");
			for (int i = 1; i <= iterations; i++) {
				System.out.print(i + " ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, 1, 3, 6);
				}
			}
			System.out.println();
		}
		System.out.println("Completed first test");

		// also try to make the execution hang after many iterations of making connections (each their own socket) and sending large scripts
		System.out.println("Second test repeat " + iterations + " times");
		System.out.print("Iteration: ");
		for (int i = 1; i <= iterations; i++) {
			try (Connection con = DriverManager.getConnection(conURL)) {
				System.out.print(i + " ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
				}
			}
		}
		System.out.println();
		System.out.println("Completed second test");

		// next try to make the execution hang by sending very many queries combined in 1 large script
		final int queries = 100;
		StringBuilder sb = new StringBuilder(queries * 13);
		for (int i = 1; i <= queries; i++)
			sb.append(" SELECT ").append(i).append(';');
		script = sb.toString();
		System.out.println("Script size is " + script.length());
		iterations = 3;
		System.out.println("Third test repeat " + iterations + " times");
		try (Connection con = DriverManager.getConnection(conURL)) {
			System.out.print("Iteration: ");
			for (int i = 1; i <= iterations; i++) {
				System.out.print(i + " ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, queries, queries, 0);
				}
			}
			System.out.println();
		}
		System.out.println("Completed third test");
	}

	private static void process_script(Statement stmt, String script,
				int expectedResults, int expectedTotalRows, int expectedUpdates) throws SQLException {
		int results = 0;
		int rows = 0;
		int updates = 0;
		stmt.execute(script);
		do {
			ResultSet rs = stmt.getResultSet();
			if (rs != null) {
				results++;
				while(rs.next()) {
					String val = rs.getString(1);
					rows++;
				}
				rs.close();
			} else {
				int uc = stmt.getUpdateCount();
				if (uc > 0)
					updates += uc;
			}
		} while (stmt.getMoreResults() || stmt.getUpdateCount() != -1);

		/* verify nr of processed resultsets and retrieved rows are as expected */
		if (results != expectedResults)
			System.out.print(results + "!=" + expectedResults + " ");
		if (rows != expectedTotalRows)
			System.out.print(rows + "!=" + expectedTotalRows + " ");
		if (updates != expectedUpdates)
			System.out.print(updates + "!=" + expectedUpdates + " ");
	}
}
