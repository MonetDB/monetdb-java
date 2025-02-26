/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import org.monetdb.jdbc.MonetConnection;
import org.monetdb.jdbc.types.INET;
import org.monetdb.jdbc.types.URL;

/**
 * class to test JDBC Driver API methods and behavior of MonetDB server.
 *
 * It combines 40+ tests which were previous individual test programs
 * into one large test program, reusing the connection.
 * This speeds up testing considerably as the overhead of starting a JVM and
 * loading the java test program class and MonetDB JDBC driver is now reduced
 * to only one time instead of 40+ times.
 * Also all output is no longer send to system out/err but collected in a global StringBuilder.
 * The contents of it is compared with the expected output at the end of each test.
 * Only when it deviates the output is sent to system err, see compareExpectedOutput().
 *
 * @author Martin van Dinther
 * @version 0.3
 */
public final class JDBC_API_Tester {
	private StringBuilder sb;	// buffer to collect the test output
	private Connection con;		// main connection shared by all tests
	final private int dbmsMajorVersion;
	final private int dbmsMinorVersion;
	final private boolean isPostDec2023;	// flags to support version specific output
	final private boolean isPostMar2025;
	private boolean foundDifferences = false;

	final private static int sbInitLen = 5468; // max needed size of sb

	/**
	 * constructor
	 * @param con_ an active connection
	 * @throws SQLException if a connection or database access error occurs
	 */
	JDBC_API_Tester(Connection con_) throws SQLException {
		this.con = con_;
		sb = new StringBuilder(sbInitLen);

		DatabaseMetaData dbmd = con_.getMetaData();
		dbmsMajorVersion = dbmd.getDatabaseMajorVersion();
		dbmsMinorVersion = dbmd.getDatabaseMinorVersion();
		// from version 11.50 on, the MonetDB server returns different metadata for
		// integer digits (1 less) and for clob and char columns (now return varchar).
		isPostDec2023 = versionIsAtLeast(11, 50);
		isPostMar2025 = versionIsAtLeast(11, 54);
	}

	/**
	 * main function
	 * @param args args[0] should contain the connectionURL string, args[1] an optional flag: -skipMALoutput
	 * @throws Exception if a connection or database access error occurs
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Error: Missing required connection URL as first startup argument!");
			System.exit(-1);
		}

		final String con_URL = args[0];
		final boolean skipMALoutput = (args.length >= 2) ? args[1].equals("-skipMALoutput") : false;
		// System.err.println("skipMALoutput = " + skipMALoutput);

		// Test this before trying to connect
		UrlTester.runAllTests();

		final Connection conn = DriverManager.getConnection(con_URL);
		JDBC_API_Tester jt = new JDBC_API_Tester(conn);

		// run the tests
		jt.Test_Cautocommit(con_URL);
		jt.Test_CisValid();
		jt.Test_Clargequery();
		jt.Test_Cmanycon(con_URL);
		jt.Test_Creplysize();
		jt.Test_Csavepoints();
		jt.Test_Ctransaction();
		jt.Test_Driver(con_URL);
		jt.Test_Dobjects();
		jt.Test_DBCmetadata();
		jt.Test_EmptySql();
		jt.Test_FetchSize();
		jt.Test_Int128();
		jt.Test_Interval_Types();
		jt.Test_PlanExplainTraceDebugCmds(skipMALoutput);
		jt.Test_PSgeneratedkeys();
		jt.Test_PSgetObject();
		jt.Test_PSlargebatchval();
		jt.Test_PSlargeresponse(con_URL);
		jt.Test_PSmanycon(con_URL);
		jt.Test_PSmetadata();
		jt.Test_PSsetBytes();
		jt.Test_PSsomeamount();
		jt.Test_PSsqldata();
		jt.Test_PStimedate();
		jt.Test_PStimezone();
		jt.Test_PStypes();
		jt.Test_CallableStmt();
		jt.Test_Rbooleans();
		jt.Test_Rmetadata();
		jt.Test_RfetchManyColumnsInfo();
		jt.Test_Rpositioning();
		jt.Test_Rsqldata();
		jt.Test_Rtimedate();
		jt.Test_RSgetMetaData();
		jt.Test_Sbatching();
		jt.Test_SgeneratedKeys();
		jt.Test_Smoreresults();
		jt.Test_Wrapper();
		if (jt.isPostDec2023)
			jt.Test_ClientInfo(con_URL);
		jt.bogus_auto_generated_keys();
		jt.BugConcurrent_clients_SF_1504657(con_URL);
		jt.BugConcurrent_sequences(con_URL);
		jt.Bug_Connect_as_voc_getMetaData_Failure_Bug_6388(con_URL);
		jt.BugDatabaseMetaData_Bug_3356();
		jt.BugDecimalRound_Bug_3561();
		jt.BugExecuteUpdate_Bug_3350();
		jt.Bug_IsValid_Timeout_Bug_6782(con_URL);
		jt.Bug_LargeQueries_6571_6693(con_URL);
		jt.Bug_PrepStmtManyParams_7337(480);
		jt.Bug_PrepStmtSetObject_CLOB_6349();
		jt.Bug_PrepStmtSetString_6382();
		jt.Bug_PrepStmt_With_Errors_Jira292();
		jt.BugResultSetMetaData_Bug_6183();
		jt.BugSetQueryTimeout_Bug_3357();
		jt.SQLcopyinto(con_URL);
		jt.DecimalPrecisionAndScale();

		/* run next long running test (11 minutes) only before a new release */
	/*	jt.Test_PSlargeamount(); */

		jt.closeConx(jt.con);

		if (jt.foundDifferences)
			System.exit(-1);

		ConnectionTests.runTests(con_URL);

		// invoke running OnClientTester only on Oct2020 (11.39) or older servers
		if (!jt.versionIsAtLeast(11,40)) {
			OnClientTester oct = new OnClientTester(con_URL, 0);
			int failures = oct.runTests();
			if (failures > 0)
				System.exit(-1);
		}
	}

	private boolean versionIsAtLeast(int major, int minor) {
		return ((dbmsMajorVersion == major && dbmsMinorVersion >= minor) || dbmsMajorVersion > major);
	}

	private void Test_Cautocommit(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		Connection con1 = con;
		Connection con2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		try {
			con2 = DriverManager.getConnection(arg0);

			// >> true: auto commit should be on by default
			if (con1.getAutoCommit() != true)
				sb.append("expecting con1 to have autocommit on/true");
			if (con2.getAutoCommit() != true)
				sb.append("expecting con2 to have autocommit on/true");

			// test commit by checking if a change is visible in another connection
			stmt1 = con1.createStatement();
			sb.append("1. create...");
			stmt1.executeUpdate("CREATE TABLE table_Test_Cautocommit ( id int )");
			sb.append("passed :)\n");

			stmt2 = con2.createStatement();
			sb.append("2. select...");
			rs = stmt2.executeQuery("SELECT * FROM table_Test_Cautocommit");
			sb.append("passed :)\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
			closeStmtResSet(stmt2, rs);
			closeStmtResSet(stmt1, null);
			closeConx(con2);
			sb.append("ABORTING TEST!!!");
			return;
		}

		try {
			// turn off auto commit
			con1.setAutoCommit(false);
			con2.setAutoCommit(false);

			// >> false: we just disabled it
			if (con1.getAutoCommit() != false)
				sb.append("expecting con1 to have autocommit off/false");
			if (con2.getAutoCommit() != false)
				sb.append("expecting con2 to have autocommit off/false");

			// a change would not be visible now
			sb.append("3. drop...");
			stmt2.executeUpdate("DROP TABLE table_Test_Cautocommit");
			sb.append("passed :)\n");

			sb.append("4. select...");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
			sb.append("passed :)\n");

			sb.append("5. commit...");
			con2.commit();
			sb.append("passed :)\n");

			sb.append("6. select...");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
			sb.append("passed :)\n");

			sb.append("7. commit...");
			con1.commit();
			sb.append("passed :)\n");

			// restore original auto commit setting
			con1.setAutoCommit(true);
			con2.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt1, rs);
		closeStmtResSet(stmt2, null);

		closeConx(con2);

		compareExpectedOutput("Test_Cautocommit",
				"1. create...passed :)\n" +
				"2. select...passed :)\n" +
				"3. drop...passed :)\n" +
				"4. select...passed :)\n" +
				"5. commit...passed :)\n" +
				"6. select...passed :)\n" +
				"7. commit...passed :)\n");
	}

	private void Test_CisValid() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
			con.setAutoCommit(false); // start a transaction
			stmt.executeQuery("SELECT COUNT(*) FROM doesnotexist;"); // let's trigger an error
		} catch (SQLException e) {
			// e.printStackTrace();
			sb.append("Expected error: ").append(e).append("\n");
			try {
				// test calling conn.isValid()
				sb.append("Validating connection: con.isValid? ").append(con.isValid(30));
				// Can we rollback on this connection without causing an error?
				con.rollback();
			} catch (SQLException e2) {
				sb.append("UnExpected error: ").append(e2);
			}
		}

		try {
			// restore auto commit mode
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_CisValid",
				"Expected error: java.sql.SQLException: SELECT: no such table 'doesnotexist'\n" +
				"Validating connection: con.isValid? true");
	}

	private void Test_Clargequery() {
		sb.setLength(0);	// clear the output log buffer
		final String query =
			"-- When a query larger than the send buffer is being " +
			"sent, a deadlock situation can occur when the server writes " +
			"data back, blocking because we as client are sending as well " +
			"and not reading.  Hence, to avoid this deadlock, in JDBC a " +
			"separate thread is started in the background such that results " +
			"from the server can be read, while data is still being sent to " +
			"the server.  To test this, we need to trigger the SendThread " +
			"being started, which we do with a quite large query.  We " +
			"construct it by repeating some stupid query plus a comment " +
			"a lot of times.  And as you're guessing by now, you're reading " +
			"this stupid comment that we use :)\n" +
			"select 1;\n";

		final int size = 1234;
		StringBuilder bigq = new StringBuilder(query.length() * size);
		for (int i = 0; i < size; i++) {
			bigq.append(query);
		}

		Statement stmt = null;
		try {
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
			stmt = con.createStatement();

			// sending big script with many simple queries
			sb.append("1. executing script\n");
			stmt.execute(bigq.toString());

			int i = 1;	// we skip the first "getResultSet()"
			while (stmt.getMoreResults() != false) {
				i++;
			}
			if (stmt.getUpdateCount() != -1) {
				sb.append("Error: found an update count for a SELECT query\n");
			}
			if (i != size) {
				sb.append("Error: expecting ").append(size).append(" tuples, only got ").append(i).append("\n");
			}
			sb.append("2. queries processed\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Clargequery",
				"0. true	true\n" +
				"1. executing script\n" +
				"2. queries processed\n");
	}

	private void Test_Cmanycon(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		final int maxCons = 60;	// default max_clients is 64, 2 connections are already open from this program
		List<Connection> cons = new ArrayList<Connection>(maxCons);	// Connections go in here
		try {
			// spawn a lot of Connections, just for fun...
			int i = 1;
			sb.append("Establishing Connection ");
			for (; i <= maxCons; i++) {
				sb.append(i);
				Connection conx = DriverManager.getConnection(arg0);
				sb.append(",");
				cons.add(conx);

				// do something with the connection to test if it works
				conx.setAutoCommit(false);
				sb.append(" ");
				conx.createStatement();
			}
			sb.append("\n");

			// now try to nicely close them
			i = 1;
			sb.append("Closing Connection ");
			for (Iterator<Connection> it = cons.iterator(); it.hasNext(); i++) {
				Connection conx = it.next();
				// see if the connection still works
				sb.append(i);
				conx.setAutoCommit(true);
				sb.append(",");
				conx.close();	// this will also implicitly close the created statement object
				sb.append(" ");
			}
		} catch (SQLException e) {
			sb.append(" FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_Cmanycon",
			"Establishing Connection 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, " +
			"11, 12, 13, 14, 15, 16, 17, 18, 19, 20, " +
			"21, 22, 23, 24, 25, 26, 27, 28, 29, 30, " +
			"31, 32, 33, 34, 35, 36, 37, 38, 39, 40, " +
			"41, 42, 43, 44, 45, 46, 47, 48, 49, 50, " +
			"51, 52, 53, 54, 55, 56, 57, 58, 59, 60, \n" +
			"Closing Connection 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, " +
			"11, 12, 13, 14, 15, 16, 17, 18, 19, 20, " +
			"21, 22, 23, 24, 25, 26, 27, 28, 29, 30, " +
			"31, 32, 33, 34, 35, 36, 37, 38, 39, 40, " +
			"41, 42, 43, 44, 45, 46, 47, 48, 49, 50, " +
			"51, 52, 53, 54, 55, 56, 57, 58, 59, 60, ");
	}

	private void Test_Creplysize() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt1 = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> true: auto commit should be off by now
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			stmt1 = con.createStatement();
			// test commit by checking if a change is visible in another connection
			sb.append("1. create... ");
			stmt1.executeUpdate("CREATE TABLE table_Test_Creplysize ( id int )");
			sb.append("passed\n");

			sb.append("2. populating with 21 records... ");
			for (int i = 0; i < 21; i++)
				stmt1.executeUpdate("INSERT INTO table_Test_Creplysize (id) values (" + (i + 1) + ")");
			sb.append("passed\n");

			sb.append("3. hinting the driver to use fetchsize 10... ");
			stmt1.setFetchSize(10);
			sb.append("passed\n");

			sb.append("4. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			int i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 21) {
				sb.append("passed");
			} else {
				sb.append("got ").append(i).append(" records!!!");
			}
			sb.append("\n");

			sb.append("5. resetting driver fetchsize hint... ");
			stmt1.setFetchSize(0);
			sb.append("passed\n");

			sb.append("6. instructing the driver to return at max 10 rows...  ");
			stmt1.setMaxRows(10);
			sb.append("passed\n");

			sb.append("7. selecting all values...  ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 10) {
				sb.append("passed");
			} else {
				sb.append("got ").append(i).append(" records!!!");
			}
			sb.append("\n");

			sb.append("8. hinting the driver to use fetchsize 5... ");
			stmt1.setFetchSize(5);
			sb.append("passed\n");

			sb.append("9. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 10) {
				sb.append("passed");
			} else {
				sb.append("got ").append(i).append(" records!!!");
			}
			sb.append("\n");

			sb.append("10. drop... ");
			stmt1.executeUpdate("DROP TABLE table_Test_Creplysize");
			sb.append("passed\n");

			con.rollback();

			// restore auto commit mode
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt1, rs);

		compareExpectedOutput("Test_Creplysize",
			"0. true	false\n" +
			"1. create... passed\n" +
			"2. populating with 21 records... passed\n" +
			"3. hinting the driver to use fetchsize 10... passed\n" +
			"4. selecting all values... passed\n" +
			"5. resetting driver fetchsize hint... passed\n" +
			"6. instructing the driver to return at max 10 rows...  passed\n" +
			"7. selecting all values...  passed\n" +
			"8. hinting the driver to use fetchsize 5... passed\n" +
			"9. selecting all values... passed\n" +
			"10. drop... passed\n");
	}

	private void Test_Csavepoints() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			// savepoints require a non-autocommit connection
			try {
				sb.append("1. savepoint...");
				con.setSavepoint();
				sb.append("passed !!");
			} catch (SQLException e) {
				sb.append("expected msg: ").append(e.getMessage());
			}
			sb.append("\n");

			con.setAutoCommit(false);
			// >> true: auto commit should be on by default
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			sb.append("2. savepoint...");
			/* make a savepoint, and discard it */
			con.setSavepoint();
			sb.append("passed\n");

			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE table_Test_Csavepoints ( id int, PRIMARY KEY (id) )");

			sb.append("3. savepoint...");
			Savepoint sp2 = con.setSavepoint("empty table");
			sb.append("passed\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			int i = 0;
			int items = 0;
			sb.append("4. table ").append(items).append(" items");
			while (rs.next()) {
				sb.append(", ").append(rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (").append(i).append(")");
			}
			sb.append(" passed\n");

			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (1)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (2)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (3)");

			sb.append("5. savepoint...");
			Savepoint sp3 = con.setSavepoint("three values");
			sb.append("passed\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			sb.append("6. table ").append(items).append(" items");
			while (rs.next()) {
				sb.append(", ").append(rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (").append(i).append(")");
			}
			sb.append(" passed\n");

			sb.append("7. release...");
			con.releaseSavepoint(sp3);
			sb.append("passed\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			sb.append("8. table ").append(items).append(" items");
			while (rs.next()) {
				sb.append(", ").append(rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (").append(i).append(") :(");
			}
			sb.append(" passed\n");

			sb.append("9. rollback...");
			con.rollback(sp2);
			sb.append("passed\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 0;
			sb.append("10. table ").append(items).append(" items");
			while (rs.next()) {
				sb.append(", ").append(rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (").append(i).append(") :(");
			}
			sb.append(" passed");

			con.rollback();

			// restore auto commit mode
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Csavepoints",
			"0. true	true\n" +
			"1. savepoint...expected msg: SAVEPOINT: not allowed in auto commit mode\n" +
			"0. false	false\n" +
			"2. savepoint...passed\n" +
			"3. savepoint...passed\n" +
			"4. table 0 items passed\n" +
			"5. savepoint...passed\n" +
			"6. table 3 items, 1, 2, 3 passed\n" +
			"7. release...passed\n" +
			"8. table 3 items, 1, 2, 3 passed\n" +
			"9. rollback...passed\n" +
			"10. table 0 items passed");
	}

	private void Test_Ctransaction() {
		sb.setLength(0);	// clear the output log buffer

		try {
			// test commit by checking if a change is visible in another connection
			sb.append("1. commit...");
			con.commit();
			sb.append("passed");
		} catch (SQLException e) {
			// this means we get what we expect
			sb.append("failed as expected: ").append(e.getMessage());
		}
		sb.append("\n");

		try {
			// turn off auto commit
			con.setAutoCommit(false);
			// >> false: we just disabled it
			sb.append("2. false\t").append(con.getAutoCommit()).append("\n");

			// a change would not be visible now
			sb.append("3. commit...");
			con.commit();
			sb.append("passed\n");

			sb.append("4. commit...");
			con.commit();
			sb.append("passed\n");

			sb.append("5. rollback...");
			con.rollback();
			sb.append("passed");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage());
		}
		sb.append("\n");

		Statement stmt = null;
		try {
			// turn on auto commit
			con.setAutoCommit(true);
			// >> false: we just disabled it
			sb.append("6. true\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("7. start transaction...");
			stmt.executeUpdate("START TRANSACTION");
			sb.append("passed\n");

			sb.append("8. commit...");
			con.commit();
			sb.append("passed\n");

			sb.append("9. true\t").append(con.getAutoCommit());
			sb.append("\n");

			sb.append("10. start transaction...");
			stmt.executeUpdate("START TRANSACTION");
			sb.append("passed\n");

			sb.append("11. rollback...");
			con.rollback();
			sb.append("passed\n");

			sb.append("12. true\t").append(con.getAutoCommit());
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage());
		}
		sb.append("\n");

		try {
			// a commit now should fail
			sb.append("13. commit...");
			con.commit();
			sb.append("passed");
		} catch (SQLException e) {
			// this means we get what we expect
			sb.append("failed as expected: ").append(e.getMessage());
		}
		sb.append("\n");

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Ctransaction",
			"1. commit...failed as expected: COMMIT: not allowed in auto commit mode\n" +
			"2. false	false\n" +
			"3. commit...passed\n" +
			"4. commit...passed\n" +
			"5. rollback...passed\n" +
			"6. true	true\n" +
			"7. start transaction...passed\n" +
			"8. commit...passed\n" +
			"9. true	true\n" +
			"10. start transaction...passed\n" +
			"11. rollback...passed\n" +
			"12. true	true\n" +
			"13. commit...failed as expected: COMMIT: not allowed in auto commit mode\n");
	}

	private void Test_Driver(String con_URL) {
		sb.setLength(0);	// clear the output log buffer

		try {
			listDriverProperties(con_URL);
			// also test against monetdbs, this should make tls and cert required.
			sb.append("getPropertyInfo of jdbc:monetdbs:").append("\n");
			listDriverProperties("jdbc:monetdbs:");
		} catch (SQLException e) {
			// this means we get what we expect
			sb.append("failed to get Driver class: ").append(e.getMessage());
			sb.append("\n");
		}

		compareExpectedOutput("Test_Driver",
			"0  user  true    user name to authenticate as\n" +
			"1  password  true    password to authenticate with\n" +
			"2  host  false    IP number, domain name or one of the special values `localhost` and `localhost.`\n" +
			"3  port  false  -1  Port to connect to, 1..65535 or -1 for 'not set'\n" +
			"4  database  false    name of database to connect to\n" +
			"5  autocommit  false  true  initial value of autocommit\n" +
			"6  schema  false    initial schema\n" +
			"7  timezone  false  <tz>  client time zone as minutes east of UTC\n" +
			"8  replysize  false  250  rows beyond this limit are retrieved on demand, <1 means unlimited\n" +
			"9  debug  false  false  enable tracing of socket communication for debugging\n" +
			"10  logfile  false    when debug is enabled its output will be written to this logfile\n" +
			"11  so_timeout  false  0  abort if network I/O does not complete in this many milliseconds, 0 means no timeout\n" +
			"12  treat_clob_as_varchar  false  true  map CLOB/TEXT data to type VARCHAR instead of type CLOB\n" +
			"13  treat_blob_as_binary  false  true  map BLOB data to type BINARY instead of type BLOB\n" +
			"14  client_info  false  true  whether to send ClientInfo when connecting\n" +
			"15  client_application  false    application name to send in ClientInfo\n" +
			"16  client_remark  false    any client remark to send in ClientInfo\n" +
			"getPropertyInfo of jdbc:monetdbs:\n" +
			"0  user  true    user name to authenticate as\n" +
			"1  password  true    password to authenticate with\n" +
			"2  tls  false  false  secure the connection using TLS\n" +
			"3  host  false    IP number, domain name or one of the special values `localhost` and `localhost.`\n" +
			"4  port  false  -1  Port to connect to, 1..65535 or -1 for 'not set'\n" +
			"5  database  false    name of database to connect to\n" +
			"6  cert  false    path to TLS certificate to authenticate server with\n" +
			"7  certhash  false    hash of server TLS certificate must start with these hex digits; overrides cert\n" +
			"8  autocommit  false  true  initial value of autocommit\n" +
			"9  schema  false    initial schema\n" +
			"10  timezone  false  <tz>  client time zone as minutes east of UTC\n" +
			"11  replysize  false  250  rows beyond this limit are retrieved on demand, <1 means unlimited\n" +
			"12  debug  false  false  enable tracing of socket communication for debugging\n" +
			"13  logfile  false    when debug is enabled its output will be written to this logfile\n" +
			"14  so_timeout  false  0  abort if network I/O does not complete in this many milliseconds, 0 means no timeout\n" +
			"15  treat_clob_as_varchar  false  true  map CLOB/TEXT data to type VARCHAR instead of type CLOB\n" +
			"16  treat_blob_as_binary  false  true  map BLOB data to type BINARY instead of type BLOB\n" +
			"17  client_info  false  true  whether to send ClientInfo when connecting\n" +
			"18  client_application  false    application name to send in ClientInfo\n" +
			"19  client_remark  false    any client remark to send in ClientInfo\n");
	}

	private void listDriverProperties(String url) throws SQLException {
		final Driver driver = DriverManager.getDriver(url);
		DriverPropertyInfo[] props = driver.getPropertyInfo(url, null);
		for (int i = 0; i < props.length; i++) {
			DriverPropertyInfo prop = props[i];
			final String name = prop.name;
			String value = prop.value;
			if (name.equals("timezone"))
				value = "<tz>";
			sb.append(i).append("  ");
			sb.append(name).append("  ");
			sb.append(prop.required).append("  ");
			sb.append(value).append("  ");
			sb.append(prop.description).append("\n");
		}

	}

	private void handleExecuteDDL(Statement stmt, String action, String objtype, String objname, String sql) {
		try {
			int response = stmt.executeUpdate(sql);
			if (response != 0)
				sb.append(action).append(" ").append(objtype).append(" ").append(objname).append(" failed to return 0!! It returned: ").append(response).append("\n");
		} catch (SQLException e) {
			sb.append("Failed to ").append(action).append(" ").append(objtype).append(" ").append(objname).append(": ").append(e.getMessage()).append("\n");
		}
	}

	private void Test_Dobjects() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("failed to createStatement: ").append(e.getMessage()).append("\n");
		}

		String action = "Create";
		final String objtype = "table";
		handleExecuteDDL(stmt, action, objtype, "nopk_twoucs",
			"CREATE TABLE nopk_twoucs (id INT NOT NULL UNIQUE, name VARCHAR(99) UNIQUE)");

		handleExecuteDDL(stmt, action, objtype, "tmp_nopk_twoucs",
			"CREATE LOCAL TEMP TABLE tmp_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE)");

		handleExecuteDDL(stmt, action, objtype, "tmp_pk_uc",
			"CREATE LOCAL TEMP TABLE tmp_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE)");

		handleExecuteDDL(stmt, action, objtype, "glbl_nopk_twoucs",
			"CREATE GLOBAL TEMP TABLE glbl_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE)");

		handleExecuteDDL(stmt, action, objtype, "glbl_pk_uc",
			"CREATE GLOBAL TEMP TABLE glbl_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE)");

		handleExecuteDDL(stmt, action, "type", "xml", "CREATE TYPE xml EXTERNAL NAME xml");

		try {
			DatabaseMetaData dbmd = con.getMetaData();

			// inspect the catalog by use of dbmd functions
			compareResultSet(dbmd.getCatalogs(), "getCatalogs()",
			"Resultset with 1 columns\n" +
			"TABLE_CAT\n" +
			"char(1)\n");

			compareResultSet(dbmd.getSchemas(null, "sys"), "getSchemas(null, sys)",
			"Resultset with 2 columns\n" +
			"TABLE_SCHEM	TABLE_CATALOG\n" +
			"varchar(1024)	char(1)\n" +
			"sys	null\n");

			compareResultSet(dbmd.getTables(null, "tmp", null, null), "getTables(null, tmp, null, null)",	// schema tmp has 6 system tables and 4 temporary test tables
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n" +
			"null	tmp	glbl_nopk_twoucs	GLOBAL TEMPORARY TABLE	null	null	null	null	null	null\n" +
			"null	tmp	glbl_pk_uc	GLOBAL TEMPORARY TABLE	null	null	null	null	null	null\n" +
			"null	tmp	tmp_nopk_twoucs	LOCAL TEMPORARY TABLE	null	null	null	null	null	null\n" +
			"null	tmp	tmp_pk_uc	LOCAL TEMPORARY TABLE	null	null	null	null	null	null\n" +
			"null	tmp	_columns	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	_tables	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	idxs	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	keys	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	objects	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	triggers	SYSTEM TABLE	null	null	null	null	null	null\n");

			compareResultSet(dbmd.getTables(null, "sys", "schemas", null), "getTables(null, sys, schemas, null)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n" +
			"null	sys	schemas	SYSTEM TABLE	null	null	null	null	null	null\n");

			compareResultSet(dbmd.getColumns(null, "sys", "table\\_types", null), "getColumns(null, sys, table\\_types, null)",
			"Resultset with 24 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE	IS_AUTOINCREMENT	IS_GENERATEDCOLUMN\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	int	varchar(1024)	int	int	int	int	int	varchar(65000)	varchar(2048)	int	int	bigint	int	varchar(3)	char(1)	char(1)	char(1)	smallint	" + (isPostDec2023 ? "var" : "") + "char(3)	varchar(3)\n" +
			"null	sys	table_types	table_type_id	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	2	0	null	null	0	0	null	1	NO	null	null	null	null	NO	NO\n" +
			"null	sys	table_types	table_type_name	12	varchar	25	0	0	0	0	null	null	0	0	100	2	NO	null	null	null	null	NO	NO\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "sys", "table\\_types"), "getPrimaryKeys(null, sys, table\\_types)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	sys	table_types	table_type_id	1	table_types_table_type_id_pkey\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "tmp", "tmp_pk_uc"), "getPrimaryKeys(null, tmp, tmp_pk_uc)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	tmp	tmp_pk_uc	id1	1	tmp_pk_uc_id1_pkey\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "tmp", "glbl_pk_uc"), "getPrimaryKeys(null, tmp, glbl_pk_uc)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	tmp	glbl_pk_uc	id1	1	glbl_pk_uc_id1_pkey\n");

			compareResultSet(dbmd.getExportedKeys(null, "sys", "table\\_types"), "getExportedKeys(null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getCrossReference(null, "sys", "tables", null, "sys", "table\\_types"), "getCrossReference(null, sys, tables, null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getImportedKeys(null, "sys", "table\\_types"), "getImportedKeys(null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getIndexInfo(null, "sys", "key_types", false, false), "getIndexInfo(null, sys, key_types, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	sys	key_types	false	null	key_types_key_type_id_pkey	2	1	key_type_id	null	" + (isPostDec2023 ? "5" : "3") + "	0	null\n" +
			"null	sys	key_types	false	null	key_types_key_type_name_unique	2	1	key_type_name	null	" + (isPostDec2023 ? "5" : "3") + "	0	null\n");

			compareResultSet(dbmd.getIndexInfo(null, "tmp", "tmp_pk_uc", false, false), "getIndexInfo(null, tmp, tmp_pk_uc, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	tmp	tmp_pk_uc	false	null	tmp_pk_uc_id1_pkey	2	1	id1	null	0	0	null\n" +
			"null	tmp	tmp_pk_uc	false	null	tmp_pk_uc_name1_unique	2	1	name1	null	0	0	null\n");

			compareResultSet(dbmd.getIndexInfo(null, "tmp", "glbl_pk_uc", false, false), "getIndexInfo(null, tmp, glbl_pk_uc, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	tmp	glbl_pk_uc	false	null	glbl_pk_uc_id1_pkey	2	1	id1	null	0	0	null\n" +
			"null	tmp	glbl_pk_uc	false	null	glbl_pk_uc_name1_unique	2	1	name1	null	0	0	null\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "function_languages", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, function_languages, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	language_id	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, sys, nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_pk_uc", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, tmp_pk_uc, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id1	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, tmp_nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, tmp, tmp_nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_pk_uc", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, glbl_pk_uc, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id1	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, glbl_nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, tmp, glbl_nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getTablePrivileges(null, "sys", "table\\_types"), "getTablePrivileges(null, sys, table\\_types)",
			"Resultset with 7 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(40)	varchar(3)\n" +
			"null	sys	table_types	monetdb	public	SELECT	NO\n");

			compareResultSet(dbmd.getColumnPrivileges(null, "sys", "table\\_types", null), "getColumnPrivileges(null, sys, table\\_types, null)",
			"Resultset with 8 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(40)	varchar(3)\n");

			compareResultSet(dbmd.getUDTs(null, "sys", null, null), "getUDTs(null, sys, null, null)",
			"Resultset with 7 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	CLASS_NAME	DATA_TYPE	REMARKS	BASE_TYPE\n" +
			"char(1)	varchar(1024)	varchar(1024)	" + (isPostDec2023 ? "var" : "") + "char(16)	int	varchar(256)	smallint\n" +
			"null	sys	xml	java.lang.String	2000	xml	null\n");

			int[] UDTtypes = { Types.STRUCT, Types.DISTINCT };
			compareResultSet(dbmd.getUDTs(null, "sys", null, UDTtypes), "getUDTs(null, sys, null, UDTtypes",
			"Resultset with 7 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	CLASS_NAME	DATA_TYPE	REMARKS	BASE_TYPE\n" +
			"char(1)	varchar(1024)	varchar(1024)	" + (isPostDec2023 ? "var" : "") + "char(16)	int	varchar(256)	smallint\n");

			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.setLength(0);	// clear the output log buffer
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup created db objects
		action = "Drop";
		handleExecuteDDL(stmt, action, objtype, "nopk_twoucs", "DROP TABLE nopk_twoucs");
		handleExecuteDDL(stmt, action, objtype, "tmp_nopk_twoucs", "DROP TABLE tmp_nopk_twoucs");
		handleExecuteDDL(stmt, action, objtype, "tmp_pk_uc", "DROP TABLE tmp_pk_uc");
		handleExecuteDDL(stmt, action, objtype, "glbl_nopk_twoucs", "DROP TABLE glbl_nopk_twoucs");
		handleExecuteDDL(stmt, action, objtype, "glbl_pk_uc", "DROP TABLE glbl_pk_uc");
		handleExecuteDDL(stmt, action, "type", "xml", "DROP TYPE xml");

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Dobjects", "");
	}

	private void compareResultSet(ResultSet rs, String methodnm, String expected) throws SQLException {
		sb.setLength(0);	// clear the output log buffer

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		int suppressSPECIFIC_NAME = 0;
		sb.append("Resultset with ").append(columnCount).append(" columns\n");
		// print result column header names
		for (int col = 1; col <= columnCount; col++) {
			if (col > 1)
				sb.append("\t");
			sb.append(rsmd.getColumnName(col));
			if ("SPECIFIC_NAME".equals(rsmd.getColumnName(col)))
				suppressSPECIFIC_NAME = col;	// contains internal id values which change
		}
		sb.append("\n");
		// print result column data type info
		for (int col = 1; col <= columnCount; col++) {
			if (col > 1)
				sb.append("\t");
			sb.append(rsmd.getColumnTypeName(col));
			switch (rsmd.getColumnType(col)) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.CLOB:
				case Types.BLOB:
				case Types.DECIMAL:
				case Types.NUMERIC:
				{
					int prec = rsmd.getPrecision(col);
					if (prec != 0) {
						sb.append('(').append(prec);
						int scale = rsmd.getScale(col);
						if (scale != 0) {
							sb.append(',').append(scale);
						}
						sb.append(')');
					}
				}
			}
		}
		sb.append("\n");

		// print result rows data
		while (rs.next()) {
			for (int col = 1; col <= columnCount; col++) {
				if (col > 1)
					sb.append("\t");
				if (col == suppressSPECIFIC_NAME)
					sb.append("suppressed");
				else
					sb.append(rs.getString(col));
			}
			sb.append("\n");
		}
		rs.close();

		compareExpectedOutput(methodnm, expected);
	}

	// same tests as done in clients/odbc/tests/ODBCmetadata.c
	private void Test_DBCmetadata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		DatabaseMetaData dbmd = null;
		try {
			stmt = con.createStatement();
			dbmd = con.getMetaData();
		} catch (SQLException e) {
			sb.append("Failed to createStatement: ").append(e.getMessage()).append("\n");
		}

		try {
			// test dbmd methods which sent a query to the server and return a String.
			String s = dbmd.getSQLKeywords();
			if (s == null || s.length() < 10)
				sb.append("getSQLKeywords()").append(" failed to return a large enough list\n");
			s = dbmd.getNumericFunctions();
			if (s == null || s.length() < 10)
				sb.append("getNumericFunctions()").append(" failed to return a large enough list\n");
			s = dbmd.getStringFunctions();
			if (s == null || s.length() < 10)
				sb.append("getStringFunctions()").append(" failed to return a large enough list\n");
			s = dbmd.getSystemFunctions();
			if (s == null || s.length() < 10)
				sb.append("getSystemFunctions()").append(" failed to return a large enough list\n");
			s = dbmd.getTimeDateFunctions();
			if (s == null || s.length() < 10)
				sb.append("getTimeDateFunctions()").append(" failed to return a large enough list\n");
			s = dbmd.getUserName();
			if (s == null || s.length() < 1)
				sb.append("getUserName() failed to return a valid name\n");
			s = dbmd.getDatabaseProductVersion();
			if (s == null || s.length() < 1)
				sb.append("getDatabaseProductVersion() failed to return a valid version\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		compareExpectedOutput("Test_DBCmetadata() testing dbmd.get...Functions() as String", "");
		sb.setLength(0);	// clear the output log buffer

		/* servers before Jan2022 (11.45) have a problem (server crash and db corruption)
		   with indexes created on temp tables, so conditionally execute those commands and tests */
		final boolean testCreateDropIndexOnTmpTables = !(dbmsMajorVersion == 11 && dbmsMinorVersion <= 43);

		String action = "Create";
		handleExecuteDDL(stmt, action, "schema", "jdbctst", "CREATE SCHEMA jdbctst; SET SCHEMA jdbctst;");

		String objtype = "table";
		// create tables to populate data dictionary. Used for testing getTables(),
		// getColumns(), getBestRowIdentifier(), getPrimaryKeys(),
		// getExportedKeys(), getImportedKeys(), getCrossReference()
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc",
			"CREATE TABLE jdbctst.pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc",
			"CREATE LOCAL TEMP TABLE tmp.tmp_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_pk_uc",
			"CREATE GLOBAL TEMP TABLE tmp.glbl_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs",
			"CREATE TABLE jdbctst.nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_nopk_twoucs",
			"CREATE LOCAL TEMP TABLE tmp.tmp_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_nopk_twoucs",
			"CREATE GLOBAL TEMP TABLE tmp.glbl_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);");
		handleExecuteDDL(stmt, action, objtype, "tmp.tlargechar",
			"CREATE TEMP TABLE tlargechar (c1 varchar(2147483647), c2 char(2147483646), c3 clob(2147483645), c4 json(2147483644), c5 url(2147483643)) ON COMMIT PRESERVE ROWS;");
		/* next 3 tables copied from example in https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function?view=sql-server-ver15 */
		handleExecuteDDL(stmt, action, objtype, "\"CUSTOMERS\"",
			"CREATE TABLE \"CUSTOMERS\" (\"CUSTID\" INT PRIMARY KEY, \"NAME\" VARCHAR(60) NOT NULL, \"ADDRESS\" VARCHAR(90), \"PHONE\" VARCHAR(20));");
		handleExecuteDDL(stmt, action, objtype, "\"ORDERS\"",
			"CREATE TABLE \"ORDERS\" (\"ORDERID\" INT PRIMARY KEY, \"CUSTID\" INT NOT NULL REFERENCES \"CUSTOMERS\" (\"CUSTID\")" +
			", \"OPENDATE\" DATE NOT NULL, \"SALESPERSON\" VARCHAR(60), \"STATUS\" VARCHAR(10) NOT NULL);");
		handleExecuteDDL(stmt, action, objtype, "\"LINES\"",
			"CREATE TABLE \"LINES\" (\"ORDERID\" INT NOT NULL REFERENCES \"ORDERS\" (\"ORDERID\"), \"LINES\" INT" +
			", PRIMARY KEY (\"ORDERID\", \"LINES\"), \"PARTID\" INT NOT NULL, \"QUANTITY\" DECIMAL(9,3) NOT NULL);");
		/* also test situation where one table has multiple fks to the same multi column pk */
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk2c",
			"CREATE TABLE jdbctst.pk2c (pkc1 INT, pkc2 VARCHAR(99), name1 VARCHAR(99) UNIQUE, PRIMARY KEY (pkc2, pkc1));");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.fk2c",
			"CREATE TABLE jdbctst.fk2c (fkc1 INT NOT NULL PRIMARY KEY, fkc2 VARCHAR(99), fkc3 INT" +
			", FOREIGN KEY (fkc2, fkc1) REFERENCES jdbctst.pk2c (pkc2, pkc1) ON UPDATE CASCADE ON DELETE RESTRICT" +
			", FOREIGN KEY (fkc2, fkc3) REFERENCES jdbctst.pk2c (pkc2, pkc1) ON UPDATE SET NULL ON DELETE NO ACTION);");

		// create indexes to populate catalog. Used for testing getIndexInfo()
		objtype = "index";
		handleExecuteDDL(stmt, action, objtype, "pk_uc_i",
			"CREATE INDEX pk_uc_i ON jdbctst.pk_uc (id1, name1);");
		handleExecuteDDL(stmt, action, objtype, "nopk_twoucs_i",
			"CREATE INDEX nopk_twoucs_i ON jdbctst.nopk_twoucs (id2, name2);");
		if (testCreateDropIndexOnTmpTables) {
			handleExecuteDDL(stmt, action, objtype, "tmp_pk_uc_i",
				"CREATE INDEX tmp_pk_uc_i ON tmp.tmp_pk_uc (id1, name1);");
			handleExecuteDDL(stmt, action, objtype, "glbl_pk_uc_i",
				"CREATE INDEX glbl_pk_uc_i ON tmp.glbl_pk_uc (id1, name1);");
			handleExecuteDDL(stmt, action, objtype, "tmp_nopk_twoucs_i",
				"CREATE INDEX tmp_nopk_twoucs_i ON tmp.tmp_nopk_twoucs (id2, name2);");
			handleExecuteDDL(stmt, action, objtype, "glbl_nopk_twoucs_i",
				"CREATE INDEX glbl_nopk_twoucs_i ON tmp.glbl_nopk_twoucs (id2, name2);");
		}

		// grant privileges to populate catalog. Used for testing getTablePrivileges() and getColumnPrivileges()
		action = "grant";
		objtype = "table";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc",
			"GRANT SELECT ON TABLE jdbctst.pk_uc TO PUBLIC;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc",
			"GRANT INSERT, UPDATE, DELETE ON TABLE jdbctst.pk_uc TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs",
			"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE jdbctst.nopk_twoucs TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc",
			"GRANT INSERT, DELETE ON TABLE tmp.tmp_pk_uc TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc",
			"GRANT SELECT (id1, name1), UPDATE (name1) ON TABLE tmp.tmp_pk_uc TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc ",
			"GRANT INSERT, DELETE ON TABLE tmp.tmp_pk_uc TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_pk_uc",
			"GRANT SELECT (id1, name1), UPDATE (name1) ON TABLE tmp.glbl_pk_uc TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_nopk_twoucs",
			"GRANT INSERT, DELETE ON TABLE tmp.tmp_nopk_twoucs TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_nopk_twoucs",
			"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE tmp.tmp_nopk_twoucs TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_nopk_twoucs",
			"GRANT DELETE, INSERT ON TABLE tmp.glbl_nopk_twoucs TO monetdb;");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_nopk_twoucs",
			"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE tmp.glbl_nopk_twoucs TO monetdb;");

		// create user procedure to test getProcedures() and getProcedureColumns()
		action = "create";
		objtype = "procedure";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.proc",
			"CREATE PROCEDURE jdbctst.proc(id bigint, name varchar(999), dt date) BEGIN  SET SCHEMA jdbctst; /* ... */ END;");

		// create user function to test getFunctions() and getFunctionColumns()
		action = "create";
		objtype = "function";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.func",
			"CREATE FUNCTION jdbctst.func(id bigint, name varchar(999), dt date) RETURNS TABLE (val varchar(1022), dt date, id int) BEGIN return SELECT id||' '||name ||' '|| dt, dt, id WHERE id > 0; END;");

		// set COMMENT ON schema, tables, columns, indexes, procedures and functions to fetch (and test) data in the REMARKS result column
		action = "comment on";
		handleExecuteDDL(stmt, action, "schema", "jdbctst",
			"COMMENT ON SCHEMA jdbctst IS 'jdbctst schema comment';");
		objtype = "table";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc",
			"COMMENT ON TABLE jdbctst.pk_uc IS 'jdbctst.pk_uc table comment';");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs",
			"COMMENT ON TABLE jdbctst.nopk_twoucs IS 'jdbctst.nopk_twoucs table comment';");
		objtype = "column";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs.id2",
			"COMMENT ON COLUMN jdbctst.nopk_twoucs.id2 IS 'jdbctst.nopk_twoucs.id2 column comment';");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs.name2",
			"COMMENT ON COLUMN jdbctst.nopk_twoucs.name2 IS 'jdbctst.nopk_twoucs.name2 column comment';");
		objtype = "index";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc_i",
			"COMMENT ON INDEX jdbctst.pk_uc_i IS 'jdbctst.pk_uc_i index comment';");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs_i",
			"COMMENT ON INDEX jdbctst.nopk_twoucs_i IS 'jdbctst.nopk_twoucs_i index comment';");
		objtype = "procedure";
		handleExecuteDDL(stmt, action, objtype, "sys.analyze()",
			"COMMENT ON PROCEDURE sys.analyze() IS 'sys.analyze() procedure comment';");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.proc",
			"COMMENT ON PROCEDURE jdbctst.proc IS 'jdbctst.proc procedure comment';");
		objtype = "function";
		handleExecuteDDL(stmt, action, objtype, "sys.sin(double)",
			"COMMENT ON FUNCTION sys.sin(double) IS 'sys.sin(double) function comment';");
		handleExecuteDDL(stmt, action, objtype, "sys.env()",
			"COMMENT ON FUNCTION sys.env() IS 'sys.env() function comment';");
		handleExecuteDDL(stmt, action, objtype, "sys.statistics()",
			"COMMENT ON FUNCTION sys.statistics() IS 'sys.statistics() function comment';");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.func",
			"COMMENT ON FUNCTION jdbctst.func IS 'jdbctst.func function comment';");

		try {
			// query the catalog by calling DatabaseMetaData methods
			compareResultSet(dbmd.getCatalogs(), "getCatalogs()",
			"Resultset with 1 columns\n" +
			"TABLE_CAT\n" +
			"char(1)\n");

			compareResultSet(dbmd.getSchemas(null, "jdbctst"), "getSchemas(null, jdbctst)",
			"Resultset with 2 columns\n" +
			"TABLE_SCHEM	TABLE_CATALOG\n" +
			"varchar(1024)	char(1)\n" +
			"jdbctst	null\n");

			compareResultSet(dbmd.getSchemas(null, ""), "getSchemas(null, emptystring)",
			"Resultset with 2 columns\n" +
			"TABLE_SCHEM	TABLE_CATALOG\n" +
			"varchar(1024)	char(1)\n");

			compareResultSet(dbmd.getSchemas("%", "%%"), "getSchemas(%, %%)",
			"Resultset with 2 columns\n" +
			"TABLE_SCHEM	TABLE_CATALOG\n" +
			"varchar(1024)	char(1)\n");

			compareResultSet(dbmd.getTables(null, "jdbctst", null, null), "getTables(null, jdbctst, null, null)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n" +
			"null	jdbctst	CUSTOMERS	TABLE	null	null	null	null	null	null\n" +
			"null	jdbctst	LINES	TABLE	null	null	null	null	null	null\n" +
			"null	jdbctst	ORDERS	TABLE	null	null	null	null	null	null\n" +
			"null	jdbctst	fk2c	TABLE	null	null	null	null	null	null\n" +
			"null	jdbctst	nopk_twoucs	TABLE	jdbctst.nopk_twoucs table comment	null	null	null	null	null\n" +
			"null	jdbctst	pk2c	TABLE	null	null	null	null	null	null\n" +
			"null	jdbctst	pk_uc	TABLE	jdbctst.pk_uc table comment	null	null	null	null	null\n");

			String tabletypes[] = {"BASE TABLE", "LOCAL TEMPORARY", "GLOBAL TEMPORARY"};
			compareResultSet(dbmd.getTables(null, "tmp", "tlargechar", tabletypes), "getTables(null, tmp, tlargechar, tabletypes)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n" +
			"null	tmp	tlargechar	LOCAL TEMPORARY TABLE	null	null	null	null	null	null\n");

			String badtabletypes[] = {null, "", "'", "\\'", "\"", null, "bad", "%", "% %"};
			compareResultSet(dbmd.getTables(null, "tmp", "tlargechar", badtabletypes), "getTables(null, tmp, tlargechar, badtabletypes)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n");

			compareResultSet(dbmd.getTables(null, "jdbctst", "schemas", null), "getTables(null, jdbctst, schemas, null)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n");

			compareResultSet(dbmd.getColumns(null, "jdbctst", "pk\\_uc", null), "getColumns(null, jdbctst, pk\\_uc, null)",
			"Resultset with 24 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE	IS_AUTOINCREMENT	IS_GENERATEDCOLUMN\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	int	varchar(1024)	int	int	int	int	int	varchar(65000)	varchar(2048)	int	int	bigint	int	varchar(3)	char(1)	char(1)	char(1)	smallint	" + (isPostDec2023 ? "var" : "") + "char(3)	varchar(3)\n" +
			"null	jdbctst	pk_uc	id1	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	2	0	null	null	0	0	null	1	NO	null	null	null	null	NO	NO\n" +
			"null	jdbctst	pk_uc	name1	12	varchar	99	0	0	0	1	null	null	0	0	396	2	YES	null	null	null	null	NO	NO\n");

			compareResultSet(dbmd.getColumns(null, "tmp", "tlargechar", null), "getColumns(null, tmp, tlargechar, null)",
			"Resultset with 24 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE	IS_AUTOINCREMENT	IS_GENERATEDCOLUMN\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	int	varchar(1024)	int	int	int	int	int	varchar(65000)	varchar(2048)	int	int	bigint	int	varchar(3)	char(1)	char(1)	char(1)	smallint	" + (isPostDec2023 ? "var" : "") + "char(3)	varchar(3)\n" +
			"null	tmp	tlargechar	c1	12	varchar	2147483647	0	0	0	1	null	null	0	0	8589934588	1	YES	null	null	null	null	NO	NO\n" +
			"null	tmp	tlargechar	c2	1	char	2147483646	0	0	0	1	null	null	0	0	8589934584	2	YES	null	null	null	null	NO	NO\n" +
			"null	tmp	tlargechar	c3	" + (isPostDec2023 ? "12	varchar" : "2005	clob") + "	2147483645	0	0	0	1	null	null	0	0	8589934580	3	YES	null	null	null	null	NO	NO\n" +
			"null	tmp	tlargechar	c4	12	json	2147483644	0	0	0	1	null	null	0	0	8589934576	4	YES	null	null	null	null	NO	NO\n" +
			"null	tmp	tlargechar	c5	12	url	2147483643	0	0	0	1	null	null	0	0	8589934572	5	YES	null	null	null	null	NO	NO\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "jdbctst", "pk\\_uc"), "getPrimaryKeys(null, jdbctst, pk\\_uc)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	jdbctst	pk_uc	id1	1	pk_uc_id1_pkey\n");

/* MvD: hier verder */
			compareResultSet(dbmd.getPrimaryKeys(null, "tmp", "tmp_pk_uc"), "getPrimaryKeys(null, tmp, tmp_pk_uc)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	tmp	tmp_pk_uc	id1	1	tmp_pk_uc_id1_pkey\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "tmp", "glbl_pk_uc"), "getPrimaryKeys(null, tmp, glbl_pk_uc)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	varchar(1024)\n" +
			"null	tmp	glbl_pk_uc	id1	1	glbl_pk_uc_id1_pkey\n");

			compareResultSet(dbmd.getExportedKeys(null, "sys", "table\\_types"), "getExportedKeys(null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getCrossReference(null, "sys", "tables", null, "sys", "table\\_types"), "getCrossReference(null, sys, tables, null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getImportedKeys(null, "sys", "table\\_types"), "getImportedKeys(null, sys, table\\_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	char(1)	varchar(1024)	varchar(1024)	varchar(1024)	smallint	smallint	smallint	varchar(1024)	varchar(1024)	smallint\n");

			compareResultSet(dbmd.getIndexInfo(null, "sys", "key_types", false, false), "getIndexInfo(null, sys, key_types, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	sys	key_types	false	null	key_types_key_type_id_pkey	2	1	key_type_id	null	" + (isPostDec2023 ? "5" : "3") + "	0	null\n" +
			"null	sys	key_types	false	null	key_types_key_type_name_unique	2	1	key_type_name	null	" + (isPostDec2023 ? "5" : "3") + "	0	null\n");

			if (testCreateDropIndexOnTmpTables) {
			compareResultSet(dbmd.getIndexInfo(null, "tmp", "tmp_pk_uc", false, false), "getIndexInfo(null, tmp, tmp_pk_uc, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	tmp	tmp_pk_uc	false	null	tmp_pk_uc_id1_pkey	2	1	id1	null	0	0	null\n" +
			"null	tmp	tmp_pk_uc	false	null	tmp_pk_uc_name1_unique	2	1	name1	null	0	0	null\n" +
			"null	tmp	tmp_pk_uc	true	null	tmp_pk_uc_i	2	1	id1	null	0	0	null\n" +
			"null	tmp	tmp_pk_uc	true	null	tmp_pk_uc_i	2	2	name1	null	0	0	null\n");
			}

			if (testCreateDropIndexOnTmpTables) {
			compareResultSet(dbmd.getIndexInfo(null, "tmp", "glbl_pk_uc", false, false), "getIndexInfo(null, tmp, glbl_pk_uc, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"char(1)	varchar(1024)	varchar(1024)	boolean	char(1)	varchar(1024)	tinyint	smallint	varchar(1024)	char(1)	int	int	char(1)\n" +
			"null	tmp	glbl_pk_uc	false	null	glbl_pk_uc_id1_pkey	2	1	id1	null	0	0	null\n" +
			"null	tmp	glbl_pk_uc	false	null	glbl_pk_uc_name1_unique	2	1	name1	null	0	0	null\n" +
			"null	tmp	glbl_pk_uc	true	null	glbl_pk_uc_i	2	1	id1	null	0	0	null\n" +
			"null	tmp	glbl_pk_uc	true	null	glbl_pk_uc_i	2	2	name1	null	0	0	null\n");
			}

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "function_languages", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, function_languages, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	language_id	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "jdbctst", "nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, jdbctst, nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "jdbctst", "nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, jdbctst, nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_pk_uc", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, tmp_pk_uc, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id1	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, tmp_nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "tmp_nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, tmp, tmp_nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_pk_uc", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, glbl_pk_uc, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id1	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_nopk_twoucs", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, tmp, glbl_nopk_twoucs, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "tmp", "glbl_nopk_twoucs", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, tmp, glbl_nopk_twoucs, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id2	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n");

			// also test a table without pk or uc, it should return a row for each column
			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "schemas", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, schemas, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n" +
			"2	name	12	varchar	1024	0	0	1\n" +
			"2	authorization	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n" +
			"2	owner	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n" +
			"2	system	16	boolean	1	0	0	1\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "_tables", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, _tables, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	id	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n" +
			"2	name	12	varchar	1024	0	0	1\n" +
			"2	schema_id	4	int	" + (isPostDec2023 ? "31" : "32") + "	0	0	1\n" +
			"2	query	12	varchar	1048576	0	0	1\n" +
			"2	type	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n" +
			"2	system	16	boolean	1	0	0	1\n" +
			"2	commit_action	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n" +
			"2	access	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n");

			// also test a view (without pk or uc of course), it should return no rows
			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "tables", DatabaseMetaData.bestRowTransaction, true),
						"getBestRowIdentifier(null, sys, tables, DatabaseMetaData.bestRowTransaction, true)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n");

			compareResultSet(dbmd.getBestRowIdentifier(null, "sys", "table_types", DatabaseMetaData.bestRowTransaction, false),
						"getBestRowIdentifier(null, sys, table_types, DatabaseMetaData.bestRowTransaction, false)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	varchar(1024)	int	varchar(1024)	int	int	smallint	smallint\n" +
			"2	table_type_id	5	smallint	" + (isPostDec2023 ? "15" : "16") + "	0	0	1\n");

			compareResultSet(dbmd.getProcedures(null, "jdbctst", "proc"), "getProcedures(null, jdbctst, proc)",
			"Resultset with 9 columns\n" +
			"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	Field4	Field5	Field6	REMARKS	PROCEDURE_TYPE	SPECIFIC_NAME\n" +
			"char(1)	varchar(1024)	varchar(256)	char(1)	char(1)	char(1)	varchar(65000)	smallint	varchar(10)\n" +
			"null	jdbctst	proc	null	null	null	jdbctst.proc procedure comment	1	suppressed\n");
			// note that the real SPECIFIC_NAME value (containing the internal functions.id) is replaced with "suppressed" in order to get stable output

			compareResultSet(dbmd.getProcedureColumns(null, "jdbctst", "proc", "%%"), "getProcedureColumns(null, jdbctst, proc, %%)",
			"Resultset with 20 columns\n" +
			"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	PRECISION	LENGTH	SCALE	RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n" +
			"char(1)	varchar(1024)	varchar(256)	varchar(256)	smallint	int	varchar(1024)	int	int	smallint	smallint	smallint	char(1)	char(1)	int	int	bigint	int	varchar(3)	varchar(10)\n" +
			"null	jdbctst	proc	id	1	-5	bigint	19	8	0	10	2	null	null	0	0	null	1		suppressed\n" +
			"null	jdbctst	proc	name	1	12	varchar	999	999	null	null	2	null	null	0	0	3996	2		suppressed\n" +
			"null	jdbctst	proc	dt	1	91	date	0	0	null	null	2	null	null	0	0	null	3		suppressed\n");
			// note that the real SPECIFIC_NAME value (containing the internal functions.id) is replaced with "suppressed" in order to get stable output

			compareResultSet(dbmd.getFunctions(null, "jdbctst", "func"), "getFunctions(null, jdbctst, func)",
			"Resultset with 6 columns\n" +
			"FUNCTION_CAT	FUNCTION_SCHEM	FUNCTION_NAME	REMARKS	FUNCTION_TYPE	SPECIFIC_NAME\n" +
			"char(1)	varchar(1024)	varchar(256)	varchar(65000)	tinyint	varchar(10)\n" +
			"null	jdbctst	func	jdbctst.func function comment	2	suppressed\n");
			// note that the real SPECIFIC_NAME value (containing the internal functions.id) is replaced with "suppressed" in order to get stable output

			compareResultSet(dbmd.getFunctionColumns(null, "jdbctst", "func", "%%"), "getFunctionColumns(null, jdbctst, func, %%)",
			"Resultset with 17 columns\n" +
			"FUNCTION_CAT	FUNCTION_SCHEM	FUNCTION_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	PRECISION	LENGTH	SCALE	RADIX	NULLABLE	REMARKS	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n" +
			"char(1)	varchar(1024)	varchar(256)	varchar(256)	smallint	int	varchar(1024)	int	int	smallint	smallint	smallint	char(1)	bigint	int	varchar(3)	varchar(10)\n" +
			"null	jdbctst	func	val	4	12	varchar	1022	1022	null	null	2	null	4088	0		suppressed\n" +
			"null	jdbctst	func	dt	3	91	date	0	0	null	null	2	null	null	1		suppressed\n" +
			"null	jdbctst	func	id	3	4	int	10	4	0	10	2	null	null	2		suppressed\n" +
			"null	jdbctst	func	id	1	-5	bigint	19	8	0	10	2	null	null	3		suppressed\n" +
			"null	jdbctst	func	name	1	12	varchar	999	999	null	null	2	null	3996	4		suppressed\n" +
			"null	jdbctst	func	dt	1	91	date	0	0	null	null	2	null	null	5		suppressed\n");
			// note that the real SPECIFIC_NAME value (containing the internal functions.id) is replaced with "suppressed" in order to get stable output

			compareResultSet(dbmd.getTablePrivileges(null, "sys", "table\\_types"), "getTablePrivileges(null, sys, table\\_types)",
			"Resultset with 7 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(40)	varchar(3)\n" +
			"null	sys	table_types	monetdb	public	SELECT	NO\n");

			compareResultSet(dbmd.getColumnPrivileges(null, "sys", "table\\_types", null), "getColumnPrivileges(null, sys, table\\_types, null)",
			"Resultset with 8 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(1024)	varchar(40)	varchar(3)\n");

			if (!isPostDec2023) {
				compareResultSet(dbmd.getClientInfoProperties(), "getClientInfoProperties()",
				"Resultset with 4 columns\n" +
				"NAME	MAX_LEN	DEFAULT_VALUE	DESCRIPTION\n" +
				"varchar(40)	int	varchar(128)	varchar(256)\n");
			}

			compareResultSet(dbmd.getSuperTables(null, "jdbctst", "pk_uc"), "getSuperTables(null, jdbctst, pk_uc)",
			"Resultset with 4 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	SUPERTABLE_NAME\n" +
			"char(1)	" + (isPostDec2023 ? "varchar	varchar	varchar\n" : "char	char	char\n"));

			compareResultSet(dbmd.getPseudoColumns(null, "jdbctst", "pk_uc", "%"), "getPseudoColumns(null, jdbctst, pk_uc, %)",
			"Resultset with 12 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	COLUMN_SIZE	DECIMAL_DIGITS	NUM_PREC_RADIX	COLUMN_USAGE	REMARKS	CHAR_OCTET_LENGTH	IS_NULLABLE\n" +
			"char(1)	" + (isPostDec2023 ? "varchar	varchar	varchar" : "char	char	char") + "	int	int	int	int	" + (isPostDec2023 ? "varchar	varchar	int	varchar\n" : "char	char	int	char\n"));

			compareResultSet(dbmd.getVersionColumns(null, "jdbctst", "pk_uc"), "getVersionColumns(null, jdbctst, pk_uc)",
			"Resultset with 8 columns\n" +
			"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n" +
			"smallint	char(1)	int	char(1)	int	int	smallint	smallint\n");

			compareResultSet(dbmd.getSuperTypes(null, "sys", "xml"), "getSuperTypes(null, sys, xml)",
			"Resultset with 6 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SUPERTYPE_CAT	SUPERTYPE_SCHEM	SUPERTYPE_NAME\n" +
			"char(1)	" + (isPostDec2023 ? "varchar	varchar	char(1)	varchar	varchar\n" : "char	char	char(1)	char	char\n"));

			compareResultSet(dbmd.getAttributes(null, "sys", "xml", "%"), "getAttributes(null, sys, xml, %)",
			"Resultset with 21 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	ATTR_NAME	DATA_TYPE	ATTR_TYPE_NAME	ATTR_SIZE	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	ATTR_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE\n" +
			"char(1)	" + (isPostDec2023 ? "varchar	varchar	varchar" : "char	char	char") + "	int	" + (isPostDec2023 ? "var" : "") + "char	int	int	int	int	" + (isPostDec2023 ? "varchar	varchar" : "char	char") + "	int	int	int	int	" + (isPostDec2023 ? "varchar(3)	varchar	varchar	varchar" : "char(3)	char	char	char") + "	smallint\n");

			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.setLength(0);	// clear the output log buffer
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup created db objects
		action = "Drop";
		objtype = "index";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc_i", "DROP INDEX jdbctst.pk_uc_i;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs_i", "DROP INDEX jdbctst.nopk_twoucs_i;");
		if (testCreateDropIndexOnTmpTables) {
			handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc_i;", "DROP INDEX tmp.tmp_pk_uc_i;");
			handleExecuteDDL(stmt, action, objtype, "tmp.glbl_pk_uc_i", "DROP INDEX tmp.glbl_pk_uc_i;");
			handleExecuteDDL(stmt, action, objtype, "tmp.tmp_nopk_twoucs_i", "DROP INDEX tmp.tmp_nopk_twoucs_i;");
			handleExecuteDDL(stmt, action, objtype, "tmp.glbl_nopk_twoucs_i", "DROP INDEX tmp.glbl_nopk_twoucs_i;");
		}
		objtype = "table";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk_uc", "DROP TABLE jdbctst.pk_uc;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_pk_uc", "DROP TABLE tmp.tmp_pk_uc;");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_pk_uc", "DROP TABLE tmp.glbl_pk_uc;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.nopk_twoucs", "DROP TABLE jdbctst.nopk_twoucs;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tmp_nopk_twoucs", "DROP TABLE tmp.tmp_nopk_twoucs;");
		handleExecuteDDL(stmt, action, objtype, "tmp.glbl_nopk_twoucs", "DROP TABLE tmp.glbl_nopk_twoucs;");
		handleExecuteDDL(stmt, action, objtype, "tmp.tlargechar", "DROP TABLE tmp.tlargechar;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.\"LINES\"", "DROP TABLE jdbctst.\"LINES\";");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.\"ORDERS\"", "DROP TABLE jdbctst.\"ORDERS\";");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.\"CUSTOMERS\"", "DROP TABLE jdbctst.\"CUSTOMERS\";");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.fk2c", "DROP TABLE jdbctst.fk2c;");
		handleExecuteDDL(stmt, action, objtype, "jdbctst.pk2c", "DROP TABLE jdbctst.pk2c;");
		objtype = "procedure";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.proc()", "DROP PROCEDURE jdbctst.proc(bigint, varchar(999), date);");
		handleExecuteDDL(stmt, action, objtype, "sys.analyze()", "COMMENT ON PROCEDURE sys.analyze() IS NULL;");
		objtype = "function";
		handleExecuteDDL(stmt, action, objtype, "jdbctst.func()", "DROP FUNCTION jdbctst.func(bigint, varchar(999), date);");
		handleExecuteDDL(stmt, action, objtype, "sys.sin(double)", "COMMENT ON FUNCTION sys.sin(double) IS NULL;");
		handleExecuteDDL(stmt, action, objtype, "sys.env()", "COMMENT ON FUNCTION sys.env() IS NULL;");
		handleExecuteDDL(stmt, action, objtype, "sys.statistics()", "COMMENT ON FUNCTION sys.statistics() IS NULL;");

		// All tables in schema jdbctst should now be gone, else we missed some DROP statements
		try {
			compareResultSet(dbmd.getTables(null, "jdbctst", "%%", null), "getTables(null, jdbctst, '%%', null)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"char(1)	varchar(1024)	varchar(1024)	varchar(25)	varchar(1048576)	char(1)	char(1)	char(1)	char(1)	char(1)\n");
			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.setLength(0);	// clear the output log buffer
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		handleExecuteDDL(stmt, action, "schema", "jdbctst", "SET SCHEMA sys; DROP SCHEMA jdbctst;");

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_DBCmetadata", "");
	}

	private void Test_EmptySql() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		try {
			stmt.execute(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			stmt.execute("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}

		try {
			int ret = stmt.executeUpdate(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			int ret = stmt.executeUpdate("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			long ret = stmt.executeLargeUpdate(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			long ret = stmt.executeLargeUpdate("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			stmt.addBatch(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			stmt.addBatch("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}

		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			rs = stmt.executeQuery("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);

		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			pstmt = con.prepareStatement("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			pstmt = con.prepareStatement(null, Statement.RETURN_GENERATED_KEYS);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			pstmt = con.prepareStatement("", Statement.RETURN_GENERATED_KEYS);
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, null);

		CallableStatement cstmt = null;
		try {
			pstmt = con.prepareCall(null);
			sb.append("Failed to check null parameter!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		try {
			pstmt = con.prepareCall("");
			sb.append("Failed to check empty sql string!\n");
		} catch (SQLException e) {
			sb.append(e.getMessage()).append("\n");
		}
		closeStmtResSet(cstmt, null);

		compareExpectedOutput("Test_EmptySql",
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n" +
			"Missing SQL statement\n");
	}

	private void Test_FetchSize() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT * FROM _tables");

			sb.append("Statement fetch size before set: ").append(stmt.getFetchSize()).append("\n");
			sb.append("ResultSet fetch size before set: ").append(rs.getFetchSize()).append("\n");

			stmt.setFetchSize(40);
			rs.setFetchSize(16384);

			sb.append("Statement fetch size after set: ").append(stmt.getFetchSize()).append("\n");
			sb.append("ResultSet fetch size after set: ").append(rs.getFetchSize()).append("\n");

		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_FetchSize",
			"Statement fetch size before set: 250\n" +
			"ResultSet fetch size before set: 250\n" +
			"Statement fetch size after set: 40\n" +
			"ResultSet fetch size after set: 16384\n");
	}

	private void Test_Int128() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;

		// check first if datatype hugeint is supported on this server
		boolean supportsHugeInt = false;
		try {
			stmt = con.createStatement();
			// query sys.types to find out if sql datatype hugeint is supported
			rs = stmt.executeQuery("SELECT sqlname from sys.types where sqlname = 'hugeint';");
			if (rs != null) {
				if (rs.next()) {
					if ("hugeint".equals(rs.getString(1)))
						supportsHugeInt = true;
				}
				rs.close();
				rs = null;
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		if (!supportsHugeInt) {
			closeStmtResSet(stmt, rs);
			compareExpectedOutput("Test_Int128", "");
			return;	// skip the rest of the test
		}

		// test whether we can represent a full-size int128 as JDBC results
		PreparedStatement insertStatement = null;
		try {
			stmt.executeUpdate("CREATE TABLE HUGEINTT (I HUGEINT)");
			stmt.executeUpdate("CREATE TABLE HUGEDECT (I DECIMAL(38,19))");

			BigInteger bi = new BigInteger("123456789012345678909876543210987654321");
			BigDecimal bd = new BigDecimal("1234567890123456789.9876543210987654321");

			insertStatement = con.prepareStatement("INSERT INTO HUGEINTT VALUES (?)");
			insertStatement.setBigDecimal(1, new BigDecimal(bi));
			insertStatement.executeUpdate();
			insertStatement.close();

			stmt.executeUpdate("INSERT INTO HUGEDECT VALUES (" + bd + ");");

			rs = stmt.executeQuery("SELECT I FROM HUGEINTT");
			rs.next();
			BigInteger biRes = rs.getBigDecimal(1).toBigInteger();
			rs.close();
			sb.append("Expecting " + bi + ", got " + biRes).append("\n");
			if (!bi.equals(biRes)) {
				sb.append("value of bi is NOT equal to biRes!\n");
			}

			rs = stmt.executeQuery("SELECT I FROM HUGEDECT");
			rs.next();
			BigDecimal bdRes = rs.getBigDecimal(1);
			rs.close();
			sb.append("Expecting " + bd + ", got " + bdRes).append("\n");
			if (!bd.equals(bdRes)) {
				sb.append("value of bd is NOT equal to bdRes!\n");
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			stmt.executeUpdate("DROP TABLE IF EXISTS HUGEINTT");
			stmt.executeUpdate("DROP TABLE IF EXISTS HUGEDECT");
			sb.append("SUCCESS\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(insertStatement, null);
		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Int128",
			"Expecting 123456789012345678909876543210987654321, got 123456789012345678909876543210987654321\n" +
			"Expecting 1234567890123456789.9876543210987654321, got 1234567890123456789.9876543210987654321\n" +
			"SUCCESS\n");
	}

	private void Test_Interval_Types() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("create table Test_Interval_Types ("
					+ "c1 interval year, c2 interval month, c3 interval day, "
					+ "c4 interval hour, c5 interval minute, c6 interval second, "
					+ "c7 interval year to month, c8 interval day to hour, "
					+ "c9 interval day to minute, c10 interval day to second, "
					+ "c11 interval hour to minute, c12 interval hour to second, "
					+ "c13 interval minute to second)");
			rs = stmt.executeQuery("select * from Test_Interval_Types");
			if (rs != null) {
				sb.append("Showing query ResultSetMetaData\n");
				ResultSetMetaData rsmd = rs.getMetaData();
				int colCount = rsmd.getColumnCount();
				for (int col = 1; col <= colCount; col++) {
					sb.append("ColumnName: ").append(rsmd.getColumnName(col))
					.append("\tColumnTypeName: ").append(rsmd.getColumnTypeName(col))
					.append("\tPrecision: ").append(rsmd.getPrecision(col))
					.append("\tScale: ").append(rsmd.getScale(col))
					.append("\tColumnDisplaySize: ").append(rsmd.getColumnDisplaySize(col))
					.append("\tColumnType: ").append(rsmd.getColumnType(col))
					.append("\tColumnClassName: ").append(rsmd.getColumnClassName(col))
					.append("\n");
				}
				rs.close();
				rs = null;
			}
			pstmt = con.prepareStatement("select * from Test_Interval_Types where "
					+ "c1=? or c2=? or c3=? or c4=? or c5=? or c6=? or c7=? or c8=? or c9=? or c10=? or c11=? or c12=? or c13=?");
			if (pstmt != null) {
				sb.append("Showing prepared query ResultSetMetaData\n");
				ResultSetMetaData rsmd = pstmt.getMetaData();
				int colCount = rsmd.getColumnCount();
				for (int col = 1; col <= colCount; col++) {
					sb.append("ColumnName: ").append(rsmd.getColumnName(col))
					.append("\tColumnTypeName: ").append(rsmd.getColumnTypeName(col))
					.append("\tPrecision: ").append(rsmd.getPrecision(col))
					.append("\tScale: ").append(rsmd.getScale(col))
					.append("\tColumnDisplaySize: ").append(rsmd.getColumnDisplaySize(col))
					.append("\tColumnType: ").append(rsmd.getColumnType(col))
					.append("\tColumnClassName: ").append(rsmd.getColumnClassName(col))
					.append("\n");
				}
				sb.append("Showing prepared query ParameterMetaData\n");
				ParameterMetaData pmd = pstmt.getParameterMetaData();
				int paramCount = pmd.getParameterCount();
				for (int param = 1; param <= paramCount; param++) {
					sb.append("ParameterTypeName: ").append(pmd.getParameterTypeName(param))
					.append("\tPrecision: ").append(pmd.getPrecision(param))
					.append("\tScale: ").append(pmd.getScale(param))
					.append("\tParameterType: ").append(pmd.getParameterType(param))
					.append("\tParameterClassName: ").append(pmd.getParameterClassName(param))
					.append("\n");
				}
				pstmt.close();
				pstmt = null;
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		closeStmtResSet(pstmt, null);
		try {
			stmt.executeUpdate("drop table Test_Interval_Types");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Interval_Types",
			"Showing query ResultSetMetaData\n" +
			"ColumnName: c1	ColumnTypeName: interval year	Precision: 10	Scale: 0	ColumnDisplaySize: 4	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c2	ColumnTypeName: interval month	Precision: 10	Scale: 0	ColumnDisplaySize: 6	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c3	ColumnTypeName: interval day	Precision: 9	Scale: 0	ColumnDisplaySize: 9	ColumnType: 2	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c4	ColumnTypeName: interval hour	Precision: 11	Scale: 3	ColumnDisplaySize: 11	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c5	ColumnTypeName: interval minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c6	ColumnTypeName: interval second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c7	ColumnTypeName: interval year to month	Precision: 10	Scale: 0	ColumnDisplaySize: 6	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c8	ColumnTypeName: interval day to hour	Precision: 11	Scale: 3	ColumnDisplaySize: 11	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c9	ColumnTypeName: interval day to minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c10	ColumnTypeName: interval day to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c11	ColumnTypeName: interval hour to minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c12	ColumnTypeName: interval hour to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c13	ColumnTypeName: interval minute to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"Showing prepared query ResultSetMetaData\n" +
			"ColumnName: c1	ColumnTypeName: interval year	Precision: 10	Scale: 0	ColumnDisplaySize: 4	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c2	ColumnTypeName: interval month	Precision: 10	Scale: 0	ColumnDisplaySize: 6	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c3	ColumnTypeName: interval day	Precision: 9	Scale: 0	ColumnDisplaySize: 9	ColumnType: 2	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c4	ColumnTypeName: interval hour	Precision: 11	Scale: 3	ColumnDisplaySize: 11	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c5	ColumnTypeName: interval minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c6	ColumnTypeName: interval second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c7	ColumnTypeName: interval year to month	Precision: 10	Scale: 0	ColumnDisplaySize: 6	ColumnType: 4	ColumnClassName: java.lang.Integer\n" +
			"ColumnName: c8	ColumnTypeName: interval day to hour	Precision: 11	Scale: 3	ColumnDisplaySize: 11	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c9	ColumnTypeName: interval day to minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c10	ColumnTypeName: interval day to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c11	ColumnTypeName: interval hour to minute	Precision: 13	Scale: 3	ColumnDisplaySize: 13	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c12	ColumnTypeName: interval hour to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"ColumnName: c13	ColumnTypeName: interval minute to second	Precision: 15	Scale: 3	ColumnDisplaySize: 15	ColumnType: 3	ColumnClassName: java.math.BigDecimal\n" +
			"Showing prepared query ParameterMetaData\n" +
			"ParameterTypeName: interval year	Precision: 10	Scale: 0	ParameterType: 4	ParameterClassName: java.lang.Integer\n" +
			"ParameterTypeName: interval month	Precision: 10	Scale: 0	ParameterType: 4	ParameterClassName: java.lang.Integer\n" +
			"ParameterTypeName: interval day	Precision: 9	Scale: 0	ParameterType: 2	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval hour	Precision: 11	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval minute	Precision: 13	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval second	Precision: 15	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval year to month	Precision: 10	Scale: 0	ParameterType: 4	ParameterClassName: java.lang.Integer\n" +
			"ParameterTypeName: interval day to hour	Precision: 11	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval day to minute	Precision: 13	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval day to second	Precision: 15	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval hour to minute	Precision: 13	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval hour to second	Precision: 15	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n" +
			"ParameterTypeName: interval minute to second	Precision: 15	Scale: 0	ParameterType: 3	ParameterClassName: java.math.BigDecimal\n");
	}

	private void Test_PlanExplainTraceDebugCmds(boolean skipMALoutput) {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			final boolean isPreJan2022 = (dbmsMajorVersion == 11 && dbmsMinorVersion <= 41);

			stmt = con.createStatement();
			String qry = "SELECT 1;";
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				sb.append(rs.getString(1)).append("\n");
			}
			rs.close();
			rs = null;
			compareExpectedOutput("Test_PlanExplainTraceDebugCmds: " + qry, "1\n");
			sb.setLength(0);	// clear the output log buffer

			// plan statements are supported via JDBC
			qry = "plan SELECT 2;";
			rs = stmt.executeQuery(qry);
			if (!skipMALoutput) {
				compareResultSet(rs, qry,
					! isPreJan2022 ?
					"Resultset with 1 columns\n" +
					"rel\n" +
					(isPostDec2023 ? "varchar" : "clob") + "(37)\n" +
					"project (\n" +
					"|  [ boolean(1) \"true\" as \"%1\".\"%1\" ]\n" +
					") [ tinyint(2) \"2\" ]\n"
					:
					"Resultset with 1 columns\n" +
					"rel\n" +
					"clob(21)\n" +
					"project (\n" +
					"|  [ boolean \"true\" ]\n" +
					") [ tinyint \"2\" ]\n");
			}
			rs.close();
			rs = null;
			sb.setLength(0);	// clear the output log buffer

			// explain statements are supported via JDBC
			qry = "explain SELECT 3;";
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				String val = rs.getString(1);
				if (!val.startsWith("#"))
					sb.append(val).append("\n");
			}
			rs.close();
			rs = null;
			if (!skipMALoutput) {
				compareExpectedOutput("Test_PlanExplainTraceDebugCmds: " + qry,
					"function user.main():void;\n" +
					"    X_1:void := querylog.define(\"explain select 3;\":str, \"default_pipe\":str, " + (isPostMar2025 ? "4" : "6") + ":int);\n" +
					"    X_" + (isPostMar2025 ? "8" : "10") + ":int := sql.resultSet(\".%2\":str, \"%2\":str, \"tinyint\":str, 2:int, 0:int, 7:int, 3:bte);\n" +
					"end user.main;\n");
			}
			sb.setLength(0);	// clear the output log buffer

			// trace statements are supported via JDBC. Note that it returns two resultsets, one with the query result and next one with the trace result.
			qry = "trace SELECT 4;";
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				sb.append(rs.getString(1)).append("\n");
			}
			if (stmt.getMoreResults()) {
				sb.append("Another resultset\n");
				rs = stmt.getResultSet();
				while (rs.next()) {
					sb.append(rs.getString(2)).append("\n");
				}
			}
			rs.close();
			rs = null;
			if (!skipMALoutput) {
				compareExpectedOutput("Test_PlanExplainTraceDebugCmds: " + qry,
					! isPreJan2022 ?
					"4\n" +
					"Another resultset\n" +
					"    X_1=0@0:void := querylog.define(\"trace select 4;\":str, \"default_pipe\":str, " + (isPostMar2025 ? "4" : "6") + ":int);\n" +
					"    X_" + (isPostMar2025 ? "8" : "10") + "=0:int := sql.resultSet(\".%2\":str, \"%2\":str, \"tinyint\":str, 3:int, 0:int, 7:int, 4:bte);\n"
					:
					"4\n" +
					"Another resultset\n" +
					"X_0=0@0:void := querylog.define(\"trace select 4;\":str, \"default_pipe\":str, 6:int);\n" +
					"X_1=0:int := sql.resultSet(\".%2\":str, \"%2\":str, \"tinyint\":str, 3:int, 0:int, 7:int, 4:bte);\n");
			}
			sb.setLength(0);	// clear the output log buffer

			// debug statements are NOT supported via JDBC driver, so the execution should throw an SQLException
			// Note: as of release 11.47 (Jun2023) the debug statement is not supported anymore, error msg: syntax error in: "debug"
			qry = "debug SELECT 5;";
			sb.append(qry).append("\n");
			rs = stmt.executeQuery(qry);	// this should throw an SQLException
			while (rs.next()) {
				sb.append(rs.getString(1)).append("\n");
			}
			rs.close();
			rs = null;
			compareExpectedOutput("Test_PlanExplainTraceDebugCmds: " + qry, qry + "\n5\n");
			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.append("FAILED: ");
			while (e != null) {
				sb.append(e.getMessage()).append("\n");
				e = e.getNextException();
			}

			final boolean isPreJun2023 = (dbmsMajorVersion == 11 && dbmsMinorVersion <= 45);
			// From Jun2023 we skip the comparison as it gives a different error msg on power8 platform: syntax error, unexpected IDENT in: "debug"
			if (isPreJun2023) {
				compareExpectedOutput("Test_PlanExplainTraceDebugCmds",
					"debug SELECT 5;\n" +
					"FAILED: SQL debugging only supported in interactive mode in: \"debug\"\n" +
					"Current transaction is aborted (please ROLLBACK)\n");
			}
		}

		closeStmtResSet(stmt, rs);
	}

	private void Test_PSgeneratedkeys() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit was just switched off
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			stmt.executeUpdate(
				"CREATE TABLE psgenkey (" +
				"       id       serial," +
				"       val      varchar(20)" +
				")");
			stmt.close();
		} catch (SQLException e) {
			sb.append("FAILED to CREATE TABLE psgenkey: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		PreparedStatement pstmt = null;
		ResultSet keys = null;
		try {
			pstmt = con.prepareStatement(
				"INSERT INTO psgenkey (val) VALUES ('this is a test')",
				Statement.RETURN_GENERATED_KEYS);

			sb.append("1. inserting 3 records...");
			pstmt.executeUpdate();
			pstmt.executeUpdate();
			pstmt.executeUpdate();
			sb.append("success\n");

			// now get the generated keys
			sb.append("2. getting generated keys...");
			keys = pstmt.getGeneratedKeys();
			if (keys == null) {
				sb.append("there are no keys!\n");
			} else {
				while (keys.next()) {
					sb.append("generated key index: ").append(keys.getInt(1)).append("\n");
				}
				if (keys.getStatement() == null) {
					sb.append("ResultSet.getStatement() should never return null!\n");
				}
				keys.close();
			}
			pstmt.close();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, keys);

		try {
			con.rollback();
			// restore default setting
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED to rollback: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_PSgeneratedkeys",
			"0. false	false\n" +
			"1. inserting 3 records...success\n" +
			"2. getting generated keys...generated key index: 3\n");
	}

	private void Test_PSgetObject() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit was just switched off
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. creating test table...");
			stmt.executeUpdate("CREATE TABLE table_Test_PSgetObject (ti tinyint, si smallint, i int, bi bigint)");
			sb.append("success\n");
			stmt.close();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sb.append("2a. inserting 3 records as batch...");
			pstmt = con.prepareStatement("INSERT INTO table_Test_PSgetObject (ti,si,i,bi) VALUES (?,?,?,?)");
			pstmt.setShort(1, (short)1);
			pstmt.setShort(2, (short)1);
			pstmt.setInt (3, 1);
			pstmt.setLong(4, (long)1);
			pstmt.addBatch();

			pstmt.setShort(1, (short)127);
			pstmt.setShort(2, (short)12700);
			pstmt.setInt (3, 1270000);
			pstmt.setLong(4, (long)127000000);
			pstmt.addBatch();

			pstmt.setShort(1, (short)-127);
			pstmt.setShort(2, (short)-12700);
			pstmt.setInt (3, -1270000);
			pstmt.setLong(4, (long)-127000000);
			pstmt.addBatch();

			pstmt.executeBatch();
			sb.append(" passed\n");
			sb.append(pstmt.toString());	// test showing prepared statement

			sb.append("2b. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed\n");
		} catch (SQLException e) {
			sb.append("FAILED to INSERT data: ").append(e.getMessage()).append("\n");
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		try {
			sb.append("3a. selecting records...");
			pstmt = con.prepareStatement("SELECT ti,si,i,bi FROM table_Test_PSgetObject ORDER BY ti,si,i,bi");
			rs = pstmt.executeQuery();
			sb.append(" passed\n");

			while (rs.next()) {
				// test fix for https://github.com/MonetDB/MonetDB/issues/4026
				Short ti = (Short) rs.getObject(1);
				Short si = (Short) rs.getObject(2);
				Integer i = (Integer) rs.getObject(3);
				Long bi = (Long) rs.getObject(4);

				sb.append("  Retrieved row data: ti=").append(ti).append(" si=").append(si).append(" i=").append(i).append(" bi=").append(bi).append("\n");
			}

			sb.append("3b. closing ResultSet...");
			rs.close();
			sb.append(" passed\n");

			sb.append("3c. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed\n");
		} catch (SQLException e) {
			sb.append("FAILED to RETRIEVE data: ").append(e.getMessage()).append("\n");
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, rs);

		try {
			sb.append("4. Rollback changes...");
			con.rollback();
			sb.append(" passed\n");

			// restore default setting
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED to rollback: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_PSgetObject",
			"0. false	false\n" +
			"1. creating test table...success\n" +
			"2a. inserting 3 records as batch... passed\n" +
			"Prepared SQL: INSERT INTO table_Test_PSgetObject (ti,si,i,bi) VALUES (?,?,?,?)\n" +
			" parameter 1 tinyint, set value: -127\n" +
			" parameter 2 smallint, set value: -12700\n" +
			" parameter 3 int, set value: -1270000\n" +
			" parameter 4 bigint, set value: -127000000\n" +
			"2b. closing PreparedStatement... passed\n" +
			"3a. selecting records... passed\n" +
			"  Retrieved row data: ti=-127 si=-12700 i=-1270000 bi=-127000000\n" +
			"  Retrieved row data: ti=1 si=1 i=1 bi=1\n" +
			"  Retrieved row data: ti=127 si=12700 i=1270000 bi=127000000\n" +
			"3b. closing ResultSet... passed\n" +
			"3c. closing PreparedStatement... passed\n" +
			"4. Rollback changes... passed\n");
	}

	private void Test_PSlargebatchval() {
		sb.setLength(0);	// clear the output log buffer

		byte[] errorBytes = new byte[] { (byte) 0xe2, (byte) 0x80, (byte) 0xa7 };
		String errorStr = new String(errorBytes, StandardCharsets.UTF_8);
		StringBuilder repeatedErrorStr = new StringBuilder();
		for (int i = 0; i < 8170;i++) {
			repeatedErrorStr.append(errorStr);
		}
		String largeStr = repeatedErrorStr.toString();

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			// >> true: auto commit should be on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. creating test table...");
			stmt.execute("CREATE TABLE Test_PSlargebatchval (c INT, a CLOB, b DOUBLE)");
			sb.append("success\n");

			sb.append("2. prepare insert...");
			pstmt = con.prepareStatement("INSERT INTO Test_PSlargebatchval VALUES (?,?,?)");
			sb.append("success\n");

			pstmt.setLong(1, 1L);
			pstmt.setString(2, largeStr);
			pstmt.setDouble(3, 1.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("3. inserted 1 large string\n");

			/* test issue reported at https://github.com/MonetDB/MonetDB/issues/3470 */
			pstmt.setLong(1, -2L);
			pstmt.setClob(2, new StringReader(largeStr));
			pstmt.setDouble(3, -2.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("4. inserted 1 large clob via StringReader() object\n");

			Clob myClob = con.createClob();
			myClob.setString(1L, largeStr);

			pstmt.setLong(1, 123456789L);
			pstmt.setClob(2, myClob);
			pstmt.setDouble(3, 12345678901.98765);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("5. inserted 1 large clob via createClob() object\n");

			pstmt.close();

			sb.append("6. select count(*)... ");
			rs = stmt.executeQuery("SELECT COUNT(*) FROM Test_PSlargebatchval");
			if (rs.next())
				sb.append(rs.getInt(1)).append(" rows inserted.\n");
			rs.close();

			sb.append("7. drop table...");
			stmt.execute("DROP TABLE Test_PSlargebatchval");
			sb.append("success\n");
			stmt.close();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_PSlargebatchval",
			"0. true	true\n" +
			"1. creating test table...success\n" +
			"2. prepare insert...success\n" +
			"3. inserted 1 large string\n" +
			"4. inserted 1 large clob via StringReader() object\n" +
			"5. inserted 1 large clob via createClob() object\n" +
			"6. select count(*)... 3 rows inserted.\n" +
			"7. drop table...success\n");
	}

	private void Test_PSlargeresponse(String conURL) {
		sb.setLength(0);	// clear the output log buffer

		PreparedStatement pstmt = null;
		try {
			sb.append("1. DatabaseMetadata environment retrieval... ");

			// retrieve this to simulate a bug report
			con.getMetaData().getURL();
			// There used to be a test "if (conURL.startsWith(dbmdURL))" here
			// but with the new URLs that is too simplistic.
			sb.append("oke").append("\n");

			pstmt = con.prepareStatement("select * from columns");
			sb.append("2. empty call...");
			// should succeed (no arguments given)
			pstmt.execute();
			sb.append(" passed\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_PSlargeresponse",
			"1. DatabaseMetadata environment retrieval... oke\n" +
			"2. empty call... passed\n");
	}

	private void Test_PSmanycon(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		final int maxCons = 60;	// default max_clients is 64, 2 connections are already open from this program
		List<PreparedStatement> pss = new ArrayList<PreparedStatement>(maxCons);	// PreparedStatements go in here

		try {
			// spawn a lot of Connections with 1 PreparedStatement, just for fun...
			int i = 1;
			sb.append("Establishing Connection ");
			for (; i <= maxCons; i++) {
				sb.append(i);
				Connection conx = DriverManager.getConnection(arg0);
				sb.append(",");

				// do something with the connection to test if it works
				PreparedStatement pstmt = conx.prepareStatement("select " + i);
				sb.append(" ");

				pss.add(pstmt);
			}
			sb.append("\n");

			// now try to nicely execute them
			i = 1;
			sb.append("Executing PreparedStatement\n");
			for (Iterator<PreparedStatement> it = pss.iterator(); it.hasNext(); i++) {
				PreparedStatement pstmt = it.next();

				// see if the connection still works
				sb.append(i).append("...");
				if (!pstmt.execute())
					sb.append("should have seen a ResultSet!");

				ResultSet rs = pstmt.getResultSet();
				if (rs != null) {
					if (!rs.next())
						sb.append("ResultSet is empty");
					sb.append(" result: ").append(rs.getString(1));
				}

				// close the connection and associated resources
				pstmt.getConnection().close();
				sb.append(", closed. ");

				if (i % 5 == 0) {
					// inject a failed transaction
					Connection conZZ = DriverManager.getConnection(arg0);
					Statement stmt = con.createStatement();
					try {
						int affrows = stmt.executeUpdate("update foo where bar is wrong");
						sb.append("oops, faulty statement just got through");
					} catch (SQLException e) {
						sb.append("Forced transaction failure");
					}
					sb.append("\n");
					closeStmtResSet(stmt, null);
					conZZ.close();
				}
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_PSmanycon",
			"Establishing Connection 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, " +
			"11, 12, 13, 14, 15, 16, 17, 18, 19, 20, " +
			"21, 22, 23, 24, 25, 26, 27, 28, 29, 30, " +
			"31, 32, 33, 34, 35, 36, 37, 38, 39, 40, " +
			"41, 42, 43, 44, 45, 46, 47, 48, 49, 50, " +
			"51, 52, 53, 54, 55, 56, 57, 58, 59, 60, \n" +
			"Executing PreparedStatement\n" +
			"1... result: 1, closed. 2... result: 2, closed. 3... result: 3, closed. 4... result: 4, closed. 5... result: 5, closed. Forced transaction failure\n" +
			"6... result: 6, closed. 7... result: 7, closed. 8... result: 8, closed. 9... result: 9, closed. 10... result: 10, closed. Forced transaction failure\n" +
			"11... result: 11, closed. 12... result: 12, closed. 13... result: 13, closed. 14... result: 14, closed. 15... result: 15, closed. Forced transaction failure\n" +
			"16... result: 16, closed. 17... result: 17, closed. 18... result: 18, closed. 19... result: 19, closed. 20... result: 20, closed. Forced transaction failure\n" +
			"21... result: 21, closed. 22... result: 22, closed. 23... result: 23, closed. 24... result: 24, closed. 25... result: 25, closed. Forced transaction failure\n" +
			"26... result: 26, closed. 27... result: 27, closed. 28... result: 28, closed. 29... result: 29, closed. 30... result: 30, closed. Forced transaction failure\n" +
			"31... result: 31, closed. 32... result: 32, closed. 33... result: 33, closed. 34... result: 34, closed. 35... result: 35, closed. Forced transaction failure\n" +
			"36... result: 36, closed. 37... result: 37, closed. 38... result: 38, closed. 39... result: 39, closed. 40... result: 40, closed. Forced transaction failure\n" +
			"41... result: 41, closed. 42... result: 42, closed. 43... result: 43, closed. 44... result: 44, closed. 45... result: 45, closed. Forced transaction failure\n" +
			"46... result: 46, closed. 47... result: 47, closed. 48... result: 48, closed. 49... result: 49, closed. 50... result: 50, closed. Forced transaction failure\n" +
			"51... result: 51, closed. 52... result: 52, closed. 53... result: 53, closed. 54... result: 54, closed. 55... result: 55, closed. Forced transaction failure\n" +
			"56... result: 56, closed. 57... result: 57, closed. 58... result: 58, closed. 59... result: 59, closed. 60... result: 60, closed. Forced transaction failure\n");
	}

	private void Test_PSmetadata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit was just switched off
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = 0;
			updates = stmt.executeUpdate("CREATE TABLE table_Test_PSmetadata ( myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob )");
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			// all NULLs
			updates = stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (NULL, NULL,            NULL,           NULL,                  NULL)");
			if (updates != 1)
				sb.append("2a. Expected 1 got ").append(updates).append(" instead\n");

			// all filled in
			updates = stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (2   , 3.0,             true,           'A string',            'bla bla bla')");
			if (updates != 1)
				sb.append("2b. Expected 1 got ").append(updates).append(" instead\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement("SELECT CASE WHEN myint IS NULL THEN 0 ELSE 1 END AS intnull, * FROM table_Test_PSmetadata WHERE myint = ?");
			sb.append(pstmt.toString());	// test showing prepared statement

			// testing and showing result set meta data
			ResultSetMetaData rsmd = pstmt.getMetaData();
			sb.append("rsmd. ").append(rsmd.getColumnCount()).append(" columns:\n");
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				sb.append("RCol ").append(col).append("\n");
				sb.append("  classname     ").append(rsmd.getColumnClassName(col)).append("\n");
				sb.append("  displaysize   ").append(rsmd.getColumnDisplaySize(col)).append("\n");
				sb.append("  label         ").append(rsmd.getColumnLabel(col)).append("\n");
				sb.append("  name          ").append(rsmd.getColumnName(col)).append("\n");
				sb.append("  type          ").append(rsmd.getColumnType(col)).append("\n");
				sb.append("  typename      ").append(rsmd.getColumnTypeName(col)).append("\n");
				sb.append("  precision     ").append(rsmd.getPrecision(col)).append("\n");
				sb.append("  scale         ").append(rsmd.getScale(col)).append("\n");
				sb.append("  catalogname   ").append(rsmd.getCatalogName(col)).append("\n");
				sb.append("  schemaname    ").append(rsmd.getSchemaName(col)).append("\n");
				sb.append("  tablename     ").append(rsmd.getTableName(col)).append("\n");
				sb.append("  autoincrement ").append(rsmd.isAutoIncrement(col)).append("\n");
				sb.append("  casesensitive ").append(rsmd.isCaseSensitive(col)).append("\n");
				sb.append("  currency      ").append(rsmd.isCurrency(col)).append("\n");
				sb.append("  defwritable   ").append(rsmd.isDefinitelyWritable(col)).append("\n");
				sb.append("  nullable      ").append(rsmd.isNullable(col)).append("\n");
				sb.append("  readonly      ").append(rsmd.isReadOnly(col)).append("\n");
				sb.append("  searchable    ").append(rsmd.isSearchable(col)).append("\n");
				sb.append("  signed        ").append(rsmd.isSigned(col)).append("\n");
				sb.append("  writable      ").append(rsmd.isWritable(col)).append("\n");
			}

			showParams(pstmt);

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_PSmetadata",
			"0. false\tfalse\n" +
			"Prepared SQL: SELECT CASE WHEN myint IS NULL THEN 0 ELSE 1 END AS intnull, * FROM table_Test_PSmetadata WHERE myint = ?\n" +
			" parameter 1 int, set value: <null>\n" +
			"rsmd. 6 columns:\n" +
			"RCol 1\n" +
			"  classname     java.lang.Short\n" +
			"  displaysize   3\n" +
			"  label         intnull\n" +
			"  name          intnull\n" +
			"  type          -6\n" +
			"  typename      tinyint\n" +
			"  precision     3\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     \n" +
			"  autoincrement false\n" +
			"  casesensitive false\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        true\n" +
			"  writable      false\n" +
			"RCol 2\n" +
			"  classname     java.lang.Integer\n" +
			"  displaysize   10\n" +
			"  label         myint\n" +
			"  name          myint\n" +
			"  type          4\n" +
			"  typename      int\n" +
			"  precision     10\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     table_test_psmetadata\n" +
			"  autoincrement false\n" +
			"  casesensitive false\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        true\n" +
			"  writable      false\n" +
			"RCol 3\n" +
			"  classname     java.lang.Double\n" +
			"  displaysize   15\n" +
			"  label         mydouble\n" +
			"  name          mydouble\n" +
			"  type          8\n" +
			"  typename      double\n" +
			"  precision     15\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     table_test_psmetadata\n" +
			"  autoincrement false\n" +
			"  casesensitive false\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        true\n" +
			"  writable      false\n" +
			"RCol 4\n" +
			"  classname     java.lang.Boolean\n" +
			"  displaysize   5\n" +
			"  label         mybool\n" +
			"  name          mybool\n" +
			"  type          16\n" +
			"  typename      boolean\n" +
			"  precision     1\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     table_test_psmetadata\n" +
			"  autoincrement false\n" +
			"  casesensitive false\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        false\n" +
			"  writable      false\n" +
			"RCol 5\n" +
			"  classname     java.lang.String\n" +
			"  displaysize   15\n" +
			"  label         myvarchar\n" +
			"  name          myvarchar\n" +
			"  type          12\n" +
			"  typename      varchar\n" +
			"  precision     15\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     table_test_psmetadata\n" +
			"  autoincrement false\n" +
			"  casesensitive true\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        false\n" +
			"  writable      false\n" +
			"RCol 6\n" +
			"  classname     java.lang.String\n" +
			"  displaysize   0\n" +
			"  label         myclob\n" +
			"  name          myclob\n" +
			"  type          12\n" +
			"  typename      " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
			"  precision     0\n" +
			"  scale         0\n" +
			"  catalogname   null\n" +
			"  schemaname    \n" +
			"  tablename     table_test_psmetadata\n" +
			"  autoincrement false\n" +
			"  casesensitive true\n" +
			"  currency      false\n" +
			"  defwritable   false\n" +
			"  nullable      2\n" +
			"  readonly      true\n" +
			"  searchable    true\n" +
			"  signed        false\n" +
			"  writable      false\n" +
			"pmd. 1 parameters:\n" +
			"Param 1\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    true\n" +
			"  precision 10\n" +
			"  scale     0\n" +
			"  type      4\n" +
			"  typename  int\n" +
			"  classname java.lang.Integer\n" +
			"  mode      1 (IN)\n" +
			"0. true\ttrue\n");
	}

	private void Test_PSsetBytes() {
		sb.setLength(0);	// clear the output log buffer

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sb.append("1 Create table\n");
			pstmt = con.prepareStatement("create table t7346(col1 clob, col2 blob)");
			sb.append("  pstmt has ")
			.append(pstmt.getMetaData().getColumnCount())
			.append(" result columns and ")
			.append(pstmt.getParameterMetaData().getParameterCount())
			.append(" parameters\n");
			pstmt.execute();
			pstmt.close();

			sb.append("2 Prepare Insert data\n");
			pstmt = con.prepareStatement("insert into t7346 (col1,col2) values(?,?)");
			sb.append("  pstmt has ")
			.append(pstmt.getMetaData().getColumnCount())
			.append(" result columns and ")
			.append(pstmt.getParameterMetaData().getParameterCount())
			.append(" parameters\n");

			String val = "0123456789abcdef";
			pstmt.setString(1, val);
			pstmt.setBytes(2, val.getBytes(StandardCharsets.UTF_8));
			sb.append("3 Insert data row 1\n");
			pstmt.execute();

			val = "~!@#$%^&*()_+`1-=][{}\\|';:,<.>/?";
			pstmt.setString(1, val);
			pstmt.setBytes(2, val.getBytes(StandardCharsets.UTF_8));
			sb.append("4 Insert data row 2\n");
			pstmt.execute();

			val = "\u00e0\u004f\u20f0\u0020\u00ea\u003a\u0069\u0010\u00a2\u00d8\u0008\u0001\u002b\u0030\u019c\u129e";
			pstmt.setString(1, val);
			pstmt.setBytes(2, val.getBytes(StandardCharsets.UTF_8));
			sb.append("4 Insert data row 3\n");
			pstmt.execute();

			val = "\\X\\Y\\\\";
			pstmt.setString(1, val);
			pstmt.setBytes(2, val.getBytes(StandardCharsets.UTF_8));
			sb.append("4 Insert data row 4\n");
			pstmt.execute();

			pstmt.close();

			sb.append("5 Prepare Select data\n");
			pstmt = con.prepareStatement("select col1, length(col1) as len_col1, col2, length(col2) as len_col2 from t7346");
			sb.append("  pstmt has ")
			.append(pstmt.getMetaData().getColumnCount())
			.append(" result columns and ")
			.append(pstmt.getParameterMetaData().getParameterCount())
			.append(" parameters\n");
			sb.append("6 Execute Select\n");
			rs = pstmt.executeQuery();
			if (rs != null) {
				sb.append("  rs has ")
				.append(rs.getMetaData().getColumnCount())
				.append(" result columns\n");
				sb.append("7 Show data rows\n");
				for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
					if (c > 1)
						sb.append("\t");
					sb.append(rs.getMetaData().getColumnName(c));
				}
				sb.append("\n");
				while (rs.next()) {
					sb.append(rs.getString("col1")).append("\t")
					.append(rs.getInt("len_col1")).append("\t")
					.append(byteArrayToHexStr(rs.getBytes("col2"))).append("\t")
					.append(rs.getString("col2")).append("\t")
					.append(rs.getInt("len_col2")).append("\n");
				}
				rs.close();
				rs = null;
			}
			pstmt.close();
			pstmt = null;
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(pstmt, rs);

		// cleanup created test table
		try {
			sb.append("8 Drop table\n");
			pstmt = con.prepareStatement("drop table if exists t7346");
			sb.append("  pstmt has ")
			.append(pstmt.getMetaData().getColumnCount())
			.append(" result columns and ")
			.append(pstmt.getParameterMetaData().getParameterCount())
			.append(" parameters\n");
			pstmt.execute();
			pstmt.close();
			pstmt = null;
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Test_PSsetBytes",
			"1 Create table\n" +
			"  pstmt has 0 result columns and 0 parameters\n" +
			"2 Prepare Insert data\n" +
			"  pstmt has 0 result columns and 2 parameters\n" +
			"3 Insert data row 1\n" +
			"4 Insert data row 2\n" +
			"4 Insert data row 3\n" +
			"4 Insert data row 4\n" +
			"5 Prepare Select data\n" +
			"  pstmt has 4 result columns and 0 parameters\n" +
			"6 Execute Select\n" +
			"  rs has 4 result columns\n" +
			"7 Show data rows\n" +
			"col1	len_col1	col2	len_col2\n" +
			"0123456789abcdef	16	30313233343536373839616263646566	30313233343536373839616263646566	16\n" +
			"~!@#$%^&*()_+`1-=][{}\\|';:,<.>/?	32	7E21402324255E262A28295F2B60312D3D5D5B7B7D5C7C273B3A2C3C2E3E2F3F	7E21402324255E262A28295F2B60312D3D5D5B7B7D5C7C273B3A2C3C2E3E2F3F	32\n" +
			"\u00e0\u004f\u20f0\u0020\u00ea\u003a\u0069\u0010\u00a2\u00d8\u0008\u0001\u002b\u0030\u019c\u129e	16	C3A04FE283B020C3AA3A6910C2A2C39808012B30C69CE18A9E	C3A04FE283B020C3AA3A6910C2A2C39808012B30C69CE18A9E	25\n" +
			"\\X\\Y\\\\	6	5C585C595C5C	5C585C595C5C	6\n" +
			"8 Drop table\n" +
			"  pstmt has 0 result columns and 0 parameters\n");
	}

	static private final char[] HEXES = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	private String byteArrayToHexStr(final byte[] bytes) {
		if (bytes == null)
			return null;
		final int len = bytes.length;
		final StringBuilder buf = new StringBuilder(len * 2);
		// convert the bytes into hex codes
		for (int i = 0; i < len; i++) {
			byte b = bytes[i];
			buf.append(HEXES[(b & 0xF0) >> 4])
			   .append(HEXES[(b & 0x0F)]);
		}
		return buf.toString();
	}

	private void Test_PSsomeamount() {
		sb.setLength(0);	// clear the output log buffer

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			// >> true: auto commit should be on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			sb.append("1. Preparing and executing a unique statement\n");
			for (int i = 0; i < 120; i++) {
				pstmt = con.prepareStatement("select " + i + ", " + i + " = ?");
				pstmt.setInt(1, i);
				rs = pstmt.executeQuery();
				if (rs != null && rs.next() && i % 20 == 0) {
					sb.append(rs.getInt(1)).append(", ").append(rs.getBoolean(2)).append("\n");
				}
				/* next call should cause resources on the server to be freed */
				pstmt.close();
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Test_PSsomeamount",
			"0. true	true\n" +
			"1. Preparing and executing a unique statement\n" +
			"0, true\n" +
			"20, true\n" +
			"40, true\n" +
			"60, true\n" +
			"80, true\n" +
			"100, true\n");
	}

	/* Create a lot of PreparedStatements, to emulate webloads such as those from Hibernate. */
	/* this test is same as Test_PSsomeamount() but for many more PreparedStatements to stress the server */
	private void Test_PSlargeamount() {
		sb.setLength(0);	// clear the output log buffer

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			// >> true: auto commit should be on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			sb.append("1. Preparing and executing a unique statement\n");
			for (int i = 0; i < 50001; i++) {
				pstmt = con.prepareStatement("select " + i + ", " + i + " = ?");
				pstmt.setInt(1, i);
				rs = pstmt.executeQuery();
				if (rs != null && rs.next() && i % 1000 == 0) {
					sb.append(rs.getInt(1)).append(", ").append(rs.getBoolean(2)).append("\n");
				}
				/* next call should cause resources on the server to be freed */
				pstmt.close();
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Test_PSlargeamount",
			"0. true	true\n" +
			"1. Preparing and executing a unique statement\n" +
			"0, true\n" +
			"1000, true\n" +
			"2000, true\n" +
			"3000, true\n" +
			"4000, true\n" +
			"5000, true\n" +
			"6000, true\n" +
			"7000, true\n" +
			"8000, true\n" +
			"9000, true\n" +
			"10000, true\n" +
			"11000, true\n" +
			"12000, true\n" +
			"13000, true\n" +
			"14000, true\n" +
			"15000, true\n" +
			"16000, true\n" +
			"17000, true\n" +
			"18000, true\n" +
			"19000, true\n" +
			"20000, true\n" +
			"21000, true\n" +
			"22000, true\n" +
			"23000, true\n" +
			"24000, true\n" +
			"25000, true\n" +
			"26000, true\n" +
			"27000, true\n" +
			"28000, true\n" +
			"29000, true\n" +
			"30000, true\n" +
			"31000, true\n" +
			"32000, true\n" +
			"33000, true\n" +
			"34000, true\n" +
			"35000, true\n" +
			"36000, true\n" +
			"37000, true\n" +
			"38000, true\n" +
			"39000, true\n" +
			"40000, true\n" +
			"41000, true\n" +
			"42000, true\n" +
			"43000, true\n" +
			"44000, true\n" +
			"45000, true\n" +
			"46000, true\n" +
			"47000, true\n" +
			"48000, true\n" +
			"49000, true\n" +
			"50000, true\n");
	}

	private void Test_PSsqldata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = stmt.executeUpdate("CREATE TABLE table_Test_PSsqldata ( myinet inet, myurl url )");
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			pstmt = con.prepareStatement("INSERT INTO table_Test_PSsqldata VALUES (?, ?)");
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			sb.append(pmd.getParameterCount()).append(" parameters:\n");
			for (int parm = 1; parm <= pmd.getParameterCount(); parm++) {
				sb.append("Parm ").append(parm).append("\n");
				sb.append("  type      ").append(pmd.getParameterType(parm)).append("\n");
				sb.append("  typename  ").append(pmd.getParameterTypeName(parm)).append("\n");
				sb.append("  classname ").append(pmd.getParameterClassName(parm)).append("\n");
			}

			INET tinet = new INET();
			tinet.fromString("172.5.5.5/24");

			URL turl = new URL();
			try {
				turl.fromString("http://www.monetdb.org/");
			} catch (Exception e) {
				sb.append("conversion failed: ").append(e.getMessage()).append("\n");
			}
			pstmt.setObject(1, tinet);
			pstmt.setObject(2, turl);
			// insert first record
			pstmt.execute();
			sb.append(pstmt.toString());	// test showing prepared statement

			try {
				tinet.setNetmaskBits(16);
			} catch (Exception e) {
				sb.append("setNetmaskBits failed: ").append(e.getMessage()).append("\n");
			}
			// insert second record
			pstmt.execute();

			rs = stmt.executeQuery("SELECT * FROM table_Test_PSsqldata");
			ResultSetMetaData rsmd = rs.getMetaData();
			for (int i = 1; rs.next(); i++) {
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					Object x = rs.getObject(col);
					if (x == null || rs.wasNull()) {
						sb.append(i).append(".\t<null>\n");
					} else {
						sb.append(i).append(".\t").append(x.toString()).append("\n");
						if (x instanceof INET) {
							INET inet = (INET)x;
							sb.append("  ").append(inet.getAddress()).append("/").append(inet.getNetmaskBits()).append("\n");
							sb.append("  ").append(inet.getInetAddress().toString()).append("\n");
						} else if (x instanceof URL) {
							URL url = (URL)x;
							sb.append("  ").append(url.getURL().toString()).append("\n");
						}
					}
				}
			}
			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_PSsqldata",
			"0. false	false\n" +
			"2 parameters:\n" +
			"Parm 1\n" +
			"  type      12\n" +
			"  typename  inet\n" +
			"  classname org.monetdb.jdbc.types.INET\n" +
			"Parm 2\n" +
			"  type      12\n" +
			"  typename  url\n" +
			"  classname org.monetdb.jdbc.types.URL\n" +
			"Prepared SQL: INSERT INTO table_Test_PSsqldata VALUES (?, ?)\n" +
			" parameter 1 inet, set value: inet '172.5.5.5/24'\n" +
			" parameter 2 url, set value: url 'http://www.monetdb.org/'\n" +
			"1.	172.5.5.5/24\n" +
			"  172.5.5.5/24\n" +
			"  /172.5.5.5\n" +
			"1.	http://www.monetdb.org/\n" +
			"  http://www.monetdb.org/\n" +
			"2.	172.5.5.5/24\n" +
			"  172.5.5.5/24\n" +
			"  /172.5.5.5\n" +
			"2.	http://www.monetdb.org/\n" +
			"  http://www.monetdb.org/\n" +
			"0. true	true\n");
	}

	private void Test_PStimedate() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = stmt.executeUpdate("CREATE TABLE Test_PStimedate (t time, ts timestamp, d date)");
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			pstmt = con.prepareStatement("INSERT INTO Test_PStimedate VALUES (?, ?, ?)");
			sb.append("1. empty call...");
			try {
				// should fail (as no parameters set)
				pstmt.execute();
				sb.append(" UNexpected PASS!\n");
			} catch (SQLException e) {
				sb.append(" expected exception\n");
			}

			sb.append("2. inserting a record...");
			final java.util.Date d = new java.util.Date();
			final long tm = d.getTime();
			pstmt.setTime(1, new java.sql.Time(tm));
			pstmt.setTimestamp(2, new java.sql.Timestamp(tm));
			pstmt.setDate(3, new java.sql.Date(tm));

			pstmt.executeUpdate();
			sb.append(" passed\n");

			sb.append("3. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed\n");

			sb.append("4. selecting record...");
			pstmt = con.prepareStatement("SELECT * FROM Test_PStimedate");
			rs = pstmt.executeQuery();
			sb.append(" passed\n");

			while (rs != null && rs.next()) {
				for (int j = 1; j <= 3; j++) {
					sb.append((j+4)).append(". retrieving...");
					Object x = rs.getObject(j);
					boolean matches = false;
					if (x instanceof java.sql.Time) {
						sb.append(" (Time)");
						matches = (new java.sql.Time(tm)).toString().equals(x.toString());
					} else if (x instanceof java.sql.Date) {
						sb.append(" (Date)");
						matches = (new java.sql.Date(tm)).toString().equals(x.toString());
					} else if (x instanceof java.sql.Timestamp) {
						sb.append(" (Timestamp)");
						matches = (new java.sql.Timestamp(tm)).toString().equals(x.toString());
					}
					if (matches) {
						sb.append(" passed\n");
					} else {
						sb.append(" FAILED (").append(x).append(" is not ").append(d).append(")\n");
					}
				}
			}

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);
		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Test_PStimedate",
			"0. false	false\n" +
			"1. empty call... expected exception\n" +
			"2. inserting a record... passed\n" +
			"3. closing PreparedStatement... passed\n" +
			"4. selecting record... passed\n" +
			"5. retrieving... (Time) passed\n" +
			"6. retrieving... (Timestamp) passed\n" +
			"7. retrieving... (Date) passed\n" +
			"0. true	true\n");
	}

	private void Test_PStimezone() {
		sb.setLength(0);	// clear the output log buffer

		// make sure this test is reproducible regardless timezone
		// setting, by overriding the VM's default
		// we have to make sure that one doesn't have daylight
		// savings corrections
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = stmt.executeUpdate("CREATE TABLE Test_PStimezone (ts timestamp, tsz timestamp with time zone, t time, tz time with time zone)");
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			pstmt = con.prepareStatement("INSERT INTO Test_PStimezone VALUES (?, ?, ?, ?)");
			sb.append("1. empty call...");
			try {
				// should fail (as no parameters set)
				pstmt.execute();
				sb.append(" UNexpected PASS!\n");
			} catch (SQLException e) {
				sb.append(" expected exception\n");
			}

			sb.append("2. inserting records...\n");
			java.sql.Timestamp ts = new java.sql.Timestamp(0L);
			java.sql.Time t = new java.sql.Time(0L);
			Calendar c = Calendar.getInstance();
			SimpleDateFormat tsz = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
			SimpleDateFormat tz = new SimpleDateFormat("HH:mm:ss.SSSZ");

			tsz.setTimeZone(c.getTimeZone());
			tz.setTimeZone(tsz.getTimeZone());
			sb.append("inserting (").append(c.getTimeZone().getID()).append(") ").append(tsz.format(ts)).append(", ").append(tz.format(t)).append("\n");

			pstmt.setTimestamp(1, ts);
			pstmt.setTimestamp(2, ts);
			pstmt.setTime(3, t);
			pstmt.setTime(4, t);
			pstmt.executeUpdate();

			c.setTimeZone(TimeZone.getTimeZone("UTC"));
			sb.append("inserting with calendar timezone ").append(c.getTimeZone().getID()).append("\n");
			pstmt.setTimestamp(1, ts, c);
			pstmt.setTimestamp(2, ts, c);
			pstmt.setTime(3, t, c);
			pstmt.setTime(4, t, c);
			pstmt.executeUpdate();

			c.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
			sb.append("inserting with calendar timezone ").append(c.getTimeZone().getID()).append("\n");
			pstmt.setTimestamp(1, ts, c);
			pstmt.setTimestamp(2, ts);
			pstmt.setTime(3, t, c);
			pstmt.setTime(4, t);
			pstmt.executeUpdate();

			c.setTimeZone(TimeZone.getTimeZone("GMT+04:15"));
			sb.append("inserting with calendar timezone ").append(c.getTimeZone().getID()).append("\n");
			pstmt.setTimestamp(1, ts);
			pstmt.setTimestamp(2, ts, c);
			pstmt.setTime(3, t);
			pstmt.setTime(4, t, c);
			pstmt.executeUpdate();
			sb.append(" done\n");

			sb.append("3. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed\n");

			sb.append("4. selecting records...");
			pstmt = con.prepareStatement("SELECT * FROM Test_PStimezone");
			rs = pstmt.executeQuery();
			sb.append(" passed\n");

			// The tz fields should basically always be the same
			// (exactly 1st Jan 1970) since whatever timezone is used,
			// the server retains it, and Java restores it.
			// The zoneless fields will show differences since the time
			// is inserted translated to the given timezones, and
			// retrieved as in they were given in those timezones.
			// When the insert zone matches the retrieve zone, Java should
			// eventually see 1st Jan 1970.
			while (rs.next()) {
				sb.append("retrieved row (String):\n")
				.append(rs.getString("ts")).append(" | ")
			//	.append(rs.getString("tsz")).append(" | ")	-- this values changes when summer or wintertime changes so no stable output
				.append(rs.getString("t")).append(" | ")
			//	.append(rs.getString("tz"))	-- this values changes when summer or wintertime changes so no stable output
				.append("\n");
				rs.getString("tsz");
				rs.getString("tz");

				tsz.setTimeZone(TimeZone.getDefault());
				tz.setTimeZone(tsz.getTimeZone());
				sb.append("default (").append(tsz.getTimeZone().getID()).append("):\n")
				.append(tsz.format(rs.getTimestamp("ts"))).append(" | ")
				.append(tsz.format(rs.getTimestamp("tsz"))).append(" | ")
				.append(tz.format(rs.getTime("t"))).append(" | ")
				.append(tz.format(rs.getTime("tz"))).append("\n");

				c.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
				sb.append(c.getTimeZone().getID()).append(":\n")
				.append(rs.getTimestamp("ts", c)).append(" | ")
				.append(rs.getTimestamp("tsz", c)).append(" | ")
				.append(rs.getTime("t", c)).append(" | ")
				.append(rs.getTime("tz", c)).append("\n");

				c.setTimeZone(TimeZone.getTimeZone("Africa/Windhoek"));
				sb.append(c.getTimeZone().getID()).append(":\n")
				.append(rs.getTimestamp("ts", c)).append(" | ")
				.append(rs.getTimestamp("tsz", c)).append(" | ")
				.append(rs.getTime("t", c)).append(" | ")
				.append(rs.getTime("tz", c)).append("\n");

				sb.append("getObject:\n")
				.append(rs.getObject("ts")).append(" | ")
			//	.append(rs.getObject("tsz")).append(" | ")	-- this value changes on different time zones, so no stable output
				.append(rs.getObject("t")).append(" | ")
			//	.append(rs.getObject("tz"))	-- this value changes on different time zones, so no stable output
				.append("\n");
				rs.getObject("tsz");
				rs.getObject("tz");

				SQLWarning w = rs.getWarnings();
				while (w != null) {
					sb.append(w.getMessage()).append("\n");
					w = w.getNextWarning();
				}
			}

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);
		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Test_PStimezone",
			"0. false	false\n" +
			"1. empty call... expected exception\n" +
			"2. inserting records...\n" +
			"inserting (UTC) 1970-01-01 00:00:00.000+0000, 00:00:00.000+0000\n" +
			"inserting with calendar timezone UTC\n" +
			"inserting with calendar timezone America/Los_Angeles\n" +
			"inserting with calendar timezone GMT+04:15\n" +
			" done\n" +
			"3. closing PreparedStatement... passed\n" +
			"4. selecting records... passed\n" +
			"retrieved row (String):\n" +
// old output		"1970-01-01 00:00:00.000000 | 1970-01-01 01:00:00.000000+01:00 | 00:00:00 | 01:00:00+01:00\n" +
			"1970-01-01 00:00:00.000000 | 00:00:00 | \n" +
			"default (UTC):\n" +
			"1970-01-01 00:00:00.000+0000 | 1970-01-01 00:00:00.000+0000 | 00:00:00.000+0000 | 00:00:00.000+0000\n" +
			"America/Los_Angeles:\n" +
			"1970-01-01 08:00:00.0 | 1970-01-01 00:00:00.0 | 08:00:00 | 00:00:00\n" +
			"Africa/Windhoek:\n" +
			"1969-12-31 22:00:00.0 | 1970-01-01 00:00:00.0 | 22:00:00 | 00:00:00\n" +
			"getObject:\n" +
// old output		"1970-01-01 00:00:00.0 | 1970-01-01T01:00+01:00 | 00:00:00 | 01:00+01:00\n" +
			"1970-01-01 00:00:00.0 | 00:00:00 | \n" +
			"retrieved row (String):\n" +
// old output		"1970-01-01 00:00:00.000000 | 1970-01-01 01:00:00.000000+01:00 | 00:00:00 | 01:00:00+01:00\n" +
			"1970-01-01 00:00:00.000000 | 00:00:00 | \n" +
			"default (UTC):\n" +
			"1970-01-01 00:00:00.000+0000 | 1970-01-01 00:00:00.000+0000 | 00:00:00.000+0000 | 00:00:00.000+0000\n" +
			"America/Los_Angeles:\n" +
			"1970-01-01 08:00:00.0 | 1970-01-01 00:00:00.0 | 08:00:00 | 00:00:00\n" +
			"Africa/Windhoek:\n" +
			"1969-12-31 22:00:00.0 | 1970-01-01 00:00:00.0 | 22:00:00 | 00:00:00\n" +
			"getObject:\n" +
// old output		"1970-01-01 00:00:00.0 | 1970-01-01T01:00+01:00 | 00:00:00 | 01:00+01:00\n" +
			"1970-01-01 00:00:00.0 | 00:00:00 | \n" +
			"retrieved row (String):\n" +
// old output		"1969-12-31 16:00:00.000000 | 1970-01-01 01:00:00.000000+01:00 | 16:00:00 | 01:00:00+01:00\n" +
			"1969-12-31 16:00:00.000000 | 16:00:00 | \n" +
			"default (UTC):\n" +
			"1969-12-31 16:00:00.000+0000 | 1970-01-01 00:00:00.000+0000 | 16:00:00.000+0000 | 00:00:00.000+0000\n" +
			"America/Los_Angeles:\n" +
			"1970-01-01 00:00:00.0 | 1970-01-01 00:00:00.0 | 00:00:00 | 00:00:00\n" +
			"Africa/Windhoek:\n" +
			"1969-12-31 14:00:00.0 | 1970-01-01 00:00:00.0 | 14:00:00 | 00:00:00\n" +
			"getObject:\n" +
// old output		"1969-12-31 16:00:00.0 | 1970-01-01T01:00+01:00 | 16:00:00 | 01:00+01:00\n" +
			"1969-12-31 16:00:00.0 | 16:00:00 | \n" +
			"retrieved row (String):\n" +
// old output		"1970-01-01 00:00:00.000000 | 1970-01-01 01:00:00.000000+01:00 | 00:00:00 | 01:00:00+01:00\n" +
			"1970-01-01 00:00:00.000000 | 00:00:00 | \n" +
			"default (UTC):\n" +
			"1970-01-01 00:00:00.000+0000 | 1970-01-01 00:00:00.000+0000 | 00:00:00.000+0000 | 00:00:00.000+0000\n" +
			"America/Los_Angeles:\n" +
			"1970-01-01 08:00:00.0 | 1970-01-01 00:00:00.0 | 08:00:00 | 00:00:00\n" +
			"Africa/Windhoek:\n" +
			"1969-12-31 22:00:00.0 | 1970-01-01 00:00:00.0 | 22:00:00 | 00:00:00\n" +
			"getObject:\n" +
// old output		"1970-01-01 00:00:00.0 | 1970-01-01T01:00+01:00 | 00:00:00 | 01:00+01:00\n" +
			"1970-01-01 00:00:00.0 | 00:00:00 | \n" +
			"0. true	true\n");
	}

	private void Test_PStypes() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = stmt.executeUpdate(
				"CREATE TABLE htmtest (" +
				"       htmid    bigint       NOT NULL," +
				"       ra       double ," +
				"       decl     double ," +
				"       dra      double ," +
				"       ddecl    double ," +
				"       flux     double ," +
				"       dflux    double ," +
				"       freq     double ," +
				"       bw       double ," +
				"       type     decimal(1,0)," +
				"       imageurl url(100)," +
				"       comment  varchar(100)," +
				"       CONSTRAINT htmtest_htmid_pkey PRIMARY KEY (htmid)" +
				")" );
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			// index is not used, but the original bug had it too
			updates = stmt.executeUpdate("CREATE INDEX htmid ON htmtest (htmid)");
			if (updates != 0)
				sb.append("1. Expected 0 got ").append(updates).append(" instead\n");

			stmt.close();

			pstmt = con.prepareStatement("INSERT INTO HTMTEST (HTMID,RA,DECL,FLUX,COMMENT) VALUES (?,?,?,?,?)");
			sb.append("1. inserting a record...");
			pstmt.setLong(1, 1L);
			pstmt.setFloat(2, (float)1.2);
			pstmt.setDouble(3, 2.4);
			pstmt.setDouble(4, 3.2);
			pstmt.setString(5, "vlavbla");
			pstmt.executeUpdate();
			sb.append("success\n");

			// try an update like bug #1757923
			pstmt = con.prepareStatement("UPDATE HTMTEST set COMMENT=?, TYPE=? WHERE HTMID=?");
			sb.append("2. updating record...");
			pstmt.setString(1, "some update");
			pstmt.setObject(2, (float)3.2);
			pstmt.setLong(3, 1L);
			pstmt.executeUpdate();
			sb.append("success\n");

			pstmt.close();

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_PStypes",
			"0. false	false\n" +
			"1. inserting a record...success\n" +
			"2. updating record...success\n" +
			"0. true	true\n");
	}

	private void Test_CallableStmt() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		CallableStatement cstmt = null;
		try {
			String tbl_nm = "tbl6402";
			String proc_nm = "proc6402";

			stmt = con.createStatement();

			// create a test table.
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tbl_nm + " (tint int, tdouble double, tbool boolean, tvarchar varchar(15), tclob clob, turl url, tclen int);");
			sb.append("Created table: ").append(tbl_nm).append("\n");

			// create a procedure with multiple different IN parameters which inserts a row into a table of which one column is computed.
			stmt.executeUpdate("CREATE PROCEDURE " + proc_nm + " (myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob, myurl url) BEGIN" +
				" INSERT INTO " + tbl_nm + " (tint, tdouble, tbool, tvarchar, tclob, turl, tclen) VALUES (myint, mydouble, mybool, myvarchar, myclob, myurl, LENGTH(myvarchar) + LENGTH(myclob)); " +
				"END;");
			sb.append("Created procedure: ").append(proc_nm).append("\n");

			// make sure we can call the procedure the old way (as string)
			stmt.executeUpdate("call " + proc_nm + "(1, 1.1, true,'one','ONE', 'www.monetdb.org');");
			sb.append("Called procedure (1): ").append(proc_nm).append("\n");
			showTblContents(tbl_nm);

			// now use a CallableStament object
			cstmt = con.prepareCall(" { call " + proc_nm + " (?,?, ?, ? , ?,?) } ;");
			sb.append("Prepared Callable procedure: ").append(proc_nm).append("\n");

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
			sb.append("Called Prepared procedure (1): ").append(proc_nm).append("\n");
			showParams(cstmt);
			showTblContents(tbl_nm);

			myclob.setString(1, "TREEs");
			// specify second set of params (some (1 and 3 and 5) are left the same)
			cstmt.setDouble(2, 3.02);
			cstmt.setString(4, "Tree");
			try {
				cstmt.setURL(6, new java.net.URI("https://www.monetdb.org/").toURL());
			} catch (java.net.URISyntaxException | java.net.MalformedURLException mfue) {
				sb.append("Invalid URL: ").append(mfue.getMessage()).append("\n");
			}
			cstmt.execute();
			sb.append("Called Prepared procedure (2): ").append(proc_nm).append("\n");
			// showParams(cstmt);
			showTblContents(tbl_nm);

			// specify third set of params (some (1 and 2) are left the same)
			cstmt.setInt(1, 4);
			cstmt.setBoolean(3, false);
			cstmt.setString(4, "Four");
			cstmt.executeUpdate();
			sb.append("Called Prepared procedure (3): ").append(proc_nm).append("\n");
			showTblContents(tbl_nm);

			// test setNull() also
			cstmt.setNull(3, Types.BOOLEAN);
			cstmt.setNull(5, Types.CLOB);
			cstmt.setNull(2, Types.DOUBLE);
			cstmt.setNull(4, Types.VARCHAR);
			cstmt.setNull(1, Types.INTEGER);
			cstmt.executeUpdate();
			sb.append("Called Prepared procedure (with NULLs): ").append(proc_nm).append("\n");
			showTblContents(tbl_nm);

			cstmt.clearParameters();

			sb.append("Test completed. Cleanup procedure and table.\n");
			stmt.execute("DROP PROCEDURE IF EXISTS " + proc_nm + ";");
			stmt.execute("DROP TABLE     IF EXISTS " + tbl_nm + ";");

		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);
		closeStmtResSet(cstmt, null);

		compareExpectedOutput("Test_CallableStmt",
			"Created table: tbl6402\n" +
			"Created procedure: proc6402\n" +
			"Called procedure (1): proc6402\n" +
			"Table tbl6402 has 7 columns:\n" +
			"	tint	tdouble	tbool	tvarchar	tclob	turl	tclen\n" +
			"	1	1.1	true	one	ONE	www.monetdb.org	6\n" +
			"Prepared Callable procedure: proc6402\n" +
			"Called Prepared procedure (1): proc6402\n" +
			"pmd. 6 parameters:\n" +
			"Param 1\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    true\n" +
			"  precision 10\n" +
			"  scale     0\n" +
			"  type      4\n" +
			"  typename  int\n" +
			"  classname java.lang.Integer\n" +
			"  mode      1 (IN)\n" +
			"Param 2\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    true\n" +
			"  precision 15\n" +
			"  scale     0\n" +
			"  type      8\n" +
			"  typename  double\n" +
			"  classname java.lang.Double\n" +
			"  mode      1 (IN)\n" +
			"Param 3\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    false\n" +
			"  precision 1\n" +
			"  scale     0\n" +
			"  type      16\n" +
			"  typename  boolean\n" +
			"  classname java.lang.Boolean\n" +
			"  mode      1 (IN)\n" +
			"Param 4\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    false\n" +
			"  precision 15\n" +
			"  scale     0\n" +
			"  type      12\n" +
			"  typename  varchar\n" +
			"  classname java.lang.String\n" +
			"  mode      1 (IN)\n" +
			"Param 5\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    false\n" +
			"  precision 0\n" +
			"  scale     0\n" +
			"  type      12\n" +
			"  typename  " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
			"  classname java.lang.String\n" +
			"  mode      1 (IN)\n" +
			"Param 6\n" +
			"  nullable  2 (UNKNOWN)\n" +
			"  signed    false\n" +
			"  precision 0\n" +
			"  scale     0\n" +
			"  type      12\n" +
			"  typename  url\n" +
			"  classname org.monetdb.jdbc.types.URL\n" +
			"  mode      1 (IN)\n" +
			"Table tbl6402 has 7 columns:\n" +
			"	tint	tdouble	tbool	tvarchar	tclob	turl	tclen\n" +
			"	1	1.1	true	one	ONE	www.monetdb.org	6\n" +
			"	2	2.02	true	Two	TWOs	http://www.monetdb.org/	7\n" +
			"Called Prepared procedure (2): proc6402\n" +
			"Table tbl6402 has 7 columns:\n" +
			"	tint	tdouble	tbool	tvarchar	tclob	turl	tclen\n" +
			"	1	1.1	true	one	ONE	www.monetdb.org	6\n" +
			"	2	2.02	true	Two	TWOs	http://www.monetdb.org/	7\n" +
			"	2	3.02	true	Tree	TWOs	https://www.monetdb.org/	8\n" +
			"Called Prepared procedure (3): proc6402\n" +
			"Table tbl6402 has 7 columns:\n" +
			"	tint	tdouble	tbool	tvarchar	tclob	turl	tclen\n" +
			"	1	1.1	true	one	ONE	www.monetdb.org	6\n" +
			"	2	2.02	true	Two	TWOs	http://www.monetdb.org/	7\n" +
			"	2	3.02	true	Tree	TWOs	https://www.monetdb.org/	8\n" +
			"	4	3.02	false	Four	TWOs	https://www.monetdb.org/	8\n" +
			"Called Prepared procedure (with NULLs): proc6402\n" +
			"Table tbl6402 has 7 columns:\n" +
			"	tint	tdouble	tbool	tvarchar	tclob	turl	tclen\n" +
			"	1	1.1	true	one	ONE	www.monetdb.org	6\n" +
			"	2	2.02	true	Two	TWOs	http://www.monetdb.org/	7\n" +
			"	2	3.02	true	Tree	TWOs	https://www.monetdb.org/	8\n" +
			"	4	3.02	false	Four	TWOs	https://www.monetdb.org/	8\n" +
			"	null	null	null	null	null	https://www.monetdb.org/	null\n" +
			"Test completed. Cleanup procedure and table.\n");
	}

	private void Test_Rbooleans() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			int updates = stmt.executeUpdate(
				"CREATE TABLE Test_Rbooleans (" +
				" id int, tiny_int tinyint, small_int smallint, medium_int mediumint, \"integer\" int, big_int bigint," +
				" a_real real, a_float float, a_double double, a_decimal decimal(8,2), a_numeric numeric(8)," +
				" bool boolean, a_char char(4), b_char char(5), a_varchar varchar(20), PRIMARY KEY (id) )");
			if (updates != 0)
				sb.append("1a. Expected 0 got ").append(updates).append(" instead\n");

			// all falses
			updates = stmt.executeUpdate("INSERT INTO Test_Rbooleans VALUES (1,0,0,0,0,0,0.0,0.0,0.0,0.0,0,false,'fals','false','false')");
			// all trues
			updates += stmt.executeUpdate("INSERT INTO Test_Rbooleans VALUES (2,1,1,1,1,1,1.0,1.0,1.0,1.0,1,true,'true','true ','true')");
			// sneakier
			updates += stmt.executeUpdate("INSERT INTO Test_Rbooleans VALUES (3,2,3,4,5,6,7.1,8.2,9.3,10.4,11,true,'TrUe','fAlSe','true/false')");
			updates += stmt.executeUpdate("INSERT INTO Test_Rbooleans VALUES (4,2,3,4,5,6,7.1,8.2,9.3,10.4,11,true,'t   ','f    ','TRUE      ')");
			if (updates != 4)
				sb.append("1b. Expected 4 got ").append(updates).append(" instead\n");

			rs = stmt.executeQuery("SELECT * FROM Test_Rbooleans ORDER BY id ASC");

			// all should give false
			rs.next();
			sb.append("1. ").append(rs.getInt("id")).append(", ").append(rs.getBoolean("tiny_int"))
			.append(", ").append(rs.getBoolean("small_int")).append(", ").append(rs.getBoolean("medium_int"))
			.append(", ").append(rs.getBoolean("integer")).append(", ").append(rs.getBoolean("big_int"))
			.append(", ").append(rs.getBoolean("a_real")).append(", ").append(rs.getBoolean("a_double"))
			.append(", ").append(rs.getBoolean("a_decimal")).append(", ").append(rs.getBoolean("a_numeric"))
			.append(", ").append(rs.getBoolean("bool")).append(", ").append(rs.getBoolean("a_char"))
			.append(", ").append(rs.getBoolean("b_char")).append(", ").append(rs.getBoolean("a_varchar")).append("\n");

			// all should give true except the one before last
			rs.next();
			sb.append("2. ").append(rs.getInt("id")).append(", ").append(rs.getBoolean("tiny_int"))
			.append(", ").append(rs.getBoolean("small_int")).append(", ").append(rs.getBoolean("medium_int"))
			.append(", ").append(rs.getBoolean("integer")).append(", ").append(rs.getBoolean("big_int"))
			.append(", ").append(rs.getBoolean("a_real")).append(", ").append(rs.getBoolean("a_double"))
			.append(", ").append(rs.getBoolean("a_decimal")).append(", ").append(rs.getBoolean("a_numeric"))
			.append(", ").append(rs.getBoolean("bool")).append(", ").append(rs.getBoolean("a_char"))
			.append(", ").append(rs.getBoolean("b_char")).append(", ").append(rs.getBoolean("a_varchar")).append("\n");

			// should give true for all but the last two
			rs.next();
			sb.append("3. ").append(rs.getInt("id")).append(", ").append(rs.getBoolean("tiny_int"))
			.append(", ").append(rs.getBoolean("small_int")).append(", ").append(rs.getBoolean("medium_int"))
			.append(", ").append(rs.getBoolean("integer")).append(", ").append(rs.getBoolean("big_int"))
			.append(", ").append(rs.getBoolean("a_real")).append(", ").append(rs.getBoolean("a_double"))
			.append(", ").append(rs.getBoolean("a_decimal")).append(", ").append(rs.getBoolean("a_numeric"))
			.append(", ").append(rs.getBoolean("bool")).append(", ").append(rs.getBoolean("a_char"))
			.append(", ").append(rs.getBoolean("b_char")).append(", ").append(rs.getBoolean("a_varchar")).append("\n");

			// should give true for all but the last three
			rs.next();
			sb.append("4. ").append(rs.getInt("id")).append(", ").append(rs.getBoolean("tiny_int"))
			.append(", ").append(rs.getBoolean("small_int")).append(", ").append(rs.getBoolean("medium_int"))
			.append(", ").append(rs.getBoolean("integer")).append(", ").append(rs.getBoolean("big_int"))
			.append(", ").append(rs.getBoolean("a_real")).append(", ").append(rs.getBoolean("a_double"))
			.append(", ").append(rs.getBoolean("a_decimal")).append(", ").append(rs.getBoolean("a_numeric"))
			.append(", ").append(rs.getBoolean("bool")).append(", ").append(rs.getBoolean("a_char"))
			.append(", ").append(rs.getBoolean("b_char")).append(", ").append(rs.getBoolean("a_varchar")).append("\n");
			rs.next();

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Rbooleans",
			"0. false	false\n" +
			"1. 1, false, false, false, false, false, false, false, false, false, false, false, false, false\n" +
			"2. 2, true, true, true, true, true, true, true, true, true, true, true, false, true\n" +
			"3. 3, true, true, true, true, true, true, true, true, true, true, true, false, false\n" +
			"4. 4, true, true, true, true, true, true, true, true, true, true, false, false, false\n" +
			"0. true	true\n");
	}

	private void Test_Rmetadata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE Test_Rmetadata ( myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob )");

			// all NULLs
			stmt.executeUpdate("INSERT INTO Test_Rmetadata VALUES (NULL, NULL, NULL, NULL, NULL)");
			// all filled in
			stmt.executeUpdate("INSERT INTO Test_Rmetadata VALUES (2 , 3.0, true, 'A string', 'bla bla bla')");

			rs = stmt.executeQuery("SELECT * FROM Test_Rmetadata");

			ResultSetMetaData rsmd = rs.getMetaData();
			sb.append("0. ").append(rsmd.getColumnCount()).append(" columns:\n");
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				sb.append("Colnr ").append(col).append(".\n");
				sb.append("\tclassname     ").append(rsmd.getColumnClassName(col)).append("\n");
				sb.append("\tdisplaysize   ").append(rsmd.getColumnDisplaySize(col)).append("\n");
				sb.append("\tlabel         ").append(rsmd.getColumnLabel(col)).append("\n");
				sb.append("\tname          ").append(rsmd.getColumnName(col)).append("\n");
				sb.append("\ttype          ").append(rsmd.getColumnType(col)).append("\n");
				sb.append("\ttypename      ").append(rsmd.getColumnTypeName(col)).append("\n");
				sb.append("\tprecision     ").append(rsmd.getPrecision(col)).append("\n");
				sb.append("\tscale         ").append(rsmd.getScale(col)).append("\n");
				sb.append("\tcatalogname   ").append(rsmd.getCatalogName(col)).append("\n");
				sb.append("\tschemaname    ").append(rsmd.getSchemaName(col)).append("\n");
				sb.append("\ttablename     ").append(rsmd.getTableName(col)).append("\n");
				sb.append("\tautoincrement ").append(rsmd.isAutoIncrement(col)).append("\n");
				sb.append("\tcasesensitive ").append(rsmd.isCaseSensitive(col)).append("\n");
				sb.append("\tcurrency      ").append(rsmd.isCurrency(col)).append("\n");
				sb.append("\tdefwritable   ").append(rsmd.isDefinitelyWritable(col)).append("\n");
				sb.append("\tnullable      ").append(rsmd.isNullable(col)).append("\n");
				sb.append("\treadonly      ").append(rsmd.isReadOnly(col)).append("\n");
				sb.append("\tsearchable    ").append(rsmd.isSearchable(col)).append("\n");
				sb.append("\tsigned        ").append(rsmd.isSigned(col)).append("\n");
				sb.append("\twritable      ").append(rsmd.isWritable(col)).append("\n");
			}

			for (int i = 6; rs.next(); i++) {
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					Object obj = rs.getObject(col);
					String type = rsmd.getColumnClassName(col);
					String isInstance = "(null)";
					if (obj != null && type != null) {
						try {
							Class<?> c = Class.forName(type);
							if (c.isInstance(obj)) {
								isInstance = (obj.getClass().getName() + " is an instance of " + type);
							} else {
								isInstance = (obj.getClass().getName() + " is NOT an instance of " + type);
							}
						} catch (ClassNotFoundException e) {
							isInstance = "No such class: " + type;
						}
					}
					sb.append(i).append(".\t").append(isInstance).append("\n");
				}
			}
			rs.close();

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Rmetadata",
			"0. false	false\n" +
			"0. 5 columns:\n" +
			"Colnr 1.\n" +
			"	classname     java.lang.Integer\n" +
			"	displaysize   1\n" +
			"	label         myint\n" +
			"	name          myint\n" +
			"	type          4\n" +
			"	typename      int\n" +
			"	precision     10\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        true\n" +
			"	writable      false\n" +
			"Colnr 2.\n" +
			"	classname     java.lang.Double\n" +
			"	displaysize   24\n" +
			"	label         mydouble\n" +
			"	name          mydouble\n" +
			"	type          8\n" +
			"	typename      double\n" +
			"	precision     15\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        true\n" +
			"	writable      false\n" +
			"Colnr 3.\n" +
			"	classname     java.lang.Boolean\n" +
			"	displaysize   5\n" +
			"	label         mybool\n" +
			"	name          mybool\n" +
			"	type          16\n" +
			"	typename      boolean\n" +
			"	precision     1\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"Colnr 4.\n" +
			"	classname     java.lang.String\n" +
			"	displaysize   8\n" +
			"	label         myvarchar\n" +
			"	name          myvarchar\n" +
			"	type          12\n" +
			"	typename      varchar\n" +
			"	precision     15\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive true\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"Colnr 5.\n" +
			"	classname     java.lang.String\n" +
			"	displaysize   11\n" +
			"	label         myclob\n" +
			"	name          myclob\n" +
			"	type          12\n" +
			"	typename      " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
			"	precision     11\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive true\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"7.	java.lang.Integer is an instance of java.lang.Integer\n" +
			"7.	java.lang.Double is an instance of java.lang.Double\n" +
			"7.	java.lang.Boolean is an instance of java.lang.Boolean\n" +
			"7.	java.lang.String is an instance of java.lang.String\n" +
			"7.	java.lang.String is an instance of java.lang.String\n" +
			"0. true	true\n");
	}

	private void Test_RSgetMetaData() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE Test_RSmetadata ( myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob )");

			// all NULLs
			stmt.executeUpdate("INSERT INTO Test_RSmetadata VALUES (NULL, NULL, NULL, NULL, NULL)");
			// all filled in
			stmt.executeUpdate("INSERT INTO Test_RSmetadata VALUES (2 , 3.0, true, 'A string', 'bla bla bla')");

			rs = stmt.executeQuery("SELECT * FROM Test_RSmetadata");

			sb.append("0. ").append(rs.getMetaData().getColumnCount()).append(" columns:\n");
			for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
				sb.append("Colnr ").append(col).append(".\n");
				sb.append("\tclassname     ").append(rs.getMetaData().getColumnClassName(col)).append("\n");
				sb.append("\tdisplaysize   ").append(rs.getMetaData().getColumnDisplaySize(col)).append("\n");
				sb.append("\tlabel         ").append(rs.getMetaData().getColumnLabel(col)).append("\n");
				sb.append("\tname          ").append(rs.getMetaData().getColumnName(col)).append("\n");
				sb.append("\ttype          ").append(rs.getMetaData().getColumnType(col)).append("\n");
				sb.append("\ttypename      ").append(rs.getMetaData().getColumnTypeName(col)).append("\n");
				sb.append("\tprecision     ").append(rs.getMetaData().getPrecision(col)).append("\n");
				sb.append("\tscale         ").append(rs.getMetaData().getScale(col)).append("\n");
				sb.append("\tcatalogname   ").append(rs.getMetaData().getCatalogName(col)).append("\n");
				sb.append("\tschemaname    ").append(rs.getMetaData().getSchemaName(col)).append("\n");
				sb.append("\ttablename     ").append(rs.getMetaData().getTableName(col)).append("\n");
				sb.append("\tautoincrement ").append(rs.getMetaData().isAutoIncrement(col)).append("\n");
				sb.append("\tcasesensitive ").append(rs.getMetaData().isCaseSensitive(col)).append("\n");
				sb.append("\tcurrency      ").append(rs.getMetaData().isCurrency(col)).append("\n");
				sb.append("\tdefwritable   ").append(rs.getMetaData().isDefinitelyWritable(col)).append("\n");
				sb.append("\tnullable      ").append(rs.getMetaData().isNullable(col)).append("\n");
				sb.append("\treadonly      ").append(rs.getMetaData().isReadOnly(col)).append("\n");
				sb.append("\tsearchable    ").append(rs.getMetaData().isSearchable(col)).append("\n");
				sb.append("\tsigned        ").append(rs.getMetaData().isSigned(col)).append("\n");
				sb.append("\twritable      ").append(rs.getMetaData().isWritable(col)).append("\n");
			}

			for (int i = 6; rs.next(); i++) {
				for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
					Object obj = rs.getObject(col);
					String type = rs.getMetaData().getColumnClassName(col);
					String isInstance = "(null)";
					if (obj != null && type != null) {
						try {
							Class<?> c = Class.forName(type);
							if (c.isInstance(obj)) {
								isInstance = (obj.getClass().getName() + " is an instance of " + type);
							} else {
								isInstance = (obj.getClass().getName() + " is NOT an instance of " + type);
							}
						} catch (ClassNotFoundException e) {
							isInstance = "No such class: " + type;
						}
					}
					sb.append(i).append(".\t").append(isInstance).append("\n");
				}
			}
			rs.close();

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_RSgetMetaData",
			"0. false	false\n" +
			"0. 5 columns:\n" +
			"Colnr 1.\n" +
			"	classname     java.lang.Integer\n" +
			"	displaysize   1\n" +
			"	label         myint\n" +
			"	name          myint\n" +
			"	type          4\n" +
			"	typename      int\n" +
			"	precision     10\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rsmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        true\n" +
			"	writable      false\n" +
			"Colnr 2.\n" +
			"	classname     java.lang.Double\n" +
			"	displaysize   24\n" +
			"	label         mydouble\n" +
			"	name          mydouble\n" +
			"	type          8\n" +
			"	typename      double\n" +
			"	precision     15\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rsmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        true\n" +
			"	writable      false\n" +
			"Colnr 3.\n" +
			"	classname     java.lang.Boolean\n" +
			"	displaysize   5\n" +
			"	label         mybool\n" +
			"	name          mybool\n" +
			"	type          16\n" +
			"	typename      boolean\n" +
			"	precision     1\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rsmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive false\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"Colnr 4.\n" +
			"	classname     java.lang.String\n" +
			"	displaysize   8\n" +
			"	label         myvarchar\n" +
			"	name          myvarchar\n" +
			"	type          12\n" +
			"	typename      varchar\n" +
			"	precision     15\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rsmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive true\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"Colnr 5.\n" +
			"	classname     java.lang.String\n" +
			"	displaysize   11\n" +
			"	label         myclob\n" +
			"	name          myclob\n" +
			"	type          12\n" +
			"	typename      " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
			"	precision     11\n" +
			"	scale         0\n" +
			"	catalogname   null\n" +
			"	schemaname    sys\n" +
			"	tablename     test_rsmetadata\n" +
			"	autoincrement false\n" +
			"	casesensitive true\n" +
			"	currency      false\n" +
			"	defwritable   false\n" +
			"	nullable      1\n" +
			"	readonly      true\n" +
			"	searchable    true\n" +
			"	signed        false\n" +
			"	writable      false\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"6.	(null)\n" +
			"7.	java.lang.Integer is an instance of java.lang.Integer\n" +
			"7.	java.lang.Double is an instance of java.lang.Double\n" +
			"7.	java.lang.Boolean is an instance of java.lang.Boolean\n" +
			"7.	java.lang.String is an instance of java.lang.String\n" +
			"7.	java.lang.String is an instance of java.lang.String\n" +
			"0. true	true\n");
	}

	private void Test_RfetchManyColumnsInfo() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			final int NR_COLUMNS = 180;
			final StringBuilder sql = new StringBuilder(50 + (NR_COLUMNS * 12));

			sql.append("CREATE TABLE Test_RfetchManyColumnsInfo (");
			for (int col = 1; col <= NR_COLUMNS; col++) {
				sql.append("col").append(col).append(" int");
				sql.append((col < NR_COLUMNS) ? ", " : ")");
			}

			stmt = con.createStatement();
			stmt.executeUpdate(sql.toString());

			// add 1 row (all NULLs)
			int inserted = stmt.executeUpdate("INSERT INTO Test_RfetchManyColumnsInfo (col1) VALUES (1)");
			if (inserted != 1)
				sb.append("Expected 1 row inserted, but got: ").append(inserted).append("\n");

			rs = stmt.executeQuery("SELECT * FROM Test_RfetchManyColumnsInfo");
			rs.next();
			ResultSetMetaData rsmd = rs.getMetaData();
			sb.append(rsmd.getColumnCount()).append(" columns start at columnCount\n");
			// do pulling of the metadata info in reverse order to test optimizing logic
			// in ResultSetMetaData.fetchManyColumnsInfo() to choose a lower start_col iteratively
			for (int col = rsmd.getColumnCount(); col >= 1; col--) {
				// sb.append(col).append(",");
				rsmd.getColumnClassName(col);
				rsmd.getColumnDisplaySize(col);
				rsmd.getColumnLabel(col);
				rsmd.getColumnName(col);
				rsmd.getColumnType(col);
				rsmd.getColumnTypeName(col);
				rsmd.getPrecision(col);
				rsmd.getScale(col);
				rsmd.getCatalogName(col);
				rsmd.getSchemaName(col);
				rsmd.getTableName(col);
				rsmd.isAutoIncrement(col);
				rsmd.isCaseSensitive(col);
				rsmd.isCurrency(col);
				rsmd.isDefinitelyWritable(col);
				if (rsmd.isNullable(col) != ResultSetMetaData.columnNullable)
					sb.append(col).append(" wrong isNullable()").append(rsmd.isNullable(col)).append("\n");
				rsmd.isReadOnly(col);
				rsmd.isSearchable(col);
				rsmd.isSigned(col);
				rsmd.isWritable(col);
			}
			rs.close();

			rs = stmt.executeQuery("SELECT * FROM Test_RfetchManyColumnsInfo");
			rs.next();
			rsmd = rs.getMetaData();
			sb.append(rsmd.getColumnCount()).append(" columns start at 1\n");
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				// sb.append(col).append(",");
				rsmd.getColumnClassName(col);
				rsmd.getColumnDisplaySize(col);
				rsmd.getColumnLabel(col);
				rsmd.getColumnName(col);
				rsmd.getColumnType(col);
				rsmd.getColumnTypeName(col);
				rsmd.getPrecision(col);
				rsmd.getScale(col);
				rsmd.getCatalogName(col);
				rsmd.getSchemaName(col);
				rsmd.getTableName(col);
				rsmd.isAutoIncrement(col);
				rsmd.isCaseSensitive(col);
				rsmd.isCurrency(col);
				rsmd.isDefinitelyWritable(col);
				if (rsmd.isNullable(col) != ResultSetMetaData.columnNullable)
					sb.append(col).append(" wrong isNullable()").append(rsmd.isNullable(col)).append("\n");
				rsmd.isReadOnly(col);
				rsmd.isSearchable(col);
				rsmd.isSigned(col);
				rsmd.isWritable(col);
			}
			rs.close();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup table
		try {
			stmt.executeUpdate("DROP TABLE IF EXISTS Test_RfetchManyColumnsInfo;");
		} catch (SQLException e) {
			sb.append("FAILED to drop: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_RfetchManyColumnsInfo",
			"180 columns start at columnCount\n" +
			"180 columns start at 1\n");
	}

	private void Test_Rpositioning() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			// get a one rowed resultset
			rs = stmt.executeQuery("SELECT 1");

			// >> true: we should be before the first result now
			sb.append("1. true\t").append(rs.isBeforeFirst()).append("\n");
			// >> false: we're not at the first result
			sb.append("2. false\t").append(rs.isFirst()).append("\n");
			// >> true: there is one result, so we can call next once
			sb.append("3. true\t").append(rs.next()).append("\n");
			// >> false: we're not before the first row anymore
			sb.append("4. false\t").append(rs.isBeforeFirst()).append("\n");
			// >> true: we're at the first result
			sb.append("5. true\t").append(rs.isFirst()).append("\n");
			// >> false: we're on the last row
			sb.append("6. false\t").append(rs.isAfterLast()).append("\n");
			// >> true: see above
			sb.append("7. true\t").append(rs.isLast()).append("\n");
			// >> false: there is one result, so this is it
			sb.append("8. false\t").append(rs.next()).append("\n");
			// >> true: yes, we're at the end
			sb.append("9. true\t").append(rs.isAfterLast()).append("\n");
			// >> false: no we're one over it
			sb.append("10. false\t").append(rs.isLast()).append("\n");
			// >> false: another try to move on should still fail
			sb.append("11. false\t").append(rs.next()).append("\n");
			// >> true: and we should stay positioned after the last
			sb.append("12.true\t").append(rs.isAfterLast()).append("\n");

			rs.close();

			// try the same with a scrollable result set
			DatabaseMetaData dbmd = con.getMetaData();
			rs = dbmd.getTableTypes();

			// >> true: we should be before the first result now
			sb.append("1. true\t").append(rs.isBeforeFirst()).append("\n");
			// >> false: we're not at the first result
			sb.append("2. false\t").append(rs.isFirst()).append("\n");
			// >> true: there is one result, so we can call next once
			sb.append("3. true\t").append(rs.next()).append("\n");
			// >> false: we're not before the first row anymore
			sb.append("4. false\t").append(rs.isBeforeFirst()).append("\n");
			// >> true: we're at the first result
			sb.append("5. true\t").append(rs.isFirst()).append("\n");
			// move to last row
			rs.last();
			// >> false: we're on the last row
			sb.append("6. false\t").append(rs.isAfterLast()).append("\n");
			// >> true: see above
			sb.append("7. true\t").append(rs.isLast()).append("\n");
			// >> false: there is one result, so this is it
			sb.append("8. false\t").append(rs.next()).append("\n");
			// >> true: yes, we're at the end
			sb.append("9. true\t").append(rs.isAfterLast()).append("\n");
			// >> false: no we're one over it
			sb.append("10. false\t").append(rs.isLast()).append("\n");
			// >> false: another try to move on should still fail
			sb.append("11. false\t").append(rs.next()).append("\n");
			// >> true: and we should stay positioned after the last
			sb.append("12. true\t").append(rs.isAfterLast()).append("\n");

			rs.close();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Rpositioning",
			"1. true	true\n" +
			"2. false	false\n" +
			"3. true	true\n" +
			"4. false	false\n" +
			"5. true	true\n" +
			"6. false	false\n" +
			"7. true	true\n" +
			"8. false	false\n" +
			"9. true	true\n" +
			"10. false	false\n" +
			"11. false	false\n" +
			"12.true	true\n" +
			"1. true	true\n" +
			"2. false	false\n" +
			"3. true	true\n" +
			"4. false	false\n" +
			"5. true	true\n" +
			"6. false	false\n" +
			"7. true	true\n" +
			"8. false	false\n" +
			"9. true	true\n" +
			"10. false	false\n" +
			"11. false	false\n" +
			"12. true	true\n");
	}

	private void Test_Rsqldata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;

		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE Test_Rsqldata ( myinet inet, myurl url )");

			String InsertInto = "INSERT INTO Test_Rsqldata VALUES ";
			// all NULLs
			stmt.executeUpdate(InsertInto + "(NULL, NULL)");
			// all filled in
			stmt.executeUpdate(InsertInto + "('172.5.5.5' , 'http://www.monetdb.org/')");
			stmt.executeUpdate(InsertInto + "('172.5.5.5/32' , 'http://www.monetdb.org/Home')");
			stmt.executeUpdate(InsertInto + "('172.5.5.5/16' , 'http://www.monetdb.org/Home#someanchor')");
			stmt.executeUpdate(InsertInto + "('172.5.5.5/26' , 'http://www.monetdb.org/?query=bla')");

			rs = stmt.executeQuery("SELECT * FROM Test_Rsqldata");
			rsmd = rs.getMetaData();

			sb.append("0. ").append(rsmd.getColumnCount()).append(" columns:\n");
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				sb.append(col).append("\n");
				sb.append("\tclassname   ").append(rsmd.getColumnClassName(col)).append("\n");
				sb.append("\tcatalogname ").append(rsmd.getCatalogName(col)).append("\n");
				sb.append("\tschemaname  ").append(rsmd.getSchemaName(col)).append("\n");
				sb.append("\ttablename   ").append(rsmd.getTableName(col)).append("\n");
				sb.append("\tcolumnname  ").append(rsmd.getColumnName(col)).append("\n");
			}

			for (int i = 1; rs.next(); i++) {
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					sb.append(i).append(".\t");
					Object x = rs.getObject(col);
					if (x == null) {
						sb.append("<null>\n");
					} else {
						sb.append(x.toString()).append("\n");
						if (x instanceof INET) {
							INET inet = (INET)x;
							sb.append("\t").append(inet.getAddress()).append("/").append(inet.getNetmaskBits()).append("\n");
							sb.append("\t").append(inet.getInetAddress().toString()).append("\n");
						} else if (x instanceof URL) {
							URL url = (URL)x;
							sb.append("\t").append(url.getURL().toString()).append("\n");
						}
					}
				}
			}

			rs.close();

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit was just switched on
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Rsqldata",
			"0. false	false\n" +
			"0. 2 columns:\n" +
			"1\n" +
			"	classname   org.monetdb.jdbc.types.INET\n" +
			"	catalogname null\n" +
			"	schemaname  sys\n" +
			"	tablename   test_rsqldata\n" +
			"	columnname  myinet\n" +
			"2\n" +
			"	classname   org.monetdb.jdbc.types.URL\n" +
			"	catalogname null\n" +
			"	schemaname  sys\n" +
			"	tablename   test_rsqldata\n" +
			"	columnname  myurl\n" +
			"1.	<null>\n" +
			"1.	<null>\n" +
			"2.	172.5.5.5\n" +
			"	172.5.5.5/32\n" +
			"	/172.5.5.5\n" +
			"2.	http://www.monetdb.org/\n" +
			"	http://www.monetdb.org/\n" +
			"3.	172.5.5.5\n" +
			"	172.5.5.5/32\n" +
			"	/172.5.5.5\n" +
			"3.	http://www.monetdb.org/Home\n" +
			"	http://www.monetdb.org/Home\n" +
			"4.	172.5.5.5/16\n" +
			"	172.5.5.5/16\n" +
			"	/172.5.5.5\n" +
			"4.	http://www.monetdb.org/Home#someanchor\n" +
			"	http://www.monetdb.org/Home#someanchor\n" +
			"5.	172.5.5.5/26\n" +
			"	172.5.5.5/26\n" +
			"	/172.5.5.5\n" +
			"5.	http://www.monetdb.org/?query=bla\n" +
			"	http://www.monetdb.org/?query=bla\n" +
			"0. true	true\n");
	}

	private void Test_Rtimedate() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
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

			con.rollback();
			con.setAutoCommit(true);
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_Rtimedate",
			"0. false	false\n" +
			"1. ts 2004-04-24 11:43:53.123000 to ts: 2004-04-24 11:43:53.123\n" +
			"1. ts 2004-04-24 11:43:53.123000 to tm: 11:43:53\n" +
			"1. ts 2004-04-24 11:43:53.123000 to dt: 2004-04-24\n" +
			"1. ts 2004-04-24 11:43:53.123000 to LocalDateTime: 2004-04-24T11:43:53.123\n" +
			"2. t 11:43:53 to ts: 1970-01-01 11:43:53.0\n" +
			"2. t 11:43:53 to tm: 11:43:53\n" +
			"2. t 11:43:53 to dt: 1970-01-01\n" +
			"2. t 11:43:53 to LocalTime: 11:43:53\n" +
			"3. d 2004-04-24 to ts: 2004-04-24 00:00:00.0\n" +
			"3. d 2004-04-24 to tm: 00:00:00\n" +
			"3. d 2004-04-24 to dt: 2004-04-24\n" +
			"3. d 2004-04-24 to LocalDate: 2004-04-24\n" +
			"4. vc 2004-04-24 11:43:53.654321 to ts: 2004-04-24 11:43:53.654321\n" +
			"4. vc 2004-04-24 11:43:53.654321 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 5 found: '-' in '2004-04-24 11:43:53.654321'\n" +
			"4. vc 2004-04-24 11:43:53.654321 to dt: 2004-04-24\n" +
			"5. vc 11:43:53 to ts: rs.getTimestamp(colnm) failed with error: parsing failed at pos 3 found: ':' in '11:43:53'\n" +
			"5. vc 11:43:53 to tm: 11:43:53\n" +
			"5. vc 11:43:53 to dt: rs.getDate(colnm) failed with error: parsing failed at pos 3 found: ':' in '11:43:53'\n" +
			"6. vc 2004-04-24 to ts: 2004-04-24 00:00:00.0\n" +
			"6. vc 2004-04-24 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 5 found: '-' in '2004-04-24'\n" +
			"6. vc 2004-04-24 to dt: 2004-04-24\n" +
			"11. ts 904-04-24 11:43:53.567000 to ts: 0904-04-24 11:43:53.567\n" +
			"11. ts 904-04-24 11:43:53.567000 to tm: 11:43:53\n" +
			"11. ts 904-04-24 11:43:53.567000 to dt: 0904-04-24\n" +
			"11. ts 904-04-24 11:43:53.567000 to LocalDateTime: rs.getObject(colnm, class<T>): Failed to convert to LocalDateTime: Text '904-04-24T11:43:53.567000' could not be parsed at index 0\n" +
			"12. ts 74-04-24 11:43:53.567000 to ts: 0074-04-24 11:43:53.567\n" +
			"12. ts 74-04-24 11:43:53.567000 to tm: 11:43:53\n" +
			"12. ts 74-04-24 11:43:53.567000 to dt: 0074-04-24\n" +
			"12. ts 74-04-24 11:43:53.567000 to LocalDateTime: rs.getObject(colnm, class<T>): Failed to convert to LocalDateTime: Text '74-04-24T11:43:53.567000' could not be parsed at index 0\n" +
			"13. ts 4-04-24 11:43:53.567000 to ts: 0004-04-24 11:43:53.567\n" +
			"13. ts 4-04-24 11:43:53.567000 to tm: 11:43:53\n" +
			"13. ts 4-04-24 11:43:53.567000 to dt: 0004-04-24\n" +
			"13. ts 4-04-24 11:43:53.567000 to LocalDateTime: rs.getObject(colnm, class<T>): Failed to convert to LocalDateTime: Text '4-04-24T11:43:53.567000' could not be parsed at index 0\n" +
			"14. d 904-04-24 to ts: 0904-04-24 00:00:00.0\n" +
			"14. d 904-04-24 to tm: 00:00:00\n" +
			"14. d 904-04-24 to dt: 0904-04-24\n" +
			"14. d 904-04-24 to LocalDate: rs.getObject(colnm, class<T>): Failed to convert to LocalDate: Text '904-04-24' could not be parsed at index 0\n" +
			"15. d 74-04-24 to ts: 0074-04-24 00:00:00.0\n" +
			"15. d 74-04-24 to tm: 00:00:00\n" +
			"15. d 74-04-24 to dt: 0074-04-24\n" +
			"15. d 74-04-24 to LocalDate: rs.getObject(colnm, class<T>): Failed to convert to LocalDate: Text '74-04-24' could not be parsed at index 0\n" +
			"16. d 4-04-24 to ts: 0004-04-24 00:00:00.0\n" +
			"16. d 4-04-24 to tm: 00:00:00\n" +
			"16. d 4-04-24 to dt: 0004-04-24\n" +
			"16. d 4-04-24 to LocalDate: rs.getObject(colnm, class<T>): Failed to convert to LocalDate: Text '4-04-24' could not be parsed at index 0\n" +
			"17. vc 904-04-24 11:43:53.567 to ts: 0904-04-24 11:43:53.567\n" +
			"17. vc 904-04-24 11:43:53.567 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 4 found: '-' in '904-04-24 11:43:53.567'\n" +
			"17. vc 904-04-24 11:43:53.567 to dt: 0904-04-24\n" +
			"18. vc 74-04-24 11:43:53.567 to ts: 0074-04-24 11:43:53.567\n" +
			"18. vc 74-04-24 11:43:53.567 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 3 found: '-' in '74-04-24 11:43:53.567'\n" +
			"18. vc 74-04-24 11:43:53.567 to dt: 0074-04-24\n" +
			"19. vc 4-04-24 11:43:53.567 to ts: 0004-04-24 11:43:53.567\n" +
			"19. vc 4-04-24 11:43:53.567 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 2 found: '-' in '4-04-24 11:43:53.567'\n" +
			"19. vc 4-04-24 11:43:53.567 to dt: 0004-04-24\n" +
			"21. ts -4-04-24 11:43:53.567000 to ts: 0004-04-24 11:43:53.567\n" +
			"21. ts -4-04-24 11:43:53.567000 to tm: 11:43:53\n" +
			"21. ts -4-04-24 11:43:53.567000 to dt: 0004-04-24\n" +
			"21. ts -4-04-24 11:43:53.567000 to LocalDateTime: rs.getObject(colnm, class<T>): Failed to convert to LocalDateTime: Text '-4-04-24T11:43:53.567000' could not be parsed at index 1\n" +
			"22. ts -2004-04-24 11:43:53.567000 to ts: 2004-04-24 11:43:53.567\n" +
			"22. ts -2004-04-24 11:43:53.567000 to tm: 11:43:53\n" +
			"22. ts -2004-04-24 11:43:53.567000 to dt: 2004-04-24\n" +
			"22. ts -2004-04-24 11:43:53.567000 to LocalDateTime: -2004-04-24T11:43:53.567\n" +
			"23. d -4-04-24 to ts: 0004-04-24 00:00:00.0\n" +
			"23. d -4-04-24 to tm: 00:00:00\n" +
			"23. d -4-04-24 to dt: 0004-04-24\n" +
			"23. d -4-04-24 to LocalDate: rs.getObject(colnm, class<T>): Failed to convert to LocalDate: Text '-4-04-24' could not be parsed at index 1\n" +
			"24. d -3004-04-24 to ts: 3004-04-24 00:00:00.0\n" +
			"24. d -3004-04-24 to tm: 00:00:00\n" +
			"24. d -3004-04-24 to dt: 3004-04-24\n" +
			"24. d -3004-04-24 to LocalDate: -3004-04-24\n" +
			"25. vc -2004-04-24 11:43:53.654321 to ts: 2004-04-24 11:43:53.654321\n" +
			"25. vc -2004-04-24 11:43:53.654321 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 6 found: '-' in '-2004-04-24 11:43:53.654321'\n" +
			"25. vc -2004-04-24 11:43:53.654321 to dt: 2004-04-24\n" +
			"26. vc -3004-04-24 to ts: 3004-04-24 00:00:00.0\n" +
			"26. vc -3004-04-24 to tm: rs.getTime(colnm) failed with error: parsing failed at pos 6 found: '-' in '-3004-04-24'\n" +
			"26. vc -3004-04-24 to dt: 3004-04-24\n" +
			"0. true	true\n");
	}

	private void readNextRow(ResultSet rs, int rowseq, String colnm) throws SQLException {
		rs.next();
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// fetch the column value using multiple methods: getString(), getTimestamp(), getTime() and getDate()
		// to test proper conversion and error reporting
		String data = rs.getString("id") + ". " + colnm + " " + rs.getString(colnm) + " to ";

		// getTimestamp() may raise a conversion warning when the value is of type Time or a String which doesn't match format yyyy-mm-dd hh:mm:ss
		try {
			sb.append(data).append("ts: ").append(rs.getTimestamp(colnm)).append("\n");
		} catch (SQLException e) {
			sb.append("rs.getTimestamp(colnm) failed with error: ").append(e.getMessage()).append("\n");
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// getTime() may raise a conversion warning when the value is of type Date or a String which doesn't match format hh:mm:ss
		try {
			sb.append(data).append("tm: ").append(rs.getTime(colnm)).append("\n");
		} catch (SQLException e) {
			sb.append("rs.getTime(colnm) failed with error: ").append(e.getMessage()).append("\n");
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// getDate() may raise a conversion warning when the value is of type Time or a String which doesn't match format yyyy-mm-dd
		try {
			sb.append(data).append("dt: ").append(rs.getDate(colnm)).append("\n");
		} catch (SQLException e) {
			sb.append("rs.getDate(colnm) failed with error: ").append(e.getMessage()).append("\n");
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();

		// getObject(colnm, class<T>) may raise a conversion error
		try {
			switch(colnm) {
			case "d":
				sb.append(data).append("LocalDate: ").append(rs.getObject(colnm, java.time.LocalDate.class)).append("\n");
				break;
			case "ts":
				sb.append(data).append("LocalDateTime: ").append(rs.getObject(colnm, java.time.LocalDateTime.class)).append("\n");
				break;
			case "t":
				sb.append(data).append("LocalTime: ").append(rs.getObject(colnm, java.time.LocalTime.class)).append("\n");
				break;
			}
		} catch (SQLException e) {
			sb.append("rs.getObject(colnm, class<T>): ").append(e.getMessage()).append("\n");
		}
		readWarnings(rs.getWarnings());
		rs.clearWarnings();
	}

	private void Test_Sbatching() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit should be off now
			sb.append("0. false\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();

			sb.append("1. create...");
			if (stmt.executeUpdate("CREATE TABLE Test_Sbatching ( id int )") != 0)
				sb.append("Wrong return status\n");
			else
				sb.append("passed\n");

			// for large batches of DML always set sys.optimizer = 'minimal_pipe'. It makes a big difference in performance.
			stmt.execute("SET sys.optimizer = 'minimal_pipe'");

			// start batching a large amount of inserts
			for (int i = 1; i <= 3432; i++) {
				stmt.addBatch("INSERT INTO Test_Sbatching VALUES (" + i + ")");
				if (i % 1500 == 0) {
					sb.append("2. executing batch (1500 inserts)...");
					int[] cnts = stmt.executeBatch();
					sb.append("passed\n");
					sb.append("3. checking number of update counts...");
					if (cnts.length != 1500)
						sb.append("Invalid size: ").append(cnts.length);
					sb.append(cnts.length).append(" passed\n");
					sb.append("4. checking update counts (should all be 1)...");
					for (int j = 0; j < cnts.length; j++) {
						if (cnts[j] != 1)
							sb.append("Unexpected value: ").append(cnts[j]);
					}
					sb.append("passed\n");
					con.commit();
				}
			}
			sb.append("5. executing final batch...");
			stmt.executeBatch();
			con.commit();
			sb.append("passed\n");

			pstmt = con.prepareStatement("INSERT INTO Test_Sbatching VALUES (?)");
			// start batching a large amount of prepared inserts using JDBC 4.2 executeLargeBatch()
			for (int i = 1; i <= 3568; i++) {
				pstmt.setInt(1, i);
				pstmt.addBatch();
				if (i % 3000 == 0) {
					sb.append("2. executing batch (3000 inserts)...");
					long[] cnts = pstmt.executeLargeBatch();
					sb.append("passed\n");
					sb.append("3. checking number of update counts...");
					if (cnts.length != 3000)
						sb.append("Invalid size: ").append(cnts.length);
					sb.append(cnts.length).append(" passed\n");
					sb.append("4. checking update counts (should all be 1)...");
					for (int j = 0; j < cnts.length; j++) {
						if (cnts[j] != 1)
							sb.append("Unexpected value: ").append(cnts[j]);
					}
					sb.append("passed\n");
					con.commit();
				}
			}
			sb.append("5. executing final Largebatch...");
			pstmt.executeLargeBatch();
			con.commit();
			sb.append("passed\n");

			sb.append("6. clearing the batch...");
			stmt.clearBatch();
			pstmt.clearBatch();
			sb.append("passed\n");

			sb.append("7. checking table count...");
			rs = stmt.executeQuery("SELECT COUNT(*) FROM Test_Sbatching");
			rs.next();
			sb.append(rs.getInt(1)).append(" passed\n");

			sb.append("8. drop table...");
			if (stmt.executeUpdate("DROP TABLE Test_Sbatching") != 0)
				sb.append("Wrong return status\n");
			else
				sb.append("passed\n");

			// rs.close();
			stmt.close();
			pstmt.close();

			con.commit();
			con.setAutoCommit(true);
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Test_Sbatching",
			"0. false	false\n" +
			"1. create...passed\n" +
			"2. executing batch (1500 inserts)...passed\n" +
			"3. checking number of update counts...1500 passed\n" +
			"4. checking update counts (should all be 1)...passed\n" +
			"2. executing batch (1500 inserts)...passed\n" +
			"3. checking number of update counts...1500 passed\n" +
			"4. checking update counts (should all be 1)...passed\n" +
			"5. executing final batch...passed\n" +
			"2. executing batch (3000 inserts)...passed\n" +
			"3. checking number of update counts...3000 passed\n" +
			"4. checking update counts (should all be 1)...passed\n" +
			"5. executing final Largebatch...passed\n" +
			"6. clearing the batch...passed\n" +
			"7. checking table count...7000 passed\n" +
			"8. drop table...passed\n" +
			"0. true	true\n");
	}

	private void Test_SgeneratedKeys() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rsk = null;
		try {
			stmt = con.createStatement();
			// test getGeneratedKeys
			rsk = stmt.getGeneratedKeys();
			// test meta data retrieval of this MonetVirtualResultSet.
			// It used to fail with: Exception in thread "main" java.lang.IllegalArgumentException: Header may not be null!
			final ResultSetMetaData rsmd = rsk.getMetaData();
			sb.append("rsmd has ").append(rsmd.getColumnCount()).append(" columns\n");
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				sb.append("ColumnName: ").append(rsmd.getColumnName(col))
				.append(" ColumnTypeName: ").append(rsmd.getColumnTypeName(col))
				.append(" Precision: ").append(rsmd.getPrecision(col))
				.append(" Scale: ").append(rsmd.getScale(col))
				.append(" ColumnDisplaySize: ").append(rsmd.getColumnDisplaySize(col))
				.append(" ColumnType: ").append(rsmd.getColumnType(col))
				.append(" ColumnClassName: ").append(rsmd.getColumnClassName(col))
				.append(" isNullable: ").append(rsmd.isNullable(col))
				.append(" isAutoIncrement: ").append(rsmd.isAutoIncrement(col))
				.append("\n");
			}
			rsk.close();
			rsk = null;
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rsk);

		compareExpectedOutput("Test_SgeneratedKeys",
				"rsmd has 1 columns\n" +
				"ColumnName: GENERATED_KEY ColumnTypeName: bigint Precision: 19 Scale: 0 ColumnDisplaySize: 20 ColumnType: -5 ColumnClassName: java.lang.Long isNullable: 2 isAutoIncrement: false\n");
	}

	private void Test_Smoreresults() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				sb.append("more results on an uninitialized Statement, how can that be?\n");
			sb.append(" nope :)\n");

			sb.append("2. SELECT 1...");
			if (stmt.execute("SELECT 1;") == false)
				sb.append("SELECT 1 returns update or no results\n");
			sb.append(" ResultSet :)\n");

			sb.append("3. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				sb.append("more results after SELECT 1 query, how can that be?\n");
			sb.append(" nope :)\n");

			sb.append("4. even more results?...");
			if (stmt.getMoreResults() != false)
				sb.append("still more results after SELECT 1 query, how can that be?\n");
			sb.append(" nope :)\n");

		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Smoreresults",
				"0. true	true\n" +
				"1. more results?... nope :)\n" +
				"2. SELECT 1... ResultSet :)\n" +
				"3. more results?... nope :)\n" +
				"4. even more results?... nope :)\n");
	}

	private void Test_Wrapper() {
		sb.setLength(0);	// clear the output log buffer

		try {
			final String jdbc_pkg = "java.sql.";
			final String monetdb_jdbc_pkg = "org.monetdb.jdbc.";

			sb.append("Auto commit is: ").append(con.getAutoCommit()).append("\n");

			checkIsWrapperFor("Connection", con, jdbc_pkg, "Connection");
			checkIsWrapperFor("Connection", con, monetdb_jdbc_pkg, "MonetConnection");
			checkIsWrapperFor("Connection", con, jdbc_pkg, "Statement");
			checkIsWrapperFor("Connection", con, monetdb_jdbc_pkg, "MonetStatement");

			DatabaseMetaData dbmd = con.getMetaData();
			checkIsWrapperFor("DatabaseMetaData", dbmd, jdbc_pkg, "DatabaseMetaData");
			checkIsWrapperFor("DatabaseMetaData", dbmd, monetdb_jdbc_pkg, "MonetDatabaseMetaData");
			checkIsWrapperFor("DatabaseMetaData", dbmd, jdbc_pkg, "Statement");
			checkIsWrapperFor("DatabaseMetaData", dbmd, monetdb_jdbc_pkg, "MonetStatement");

			ResultSet rs = dbmd.getSchemas();
			checkIsWrapperFor("ResultSet", rs, jdbc_pkg, "ResultSet");
			checkIsWrapperFor("ResultSet", rs, monetdb_jdbc_pkg, "MonetResultSet");
			checkIsWrapperFor("ResultSet", rs, jdbc_pkg, "Statement");
			checkIsWrapperFor("ResultSet", rs, monetdb_jdbc_pkg, "MonetStatement");

			ResultSetMetaData rsmd = rs.getMetaData();
			checkIsWrapperFor("ResultSetMetaData", rsmd, jdbc_pkg, "ResultSetMetaData");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetResultSet");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetResultSetMetaData");
			checkIsWrapperFor("ResultSetMetaData", rsmd, jdbc_pkg, "Statement");
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetStatement");

			rs.close();

			Statement stmt = con.createStatement();
			checkIsWrapperFor("Statement", stmt, jdbc_pkg, "Statement");
			checkIsWrapperFor("Statement", stmt, monetdb_jdbc_pkg, "MonetStatement");
			checkIsWrapperFor("Statement", stmt, jdbc_pkg, "Connection");
			checkIsWrapperFor("Statement", stmt, monetdb_jdbc_pkg, "MonetConnection");

			stmt.close();

			PreparedStatement pstmt = con.prepareStatement("SELECT name FROM sys.tables WHERE system AND name like ?");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "PreparedStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "Statement");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetStatement");
			checkIsWrapperFor("PreparedStatement", pstmt, jdbc_pkg, "Connection");
			checkIsWrapperFor("PreparedStatement", pstmt, monetdb_jdbc_pkg, "MonetConnection");

			ParameterMetaData pmd = pstmt.getParameterMetaData();
			checkIsWrapperFor("ParameterMetaData", pmd, jdbc_pkg, "ParameterMetaData");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetParameterMetaData");
			checkIsWrapperFor("ParameterMetaData", pmd, jdbc_pkg, "Connection");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetConnection");

			ResultSetMetaData psrsmd = pstmt.getMetaData();
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, jdbc_pkg, "ResultSetMetaData");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetResultSetMetaData");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, jdbc_pkg, "Connection");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetConnection");

			pstmt.close();

		} catch (SQLException e) {
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_Wrapper", "Auto commit is: true\n" +
				"Connection. isWrapperFor(Connection) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"Connection. isWrapperFor(MonetConnection) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"Connection. isWrapperFor(Statement) returns: false\n" +
				"Connection. isWrapperFor(MonetStatement) returns: false\n" +
				"DatabaseMetaData. isWrapperFor(DatabaseMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"DatabaseMetaData. isWrapperFor(MonetDatabaseMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"DatabaseMetaData. isWrapperFor(Statement) returns: false\n" +
				"DatabaseMetaData. isWrapperFor(MonetStatement) returns: false\n" +
				"ResultSet. isWrapperFor(ResultSet) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ResultSet. isWrapperFor(MonetResultSet) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ResultSet. isWrapperFor(Statement) returns: false\n" +
				"ResultSet. isWrapperFor(MonetStatement) returns: false\n" +
				"ResultSetMetaData. isWrapperFor(ResultSetMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ResultSetMetaData. isWrapperFor(MonetResultSet) returns: false\n" +
				"ResultSetMetaData. isWrapperFor(MonetResultSetMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ResultSetMetaData. isWrapperFor(Statement) returns: false\n" +
				"ResultSetMetaData. isWrapperFor(MonetStatement) returns: false\n" +
				"Statement. isWrapperFor(Statement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"Statement. isWrapperFor(MonetStatement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"Statement. isWrapperFor(Connection) returns: false\n" +
				"Statement. isWrapperFor(MonetConnection) returns: false\n" +
				"PreparedStatement. isWrapperFor(PreparedStatement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PreparedStatement. isWrapperFor(MonetPreparedStatement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PreparedStatement. isWrapperFor(Statement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PreparedStatement. isWrapperFor(MonetStatement) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PreparedStatement. isWrapperFor(Connection) returns: false\n" +
				"PreparedStatement. isWrapperFor(MonetConnection) returns: false\n" +
				"ParameterMetaData. isWrapperFor(ParameterMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ParameterMetaData. isWrapperFor(MonetPreparedStatement) returns: false\n" +
				"ParameterMetaData. isWrapperFor(MonetParameterMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ParameterMetaData. isWrapperFor(Connection) returns: false\n" +
				"ParameterMetaData. isWrapperFor(MonetConnection) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(ResultSetMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetPreparedStatement) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetResultSetMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(Connection) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetConnection) returns: false\n");
	}

	private void checkIsWrapperFor(String objnm, Wrapper obj, String pkgnm, String classnm) {
		try {
			Class<?> clazz = Class.forName(pkgnm + classnm);
			boolean isWrapper = obj.isWrapperFor(clazz);
			sb.append(objnm).append(". isWrapperFor(").append(classnm).append(") returns: ").append(isWrapper);
			if (isWrapper) {
				Object wobj = obj.unwrap(clazz);
				sb.append("\tCalled unwrap(). Returned object is ").append((wobj != null ? "not null, so oke" : "null !!"));
			}
			sb.append("\n");
		} catch (ClassNotFoundException cnfe) {
			sb.append(cnfe.toString());
		} catch (SQLException se) {
			sb.append(se.getMessage());
		}
	}

	private void bogus_auto_generated_keys() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		int upd = 0;
		try {
			stmt = con.createStatement();
			sb.append("1. create table...");
			// create a simple table with an auto-generated key (id)
			upd = stmt.executeUpdate("CREATE TABLE bogus_gen_keys (\n	\"id\" serial,\n	\"x\" varchar(12)\n);");
			if (upd != 0)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");

			// perform an update, useless, but illustrates the bug, this time no
			// generated key is reported, which is correct
			sb.append("2. update empty table...");
			upd = stmt.executeUpdate("UPDATE bogus_gen_keys SET \"x\" = 'bla' WHERE \"id\" = 12;");
			if (upd != 0)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");

			// insert some value, should get a generated key
			sb.append("3. insert 1 row ...");
			upd = stmt.executeUpdate("INSERT INTO bogus_gen_keys (\"x\") VALUES ('boe');");
			if (upd != 1)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");

			sb.append("4. show values of inserted row ...");
			rs = stmt.executeQuery("SELECT \"id\", \"x\" from bogus_gen_keys;");
			if (rs != null && rs.next()) {
				sb.append(" id: ").append(rs.getString(1)).append("  x: ").append(rs.getString(2));
			}
			sb.append("\n");

			// update again, we expect NO generated key, but we DO get one
			sb.append("5. update row 1...");
			upd = stmt.executeUpdate("UPDATE bogus_gen_keys SET \"x\" = 'bla' WHERE \"id\" = 1;");
			if (upd != 1)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");

			sb.append("6. update row 12...");
			upd = stmt.executeUpdate("UPDATE bogus_gen_keys SET \"x\" = 'bla' WHERE \"id\" = 12;");
			if (upd != 0)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			sb.append("7. drop table...");
			upd = stmt.executeUpdate("DROP TABLE bogus_gen_keys");
			if (upd != 0)
				sb.append("Wrong return status: ").append(upd).append("\n");
			else
				sb.append("passed\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("bogus_auto_generated_keys",
				"1. create table...passed\n" +
				"2. update empty table...passed\n" +
				"3. insert 1 row ...passed\n" +
				"4. show values of inserted row ... id: 1  x: boe\n" +
				"5. update row 1...passed\n" +
				"6. update row 12...passed\n" +
				"7. drop table...passed\n");
	}

	private void BugConcurrent_clients_SF_1504657(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		Connection con1 = null, con2 = null, con3 = null;
		Statement stmt1 = null, stmt2 = null, stmt3 = null;
		ResultSet rs1 = null, rs2= null, rs3 = null;
		try {
			con1 = DriverManager.getConnection(arg0);
			con2 = DriverManager.getConnection(arg0);
			con3 = DriverManager.getConnection(arg0);
			stmt1 = con1.createStatement();
			stmt2 = con2.createStatement();
			stmt3 = con3.createStatement();

			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con1.getAutoCommit()).append("\n");
			sb.append("0. true\t").append(con2.getAutoCommit()).append("\n");
			sb.append("0. true\t").append(con3.getAutoCommit()).append("\n");

			// test the creation of a table with concurrent clients
			sb.append("1.1. create table t1504657 using client 1...\n");
			stmt1.executeUpdate("CREATE TABLE t1504657 ( id int, name varchar(1024) )");
			sb.append("passed :)\n");

			sb.append("1.2. check table existence in client 2...\n");
			rs2 = stmt2.executeQuery("SELECT name FROM tables where name LIKE 't1504657'");
			while (rs2.next())
				sb.append(rs2.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("1.3. check table existence in client 3...\n");
			rs3 = stmt3.executeQuery("SELECT name FROM tables where name LIKE 't1504657'");
			while (rs3.next())
				sb.append(rs3.getString("name")).append("\n");
			sb.append("passed :)\n");

			// test the insertion of values with concurrent clients
			sb.append("2 insert into t1504657 using client 1...\n");
			stmt1.executeUpdate("INSERT INTO t1504657 values( 1, 'monetdb' )");
			sb.append("passed :)\n");
			stmt1.executeUpdate("INSERT INTO t1504657 values( 2, 'monet' )");
			sb.append("passed :)\n");
			stmt1.executeUpdate("INSERT INTO t1504657 values( 3, 'mon' )");
			sb.append("passed :)\n");

			sb.append("2.1. check table status with client 1...\n");
			rs1 = stmt1.executeQuery("SELECT * FROM t1504657");
			while (rs1.next())
				sb.append(rs1.getInt("id")).append(", ").append(rs1.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("2.2. check table status with client 2...\n");
			rs2 = stmt2.executeQuery("SELECT * FROM t1504657");
			while (rs2.next())
				sb.append(rs2.getInt("id")).append(", ").append(rs2.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("2.3. check table status with client 3...\n");
			rs3 = stmt3.executeQuery("SELECT * FROM t1504657");
			while (rs3.next())
				sb.append(rs3.getInt("id")).append(", ").append(rs3.getString("name")).append("\n");
			sb.append("passed :)\n");

			// test the insertion of values with concurrent clients
			sb.append("3 insert into t1504657 using client 2...\n");
			stmt2.executeUpdate("INSERT INTO t1504657 values( 4, 'monetdb' )");
			sb.append("passed :)\n");
			stmt2.executeUpdate("INSERT INTO t1504657 values( 5, 'monet' )");
			sb.append("passed :)\n");
			stmt2.executeUpdate("INSERT INTO t1504657 values( 6, 'mon' )");
			sb.append("passed :)\n");

			sb.append("3.1. check table status with client 1...\n");
			rs1 = stmt1.executeQuery("SELECT * FROM t1504657");
			while (rs1.next())
				sb.append(rs1.getInt("id")).append(", ").append(rs1.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("3.2. check table status with client 2...\n");
			rs2 = stmt2.executeQuery("SELECT * FROM t1504657");
			while (rs2.next())
				sb.append(rs2.getInt("id")).append(", ").append(rs2.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("3.3. check table status with client 3...\n");
			rs3 = stmt3.executeQuery("SELECT * FROM t1504657");
			while (rs3.next())
				sb.append(rs3.getInt("id")).append(", ").append(rs3.getString("name")).append("\n");
			sb.append("passed :)\n");

			// test the insertion of values with concurrent clients
			sb.append("4 insert into t1504657 using client 3...\n");
			stmt3.executeUpdate("INSERT INTO t1504657 values( 7, 'monetdb' )");
			sb.append("passed :)\n");
			stmt3.executeUpdate("INSERT INTO t1504657 values( 8, 'monet' )");
			sb.append("passed :)\n");
			stmt3.executeUpdate("INSERT INTO t1504657 values( 9, 'mon' )");
			sb.append("passed :)\n");

			sb.append("4.1. check table status with client 1...\n");
			rs1 = stmt1.executeQuery("SELECT * FROM t1504657");
			while (rs1.next())
				sb.append(rs1.getInt("id")).append(", ").append(rs1.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("4.2. check table status with client 2...\n");
			rs2 = stmt2.executeQuery("SELECT * FROM t1504657");
			while (rs2.next())
				sb.append(rs2.getInt("id")).append(", ").append(rs2.getString("name")).append("\n");
			sb.append("passed :)\n");

			sb.append("4.3. check table status with client 3...\n");
			rs3 = stmt3.executeQuery("SELECT * FROM t1504657");
			while (rs3.next())
				sb.append(rs3.getInt("id")).append(", ").append(rs3.getString("name")).append("\n");
			sb.append("passed :)\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			sb.append("Cleanup TABLE t1504657\n");
			stmt3.executeUpdate("DROP TABLE t1504657");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt3, rs3);
		closeStmtResSet(stmt1, rs1);
		closeStmtResSet(stmt2, rs2);

		closeConx(con2);
		closeConx(con1);
		closeConx(con3);

		compareExpectedOutput("BugConcurrent_clients_SF_1504657",
				"0. true	true\n" +
				"0. true	true\n" +
				"0. true	true\n" +
				"1.1. create table t1504657 using client 1...\n" +
				"passed :)\n" +
				"1.2. check table existence in client 2...\n" +
				"t1504657\n" +
				"passed :)\n" +
				"1.3. check table existence in client 3...\n" +
				"t1504657\n" +
				"passed :)\n" +
				"2 insert into t1504657 using client 1...\n" +
				"passed :)\n" +
				"passed :)\n" +
				"passed :)\n" +
				"2.1. check table status with client 1...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"passed :)\n" +
				"2.2. check table status with client 2...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"passed :)\n" +
				"2.3. check table status with client 3...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"passed :)\n" +
				"3 insert into t1504657 using client 2...\n" +
				"passed :)\n" +
				"passed :)\n" +
				"passed :)\n" +
				"3.1. check table status with client 1...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"passed :)\n" +
				"3.2. check table status with client 2...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"passed :)\n" +
				"3.3. check table status with client 3...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"passed :)\n" +
				"4 insert into t1504657 using client 3...\n" +
				"passed :)\n" +
				"passed :)\n" +
				"passed :)\n" +
				"4.1. check table status with client 1...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"7, monetdb\n" +
				"8, monet\n" +
				"9, mon\n" +
				"passed :)\n" +
				"4.2. check table status with client 2...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"7, monetdb\n" +
				"8, monet\n" +
				"9, mon\n" +
				"passed :)\n" +
				"4.3. check table status with client 3...\n" +
				"1, monetdb\n" +
				"2, monet\n" +
				"3, mon\n" +
				"4, monetdb\n" +
				"5, monet\n" +
				"6, mon\n" +
				"7, monetdb\n" +
				"8, monet\n" +
				"9, mon\n" +
				"passed :)\n" +
				"Cleanup TABLE t1504657\n");
	}

	private void BugConcurrent_sequences(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		Connection con1 = null, con2 = null;
		Statement stmt1 = null, stmt2 = null;
		ResultSet rs1 = null, rs2 = null;
		try {
			con1 = DriverManager.getConnection(arg0);
			con2 = DriverManager.getConnection(arg0);
			stmt1 = con1.createStatement();
			stmt2 = con2.createStatement();

			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con1.getAutoCommit()).append("\n");
			sb.append("0. true\t").append(con2.getAutoCommit()).append("\n");

			// create a table
			sb.append("1. create table tconc_seq using client 1... ");
			stmt1.executeUpdate("CREATE TABLE tconc_seq ( id serial, who varchar(12) )");
			sb.append("passed :)\n");

			// test the insertion of values with concurrent clients
			sb.append("2. insert into tconc_seq using client 1 and 2... ");
			stmt1.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client1')");
			sb.append("client 1 passed :)\n");

			con2.setAutoCommit(false);
			stmt2.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client2')");
			sb.append("transaction on client 2 :)\n");

			stmt1.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client1')");
			sb.append("client 1 passed :)\n");

			try {
				con2.commit();
				sb.append("transaction client 2 passed :)\n");
			} catch (SQLException e) {
				sb.append("transaction client 2 failed!\n");
			}
			con2.setAutoCommit(true);
			stmt2.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client2')");
			sb.append("passed :)\n");

			sb.append("2.1. check table status with client 1...\n");
			rs1 = stmt1.executeQuery("SELECT * FROM tconc_seq ORDER BY id");
			while (rs1.next())
				sb.append(rs1.getInt("id")).append(", ").append(rs1.getString("who")).append("\n");
			sb.append("passed :)\n");

			sb.append("2.2. check table status with client 2...\n");
			rs2 = stmt2.executeQuery("SELECT * FROM tconc_seq ORDER BY id");
			while (rs2.next())
				sb.append(rs2.getInt("id")).append(", ").append(rs2.getString("who")).append("\n");
			sb.append("passed :)\n");

			// drop the table (not dropping the sequence) from client 1
			sb.append("3.1. drop table tconc_seq using client 1... ");
			stmt1.executeUpdate("DROP TABLE tconc_seq");
			sb.append("passed :)\n");

			sb.append("3.1. recreate tconc_seq using client 1... ");
			stmt1.executeUpdate("CREATE TABLE tconc_seq ( id serial, who varchar(12) )");
			sb.append("passed :)\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("BugConcurrent_sequences",
				"0. true	true\n" +
				"0. true	true\n" +
				"1. create table tconc_seq using client 1... passed :)\n" +
				"2. insert into tconc_seq using client 1 and 2... client 1 passed :)\n" +
				"transaction on client 2 :)\n" +
				"client 1 passed :)\n" +
				"transaction client 2 failed!\n" +
				"passed :)\n" +
				"2.1. check table status with client 1...\n" +
				"1, client1\n" +
				"3, client1\n" +
				"4, client2\n" +
				"passed :)\n" +
				"2.2. check table status with client 2...\n" +
				"1, client1\n" +
				"3, client1\n" +
				"4, client2\n" +
				"passed :)\n" +
				"3.1. drop table tconc_seq using client 1... passed :)\n" +
				"3.1. recreate tconc_seq using client 1... passed :)\n");
		sb.setLength(0);	// clear the output log buffer

		try {
			// re-establish connection
			sb.append("x. Reconnecting client 1 and 2... ");
			con1.close();
			con2.close();
			con1 = DriverManager.getConnection(arg0);
			con2 = DriverManager.getConnection(arg0);
			stmt1 = con1.createStatement();
			stmt2 = con2.createStatement();
			sb.append("passed :)\n");

			// insert and print, should get 1,2
			sb.append("4. insert into tconc_seq using client 1 and 2...\n");
			stmt1.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client1')");
			sb.append("passed :)\n");
			con2.setAutoCommit(false);
			stmt2.executeUpdate("INSERT INTO tconc_seq(who) VALUES('client2')");
			con2.commit();
			con2.setAutoCommit(true);
			sb.append("passed :)\n");

			sb.append("4.1. check table status with client 1...\n");
			rs1 = stmt1.executeQuery("SELECT * FROM tconc_seq ORDER BY who");
			for (int cntr = 1; rs1.next(); cntr++) {
				int id = rs1.getInt("id");
				sb.append(id).append(", ").append(rs1.getString("who")).append("\n");
				if (id != cntr)
					sb.append("!! expected ").append(cntr).append(", got ").append(id);
			}
			sb.append("passed :)\n");

			sb.append("4.2. check table status with client 2...\n");
			rs2 = stmt2.executeQuery("SELECT * FROM tconc_seq ORDER BY who");
			for (int cntr = 1; rs2.next(); cntr++) {
				int id = rs2.getInt("id");
				sb.append(id).append(", ").append(rs2.getString("who")).append("\n");
				if (id != cntr)
					sb.append("!! expected ").append(cntr).append(", got ").append(id);
			}
			sb.append("passed :)\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			sb.append("Cleanup TABLE tconc_seq\n");
			stmt2.executeUpdate("DROP TABLE tconc_seq");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt1, rs1);
		closeStmtResSet(stmt2, rs2);

		closeConx(con2);
		closeConx(con1);

		compareExpectedOutput("BugConcurrent_sequences",
				"x. Reconnecting client 1 and 2... passed :)\n" +
				"4. insert into tconc_seq using client 1 and 2...\n" +
				"passed :)\n" +
				"passed :)\n" +
				"4.1. check table status with client 1...\n" +
				"1, client1\n" +
				"2, client2\n" +
				"passed :)\n" +
				"4.2. check table status with client 2...\n" +
				"1, client1\n" +
				"2, client2\n" +
				"passed :)\n" +
				"Cleanup TABLE tconc_seq\n");
	}

	private void Bug_Connect_as_voc_getMetaData_Failure_Bug_6388(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt1 = null;
		// create user, schema and alter schema default schema
		try {
			sb.append("1. CREATE USER voc\n");
			stmt1 = con.createStatement();
			stmt1.executeUpdate("CREATE USER \"voc\" WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA \"sys\"");
			sb.append("2. CREATE SCHEMA voc\n");
			stmt1.executeUpdate("CREATE SCHEMA \"voc\" AUTHORIZATION \"voc\"");
			sb.append("3. ALTER USER voc\n");
			stmt1.executeUpdate("ALTER USER \"voc\" SET SCHEMA \"voc\"");
			sb.append("creation succeeded :)\n");
		} catch (SQLException e) {
			sb.append("FAILED creating user and schema voc. ").append(e.getMessage()).append("\n");
		}

		Connection con2 = null;
		ResultSet rs2 = null;
		try {
			sb.append("4.1. connect as user: voc\n");
			con2 = DriverManager.getConnection(arg0.replace("=monetdb", "=voc"));
			sb.append("connected :)\n");

			DatabaseMetaData dbmd = con2.getMetaData();

			sb.append("4.2. getUserName()\n");
			sb.append("UserName = ").append(dbmd.getUserName()).append("\n");

			sb.append("4.3. getMaxConnections()\n");
			sb.append("MaxConnections = ").append(dbmd.getMaxConnections()).append("\n");

			sb.append("4.4. getDatabaseProductVersion()\n");
			String dbmsVersion = dbmd.getDatabaseProductVersion();	// should be 11.35.1 or higher
			boolean postNov2019 = ("11.35.1".compareTo(dbmsVersion) <= 0);
			sb.append("DatabaseProductVersion = ").append((postNov2019 ? "11.35.+" : dbmsVersion)).append("\n");

			sb.append("4.5. getDatabaseMajorVersion()\n");
			sb.append("DatabaseMajorVersion = ").append(dbmd.getDatabaseMajorVersion()).append("\n");	// should be 11

			sb.append("4.6. getDatabaseMinorVersion()\n");
			int dbmsMinorVersion = dbmd.getDatabaseMinorVersion();	// should be 35 or higher
			sb.append("DatabaseMinorVersion = ").append((dbmsMinorVersion >= 35 ? "35+" : dbmsMinorVersion)).append("\n");

			sb.append("4.7. getTables(null, 'tmp', null, null)\n");
			rs2 = dbmd.getTables(null, "tmp", null, null);
			if (rs2 != null) {
				sb.append("List Tables in schema tmp:\n");
				while (rs2.next()) {
					sb.append(rs2.getString(3)).append("\n");
				}
				rs2.close();
			}
			sb.append("completed listing Tables in schema tmp\n");

			sb.append("4.8. getTableTypes()\n");
			rs2 = dbmd.getTableTypes();
			if (rs2 != null) {
				sb.append("List TableTypes:\n");
				while (rs2.next()) {
					String tt = rs2.getString(1);
					// the STREAM TABLE type is REMOVED in post Oct2020 releases, so filter it out for a stable output on all releases
					// the UNLOGGED TABLE type is ADDED in post Jan2022 releases, so filter it out for a stable output on all releases
					// the LOCAL TEMPORARY VIEW type is ADDED in 11.53.4 (Mar2025-SP1) releases, filter it out for a stable output on all releases
					if (! ("STREAM TABLE".equals(tt) || "UNLOGGED TABLE".equals(tt) || "LOCAL TEMPORARY VIEW".equals(tt)) )
						sb.append(tt).append("\n");
				}
				rs2.close();
			}
			sb.append("completed listing TableTypes\n");

			sb.append("voc meta data Test completed successfully\n");
		} catch (SQLException e) {
			sb.append("FAILED fetching MonetDatabaseMetaData. ").append(e.getMessage()).append("\n");
		} finally {
			try {
				con2.close();
			} catch (SQLException e) {
				sb.append("FAILED to close voc connection. ").append(e.getMessage()).append("\n");
			}
		}

		// cleanup: drop user, schema and alter schema default schema in reverse order
		try {
			sb.append("Cleanup created objects\n");
			sb.append("5. ALTER USER voc\n");
			stmt1.executeUpdate("ALTER USER \"voc\" SET SCHEMA \"sys\"");
			sb.append("6. DROP SCHEMA voc\n");
			stmt1.executeUpdate("DROP SCHEMA \"voc\"");
			sb.append("7. DROP USER voc\n");
			stmt1.executeUpdate("DROP USER \"voc\"");
			sb.append("cleanup succeeded :)\n");
		} catch (SQLException e) {
			sb.append("FAILED dropping user and schema voc. ").append(e.getMessage()).append("\n");
		}

		closeConx(con2);
		closeStmtResSet(stmt1, null);

		compareExpectedOutput("Bug_Connect_as_voc_getMetaData_Failure_Bug_6388",
				"1. CREATE USER voc\n" +
				"2. CREATE SCHEMA voc\n" +
				"3. ALTER USER voc\n" +
				"creation succeeded :)\n" +
				"4.1. connect as user: voc\n" +
				"connected :)\n" +
				"4.2. getUserName()\n" +
				"UserName = voc\n" +
				"4.3. getMaxConnections()\n" +
				"MaxConnections = 64\n" +
				"4.4. getDatabaseProductVersion()\n" +
				"DatabaseProductVersion = 11.35.+\n" +
				"4.5. getDatabaseMajorVersion()\n" +
				"DatabaseMajorVersion = 11\n" +
				"4.6. getDatabaseMinorVersion()\n" +
				"DatabaseMinorVersion = 35+\n" +
				"4.7. getTables(null, 'tmp', null, null)\n" +
				"List Tables in schema tmp:\n" +
				"_columns\n" +
				"_tables\n" +
				"idxs\n" +
				"keys\n" +
				"objects\n" +
				"triggers\n" +
				"completed listing Tables in schema tmp\n" +
				"4.8. getTableTypes()\n" +
				"List TableTypes:\n" +
				"GLOBAL TEMPORARY TABLE\n" +
				"LOCAL TEMPORARY TABLE\n" +
				"MERGE TABLE\n" +
				"REMOTE TABLE\n" +
				"REPLICA TABLE\n" +
				"SYSTEM TABLE\n" +
				"SYSTEM VIEW\n" +
				"TABLE\n" +
				"VIEW\n" +
				"completed listing TableTypes\n" +
				"voc meta data Test completed successfully\n" +
				"Cleanup created objects\n" +
				"5. ALTER USER voc\n" +
				"6. DROP SCHEMA voc\n" +
				"7. DROP USER voc\n" +
				"cleanup succeeded :)\n");
	}

	private void BugDatabaseMetaData_Bug_3356() {
		sb.setLength(0);	// clear the output log buffer

		ResultSet rs = null;
		try {
			DatabaseMetaData dbmd = con.getMetaData();
			rs = dbmd.getColumns("", "sys", "_tables", "id");
			rs.next();
			String tableName1 = rs.getString("TABLE_NAME");
			String tableName2 = rs.getString(3);
			String isNullable1 = rs.getString("IS_NULLABLE");
			String isNullable2 = rs.getString(18);
			sb.append(tableName1).append("\n");
			sb.append(tableName2).append("\n");
			sb.append(isNullable1).append("\n");
			sb.append(isNullable2).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(null, rs);

		compareExpectedOutput("BugDatabaseMetaData_Bug_3356",
				"_tables\n" +
				"_tables\n" +
				"YES\n" +
				"YES\n");
	}

	private void BugDecimalRound_Bug_3561() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt1 = null;
		PreparedStatement pst = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		try {
			stmt1 = con.createStatement();
			stmt1.executeUpdate("CREATE TABLE bug3561 (d decimal(7,4))");

			pst = con.prepareStatement("INSERT INTO bug3561 VALUES (?)");
			pst.setBigDecimal(1, new BigDecimal("112.125"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal("212.12345"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal("0.012345"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(0.0/10000000));	// 0.0000
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(2.0/3));	// 0.666666667
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(11.0/7));	// 1.571428571
			pst.executeUpdate();
			// repeat for negative values
			pst.setBigDecimal(1, new BigDecimal("-0112.125"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal("-0212.12345"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal("-0.012345"));
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(0.0/-10000000));	// 0.0000
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(-2.0/3));	// -0.666666667
			pst.executeUpdate();
			pst.setBigDecimal(1, new BigDecimal(-11.0/7));	// -1.571428571
			pst.executeUpdate();
			// check what happens if null is used
			pst.setBigDecimal(1, null);
			pst.executeUpdate();

			pst.close();

			stmt2 = con.createStatement();
			rs = stmt2.executeQuery("SELECT d FROM bug3561");
			while (rs.next())
				sb.append(rs.getString(1)).append("\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt2, rs);
		closeStmtResSet(pst, null);

		// cleanup
		try {
			stmt1.executeUpdate("DROP TABLE bug3561");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt1, null);

		compareExpectedOutput("BugDecimalRound_Bug_3561",
				"112.1250\n" +
				"212.1235\n" +
				"0.0123\n" +
				"0.0000\n" +
				"0.6667\n" +
				"1.5714\n" +
				"-112.1250\n" +
				"-212.1235\n" +
				"-0.0123\n" +
				"0.0000\n" +
				"-0.6667\n" +
				"-1.5714\n" +
				"null\n");
	}

	private void BugExecuteUpdate_Bug_3350() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			con.setAutoCommit(false);	// disable auto commit, so we can roll back the transaction
			stmt = con.createStatement();
			stmt.execute("CREATE TABLE t3350 (keyword VARCHAR(30) PRIMARY KEY)");
			con.commit();

			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // should insert 1 row
			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // this will result in an SQLException due to PK uniqueness violation
			con.rollback();

			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // should insert 1 row
			executeDML(stmt, "INSERT INTO t3350 VALUES ('1'), ('x'), ('3'), ('y')"); // should insert 4 rows
			executeDML(stmt, "DELETE FROM t3350 WHERE \"keyword\" = 'Bug_3350'"); // should delete 1 row
			executeDML(stmt, "DELETE FROM t3350 WHERE \"keyword\" = 'Bug_3350'"); // should delete 0 rows
			executeDML(stmt, "UPDATE t3350 set \"keyword\" = keyword||'_ext'"); // should update 4 rows
			executeDML(stmt, "DELETE FROM t3350"); // should delete 4 rows
			con.commit();
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		// cleanup
		try {
			stmt.execute("DROP TABLE IF EXISTS t3350");
			con.commit();
			con.setAutoCommit(true);	// enable auto commit
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("BugExecuteUpdate_Bug_3350",
				"executeUpdate(INSERT ...) returned: 1\n" +
				"getUpdateCount() returned: 1\n" +
				"INSERT INTO: PRIMARY KEY constraint 't3350.t3350_keyword_pkey' violated\n" +
				"getUpdateCount() returned: 1\n" +
				"executeUpdate(INSERT ...) returned: 1\n" +
				"getUpdateCount() returned: 1\n" +
				"executeUpdate(INSERT ...) returned: 4\n" +
				"getUpdateCount() returned: 4\n" +
				"executeUpdate(DELETE ...) returned: 1\n" +
				"getUpdateCount() returned: 1\n" +
				"executeUpdate(DELETE ...) returned: 0\n" +
				"getUpdateCount() returned: 0\n" +
				"executeUpdate(UPDATE ...) returned: 4\n" +
				"getUpdateCount() returned: 4\n" +
				"executeUpdate(DELETE ...) returned: 4\n" +
				"getUpdateCount() returned: 4\n");
	}

	private void executeDML(Statement st, String sql) {
		try {
			int upd_count = st.executeUpdate(sql);
			sb.append("executeUpdate(").append(sql.substring(0, 6)).append(" ...) returned: ").append(upd_count).append("\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}

		try {
			sb.append("getUpdateCount() returned: ").append(st.getUpdateCount()).append("\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}
	}

	private void Bug_IsValid_Timeout_Bug_6782(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		Connection con2 = null;
		Statement st = null;
		try {
			con2 = DriverManager.getConnection(arg0);
			sb.append("connected :)\n");

			st = con2.createStatement();
			st.setQueryTimeout(5);
			sb.append("getQueryTimeout must give 5: ").append(st.getQueryTimeout()).append("\n");
			st.close();

			con.isValid(6);

			st = con.createStatement();
			sb.append("getQueryTimeout must give 0: ").append(st.getQueryTimeout()).append("\n");

			con.isValid(4);
			sb.append("getQueryTimeout must give 0: ").append(st.getQueryTimeout()).append("\n");
			st.close();

			st.setQueryTimeout(7);
			con.isValid(3);
			sb.append("getQueryTimeout must give 7: ").append(st.getQueryTimeout()).append("\n");
			st.close();
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}
		closeStmtResSet(st, null);
		closeConx(con2);

		compareExpectedOutput("Bug_IsValid_Timeout_Bug_6782",
				"connected :)\n" +
				"getQueryTimeout must give 5: 5\n" +
				"getQueryTimeout must give 0: 0\n" +
				"getQueryTimeout must give 0: 0\n" +
				"getQueryTimeout must give 7: 7\n");
	}

	private void Bug_LargeQueries_6571_6693(String arg0) {
		sb.setLength(0);	// clear the output log buffer

		// construct a largedata string value. It must larger than the block size of MapiSocket
		final int num = 9216;
		final String repeatValue = "$-)";
		final StringBuilder ldsb = new StringBuilder(num * repeatValue.length());
		for (int i = 0; i < num; i++)
			ldsb.append(repeatValue);
		final String largedata = ldsb.toString();
		if (largedata.length() <= 8192)
			sb.append("Length (").append(largedata.length()).append(") of largedata value is too small! Should be larger than 8192!");

		final String tbl_nm = "tbl6693";
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			// create a test table.
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tbl_nm + " (attribute CLOB, value CLOB);");
			sb.append("Created table: ").append(tbl_nm).append("\n");
			sb.append("Inserting rows. ");
			String insertCmd = "INSERT INTO " + tbl_nm + " VALUES ('activeset_default_fiets', '" + largedata + "');";
			int ins = stmt.executeUpdate(insertCmd);
			ins += stmt.executeUpdate(insertCmd);
			ins += stmt.executeUpdate(insertCmd);
			sb.append(ins).append(" rows inserted\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		final int script_iterations = 10;
		try {
			run_tests(arg0, tbl_nm, script_iterations, largedata);
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}

		try (Statement stmt2 = con.createStatement()) {
			stmt2.executeUpdate("DROP TABLE IF EXISTS " + tbl_nm);
			sb.append("Cleaned up TABLE ").append(tbl_nm).append("\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}

		sb.append("Test completed without hanging\n");

		compareExpectedOutput("Bug_LargeQueries_6571_6693",
				"Created table: tbl6693\n" +
				"Inserting rows. 3 rows inserted\n" +
				"Script size is 83256\n" +
				"First test repeat 10 times. Iteration: 1 2 3 4 5 6 7 8 9 10 \n" +
				"Completed first test\n" +
				"Second test repeat 10 times. Iteration: 1 2 3 4 5 6 7 8 9 10 \n" +
				"Completed second test\n" +
				"Script size is 3012\n" +
				"Third test repeat 9 times.\n" +
				"Iteration: 1 2 3 4 5 6 7 8 9 \n" +
				"Completed third test\n" +
				"Cleaned up TABLE tbl6693\n" +
				"Test completed without hanging\n");
	}

	private void run_tests(String conURL, String tbl_nm, int iterations, String largedata) throws SQLException {
		String script =
			"delete from " + tbl_nm + " where attribute='activeset_default_fiets';\n" +
			"insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n" +
			"insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n" +
			"insert into " + tbl_nm + " values ('activeset_default_fiets', '" + largedata + "');\n" +
			"select value from " + tbl_nm + " where attribute='activeset_default_fiets';\n";
		sb.append("Script size is " + script.length()).append("\n");

		// first try to make the execution hang after many iterations of sending large data queries within one connection
		sb.append("First test repeat " + iterations + " times. ");
		try (Connection con = DriverManager.getConnection(conURL)) {
			sb.append("Iteration: ");
			for (int i = 1; i <= iterations; i++) {
				sb.append(i).append(" ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, 1, 3, 6);
				}
			}
			sb.append("\n");
		}
		sb.append("Completed first test\n");

		// also try to make the execution hang after many iterations of making connections (each their own socket) and sending large scripts
		sb.append("Second test repeat " + iterations + " times. ");
		sb.append("Iteration: ");
		for (int i = 1; i <= iterations; i++) {
			try (Connection con = DriverManager.getConnection(conURL)) {
				sb.append(i).append(" ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
					process_script(stmt, script, 1, 3, 6);
				}
			}
		}
		sb.append("\n");
		sb.append("Completed second test\n");

		// next try to make the execution hang by sending very many queries combined in 1 large script
		final int queries = 260;
		StringBuilder qry = new StringBuilder(queries * 13);
		for (int i = 1; i <= queries; i++)
			qry.append(" SELECT ").append(i).append(';');
		script = qry.toString();
		sb.append("Script size is " + script.length()).append("\n");
		iterations = 9;
		sb.append("Third test repeat " + iterations + " times.\n");
		try (Connection con = DriverManager.getConnection(conURL)) {
			sb.append("Iteration: ");
			for (int i = 1; i <= iterations; i++) {
				sb.append(i).append(" ");
				try (Statement stmt = con.createStatement()) {
					process_script(stmt, script, queries, queries, 0);
				}
			}
			sb.append("\n");
		}
		sb.append("Completed third test\n");
	}

	private void process_script(Statement stmt, String script,
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
			sb.append(results + "!=" + expectedResults + " ");
		if (rows != expectedTotalRows)
			sb.append(rows + "!=" + expectedTotalRows + " ");
		if (updates != expectedUpdates)
			sb.append(updates + "!=" + expectedUpdates + " ");
	}

	private void Bug_PrepStmtSetObject_CLOB_6349() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE PrepStmtSetObject_CLOB (myint INT, myvarchar VARCHAR(15), myclob CLOB)");
			stmt.executeUpdate("INSERT INTO PrepStmtSetObject_CLOB VALUES (123, 'A string', 'A longer string')");
			stmt.executeUpdate("INSERT INTO PrepStmtSetObject_CLOB VALUES (NULL, NULL, NULL)");  // all NULLs

			pstmt = con.prepareStatement("SELECT myclob, myvarchar, myint FROM PrepStmtSetObject_CLOB WHERE myclob = ?");
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			sb.append("Prepared Query has ").append(pmd.getParameterCount()).append(" parameters. Type of first is: ").append(pmd.getParameterTypeName(1)).append("\n");
			ResultSetMetaData rsmd = pstmt.getMetaData();
			sb.append("Prepared Query has ").append(rsmd.getColumnCount()).append(" columns. Type of first is: ").append(rsmd.getColumnTypeName(1)).append("\n");

			pstmt.setObject(1, "A longer string");
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			sb.append("Query ResultSet has ").append(rsmd.getColumnCount()).append(" columns. Type of first is: ").append(rsmd.getColumnTypeName(1)).append("\n");

			boolean has_row = rs.next();
			boolean has_rows = rs.next();
			if (has_row == false || has_rows == true)
				sb.append("Fetching Query ResultSet failed\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}

		// cleanup
		try {
			stmt.executeUpdate("DROP TABLE PrepStmtSetObject_CLOB");
			sb.append("Table dropped\n");
		} catch (SQLException se) {
			sb.append(se.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);
		closeStmtResSet(pstmt, rs);

		compareExpectedOutput("Bug_PrepStmtSetObject_CLOB_6349",
				"0. true	true\n" +
				"Prepared Query has 1 parameters. Type of first is: " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
				"Prepared Query has 3 columns. Type of first is: " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
				"Query ResultSet has 3 columns. Type of first is: " + (isPostDec2023 ? "varchar" : "clob") + "\n" +
				"Table dropped\n");
	}

	private void Bug_PrepStmtSetString_6382() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		final String tableName = "PrepStmtSetString_6382";
		try {
			// >> true: auto commit should be on by default
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. Creating table ").append(tableName);
			if (stmt.executeUpdate("CREATE TABLE " + tableName + " (myint INT, myvarchar VARCHAR(15), myjson JSON, myuuid UUID, myurl URL, myinet INET)") != 0)
				sb.append("Wrong return status\n");

			sb.append("\n2. Insert row 1, ");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'row 1', '{}', uuid '34c8deb5-e608-406b-beda-6a951f73d455', 'https://www.monetdb.org/', '128.0.0.1')");
			sb.append("2, ");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'row 2', '[]', NULL, NULL, NULL)");
			sb.append("3, ");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'row 3', '\"abc\"', NULL, NULL, NULL)");
			sb.append("4, ");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (4, 'row 4', 'true', NULL, NULL, NULL)");
			sb.append("5\n");
			stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (5, 'row 5', '-0.123', NULL, NULL, NULL)");

			sb.append("Creating a prepared statement with 6 parameters and inserting rows using setInt(), setString(), setNull(), setNString(), setURL(), setObject().\n");
			pstmt = con.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?, ? ,?,? , ?)");
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			int pcount = pmd.getParameterCount();
			sb.append("Prepared Statement has ").append(pcount).append(" parameters:").append((pcount != 6 ? " ERROR: Expected 6 parameters!" : "")).append("\n");
			for (int p = 1; p <= pcount; p++) {
				sb.append(" Parameter ").append(p).append(" type is: ").append(pmd.getParameterTypeName(p)).append(". JDBC SQL type: ").append(pmd.getParameterType(p)).append("\n");
			}

			int row = 6;
			pstmt.setInt(1, row);
			pstmt.setString(2, "row " + row);
			pstmt.setString(3, "{\"menu\": {\n  \"id\": \"file\",\n  \"value\": \"File\",\n  \"popup\": {\n    \"menuitem\": [\n      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n    ]\n  }\n}}");
			pstmt.setNull(4, 0);
			pstmt.setNull(5, 0);
			pstmt.setNull(6, 0);
			sb.append("Inserting row ").append(row).append("\n");
			int inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			row++;  // row 7
			pstmt.setShort(1, (short)row);
			pstmt.setNString(2, "row " + row);
			pstmt.setNull(3, 0);
			pstmt.setString(4, "4a148b7d-8d47-4e1e-a21e-09a71abf2215");
			sb.append("Inserting row ").append(row).append("\n");
			inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			row++;  // row 8
			pstmt.setLong(1, (long)row);
			pstmt.setString(2, "row " + row);
			pstmt.setObject(3, "[3.1415E-06]");
			pstmt.setNull(4, 0);
			try {
				pstmt.setURL(5, new java.net.URI("https://www.cwi.nl/").toURL());
			} catch (java.net.URISyntaxException | java.net.MalformedURLException mfe) {
				sb.append(mfe).append("\n");
			}
			sb.append("Inserting row ").append(row).append("\n");
			inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			row++;  // row 9
			pstmt.setBigDecimal(1, new java.math.BigDecimal(row));
			pstmt.setNString(2, "row " + row);
			pstmt.setNull(5, 0);
			pstmt.setString(6, "127.255.255.255");
			sb.append("Inserting row ").append(row).append("\n");
			inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			/* also test generic setObject(int, String) */
			row++;  // row 10
			pstmt.setObject(1, Integer.valueOf(row));
			pstmt.setObject(2, "row " + row);
			pstmt.setObject(3, "[{\"menu\": {\n    \"header\": \"SVG Viewer\",\n    \"items\": [\n        {\"id\": \"Open\"},\n        {\"id\": \"OpenNew\", \"label\": \"Open New\"},\n        null,\n        {\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},\n        {\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},\n        {\"id\": \"OriginalView\", \"label\": \"Original View\"},\n        null,\n        {\"id\": \"Quality\"},\n        {\"id\": \"Pause\"},\n        {\"id\": \"Mute\"},\n        null,\n        {\"id\": \"Help\"},\n        {\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}\n    ]\n}}]");
			pstmt.setObject(4, "b39dc76e-4faf-4fd9-bc1e-17df48acf764");
			pstmt.setObject(5, "https://en.wikipedia.org/wiki/IP_address");
			pstmt.setObject(6, "223.255.255.255");
			sb.append("Inserting row ").append(row).append("\n");
			inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			row++;  // row 11
			pstmt.setObject(1, new java.math.BigDecimal(row));
			pstmt.setObject(2, "row " + row);
			pstmt.setObject(3, "null");
			pstmt.setObject(4, java.util.UUID.fromString("ff125769-b63c-4c3c-859f-5b84a9349e24"));
			URL myURL = new URL();
			try {
				myURL.fromString("https://en.wikipedia.org/wiki/IP_address");
				pstmt.setObject(5, myURL);
			} catch (Exception mfe) {
				sb.append(mfe).append("\n");
			}
			INET myINET = new INET();
			myINET.fromString("223.234.245.255");
			pstmt.setObject(6, myINET);
			sb.append("Inserting row ").append(row).append("\n");
			inserted = pstmt.executeUpdate();
			sb.append("Inserted ").append(inserted).append(" row\n");

			sb.append("List contents of TABLE ").append(tableName).append(" after ").append(row).append(" rows inserted\n");
			rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY 1");
			ResultSetMetaData rsmd = rs.getMetaData();
			int colcount = rsmd.getColumnCount();
			sb.append("Query has ").append(colcount).append(" output columns.").append((colcount != 6 ? " ERROR: Expected 6 columns!" : "")).append("\n");
			row = 0;
			while (rs.next()) {
				sb.append("row ").append(++row);
				for (int c = 1; c <= colcount; c++) {
					sb.append("\t").append(rs.getString(c));
				}
				sb.append("\n");
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		try {
			sb.append("Cleanup TABLE ").append(tableName).append("\n");
			stmt.executeUpdate("DROP TABLE " + tableName);
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Bug_PrepStmtSetString_6382",
			/* servers Jun2023 (11.47) and older stored JSON string values as provided */
			(dbmsMajorVersion == 11 && dbmsMinorVersion <= 47) ?
				"0. true	true\n" +
				"1. Creating table PrepStmtSetString_6382\n" +
				"2. Insert row 1, 2, 3, 4, 5\n" +
				"Creating a prepared statement with 6 parameters and inserting rows using setInt(), setString(), setNull(), setNString(), setURL(), setObject().\n" +
				"Prepared Statement has 6 parameters:\n" +
				" Parameter 1 type is: int. JDBC SQL type: 4\n" +
				" Parameter 2 type is: varchar. JDBC SQL type: 12\n" +
				" Parameter 3 type is: json. JDBC SQL type: 12\n" +
				" Parameter 4 type is: uuid. JDBC SQL type: 12\n" +
				" Parameter 5 type is: url. JDBC SQL type: 12\n" +
				" Parameter 6 type is: inet. JDBC SQL type: 12\n" +
				"Inserting row 6\n" +
				"Inserted 1 row\n" +
				"Inserting row 7\n" +
				"Inserted 1 row\n" +
				"Inserting row 8\n" +
				"Inserted 1 row\n" +
				"Inserting row 9\n" +
				"Inserted 1 row\n" +
				"Inserting row 10\n" +
				"Inserted 1 row\n" +
				"Inserting row 11\n" +
				"Inserted 1 row\n" +
				"List contents of TABLE PrepStmtSetString_6382 after 11 rows inserted\n" +
				"Query has 6 output columns.\n" +
				"row 1	1	row 1	{}	34c8deb5-e608-406b-beda-6a951f73d455	https://www.monetdb.org/	128.0.0.1\n" +
				"row 2	2	row 2	[]	null	null	null\n" +
				"row 3	3	row 3	\"abc\"	null	null	null\n" +
				"row 4	4	row 4	true	null	null	null\n" +
				"row 5	5	row 5	-0.123	null	null	null\n" +
				"row 6	6	row 6	{\"menu\": {\n" +
				"  \"id\": \"file\",\n" +
				"  \"value\": \"File\",\n" +
				"  \"popup\": {\n" +
				"    \"menuitem\": [\n" +
				"      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
				"      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
				"      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
				"    ]\n" +
				"  }\n" +
				"}}	null	null	null\n" +
				"row 7	7	row 7	null	4a148b7d-8d47-4e1e-a21e-09a71abf2215	null	null\n" +
				"row 8	8	row 8	[3.1415E-06]	null	https://www.cwi.nl/	null\n" +
				"row 9	9	row 9	[3.1415E-06]	null	null	127.255.255.255\n" +
				"row 10	10	row 10	[{\"menu\": {\n" +
				"    \"header\": \"SVG Viewer\",\n" +
				"    \"items\": [\n" +
				"        {\"id\": \"Open\"},\n" +
				"        {\"id\": \"OpenNew\", \"label\": \"Open New\"},\n" +
				"        null,\n" +
				"        {\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},\n" +
				"        {\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},\n" +
				"        {\"id\": \"OriginalView\", \"label\": \"Original View\"},\n" +
				"        null,\n" +
				"        {\"id\": \"Quality\"},\n" +
				"        {\"id\": \"Pause\"},\n" +
				"        {\"id\": \"Mute\"},\n" +
				"        null,\n" +
				"        {\"id\": \"Help\"},\n" +
				"        {\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}\n" +
				"    ]\n" +
				"}}]	b39dc76e-4faf-4fd9-bc1e-17df48acf764	https://en.wikipedia.org/wiki/IP_address	223.255.255.255\n" +
				"row 11	11	row 11	null	ff125769-b63c-4c3c-859f-5b84a9349e24	https://en.wikipedia.org/wiki/IP_address	223.234.245.255\n" +
				"Cleanup TABLE PrepStmtSetString_6382\n"
			: /* for servers 11.48 and higher, JSON string values are stored in optimized form (without whitespace characters) */
				"0. true	true\n" +
				"1. Creating table PrepStmtSetString_6382\n" +
				"2. Insert row 1, 2, 3, 4, 5\n" +
				"Creating a prepared statement with 6 parameters and inserting rows using setInt(), setString(), setNull(), setNString(), setURL(), setObject().\n" +
				"Prepared Statement has 6 parameters:\n" +
				" Parameter 1 type is: int. JDBC SQL type: 4\n" +
				" Parameter 2 type is: varchar. JDBC SQL type: 12\n" +
				" Parameter 3 type is: json. JDBC SQL type: 12\n" +
				" Parameter 4 type is: uuid. JDBC SQL type: 12\n" +
				" Parameter 5 type is: url. JDBC SQL type: 12\n" +
				" Parameter 6 type is: inet. JDBC SQL type: 12\n" +
				"Inserting row 6\n" +
				"Inserted 1 row\n" +
				"Inserting row 7\n" +
				"Inserted 1 row\n" +
				"Inserting row 8\n" +
				"Inserted 1 row\n" +
				"Inserting row 9\n" +
				"Inserted 1 row\n" +
				"Inserting row 10\n" +
				"Inserted 1 row\n" +
				"Inserting row 11\n" +
				"Inserted 1 row\n" +
				"List contents of TABLE PrepStmtSetString_6382 after 11 rows inserted\n" +
				"Query has 6 output columns.\n" +
				"row 1	1	row 1	{}	34c8deb5-e608-406b-beda-6a951f73d455	https://www.monetdb.org/	128.0.0.1\n" +
				"row 2	2	row 2	[]	null	null	null\n" +
				"row 3	3	row 3	\"abc\"	null	null	null\n" +
				"row 4	4	row 4	true	null	null	null\n" +
				"row 5	5	row 5	-0.123	null	null	null\n" +
				"row 6	6	row 6	{\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"},{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}	null	null	null\n" +
				"row 7	7	row 7	null	4a148b7d-8d47-4e1e-a21e-09a71abf2215	null	null\n" +
				"row 8	8	row 8	[3.1415E-06]	null	https://www.cwi.nl/	null\n" +
				"row 9	9	row 9	[3.1415E-06]	null	null	127.255.255.255\n" +
				"row 10	10	row 10	[{\"menu\":{\"header\":\"SVG Viewer\",\"items\":[{\"id\":\"Open\"},{\"id\":\"OpenNew\",\"label\":\"Open New\"},null,{\"id\":\"ZoomIn\",\"label\":\"Zoom In\"},{\"id\":\"ZoomOut\",\"label\":\"Zoom Out\"},{\"id\":\"OriginalView\",\"label\":\"Original View\"},null,{\"id\":\"Quality\"},{\"id\":\"Pause\"},{\"id\":\"Mute\"},null,{\"id\":\"Help\"},{\"id\":\"About\",\"label\":\"About Adobe CVG Viewer...\"}]}}]	b39dc76e-4faf-4fd9-bc1e-17df48acf764	https://en.wikipedia.org/wiki/IP_address	223.255.255.255\n" +
				"row 11	11	row 11	null	ff125769-b63c-4c3c-859f-5b84a9349e24	https://en.wikipedia.org/wiki/IP_address	223.234.245.255\n" +
				"Cleanup TABLE PrepStmtSetString_6382\n");
	}

	private void Bug_PrepStmt_With_Errors_Jira292() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sb.append("0. true\t").append(con.getAutoCommit()).append("\n");
			con.setNetworkTimeout(null, (10 *1000));

			stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists abacus;");
			stmt.executeUpdate("create table abacus ( \"'Zeitachse'\" date,\"'Abo_ID'\" int,\"'description'\" varchar(256),\"'Klassierungs-Typ'\" clob,\"'KlassierungApplikation'\" clob,\"'EP Netto'\" decimal,\"'Nettoumsatz'\" decimal,\"'validfrom'\" date,\"'validuntil'\" date,\"'Abo_aufgeschaltet'\" int,\"'Abo_deaktiviert'\" int,\"'Differenz'\" decimal,\"'User_ID'\" int,\"'UserName'\" varchar(256),\"'client'\" varchar(256),\"'Installations_ID'\" int,\"'InstallationsName'\" varchar(256),\"'Installationsprovider_ID'\" int,\"'InstallationsproviderName'\" varchar(256),\"'INR'\" bigint,\"'NAME'\" varchar(256),\"'PLZ'\" varchar(256),\"'ORT'\" varchar(256),\"'STAAT'\" varchar(256),\"'Reseller_ID'\" int,\"'ResellerName'\" varchar(256),\"'ET_ABO'\" clob,\"'UserName_1'\" varchar(256),\"'Anzahl_Abos'\" decimal,\"'Anzahl_User'\" decimal,\"'Jahr'\" decimal,\"'Monat'\" decimal,\"'Jahr_Monat'\" clob,\"'IFJ'\" clob,\"'RECNUM$'\" int,\"'InlineCalc_Year_Zeitachse'\" int);");
			stmt.executeUpdate("insert into abacus values ('2019-10-30',2239,'description','Klassierungs-Typ','Klassierung-Applikation',73.28,68.29,'2018-01-01','2018-12-01',563,63,56.3,852,'UserName','client',134,'InstallationsName',892,'InstallationsproviderName',9348,'NAME','PLZ','ORT','STAAT',934,'ResellerName','ET_ABO','UserName_1',849.2739,1742.718,395.824,39.824,'Jahr_Monat','IFJ',395824,3789);");

			sb.append("1. table created and inserted 1 row\n");

			String qry = "SELECT \"'ResellerName'\" FROM abacus WHERE  ( ( (\"'InstallationsproviderName'\"='Bienz Pius Treuhand- und Revisions AG')) AND  ( (\"'validuntil'\"='2018-01-01' AND \"'description'\"='ABEA 2' AND (EXTRACT(YEAR FROM \"'Zeitachse'\")*100 + EXTRACT(MONTH FROM \"'Zeitachse'\"))/100.0='2019.010' AND \"'UserName'\"='AL - Astrid Lincke (Delphys)' AND \"'validfrom'\"='2016-12-01')) AND  ( (\"'IFJ'\"='ohne IFJ')) AND  ( (\"'InlineCalc_Year_Zeitachse'\"='2019'))) GROUP BY \"'ResellerName'\" LIMIT 1001 OFFSET 0;";
			try {
				sb.append("2. before select query execution\n");
				rs = stmt.executeQuery(qry);
				sb.append("2a. select query executed\n");
				if (rs != null) {
					if (rs.next()) {
						sb.append("2b. select query returned: " + rs.getString(1)).append("\n");
					}
					rs.close();
					rs = null;
					sb.append("2c. closed select query resultset\n");
				}
				sb.append("2d. normal end of select query\n");
			} catch (SQLException se) {
				sb.append("select query Exception: "+ se.getMessage()).append("\n");
				while ((se = se.getNextException()) != null)
					sb.append("next Exception: "+ se.getMessage()).append("\n");
			}

			try {
				sb.append("3. before creating a prepared select query\n");
				pstmt = con.prepareStatement(qry);
				sb.append("3a. prepared select query\n");

				ParameterMetaData pmd = pstmt.getParameterMetaData();
				sb.append("3b. Prepared Query has " + pmd.getParameterCount() + " parameters."); // "Type of first is: " + pmd.getParameterTypeName(1)).append("\n");
				ResultSetMetaData rsmd = pstmt.getMetaData();
				sb.append("3c. Prepared Query has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1)).append("\n");

				sb.append("3d. before executing the prepared select query\n");
				rs = pstmt.executeQuery();
				sb.append("3e. prepared select query executed\n");
				if (rs != null) {
					rsmd = rs.getMetaData();
					sb.append("3f. prepared Query ResultSet has " + rsmd.getColumnCount() + " columns. Type of first is: " + rsmd.getColumnTypeName(1)).append("\n");

					if (rs.next()) {
						sb.append("3g. prepared select query returned: " + rs.getString(1)).append("\n");
					}
					rs.close();
					rs = null;
					sb.append("3h. closed prepared select query resultset\n");
				}
				sb.append("3i. normal end of prepared select query\n");
			} catch (SQLException se) {
				sb.append("prepared select query Exception: "+ se.getMessage()).append("\n");
				while ((se = se.getNextException()) != null)
					sb.append("next Exception: "+ se.getMessage()).append("\n");
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		try {
			sb.append("4. drop table\n");
			stmt.executeUpdate("drop table abacus");
			sb.append("5. normal end of test\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(pstmt, rs);
		closeStmtResSet(stmt, null);

		compareExpectedOutput("Bug_PrepStmt_With_Errors_Jira292",
				"0. true	true\n" +
				"1. table created and inserted 1 row\n" +
				"2. before select query execution\n" +
				"2a. select query executed\n" +
				"2c. closed select query resultset\n" +
				"2d. normal end of select query\n" +
				"3. before creating a prepared select query\n" +
				"3a. prepared select query\n" +
				"3b. Prepared Query has 0 parameters.3c. Prepared Query has 1 columns. Type of first is: varchar\n" +
				"3d. before executing the prepared select query\n" +
				"3e. prepared select query executed\n" +
				"3f. prepared Query ResultSet has 1 columns. Type of first is: varchar\n" +
				"3h. closed prepared select query resultset\n" +
				"3i. normal end of prepared select query\n" +
				"4. drop table\n" +
				"5. normal end of test\n");
	}

	private void BugResultSetMetaData_Bug_6183() {
		sb.setLength(0);	// clear the output log buffer

		final String dqTblName = "\"my dq_table\"";
		final String[] dqColNames = { "\"my space\"", "\"my, comma_space\"", "\"my$dollar\"", "\"my#hash\"", "\"my	tab\""
			, "\"my	,tab_comma\"", "\"my,	comma_tab\"", "\"my\"\"double_doublequote\"", "\"Abc\"", "\" \"", "\"123\"" };
		Statement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder ctsb = new StringBuilder(30 + (dqColNames.length * (30 + 15)));
			ctsb.append("CREATE TABLE ").append(dqTblName).append(" (");
			for (int n = 0; n < dqColNames.length; n++) {
				ctsb.append(dqColNames[n]);
				ctsb.append(" varchar(").append(31 + n).append(')');
				if (n < (dqColNames.length -1))
					ctsb.append(", ");
			}
			ctsb.append(')');

			stmt = con.createStatement();
			sb.append("1. create table ").append(dqTblName).append("\n");
			int ret = stmt.executeUpdate(ctsb.toString());
			if (ret != 0)
				sb.append(" returned: ").append(ret).append(" (expected 0)\n");

			String tblName = dqTblName.substring(1, dqTblName.length() -1);	// trim the leading and trailing double quote characters
			sb.append("2. show column names of this new table (").append(tblName).append(") via sys.columns query\n");
			rs = stmt.executeQuery("SELECT number, name, type from sys.columns where table_id in (select id from sys._tables where name = '" + tblName + "') order by number");
			showResultAndClose_6183(rs);

			sb.append("3. insert 1 row of data with values same as column names\n");
			ctsb.setLength(0);
			ctsb.append("INSERT INTO ").append(dqTblName).append(" VALUES (");
			for (int n = 0; n < dqColNames.length; n++) {
				ctsb.append('\'');
				ctsb.append(dqColNames[n]);
				ctsb.append('\'');
				if (n < (dqColNames.length -1))
					ctsb.append(", ");
			}
			ctsb.append(')');
			ret = stmt.executeUpdate(ctsb.toString());
			if (ret != 1)
				sb.append(" returned: ").append(ret).append(" (expected 1)\n");

			sb.append("4. insert 1 row of data with values same as column names but without enclosing double quotes\n");
			ctsb.setLength(0);
			ctsb.append("INSERT INTO ").append(dqTblName).append(" VALUES (");
			for (int n = 0; n < dqColNames.length; n++) {
				ctsb.append('\'');
				// remove enclosing double quotes
				ctsb.append(dqColNames[n].substring(1, dqColNames[n].length() -1));
				ctsb.append('\'');
				if (n < (dqColNames.length -1))
					ctsb.append(", ");
			}
			ctsb.append(')');
			ret = stmt.executeUpdate(ctsb.toString());
			if (ret != 1)
				sb.append(" returned: ").append(ret).append(" (expected 1)\n");

			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"1. create table \"my dq_table\"\n" +
				"2. show column names of this new table (my dq_table) via sys.columns query\n" +
				"Resultset with 3 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	number	number\n" +
				"2	name	name\n" +
				"3	type	type\n" +
				"Data rows:\n" +
				"0	my space	varchar\n" +
				"1	my, comma_space	varchar\n" +
				"2	my$dollar	varchar\n" +
				"3	my#hash	varchar\n" +
				"4	my	tab	varchar\n" +
				"5	my	,tab_comma	varchar\n" +
				"6	my,	comma_tab	varchar\n" +
				"7	my\"double_doublequote	varchar\n" +
				"8	Abc	varchar\n" +
				"9	 	varchar\n" +
				"10	123	varchar\n" +
				"Listed 11 rows\n" +
				"3. insert 1 row of data with values same as column names\n" +
				"4. insert 1 row of data with values same as column names but without enclosing double quotes\n");
			sb.setLength(0);	// clear the output log buffer

			// query each column separately
			int n = 0;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"5. show content of column(s): \"my space\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my space	my space\n" +
				"Data rows:\n" +
				"\"my space\"\n" +
				"my space\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"6. show content of column(s): \"my, comma_space\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my, comma_space	my, comma_space\n" +
				"Data rows:\n" +
				"\"my, comma_space\"\n" +
				"my, comma_space\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"7. show content of column(s): \"my$dollar\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my$dollar	my$dollar\n" +
				"Data rows:\n" +
				"\"my$dollar\"\n" +
				"my$dollar\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"8. show content of column(s): \"my#hash\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my#hash	my#hash\n" +
				"Data rows:\n" +
				"\"my#hash\"\n" +
				"my#hash\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"9. show content of column(s): \"my	tab\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my	tab	my	tab\n" +
				"Data rows:\n" +
				"\"my	tab\"\n" +
				"my	tab\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"10. show content of column(s): \"my	,tab_comma\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my	,tab_comma	my	,tab_comma\n" +
				"Data rows:\n" +
				"\"my	,tab_comma\"\n" +
				"my	,tab_comma\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"11. show content of column(s): \"my,	comma_tab\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my,	comma_tab	my,	comma_tab\n" +
				"Data rows:\n" +
				"\"my,	comma_tab\"\n" +
				"my,	comma_tab\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"12. show content of column(s): \"my\"\"double_doublequote\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my\\\"double_doublequote	my\\\"double_doublequote\n" +
				"Data rows:\n" +
				"\"my\"\"double_doublequote\"\n" +
				"my\"\"double_doublequote\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"13. show content of column(s): \"Abc\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	Abc	Abc\n" +
				"Data rows:\n" +
				"\"Abc\"\n" +
				"Abc\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"14. show content of column(s): \" \"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	 	 \n" +
				"Data rows:\n" +
				"\" \"\n" +
				" \n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
			n++;
			executeQueryAndShowResult_6183(stmt, dqTblName, dqColNames[n], 5 + n);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"15. show content of column(s): \"123\"\n" +
				"Resultset with 1 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	123	123\n" +
				"Data rows:\n" +
				"\"123\"\n" +
				"123\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer

			// query all columns
			executeQueryAndShowResult_6183(stmt, dqTblName, "*", 5 + dqColNames.length);
			compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"16. show content of column(s): *\n" +
				"Resultset with 11 columns\n" +
				"	Column Name, Column Label:\n" +
				"1	my space	my space\n" +
				"2	my, comma_space	my, comma_space\n" +
				"3	my$dollar	my$dollar\n" +
				"4	my#hash	my#hash\n" +
				"5	my	tab	my	tab\n" +
				"6	my	,tab_comma	my	,tab_comma\n" +
				"7	my,	comma_tab	my,	comma_tab\n" +
				"8	my\\\"double_doublequote	my\\\"double_doublequote\n" +
				"9	Abc	Abc\n" +
				"10	 	 \n" +
				"11	123	123\n" +
				"Data rows:\n" +
				"\"my space\"	\"my, comma_space\"	\"my$dollar\"	\"my#hash\"	\"my	tab\"	\"my	,tab_comma\"	\"my,	comma_tab\"	\"my\"\"double_doublequote\"	\"Abc\"	\" \"	\"123\"\n" +
				"my space	my, comma_space	my$dollar	my#hash	my	tab	my	,tab_comma	my,	comma_tab	my\"\"double_doublequote	Abc	 	123\n" +
				"Listed 2 rows\n");
			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			sb.append("Finally drop table ").append(dqTblName).append("\n");
			int ret = stmt.executeUpdate("DROP TABLE " + dqTblName);
			if (ret != 0)
				sb.append(" returned: ").append(ret).append(" (expected 0)\n");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("BugResultSetMetaData_Bug_6183",
				"Finally drop table \"my dq_table\"\n");
	}

	private void executeQueryAndShowResult_6183(Statement st, String dqTblName, String col_list, int query_count) throws SQLException {
		sb.append(query_count).append(". show content of column(s): ").append(col_list).append("\n");
		ResultSet rs = st.executeQuery("SELECT " + col_list + " from " + dqTblName);
		showResultAndClose_6183(rs);
	}

	private void showResultAndClose_6183(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int rs_col_count = rsmd.getColumnCount();
		sb.append("Resultset with ").append(rs_col_count).append(" columns\n");
		sb.append("\tColumn Name, Column Label:\n");
		for (int col = 1; col <= rs_col_count; col++) {
			sb.append(col).append("\t").append(rsmd.getColumnName(col)).append("\t").append(rsmd.getColumnLabel(col)).append("\n");
		}

		sb.append("Data rows:\n");
		long row_count = 0;
		while (rs.next()) {
			row_count++;
			for (int col = 1; col <= rs_col_count; col++) {
				if (col > 1)
					sb.append("\t");
				sb.append(rs.getString(col));
			}
			sb.append("\n");
		}
		rs.close();
		sb.append("Listed ").append(row_count).append(" rows\n");
	}

	private void BugSetQueryTimeout_Bug_3357() {
		sb.setLength(0);	// clear the output log buffer

		int originalQueryTimeout = 0;
		Statement st = null;
		try {
			st = con.createStatement();
			originalQueryTimeout = st.getQueryTimeout();
			sb.append("original getQueryTimeout = ").append(originalQueryTimeout).append("\n");

			testTimeout_3357(st, 123);
			testTimeout_3357(st, 123456);
			testTimeout_3357(st, 2147483);
			testTimeout_3357(st, 2147484);
			testTimeout_3357(st, Integer.MAX_VALUE);
			testTimeout_3357(st, 0);
			testTimeout_3357(st, 10);
			testTimeout_3357(st, 4);
			testTimeout_3357(st, -1);	// to generate an SQLException as negative timeouts are invalid
		} catch (SQLException se) {
			sb.append("SQLException: setQueryTimeout(timeout_value) throws: ").append(se).append("\n");
		}

		// restore originalQueryTimeout
		try {
			sb.append("Restore original QueryTimeout = ").append(originalQueryTimeout).append("\n");
			testTimeout_3357(st, originalQueryTimeout);
		} catch (SQLException se) {
			sb.append("setQueryTimeout(timeout_value) throws: ").append(se).append("\n");
		}
		closeStmtResSet(st, null);

		compareExpectedOutput("BugSetQueryTimeout_Bug_3357",
				"original getQueryTimeout = 0\n" +
				"setQueryTimeout = 123. getQueryTimeout = 123\n" +
				"setQueryTimeout = 123456. getQueryTimeout = 123456\n" +
				"setQueryTimeout = 2147483. getQueryTimeout = 2147483\n" +
				"setQueryTimeout = 2147484. getQueryTimeout = 2147484\n" +
				"setQueryTimeout = 2147483647. getQueryTimeout = 2147483647\n" +
				"setQueryTimeout = 0. getQueryTimeout = 0\n" +
				"setQueryTimeout = 10. getQueryTimeout = 10\n" +
				"setQueryTimeout = 4. getQueryTimeout = 4\n" +
				"setQueryTimeout = -1. SQLException: setQueryTimeout(timeout_value) throws: java.sql.SQLException: Illegal timeout value: -1\n" +
				"Restore original QueryTimeout = 0\n" +
				"setQueryTimeout = 0. getQueryTimeout = 0\n");
	}

	private void testTimeout_3357(Statement st, int secs) throws SQLException {
		sb.append("setQueryTimeout = ").append(secs).append(". ");
		st.setQueryTimeout(secs);
		// as the call to set the timeout is delayed till a statement is executed, issue a select statement
		ResultSet rs = st.executeQuery("SELECT " + secs);
		if (rs != null)
			rs.close();
		sb.append("getQueryTimeout = ").append(st.getQueryTimeout()).append("\n");
	}

	private void Bug_PrepStmtManyParams_7337(int nrParams) {
		sb.setLength(0);	// clear the output log buffer

		final int NR_COLUMNS = nrParams;
		final StringBuilder sql = new StringBuilder(100 + (NR_COLUMNS * 25));

		int col;
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			sb.append("0. fetch size of new statement: ").append(stmt.getFetchSize()).append("\n");

			// construct the Create Table SQL text
			sql.append("CREATE TABLE t7337 (ID BIGINT AUTO_INCREMENT PRIMARY KEY, ");
			for (col = 1; col <= NR_COLUMNS; col++) {
				sql.append("column").append(col).append(" VARCHAR(256),");
			}
			sql.append("column").append(col).append(" TIMESTAMP);");

			sb.append("1. create table with ").append(NR_COLUMNS+2).append(" columns, sql has length: ").append(sql.length()).append("\n");
			int ret = stmt.executeUpdate(sql.toString());
			sb.append("2. table created. ret = ").append(ret).append("\n");
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		// test: PREPARE INSERT INTO with many parameters
		PreparedStatement pstmt = null;
		try {
			// construct the Insert Into Table SQL text, first without any parameter makers
			sql.setLength(0);	// clear the sql buffer
			sql.append("INSERT INTO t7337 \n(");
			for (col = 1; col <= NR_COLUMNS; col++) {
				sql.append("column").append(col).append(",");
			}
			sql.append("column").append(col).append(" ) VALUES \n(");
			int posFirstPart = sql.length();
			for (col = 1; col <= NR_COLUMNS; col++) {
				sql.append("'someTextHere',");
			}
			sql.append("'2022-11-11');");

			sb.append("3. prepare insert statement (no params), sql has length: ").append(sql.length()).append("\n");
			pstmt = con.prepareStatement(sql.toString());
			if (pstmt != null) {
				sb.append("   fetch size after prepare 1: ").append(pstmt.getFetchSize()).append("\n");
				ParameterMetaData pmd = pstmt.getParameterMetaData();
				sb.append("   pmd. ").append(pmd.getParameterCount()).append(" parameters\n");
				sb.append("4. execute prepared insert\n");
				int inserted = pstmt.executeUpdate();
				sb.append("5. first execute returned: ").append(inserted).append("\n");
				// do it one more time
				inserted = pstmt.executeUpdate();
				sb.append("5. second execute returned: ").append(inserted).append("\n");
				sb.append("6. inserted data committed\n");
				pstmt.close();
				pstmt = null;
			}

			// construct the Insert Into Table SQL text, now with parameter makers
			sql.setLength(posFirstPart);	// clear the sql part after the: VALUES (
			for (col = 1; col <= nrParams; col++) {
				sql.append("?,");
			}
			sql.append("'2022-11-16');");

			sb.append("7. prepare insert statement (with params), sql has length: ").append(sql.length()).append("\n");
			pstmt = con.prepareStatement(sql.toString());
			if (pstmt != null) {
				sb.append("   fetch size after prepare 2: ").append(pstmt.getFetchSize()).append("\n");
				ParameterMetaData pmd = pstmt.getParameterMetaData();
				sb.append("   pmd. ").append(pmd.getParameterCount()).append(" parameters\n");
				sb.append("8. bind parameters\n");
				for (col = 1; col <= nrParams; col++) {
					pstmt.setString(col, "someMoreText");
				}
				sb.append("9. execute prepared insert with parameters\n");
				int inserted = pstmt.executeUpdate();
				sb.append("10. first execute returned: ").append(inserted).append("\n");
				// do it one more time
				inserted = pstmt.executeUpdate();
				sb.append("10. second execute returned: ").append(inserted).append("\n");
				sb.append("11. inserted data committed\n");
				pstmt.close();
				pstmt = null;
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, null);

		compareExpectedOutput("Bug_PrepStmtManyParams_7337(" + nrParams + ")",
			"0. fetch size of new statement: 250\n" +
			"1. create table with " + (NR_COLUMNS+2) + " columns, sql has length: " + ((NR_COLUMNS * 23) -29) + "\n" +
			"2. table created. ret = 0\n" +
			"3. prepare insert statement (no params), sql has length: " + ((NR_COLUMNS * 25) -53) + "\n" +
			"   fetch size after prepare 1: 250\n" +
			"   pmd. 0 parameters\n" +
			"4. execute prepared insert\n" +
			"5. first execute returned: 1\n" +
			"5. second execute returned: 1\n" +
			"6. inserted data committed\n" +
			"7. prepare insert statement (with params), sql has length: " + ((nrParams * 12) -53) + "\n" +
			"   fetch size after prepare 2: 250\n" +
			"   pmd. " + nrParams + " parameters\n" +
			"8. bind parameters\n" +
			"9. execute prepared insert with parameters\n" +
			"10. first execute returned: 1\n" +
			"10. second execute returned: 1\n" +
			"11. inserted data committed\n");

		// test also: PREPARE SELECT * FROM .. without and with many parameters
		sb.setLength(0);	// clear the output log buffer
		ResultSet rs = null;
		try {
			// construct the Select SQL text, first without any parameter makers
			sql.setLength(0);	// clear the sql buffer
			sql.append("SELECT * FROM t7337");

			sb.append("12. prepare select statement (no params), sql has length: ").append(sql.length()).append("\n");
			pstmt = con.prepareStatement(sql.toString());
			if (pstmt != null) {
				sb.append("   fetch size after prepare 3: ").append(pstmt.getFetchSize()).append("\n");
				ParameterMetaData pmd = pstmt.getParameterMetaData();
				sb.append("   pmd. ").append(pmd.getParameterCount()).append(" parameters\n");
				ResultSetMetaData rsmd = pstmt.getMetaData();
				sb.append("   rsmd. ").append(rsmd.getColumnCount()).append(" result columns\n");
				sb.append("13. execute prepared select\n");
				rs = pstmt.executeQuery();
				if (rs != null) {
					rsmd = rs.getMetaData();
					sb.append("14. first query execute succeeded. it has ").append(rsmd.getColumnCount()).append(" result columns\n");
					rs.close();
					rs = null;
				} else {
					sb.append("14. first query execute failed to return a result\n");
				}
				// do it one more time
				rs = pstmt.executeQuery();
				if (rs != null) {
					rsmd = rs.getMetaData();
					sb.append("15. second query execute succeeded. it has ").append(rsmd.getColumnCount()).append(" result columns\n");
					rs.close();
					rs = null;
				} else {
					sb.append("15. second query execute failed to return a result\n");
				}
				pstmt.close();
				pstmt = null;
			}

			// add the WHERE part with many parameter makers
			sql.append(" WHERE ");
			for (col = 1; col <= NR_COLUMNS; col++) {
				sql.append("column").append(col).append(" = ? AND ");
			}
			sql.append("column").append(col).append(" = '2022-11-16'");

			sb.append("16. prepare select statement (with params), sql has length: ").append(sql.length()).append("\n");
			pstmt = con.prepareStatement(sql.toString());
			if (pstmt != null) {
				sb.append("   fetch size after prepare 4: ").append(pstmt.getFetchSize()).append("\n");
				ParameterMetaData pmd = pstmt.getParameterMetaData();
				sb.append("   pmd. ").append(pmd.getParameterCount()).append(" parameters\n");
				ResultSetMetaData rsmd = pstmt.getMetaData();
				sb.append("   rsmd. ").append(rsmd.getColumnCount()).append(" result columns\n");
				sb.append("17. bind parameters\n");
				for (col = 1; col <= nrParams; col++) {
					pstmt.setString(col, "someMoreText");
				}
				sb.append("18. execute prepared select\n");
				rs = pstmt.executeQuery();
				if (rs != null) {
					rsmd = rs.getMetaData();
					sb.append("19. first query execute succeeded. it has ")
					  .append(rsmd.getColumnCount()).append(" result columns and ");
					int rows = 0;
					while (rs.next())
						rows++;
					sb.append(rows).append(" rows\n");
					rs.close();
					rs = null;
				} else {
					sb.append("19. first query execute failed to return a result\n");
				}
				// do it one more time
				sb.append("20. bind parameters\n");
				for (col = 1; col <= nrParams; col++) {
					pstmt.setString(col, "someMoreText");
				}
				sb.append("21. execute prepared select again\n");
				rs = pstmt.executeQuery();
				if (rs != null) {
					rsmd = rs.getMetaData();
					sb.append("22. second query execute succeeded. it has ")
					  .append(rsmd.getColumnCount()).append(" result columns and ");
					int rows = 0;
					while (rs.next())
						rows++;
					sb.append(rows).append(" rows\n");
					rs.close();
					rs = null;
				} else {
					sb.append("22. second query execute failed to return a result\n");
				}
				pstmt.close();
				pstmt = null;
			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, rs);

		// cleanup table
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS t7337;");
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, null);

		compareExpectedOutput("Bug_PrepStmtManyParams_7337(" + nrParams + ")",
			"12. prepare select statement (no params), sql has length: 19\n" +
			"   fetch size after prepare 3: 250\n" +
			"   pmd. 0 parameters\n" +
			"   rsmd. " + (NR_COLUMNS+2) + " result columns\n" +
			"13. execute prepared select\n" +
			"14. first query execute succeeded. it has " + (NR_COLUMNS+2) + " result columns\n" +
			"15. second query execute succeeded. it has " + (NR_COLUMNS+2) + " result columns\n" +
			"16. prepare select statement (with params), sql has length: " + ((NR_COLUMNS * 18) -58) + "\n" +
			"   fetch size after prepare 4: 250\n" +
			"   pmd. " + nrParams + " parameters\n" +
			"   rsmd. " + (NR_COLUMNS+2) + " result columns\n" +
			"17. bind parameters\n" +
			"18. execute prepared select\n" +
			"19. first query execute succeeded. it has " + (NR_COLUMNS+2) + " result columns and 2 rows\n" +
			"20. bind parameters\n" +
			"21. execute prepared select again\n" +
			"22. second query execute succeeded. it has " + (NR_COLUMNS+2) + " result columns and 2 rows\n");
	}

	/**
	 * This SQLcopyinto program demonstrates how the MonetDB JDBC driver can facilitate
	 * in performing COPY INTO ... FROM STDIN sequences.
	 * It shows how a data stream via MapiSocket to STDIN can be performed.
	 *
	 * @author Fabian Groffen, Martin van Dinther
	 */
	private void SQLcopyinto(final String conn_URL) {
		sb.setLength(0);	// clear the output log buffer

		final String tablenm = "exampleSQLCopyInto";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS " + tablenm + " (id int, val varchar(24))");

			fillTableUsingCopyIntoSTDIN(conn_URL, tablenm);

			// check content of the table populated via COPY INTO ... FROM STDIN
			sb.append("Listing uploaded data:\n");
			int row = 0;
			rs = stmt.executeQuery("SELECT * FROM " + tablenm);
			if (rs != null) {
				while (rs.next()) {
					row++;
					if ((row % 1000) == 0)
						sb.append("Row data: ").append(rs.getString(1)).append(", ").append(rs.getString(2)).append("\n");
				}
				rs.close();
				rs = null;
			}
		} catch (SQLException se) {
			sb.append("SQLException: ").append(se.getMessage()).append("\n");
		} catch (Exception e) {
			sb.append("Exception: ").append(e.getMessage()).append("\n");
		}

		// cleanup
		try {
			stmt.execute("DROP TABLE " + tablenm);
			sb.append("SQLcopyinto completed\n");
		} catch (SQLException se) {
			sb.append("SQLException: ").append(se.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);

		compareExpectedOutput("SQLcopyinto()",
				"CopyInto STDIN begin\n" +
				"Before connecting to MonetDB server via MapiSocket\n" +
				"Connected to MonetDB server via MapiSocket\n" +
				"Before sending data to STDIN\n" +
				"Completed sending data via STDIN\n" +
				"CopyInto STDIN end\n" +
				"Listing uploaded data:\n" +
				"Row data: 999, val_999\n" +
				"Row data: 1999, val_1999\n" +
				"Row data: 2999, val_2999\n" +
				"Row data: 3999, val_3999\n" +
				"Row data: 4999, val_4999\n" +
				"Row data: 5999, val_5999\n" +
				"Row data: 6999, val_6999\n" +
				"Row data: 7999, val_7999\n" +
				"Row data: 8999, val_8999\n" +
				"SQLcopyinto completed\n");
	}

	private void fillTableUsingCopyIntoSTDIN(final String conn_URL, final String tablenm) throws Exception {
		sb.append("CopyInto STDIN begin\n");

		org.monetdb.mcl.net.MapiSocket server = new org.monetdb.mcl.net.MapiSocket();
		try {
			MonetConnection mcon = (MonetConnection) con;
			Properties props = mcon.getConnectionProperties();

			sb.append("Before connecting to MonetDB server via MapiSocket\n");
			List<String> warning = server.connect("jdbc:monetdb:", props);
			if (warning != null) {
				for (Iterator<String> it = warning.iterator(); it.hasNext(); ) {
					sb.append("Warning: ").append(it.next().toString()).append("\n");
				}
			}
			sb.append("Connected to MonetDB server via MapiSocket\n");

			org.monetdb.mcl.io.BufferedMCLReader mclIn = server.getReader();
			org.monetdb.mcl.io.BufferedMCLWriter mclOut = server.getWriter();

			String error = mclIn.discardRemainder();
			if (error != null)
				sb.append("Received start error: ").append(error).append("\n");

			sb.append("Before sending data to STDIN\n");

			// the leading 's' is essential, since it is a protocol marker
			// that should not be omitted, likewise the trailing semicolon
			mclOut.write('s');
			mclOut.write("COPY INTO " + tablenm + " FROM STDIN USING DELIMITERS ',',E'\\n';");
			mclOut.newLine();
			// now write the row data values as csv data lines to the STDIN stream
			for (int i = 0; i < 9000; i++) {
				mclOut.write("" + i + ",val_" + i);
				mclOut.newLine();
			}
			mclOut.writeLine(""); // need this one for synchronisation over flush()

			error = mclIn.discardRemainder();
			if (error != null)
				sb.append("Received error: ").append(error).append("\n");

			mclOut.writeLine(""); // need this one for synchronisation over flush()

			error = mclIn.discardRemainder();
			if (error != null)
				sb.append("Received finish error: ").append(error).append("\n");

			sb.append("Completed sending data via STDIN\n");
		} catch (Exception e) {
			sb.append("Mapi Exception: ").append(e.getMessage()).append("\n");
		} finally {
			// close MAPI connection to MonetDB server
			server.close();
		}

		sb.append("CopyInto STDIN end\n");
	}

	private void DecimalPrecisionAndScale() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(
				"select" +
				"  cast(123456789   as DECIMAL(18,0)) as dec1800" +
				", cast(123456789.0 as DECIMAL(18,1)) as dec1801" +
				", cast(123456789.0 as DECIMAL(18,2)) as dec1802" +
				", cast(123456789.0 as DECIMAL(18,3)) as dec1803" +
				", cast(123456789.0 as DECIMAL(18,4)) as dec1804" +
				", cast(123456789.0 as DECIMAL(18,5)) as dec1805" +
				", cast(123456789.0 as DECIMAL(18,6)) as dec1806" +
				", cast(123456789.0 as DECIMAL(18,7)) as dec1807" +
				", cast(123456789.0 as DECIMAL(18,8)) as dec1808" +
				", cast(123456789.0 as DECIMAL(18,9)) as dec1809" +
				", cast(12345678.9 as DECIMAL(18,10)) as dec1810" +
				", cast(1234567.89 as DECIMAL(18,11)) as dec1811" +
				", cast(123456.789 as DECIMAL(18,12)) as dec1812;");
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				final int rscolcnt = rsmd.getColumnCount();
				sb.append("Query has ").append(rscolcnt).append(" columns:\n");
				sb.append("colnr\tlabel\ttypenm\tdisplaylength\tprecision\tscale\n");
				for (int col = 1; col <= rscolcnt; col++) {
					sb.append("col ").append(col);
					sb.append("\t").append(rsmd.getColumnLabel(col));
					sb.append("\t").append(rsmd.getColumnTypeName(col));
					sb.append("\t").append(rsmd.getColumnDisplaySize(col));
					sb.append("\t").append(rsmd.getPrecision(col));
					sb.append("\t").append(rsmd.getScale(col));
					sb.append("\n");
				}
				sb.append("Values\n");
				while (rs.next()) {
					sb.append("colnr\tasString\tasBigDecimal\n");
					for (int col = 1; col <= rscolcnt; col++) {
						sb.append("col ").append(col);
						sb.append("\t").append(rs.getString(col));
						sb.append("\t").append(rs.getBigDecimal(col));
						sb.append("\n");
					}
					sb.append("\n");
				}
				rs.close();
				rs = null;
			}
		} catch (SQLException se) {
			sb.append("SQLException: ").append(se.getMessage()).append("\n");
		} catch (Exception e) {
			sb.append("Exception: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);

		// The precision should be 18 and the scale should be from 0 to 12.
		compareExpectedOutput("DecimalPrecisionAndScale()",
				"Query has 13 columns:\n" +
				"colnr	label	typenm	displaylength	precision	scale\n" +
				"col 1	dec1800	decimal	19	18	0\n" +
				"col 2	dec1801	decimal	20	18	1\n" +
				"col 3	dec1802	decimal	20	18	2\n" +
				"col 4	dec1803	decimal	20	18	3\n" +
				"col 5	dec1804	decimal	20	18	4\n" +
				"col 6	dec1805	decimal	20	18	5\n" +
				"col 7	dec1806	decimal	20	18	6\n" +
				"col 8	dec1807	decimal	20	18	7\n" +
				"col 9	dec1808	decimal	20	18	8\n" +
				"col 10	dec1809	decimal	20	18	9\n" +
				"col 11	dec1810	decimal	20	18	10\n" +
				"col 12	dec1811	decimal	20	18	11\n" +
				"col 13	dec1812	decimal	20	18	12\n" +
				"Values\n" +
				"colnr	asString	asBigDecimal\n" +
				"col 1	123456789	123456789\n" +
				"col 2	123456789.0	123456789.0\n" +
				"col 3	123456789.00	123456789.00\n" +
				"col 4	123456789.000	123456789.000\n" +
				"col 5	123456789.0000	123456789.0000\n" +
				"col 6	123456789.00000	123456789.00000\n" +
				"col 7	123456789.000000	123456789.000000\n" +
				"col 8	123456789.0000000	123456789.0000000\n" +
				"col 9	123456789.00000000	123456789.00000000\n" +
				"col 10	123456789.000000000	123456789.000000000\n" +
				"col 11	12345678.9000000000	12345678.9000000000\n" +
				"col 12	1234567.89000000000	1234567.89000000000\n" +
				"col 13	123456.789000000000	123456.789000000000\n" +
				"\n");
	}

	private void Test_ClientInfo(String con_URL) {
		if (!isPostDec2023)
			return;

		sb.setLength(0);

		final String[] known = {
				"ApplicationName", "ClientHostname", "ClientLibrary",  "ClientPid", "ClientRemark"
		};

		try {
			sb.append("Connecting\n");
			try (Connection conn = DriverManager.getConnection(con_URL)) {

				// Server metadata includes list of supported clientinfo properties
				sb.append("Fetching supported clientinfo properties\n");
				DatabaseMetaData md = conn.getMetaData();
				try (ResultSet rs = md.getClientInfoProperties()) {
					HashSet<String> seen = new HashSet<>();
					while (rs.next()) {
						String name = rs.getString(1);
						if (name == null || name.isEmpty()) {
							sb.append("NAME column contains empty string\n");
						}
						seen.add(name);
						int width = rs.getInt(2);
						if (width <= 0) {
							sb.append("MAX_LEN for " + name + " is " + width + "\n");
						}
						String description = rs.getString(4);
						if (description == null || description.isEmpty()) {
							sb.append("DESCRIPTION for " + name + " is empty\n");
						}
					}
					for (String name: known) {
						boolean found = seen.contains(name);
						sb.append("- " + name + (found ? " was " : " was not ") + "found\n");
					}
				}

				// I cannot think of a way to check the default values that doesn't
				// essentially duplicate the code that came up with the default values.
				// The best we can do is verify they're not empty.
				sb.append("Check initial values.\n");
				Properties initialValues = conn.getClientInfo();
				for (String name: known) {
					String value = (String) initialValues.getOrDefault(name, "");
					sb.append("- " + name + (value.isEmpty() ? " is empty" : " is not empty") + "\n");
				}

				// We should get a fresh copy every time
				if (conn.getClientInfo() != initialValues)
					sb.append("Calls to con.getClientInfo do not return references to the same Properties object\n");
				else
					sb.append("Calls to con.getClientInfo DO return references to the same Properties object!\n");

				// Assign new values in various ways.
				// Also include some unknown properties
				sb.append("Set ApplicationName=1\n");
				conn.setClientInfo("ApplicationName", "1");
				readWarnings(conn.getWarnings());
				conn.clearWarnings();

				sb.append("Set BananaNameXYZ=99\n");
				conn.setClientInfo("BananaNameXYZ", "99");
				readWarnings(conn.getWarnings());
				conn.clearWarnings();

				sb.append("Set { ClientHostname=2, ClientLibrary=3, ClientPid=4, ClientRemark=5, ClientBananaPQR=999 }\n");
				Properties toBeInserted = new Properties();
				toBeInserted.put("ClientHostname", "2");
				toBeInserted.put("ClientLibrary", "3");
				toBeInserted.put("ClientPid", "4");
				toBeInserted.put("ClientRemark", "5");
				toBeInserted.put("ClientBananaPQR", "999");
				conn.setClientInfo(toBeInserted);
				readWarnings(conn.getWarnings());
				conn.clearWarnings();

				sb.append("Checking the results\n");
				Properties foundValues = conn.getClientInfo();
				for (String name: known) {
					sb.append("- " + name + ": prop=" );
					String propValue = (String) foundValues.getOrDefault(name, "");
					sb.append("" + propValue);
					sb.append(", single=");
					String singleValue = conn.getClientInfo(name);
					sb.append("" + singleValue);
					if (propValue != null && !propValue.equals(singleValue))
						sb.append("   !!! DIFFERENT !!!");
					sb.append("\n");
				}

			}
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_ClientInfo",
				"Connecting\n" +
				"Fetching supported clientinfo properties\n" +
				"- ApplicationName was found\n" +
				"- ClientHostname was found\n" +
				"- ClientLibrary was found\n" +
				"- ClientPid was found\n" +
				"- ClientRemark was found\n" +
				"Check initial values.\n" +
				"- ApplicationName is not empty\n" +
				"- ClientHostname is not empty\n" +
				"- ClientLibrary is not empty\n" +
				"- ClientPid is not empty\n" +
				"- ClientRemark is empty\n" +
				"Calls to con.getClientInfo do not return references to the same Properties object\n" +
				"Set ApplicationName=1\n" +
				"Set BananaNameXYZ=99\n" +
				"Warning: java.sql.SQLWarning: unknown client info property: BananaNameXYZ\n" +
				"Set { ClientHostname=2, ClientLibrary=3, ClientPid=4, ClientRemark=5, ClientBananaPQR=999 }\n" +
				"Warning: java.sql.SQLWarning: unknown client info property: ClientBananaPQR\n" +
				"Checking the results\n" +
				"- ApplicationName: prop=1, single=1\n" +
				"- ClientHostname: prop=2, single=2\n" +
				"- ClientLibrary: prop=3, single=3\n" +
				"- ClientPid: prop=4, single=4\n" +
				"- ClientRemark: prop=5, single=5\n"
				);
	}

	// some private utility methods for showing table content and params meta data
	private void showTblContents(String tblnm) {
		final String query = "SELECT * FROM \"" + tblnm + "\"";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				sb.append("Table ").append(tblnm).append(" has ").append(rsmd.getColumnCount()).append(" columns:\n");
				for (int col = 1; col <= rsmd.getColumnCount(); col++) {
					sb.append("\t").append(rsmd.getColumnLabel(col));
				}
				sb.append("\n");
				while (rs.next()) {
					for (int col = 1; col <= rsmd.getColumnCount(); col++) {
						sb.append("\t").append(rs.getString(col));
					}
					sb.append("\n");
				}
			} else
				sb.append("failed to execute query: ").append(query).append("\n");
		} catch (SQLException e) {
			sb.append("showContents failed: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(stmt, rs);
	}

	private void showParams(PreparedStatement pstmt) {
		try {
			// testing and showing parameter meta data
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			sb.append("pmd. ").append(pmd.getParameterCount()).append(" parameters:\n");
			for (int parm = 1; parm <= pmd.getParameterCount(); parm++) {
				sb.append("Param ").append(parm).append("\n");
				int nullable = pmd.isNullable(parm);
				sb.append("  nullable  ").append(nullable).append(" (");
				switch (nullable) {
					case ParameterMetaData.parameterNoNulls:	sb.append("NO"); break;
					case ParameterMetaData.parameterNullable:	sb.append("YA"); break;
					case ParameterMetaData.parameterNullableUnknown:	sb.append("UNKNOWN"); break;
					default: sb.append("INVALID ").append(nullable); break;
				}
				sb.append(")\n");
				sb.append("  signed    ").append(pmd.isSigned(parm)).append("\n");
				sb.append("  precision ").append(pmd.getPrecision(parm)).append("\n");
				sb.append("  scale     ").append(pmd.getScale(parm)).append("\n");
				sb.append("  type      ").append(pmd.getParameterType(parm)).append("\n");
				sb.append("  typename  ").append(pmd.getParameterTypeName(parm)).append("\n");
				sb.append("  classname ").append(pmd.getParameterClassName(parm)).append("\n");
				int mode = pmd.getParameterMode(parm);
				sb.append("  mode      ").append(mode).append(" (");
				switch (mode) {
					case ParameterMetaData.parameterModeIn:	sb.append("IN"); break;
					case ParameterMetaData.parameterModeInOut:	sb.append("INOUT"); break;
					case ParameterMetaData.parameterModeOut:	sb.append("OUT"); break;
					case ParameterMetaData.parameterModeUnknown:	sb.append("UNKNOWN"); break;
					default: sb.append("INVALID ").append(mode); break;
				}
				sb.append(")\n");
			}
		} catch (SQLException e) {
			sb.append("showParams() FAILED: ").append(e.getMessage()).append("\n");
		}
	}

	private void readExceptions(SQLException e) {
		while (e != null) {
			sb.append("Exception: ").append(e.toString()).append("\n");
			e = e.getNextException();
		}
	}

	private void readWarnings(SQLWarning w) {
		while (w != null) {
			sb.append("Warning: ").append(w.toString()).append("\n");
			w = w.getNextWarning();
		}
	}

	private void compareExpectedOutput(String testname, String expected) {
		final String produced = sb.toString();
		if (!expected.equals(produced)) {
			foundDifferences = true;
			System.err.print("Test '");
			System.err.print(testname);
			if (!testname.endsWith(")") && !testname.endsWith(";"))
				System.err.print("()");
			System.err.println("' produced different output!");
			int expLen = expected.length();
			int prodLen = produced.length();
			if (expLen > 0 && prodLen > 0) {
				int max_pos = expLen;
				if (prodLen > max_pos)
					max_pos = prodLen;
				int line = 1;
				int rowpos = 0;
				for (int pos = 0; pos < max_pos; pos++) {
					char a = (pos < expLen ? expected.charAt(pos) : '~');
					char b = (pos < prodLen ? produced.charAt(pos) : '~');
					if (a == '\n') {
						line++;
						rowpos = 0;
					} else {
						rowpos++;
					}
					if (a != b) {
						if (pos + 40 < expLen)
							expLen = pos + 40;
						if (pos + 40 < prodLen)
							prodLen = pos + 40;
						System.err.println("Difference found at line " + line + " position " + rowpos
							+ ". Expected:\n\"" + expected.substring(pos < expLen ? pos : expLen-1, expLen-1)
							+ "\"\nFound:\n\"" + produced.substring(pos < prodLen ? pos : prodLen-1, prodLen-1) + "\"");
						pos = max_pos;
					}
				}
			}
			System.err.println();
			System.err.println("---- Full Output: ------------");
			System.err.println(sb);
			System.err.println("---- END ---------------------");
			System.err.println("---- Expected Output: --------");
			System.err.println(expected);
			System.err.println("---- END ---------------------");
			System.err.println();
		}
		if (sb.length() > sbInitLen) {
			System.err.println("Test '" + testname
				+ "' produced output > " + sbInitLen
				+ " chars! Enlarge sbInitLen to: " + sb.length());
		}
	}

	private void closeConx(Connection cn) {
		if (cn != null) {
			try {
				cn.close();
			} catch (SQLException e) { /* ignore */ }
		}
	}

	private void closeStmtResSet(Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) { /* ignore */ }
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) { /* ignore */ }
		}
	}
}
