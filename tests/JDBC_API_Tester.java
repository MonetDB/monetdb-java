/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;
import java.io.StringReader;
import java.nio.charset.Charset;

/**
 * class to test JDBC Driver API methods and behavior of MonetDB server.
 *
 * It combines 30+ tests which were previous individual test programs
 * into one large test program, reusing the connection.
 * This speeds up testing considerably as the overhead of starting a JVM and
 * loading the java test program class and MonetDB JDBC driver is now reduced
 * to only one time instead of 30+ times.
 * Also all output is no longer send to system out/err but collected in a StringBuilder.
 * The contents of it is compared with the expected output at the end of each test.
 * Only when it deviates the output is sent to system out, see compareExpectedOutput().
 *
 * @author Martin van Dinther
 * @version 0.1
 */
final public class JDBC_API_Tester {
	StringBuilder sb;	// buffer to collect the test output
	Connection con;	// main connection shared by all tests

	public static void main(String[] args) throws Exception {
		String con_URL = args[0];

		JDBC_API_Tester jt = new JDBC_API_Tester();
		jt.sb = new StringBuilder(4200);
		jt.con = DriverManager.getConnection(con_URL);
		// we are now connected

		// run the tests
		jt.Test_Cautocommit(con_URL);
		jt.Test_CisValid();
		jt.Test_Clargequery();
		jt.Test_Cmanycon(con_URL);
		jt.Test_Creplysize();
		jt.Test_Csavepoints();
		jt.Test_Ctransaction();
		jt.Test_Dobjects();
		jt.Test_FetchSize();
		jt.Test_PSgeneratedkeys();
		jt.Test_PSgetObject();
		jt.Test_PSlargebatchval();
		jt.Test_PSlargeresponse(con_URL);
		jt.Test_PSmanycon(con_URL);
		jt.Test_PSmetadata();
		jt.Test_PSsomeamount();
		jt.Test_PSsqldata();
		jt.Test_PStimedate();
		jt.Test_PStimezone();
		jt.Test_PStypes();
		jt.Test_CallableStmt();
		jt.Test_Rbooleans();
		jt.Test_Rmetadata();
		jt.Test_Rpositioning();
		jt.Test_Rsqldata();
		jt.Test_Rtimedate();
		jt.Test_Sbatching();
		jt.Test_Smoreresults();
		jt.Test_Wrapper();

		jt.closeConx(jt.con);
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
			sb.append("Expected error: " + e).append("\n");
			try {
				// test calling conn.isValid()
				sb.append("Validating connection: con.isValid? " + con.isValid(30));
				// Can we rollback on this connection without causing an error?
				con.rollback();
			} catch (SQLException e2) {
				sb.append("UnExpected error: " + e2);
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
			sb.append("0. true\t" + con.getAutoCommit()).append("\n");
			stmt = con.createStatement();

			// sending big script with many simple queries
			sb.append("1. executing script").append("\n");
			stmt.execute(bigq.toString());

			int i = 1;	// we skip the first "getResultSet()"
			while (stmt.getMoreResults() != false) {
				i++;
			}
			if (stmt.getUpdateCount() != -1) {
				sb.append("Error: found an update count for a SELECT query").append("\n");
			}
			if (i != size) {
				sb.append("Error: expecting " + size + " tuples, only got " + i).append("\n");
			}
			sb.append("2. queries processed").append("\n");
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
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
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
			sb.append("0. true\t" + con.getAutoCommit()).append("\n");

			stmt1 = con.createStatement();
			// test commit by checking if a change is visible in another connection
			sb.append("1. create... ");
			stmt1.executeUpdate("CREATE TABLE table_Test_Creplysize ( id int )");
			sb.append("passed").append("\n");

			sb.append("2. populating with 21 records... ");
			for (int i = 0; i < 21; i++)
				stmt1.executeUpdate("INSERT INTO table_Test_Creplysize (id) values (" + (i + 1) + ")");
			sb.append("passed").append("\n");

			sb.append("3. hinting the driver to use fetchsize 10... ");
			stmt1.setFetchSize(10);
			sb.append("passed").append("\n");

			sb.append("4. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			int i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 21) {
				sb.append("passed");
			} else {
				sb.append("got " + i + " records!!!");
			}
			sb.append("\n");

			sb.append("5. resetting driver fetchsize hint... ");
			stmt1.setFetchSize(0);
			sb.append("passed").append("\n");

			sb.append("6. instructing the driver to return at max 10 rows...  ");
			stmt1.setMaxRows(10);
			sb.append("passed").append("\n");

			sb.append("7. selecting all values...  ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 10) {
				sb.append("passed");
			} else {
				sb.append("got " + i + " records!!!");
			}
			sb.append("\n");

			sb.append("8. hinting the driver to use fetchsize 5... ");
			stmt1.setFetchSize(5);
			sb.append("passed").append("\n");

			sb.append("9. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next())
				i++;
			rs.close();
			if (i == 10) {
				sb.append("passed");
			} else {
				sb.append("got " + i + " records!!!");
			}
			sb.append("\n");

			sb.append("10. drop... ");
			stmt1.executeUpdate("DROP TABLE table_Test_Creplysize");
			sb.append("passed").append("\n");

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
			sb.append("0. true\t" + con.getAutoCommit()).append("\n");

			// savepoints require a non-autocommit connection
			try {
				sb.append("1. savepoint...");
				con.setSavepoint();
				sb.append("passed !!");
			} catch (SQLException e) {
				sb.append("expected msg: " + e.getMessage());
			}
			sb.append("\n");

			con.setAutoCommit(false);
			// >> true: auto commit should be on by default
			sb.append("0. false\t" + con.getAutoCommit()).append("\n");

			sb.append("2. savepoint...");
			/* make a savepoint, and discard it */
			con.setSavepoint();
			sb.append("passed").append("\n");

			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE table_Test_Csavepoints ( id int, PRIMARY KEY (id) )");

			sb.append("3. savepoint...");
			Savepoint sp2 = con.setSavepoint("empty table");
			sb.append("passed").append("\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			int i = 0;
			int items = 0;
			sb.append("4. table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (" + i + ")");
			}
			sb.append(" passed").append("\n");

			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (1)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (2)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (3)");

			sb.append("5. savepoint...");
			Savepoint sp3 = con.setSavepoint("three values");
			sb.append("passed").append("\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			sb.append("6. table " + items + " items");
			while (rs.next()) {
				sb.append(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (" + i + ")");
			}
			sb.append(" passed").append("\n");

			sb.append("7. release...");
			con.releaseSavepoint(sp3);
			sb.append("passed").append("\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			sb.append("8. table " + items + " items");
			while (rs.next()) {
				sb.append(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (" + i + ") :(");
			}
			sb.append(" passed").append("\n");

			sb.append("9. rollback...");
			con.rollback(sp2);
			sb.append("passed").append("\n");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 0;
			sb.append("10. table " + items + " items");
			while (rs.next()) {
				sb.append(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				sb.append(" FAILED (" + i + ") :(");
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
			sb.append("failed as expected: " + e.getMessage());
		}
		sb.append("\n");

		try {
			// turn off auto commit
			con.setAutoCommit(false);
			// >> false: we just disabled it
			sb.append("2. false\t" + con.getAutoCommit()).append("\n");

			// a change would not be visible now
			sb.append("3. commit...");
			con.commit();
			sb.append("passed").append("\n");

			sb.append("4. commit...");
			con.commit();
			sb.append("passed").append("\n");

			sb.append("5. rollback...");
			con.rollback();
			sb.append("passed");
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage());
		}
		sb.append("\n");

		Statement stmt = null;
		try {
			// turn off auto commit
			con.setAutoCommit(true);
			// >> false: we just disabled it
			sb.append("6. true\t" + con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("7. start transaction...");
			stmt.executeUpdate("START TRANSACTION");
			sb.append("passed").append("\n");

			sb.append("8. commit...");
			con.commit();
			sb.append("passed").append("\n");

			sb.append("9. true\t" + con.getAutoCommit());
			sb.append("\n");

			sb.append("10. start transaction...");
			stmt.executeUpdate("START TRANSACTION");
			sb.append("passed").append("\n");

			sb.append("11. rollback...");
			con.rollback();
			sb.append("passed").append("\n");

			sb.append("12. true\t" + con.getAutoCommit());
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
			sb.append("failed as expected: " + e.getMessage());
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

	private void Test_Dobjects() {
		sb.setLength(0);	// clear the output log buffer

		try {
			DatabaseMetaData dbmd = con.getMetaData();

			// inspect the catalog by use of dbmd functions
			compareResultSet(dbmd.getCatalogs(), "getCatalogs()",
			"Resultset with 1 columns\n" +
			"TABLE_CAT\n");

			compareResultSet(dbmd.getSchemas(null, "sys"), "getSchemas(null, sys)",
			"Resultset with 2 columns\n" +
			"TABLE_SCHEM	TABLE_CATALOG\n" +
			"sys	null\n");

			compareResultSet(dbmd.getTables(null, "tmp", null, null), "getTables(null, tmp, null, null)",	// schema tmp has 6 tables
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"null	tmp	_columns	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	_tables	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	idxs	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	keys	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	objects	SYSTEM TABLE	null	null	null	null	null	null\n" +
			"null	tmp	triggers	SYSTEM TABLE	null	null	null	null	null	null\n");

			compareResultSet(dbmd.getTables(null, "sys", "schemas", null), "getTables(null, sys, schemas, null)",
			"Resultset with 10 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION\n" +
			"null	sys	schemas	SYSTEM TABLE	null	null	null	null	null	null\n");

			compareResultSet(dbmd.getColumns(null, "sys", "table_types", null), "getColumns(null, sys, table_types, null)",
			"Resultset with 24 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE	IS_AUTOINCREMENT	IS_GENERATEDCOLUMN\n" +
			"null	sys	table_types	table_type_id	5	smallint	16	0	0	2	0	null	null	0	0	null	1	NO	null	null	null	null	NO	NO\n" +
			"null	sys	table_types	table_type_name	12	varchar	25	0	0	0	0	null	null	0	0	25	2	NO	null	null	null	null	NO	NO\n");

			compareResultSet(dbmd.getPrimaryKeys(null, "sys", "table_types"), "getPrimaryKeys(null, sys, table_types)",
			"Resultset with 6 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n" +
			"null	sys	table_types	table_type_id	1	table_types_table_type_id_pkey\n");

			compareResultSet(dbmd.getCrossReference(null, "sys", "tables", null, "sys", "table_types"), "getCrossReference(null, sys, tables, null, sys, table_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n");

			compareResultSet(dbmd.getImportedKeys(null, "sys", "table_types"), "getImportedKeys(null, sys, table_types)",
			"Resultset with 14 columns\n" +
			"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n");

			compareResultSet(dbmd.getIndexInfo(null, "sys", "key_types", false, false), "getIndexInfo(null, sys, key_types, false, false)",
			"Resultset with 13 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n" +
			"null	sys	key_types	false	null	key_types_key_type_id_pkey	2	1	key_type_id	null	3	0	null\n" +
			"null	sys	key_types	false	null	key_types_key_type_name_unique	2	1	key_type_name	null	3	0	null\n");

			compareResultSet(dbmd.getTablePrivileges(null, "sys", "table_types"), "getTablePrivileges(null, sys, table_types)",
			"Resultset with 7 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n" +
			"null	sys	table_types	monetdb	public	SELECT	NO\n");

			compareResultSet(dbmd.getColumnPrivileges(null, "sys", "table_types", null), "getColumnPrivileges(null, sys, table_types, null)",
			"Resultset with 8 columns\n" +
			"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n");

			compareResultSet(dbmd.getUDTs(null, "sys", null, null), "getUDTs(null, sys, null, null)",
			"Resultset with 7 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	CLASS_NAME	DATA_TYPE	REMARKS	BASE_TYPE\n" +
			"null	sys	inet	org.monetdb.jdbc.types.INET	2000	inet	null\n" +
			"null	sys	json	java.lang.String	2000	json	null\n" +
			"null	sys	url	org.monetdb.jdbc.types.URL	2000	url	null\n" +
			"null	sys	uuid	java.lang.String	2000	uuid	null\n");

			int[] UDTtypes = { Types.STRUCT, Types.DISTINCT };
			compareResultSet(dbmd.getUDTs(null, "sys", null, UDTtypes), "getUDTs(null, sys, null, UDTtypes",
			"Resultset with 7 columns\n" +
			"TYPE_CAT	TYPE_SCHEM	TYPE_NAME	CLASS_NAME	DATA_TYPE	REMARKS	BASE_TYPE\n");

			sb.setLength(0);	// clear the output log buffer
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_Dobjects", "");
	}

	private void compareResultSet(ResultSet rs, String methodnm, String expected) throws SQLException {
		sb.setLength(0);	// clear the output log buffer

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		sb.append("Resultset with ").append(columnCount).append(" columns").append("\n");
		for (int col = 1; col <= columnCount; col++) {
			if (col > 1)
				sb.append("\t");
			sb.append(rsmd.getColumnName(col));
		}
		sb.append("\n");
		while (rs.next()) {
			for (int col = 1; col <= columnCount; col++) {
				if (col > 1)
					sb.append("\t");
				sb.append(rs.getString(col));
			}
			sb.append("\n");
		}
		rs.close();

		compareExpectedOutput(methodnm, expected);
	}

	private void Test_FetchSize() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT * FROM _tables");

			sb.append("Statement fetch size before set: " + stmt.getFetchSize()).append("\n");
			sb.append("ResultSet fetch size before set: " + rs.getFetchSize()).append("\n");

			stmt.setFetchSize(40);
			rs.setFetchSize(16384);

			sb.append("Statement fetch size after set: " + stmt.getFetchSize()).append("\n");
			sb.append("ResultSet fetch size after set: " + rs.getFetchSize()).append("\n");

		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, rs);

		compareExpectedOutput("Test_FetchSize",
			"Statement fetch size before set: 0\n" +
			"ResultSet fetch size before set: 250\n" +
			"Statement fetch size after set: 40\n" +
			"ResultSet fetch size after set: 16384\n");
	}

	private void Test_PSgeneratedkeys() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			con.setAutoCommit(false);
			// >> false: auto commit was just switched off
			sb.append("0. false\t" + con.getAutoCommit()).append("\n");

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
			sb.append("success").append("\n");

			// now get the generated keys
			sb.append("2. getting generated keys...");
			keys = pstmt.getGeneratedKeys();
			if (keys == null) {
				sb.append("there are no keys!").append("\n");
			} else {
				while (keys.next()) {
					sb.append("generated key index: " + keys.getInt(1)).append("\n");
				}
				if (keys.getStatement() == null) {
					sb.append("ResultSet.getStatement() should never return null!").append("\n");
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
			sb.append("0. false\t" + con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. creating test table...");
			stmt.executeUpdate("CREATE TABLE table_Test_PSgetObject (ti tinyint, si smallint, i int, bi bigint)");
			sb.append("success").append("\n");
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
			sb.append(" passed").append("\n");

			sb.append("2b. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed").append("\n");
		} catch (SQLException e) {
			sb.append("FAILED to INSERT data: ").append(e.getMessage()).append("\n");
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		try {
			sb.append("3a. selecting records...");
			pstmt = con.prepareStatement("SELECT ti,si,i,bi FROM table_Test_PSgetObject ORDER BY ti,si,i,bi");
			rs = pstmt.executeQuery();
			sb.append(" passed").append("\n");

			while (rs.next()) {
				// test fix for https://www.monetdb.org/bugzilla/show_bug.cgi?id=4026
				Short ti = (Short) rs.getObject(1);
				Short si = (Short) rs.getObject(2);
				Integer i = (Integer) rs.getObject(3);
				Long bi = (Long) rs.getObject(4);

				sb.append("  Retrieved row data: ti=" + ti + " si=" + si + " i=" + i + " bi=" + bi).append("\n");
			}

			sb.append("3b. closing ResultSet...");
			rs.close();
			sb.append(" passed").append("\n");

			sb.append("3c. closing PreparedStatement...");
			pstmt.close();
			sb.append(" passed").append("\n");
		} catch (SQLException e) {
			sb.append("FAILED to RETRIEVE data: ").append(e.getMessage()).append("\n");
			while ((e = e.getNextException()) != null)
				sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}
		closeStmtResSet(pstmt, rs);

		try {
			sb.append("4. Rollback changes...");
			con.rollback();
			sb.append(" passed").append("\n");

			// restore default setting
			con.setAutoCommit(true);
		} catch (SQLException e) {
			sb.append("FAILED to rollback: ").append(e.getMessage()).append("\n");
		}

		compareExpectedOutput("Test_PSgetObject",
			"0. false	false\n" +
			"1. creating test table...success\n" +
			"2a. inserting 3 records as batch... passed\n" +
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
		String errorStr = new String(errorBytes, Charset.forName("UTF-8"));
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
			sb.append("0. true\t" + con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. creating test table...");
			stmt.execute("CREATE TABLE Test_PSlargebatchval (c INT, a CLOB, b DOUBLE)");
			sb.append("success").append("\n");

			sb.append("2. prepare insert...");
			pstmt = con.prepareStatement("INSERT INTO Test_PSlargebatchval VALUES (?,?,?)");
			sb.append("success").append("\n");

			pstmt.setLong(1, 1L);
			pstmt.setString(2, largeStr);
			pstmt.setDouble(3, 1.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("3. inserted 1 large string").append("\n");

			/* test issue reported at https://www.monetdb.org/bugzilla/show_bug.cgi?id=3470 */
			pstmt.setLong(1, -2L);
			pstmt.setClob(2, new StringReader(largeStr));
			pstmt.setDouble(3, -2.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("4. inserted 1 large clob via StringReader() object").append("\n");

			Clob myClob = con.createClob();
			myClob.setString(1L, largeStr);

			pstmt.setLong(1, 123456789L);
			pstmt.setClob(2, myClob);
			pstmt.setDouble(3, 12345678901.98765);
			pstmt.addBatch();
			pstmt.executeBatch();
			sb.append("5. inserted 1 large clob via createClob() object").append("\n");

			pstmt.close();

			sb.append("6. select count(*)... ");
			rs = stmt.executeQuery("SELECT COUNT(*) FROM Test_PSlargebatchval");
			if (rs.next())
				sb.append(rs.getInt(1) + " rows inserted.").append("\n");
			rs.close();

			sb.append("7. drop table...");
			stmt.execute("DROP TABLE Test_PSlargebatchval");
			sb.append("success").append("\n");
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
			DatabaseMetaData dbmd = con.getMetaData();
			if (conURL.startsWith(dbmd.getURL()))
				sb.append("oke");
			else
				sb.append("not oke " + dbmd.getURL());
			sb.append("\n");

			pstmt = con.prepareStatement("select * from columns");
			sb.append("2. empty call...");
			// should succeed (no arguments given)
			pstmt.execute();
			sb.append(" passed").append("\n");
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
				if (!rs.next())
					sb.append("ResultSet is empty");
				sb.append(" result: " + rs.getString(1));

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
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PSmetadata", "");
	}

	private void Test_PSsomeamount() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PSsomeamount", "");
	}

	private void Test_PSsqldata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PSsqldata", "");
	}

	private void Test_PStimedate() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PStimedate", "");
	}

	private void Test_PStimezone() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PStimezone", "");
	}

	private void Test_PStypes() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_PStypes", "");
	}

	private void Test_CallableStmt() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_CallableStmt", "");
	}

	private void Test_Rbooleans() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Rbooleans", "");
	}

	private void Test_Rmetadata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Rmetadata", "");
	}

	private void Test_Rpositioning() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Rpositioning", "");
	}

	private void Test_Rsqldata() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Rsqldata", "");
	}

	private void Test_Rtimedate() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Rtimedate", "");
	}

	private void Test_Sbatching() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			sb.append("FAILED: ").append(e.getMessage()).append("\n");
		}

		closeStmtResSet(stmt, null);

		compareExpectedOutput("Test_Sbatching", "");
	}

	private void Test_Smoreresults() {
		sb.setLength(0);	// clear the output log buffer

		Statement stmt = null;
		try {
			// >> true: auto commit should be on by default
			sb.append("0. true\t" + con.getAutoCommit()).append("\n");

			stmt = con.createStatement();
			sb.append("1. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				sb.append("more results on an unitialised Statement, how can that be?").append("\n");
			sb.append(" nope :)").append("\n");

			sb.append("2. SELECT 1...");
			if (stmt.execute("SELECT 1;") == false)
				sb.append("SELECT 1 returns update or no results").append("\n");
			sb.append(" ResultSet :)").append("\n");

			sb.append("3. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				sb.append("more results after SELECT 1 query, how can that be?").append("\n");
			sb.append(" nope :)").append("\n");

			sb.append("4. even more results?...");
			if (stmt.getMoreResults() != false)
				sb.append("still more results after SELECT 1 query, how can that be?").append("\n");
			sb.append(" nope :)").append("\n");

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

			sb.append("Auto commit is: " + con.getAutoCommit()).append("\n");

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
			checkIsWrapperFor("ResultSetMetaData", rsmd, monetdb_jdbc_pkg, "MonetResultSet$rsmdw");  // it is a private class of MonetResultSet
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
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetPreparedStatement$pmdw");  // it is a private class of MonetPreparedStatement
			checkIsWrapperFor("ParameterMetaData", pmd, jdbc_pkg, "Connection");
			checkIsWrapperFor("ParameterMetaData", pmd, monetdb_jdbc_pkg, "MonetConnection");

			ResultSetMetaData psrsmd = pstmt.getMetaData();
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, jdbc_pkg, "ResultSetMetaData");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetPreparedStatement");
			checkIsWrapperFor("PrepStmt ResultSetMetaData", psrsmd, monetdb_jdbc_pkg, "MonetPreparedStatement$rsmdw");  // it is a private class of MonetPreparedStatement
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
				"ResultSetMetaData. isWrapperFor(MonetResultSet$rsmdw) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
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
				"ParameterMetaData. isWrapperFor(MonetPreparedStatement$pmdw) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"ParameterMetaData. isWrapperFor(Connection) returns: false\n" +
				"ParameterMetaData. isWrapperFor(MonetConnection) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(ResultSetMetaData) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetPreparedStatement) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetPreparedStatement$rsmdw) returns: true	Called unwrap(). Returned object is not null, so oke\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(Connection) returns: false\n" +
				"PrepStmt ResultSetMetaData. isWrapperFor(MonetConnection) returns: false\n");
	}

	private void checkIsWrapperFor(String objnm, Wrapper obj, String pkgnm, String classnm) {
		try {
			Class<?> clazz = Class.forName(pkgnm + classnm);
			boolean isWrapper = obj.isWrapperFor(clazz);
			sb.append(objnm + ". isWrapperFor(" + classnm + ") returns: " + isWrapper);
			if (isWrapper) {
				Object wobj = obj.unwrap(clazz);
				sb.append("\tCalled unwrap(). Returned object is " + (wobj != null ? "not null, so oke" : "null !!"));
			}
			sb.append("\n");
		} catch (ClassNotFoundException cnfe) {
			sb.append(cnfe.toString());
		} catch (SQLException se) {
			sb.append(se.getMessage());
		}
	}

	private void compareExpectedOutput(String testname, String expected) {
		if (expected == null || sb == null) {
			System.out.println("Cannot compare for " + testname + ": expected == null || sb == null");
		} else
		if (!expected.equals(sb.toString())) {
			System.out.println("Test '" + testname + "()' produced different output! Expected:");
			System.out.println(expected);
			System.out.println("Gotten:");
			System.out.println(sb);
			System.out.println();
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
