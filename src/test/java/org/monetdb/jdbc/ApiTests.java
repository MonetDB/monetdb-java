package org.monetdb.jdbc;

import org.junit.jupiter.api.*;
import org.monetdb.testinfra.CloseOnFailure;
import org.monetdb.testinfra.Config;
import org.monetdb.testinfra.MonetVersionNumber;

import java.sql.*;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.monetdb.testinfra.Assertions.assertSQLException;

/**
 * Tests migrated from JDBC_API_Testser
 *
 * Uses PER_CLASS so the connection is re-used between tests.
 * The @CloseOnFailure annotation closes the connection if a test fails
 * so the next test doesn't run in an uncommited transaction etc.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("api")
@Tag("converted")
public class ApiTests {
	@CloseOnFailure
	Connection conn;
	MonetVersionNumber monetVersion;
	Statement stmt;
	PreparedStatement pstmt;

	// Skip all tests if the test database isn't running
	@BeforeAll
	public static void checkConnection() throws SQLException {
		DriverManager.getConnection(Config.getServerURL()).close();
	}

	// The @CloseOnFailure annotation closes 'conn' if tests fail.
	// Make sure the next test has a connection.
	@BeforeEach
	protected void ensureConnection() throws SQLException {
		if (conn == null || conn.isClosed()) {
			conn = newConnection();
			monetVersion = MonetVersionNumber.retrieve(conn);
		}
		stmt = conn.createStatement();
	}

	// After each test, check for abandoned transactions and unclosed
	// result sets
	@AfterEach
	protected void checkLingeringState() throws SQLException {
		if (conn == null || conn.isClosed()) {
			stmt = null;
			pstmt = null;
			return;
		}

		assertTrue(conn.getAutoCommit());

		if (monetVersion.serverCanQueryUnclosedResultSets()) {
			ArrayList<String> unclosed = new ArrayList<>();
			String query = "SELECT 'query_id=' || query_id || '/res_id=' || res_id FROM sys.unclosed_result_sets()";
			try (ResultSet rs = stmt.executeQuery(query)) {
				while (rs.next())
					unclosed.add(rs.getString(1));
			}
			String unclosedResultSets = String.join(", ", unclosed);
			assertEquals("", unclosedResultSets, "this test forgot to close one or more result sets");
		}

		stmt.close();
		stmt = null;
		if (pstmt != null) {
			pstmt.close();
			pstmt = null;
		}
	}

	@AfterAll
	protected void dropConnection() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException ignored) {}
		}
	}

	private Connection newConnection() throws SQLException {
		return DriverManager.getConnection(Config.getServerURL());
	}

	private String concatenateColumn(String sep, String query) throws SQLException {
		StringBuilder builder = new StringBuilder();
		boolean first = true;
		try (ResultSet rs = stmt.executeQuery(query)) {
			while (rs.next()) {
				if (!first)
					builder.append(sep);
				first = false;
				builder.append(rs.getString(1));
			}
		}
		return builder.toString();
	}

	private int queryInt(String query) throws SQLException {
		try (ResultSet rs = stmt.executeQuery(query)) {
			assertTrue(rs.next(), query);
			int result = rs.getInt(1);
			assertFalse(rs.next(), query);
			return result;
		}
	}

	@Test
	public void testAutocommit() throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS test_autocommit");

		try (Connection conn2 = newConnection(); Statement stmt2 = conn2.createStatement()) {

			assertTrue(conn.getAutoCommit());
			assertTrue(conn2.getAutoCommit());

			// conn1 creates it, conn2 sees it
			stmt.executeUpdate("CREATE TABLE test_autocommit ( id int )");
			stmt2.executeQuery("SELECT * FROM test_autocommit").close();

			conn.setAutoCommit(false);
			assertFalse(conn.getAutoCommit());
			assertTrue(conn2.getAutoCommit()); // still true
			conn2.setAutoCommit(false);
			assertFalse(conn2.getAutoCommit()); // now false

			// conn2 drops it, conn1 still sees it
			stmt2.executeUpdate("DROP TABLE test_autocommit");
			stmt.executeQuery("SELECT * FROM test_autocommit").close();
			// conn2 commits the drop, conn1 still doesn't notice
			conn2.commit();
			stmt.executeQuery("SELECT * FROM test_autocommit").close();
			// conn can even commit because it didn't change anything
			conn.commit();

			conn.setAutoCommit(true);
		}
	}

	@Test
	public void testIsValid() throws SQLException {
		// initially valid
		assertTrue(conn.isValid(30));

		// still valid after exception
		conn.setAutoCommit(false);
		assertSQLException("no such table", () -> stmt.executeQuery("SELECT COUNT(*) FROM doesnotexist"));
		assertTrue(conn.isValid(30));

		// leave clean connection
		conn.rollback();
		conn.setAutoCommit(true);
	}

	@Test
	public void testLargeQuery() throws SQLException {
		// Build a big sql script
		StringBuilder builder = new StringBuilder();
		int nrepetitions = 1234;
		for (int i = 0; i < nrepetitions; i++) {
			builder.append("SELECT\n");
			builder.append("-- When a query larger than the send buffer is being ");
			builder.append("sent, a deadlock situation can occur when the server writes ");
			builder.append("data back, blocking because we as client are sending as well ");
			builder.append("and not reading.  Hence, to avoid this deadlock, in JDBC a ");
			builder.append("separate thread is started in the background such that results ");
			builder.append("from the server can be read, while data is still being sent to ");
			builder.append("the server.  To test this, we need to trigger the SendThread ");
			builder.append("being started, which we do with a quite large query.  We ");
			builder.append("construct it by repeating some stupid query plus a comment ");
			builder.append("a lot of times.  And as you're guessing by now, you're reading ");
			builder.append("this stupid comment that we use :)\n");
			builder.append("1;\n");
		}

		stmt.execute(builder.toString());
		int count = 0;
		do {
			assertNotNull(stmt.getResultSet());
			count++;
		} while (stmt.getMoreResults());

		assertEquals(nrepetitions, count);
	}

	@Test
	public void testManyConnections() throws SQLException {
		Connection[] conns = new Connection[60];

		try {
			// connect them all
			for (int i = 0; i < conns.length; i++) {
				try {
					conns[i] = newConnection();
					conns[i].setAutoCommit(false);
				} catch (SQLException e) {
					fail("Caught exception while opening connection #" + i, e);
				}
			}

			// check and disconnect them all
			for (int i = 0; i < conns.length; i++) {
				try {
					conns[i].setAutoCommit(true); // do something with it
					conns[i].close();
					conns[i] = null;
				} catch (SQLException e) {
					fail("Caught exception while closing connection #" + i, e);
				}
			}
		} finally {
			// all connections must be closed even if errors happened
			for (int i = 0; i < conns.length; i++) {
				if (conns[i] != null) {
					try {
						conns[i].close();
					} catch (SQLException ignored) {}
				}
			}
		}
	}

	@Test
	public void testReplySize() throws SQLException {
		int rowCount;
		conn.setAutoCommit(false);

		// Create table with 21 rows
		stmt.executeUpdate("DROP TABLE IF EXISTS test_replysize");
		stmt.executeUpdate("CREATE TABLE test_replysize(i INT)");
		stmt.executeUpdate("INSERT INTO test_replysize SELECT * FROM sys.generate_series(0, 21)");
		assertEquals(21, queryInt("SELECT COUNT(*) FROM test_replysize"));

		rowCount = 0;
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_replysize")) {
			while (rs.next())
				rowCount++;
		}
		assertEquals(21, rowCount);

		// set fetchsize to 10
		stmt.setFetchSize(10);
		rowCount = 0;
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_replysize")) {
			while (rs.next())
				rowCount++;
		}
		assertEquals(21, rowCount);

		// maxrows and fetchsize + maxrows
		stmt.setFetchSize(0);
		stmt.setMaxRows(10);
		rowCount = 0;
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_replysize")) {
			while (rs.next())
				rowCount++;
		}
		assertEquals(10, rowCount);

		stmt.setFetchSize(5);
		rowCount = 0;
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_replysize")) {
			while (rs.next())
				rowCount++;
		}
		assertEquals(10, rowCount);

		conn.setAutoCommit(true);
	}

	@Test
	public void testSavepoints() throws SQLException {
		// savepoints not allowed in autocommit mode
		assertTrue(conn.getAutoCommit());
		assertSQLException("SAVEPOINT: not allowed in auto commit", () -> conn.setSavepoint());

		conn.setAutoCommit(false);
		Savepoint savepoint1 = conn.setSavepoint();
		assertNotNull(savepoint1);

		stmt.executeUpdate("CREATE TABLE test_savepoints(i INT)");
		Savepoint savepoint2 = conn.setSavepoint("empty table");
		assertNotNull(savepoint2);

		assertEquals("", concatenateColumn(",", "SELECT i FROM test_savepoints"));
		stmt.executeUpdate("INSERT INTO test_savepoints VALUES (1), (2), (3)");
		Savepoint savepoint3 = conn.setSavepoint("three values");
		assertNotNull(savepoint3);
		assertEquals("1,2,3", concatenateColumn(",", "SELECT i FROM test_savepoints"));

		conn.releaseSavepoint(savepoint3);
		assertEquals("1,2,3", concatenateColumn(",", "SELECT i FROM test_savepoints"));

		conn.rollback(savepoint2);
		assertEquals("", concatenateColumn(",", "SELECT i FROM test_savepoints"));

		conn.rollback();
		conn.setAutoCommit(true);
	}

	@Test
	public void testAutocommitTransaction() throws SQLException {
		// NOTE: this whole test seems to be just about autcommit / commit and rollback.
		// There are no data changes.

		// commit fails in autocommit mode
		assertSQLException("not allowed in auto commit mode", () -> conn.commit());

		// it succeeds when autocommit is off
		conn.setAutoCommit(false);
		assertFalse(conn.getAutoCommit());
		conn.commit();
		// twice, even (??)
		conn.commit();
		// and so does rollback
		conn.rollback();

		// now we turn autocommit back on and start a transaction manually
		conn.setAutoCommit(true);
		assertTrue(conn.getAutoCommit());
		stmt.executeUpdate("START TRANSACTION");

		// the jdbc driver realizes that autocommit is now off
		// (NOTE: this check was not in the original test)
		assertFalse(conn.getAutoCommit());

		conn.rollback();
		assertTrue(conn.getAutoCommit());

		assertSQLException("not allowed in auto commit mode", () -> conn.commit());
	}
}

