package org.monetdb.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.monetdb.testinfra.CloseOnFailure;
import org.monetdb.testinfra.Config;
import org.monetdb.testinfra.MonetVersionNumber;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

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
	String[] serverURLs; // used by testDriverProperties
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
			serverURLs = new String[] { Config.getServerURL() };
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

	@ParameterizedTest
	@ValueSource(strings = {"jdbc:monetdb:///demo", "jdbc:monetdbs:///demo", "jdbc:monetdb:",})
	@FieldSource("serverURLs")
	public void testDriverProperties(String url) throws SQLException {
		// NOTE the previous version of this test called getDriverPropertyInfo
		// for the test database URL (which could be anything) and checked
		// that all properties came out in an exact order and with specific
		// values and doc strings.
		//
		// Nowadays, the parameters are all centralized in enum org.monetdb.mcl.net.Parameter
		// and their default values and documentation can simply be inspected there.
		// We do however want to check that some properties are always present,
		// never present, etc.

		assertTrue(url.startsWith("jdbc:monetdb:") || url.startsWith("jdbc:monetdbs:"));

		Driver driver = DriverManager.getDriver(url);
		HashMap<String, DriverPropertyInfo> props = new HashMap<>();
		for (DriverPropertyInfo entry : driver.getPropertyInfo(url, null)) {
			props.put(entry.name, entry);
		}

		// A nontrivial number of properties must be returned and they must
		// be properly filled in
		int nprops = props.size();
		assertTrue(nprops >= 15, "found " + nprops + " driver properties");
		for (DriverPropertyInfo entry : props.values()) {
			assertNotNull(entry.description, entry.name);
			assertNotNull(entry.value, entry.name);
			assertFalse(entry.description.isEmpty(), entry.name);
		}

		// Some properties must always be present. This is not an exhaustive list.
		for (String required : new String[]{ //
				"host", "port", "database", //
				"user", "password", //
				"autocommit", "timezone", "replysize" //
		}) {
			assertTrue(props.containsKey(required), required);
		}

		// Boolean properties have a choices list. Above we checked that autocommit is present.
		assertEquals(2, props.get("autocommit").choices.length);

		// Some properties exist as Parameters but are not exposed to the end user.
		assertFalse(props.containsKey("sock"));
		assertFalse(props.containsKey("sockdir"));
		assertFalse(props.containsKey("fetchsize"));
		assertFalse(props.containsKey("language"));
		assertFalse(props.containsKey("hash"));
		assertFalse(props.containsKey("tableschema"));
		assertFalse(props.containsKey("table"));
		assertFalse(props.containsKey("clientkey"));
		assertFalse(props.containsKey("clientcert"));

		// TLS-related properties must be omitted if this is a jdbc:monetdb:// URL.
		// They must be included if this is a jdbc:monetdbs:// URL or if
		// the URL is exactly "jdbc:monetdb:". In the latter case, TLS use is determined
		// by the tls property instead of the URL schema so the property must be
		// included.
		boolean expectTLS = !url.startsWith("jdbc:monetdb:/");

		assertEquals(expectTLS, props.containsKey("tls"));
		assertEquals(expectTLS, props.containsKey("cert"));
		assertEquals(expectTLS, props.containsKey("certhash"));
	}

	@Test
	public void testEmptySQL() throws SQLException {
		String msg = "Missing SQL statement";

		assertSQLException(msg, () -> stmt.execute(""));
		assertSQLException(msg, () -> stmt.execute(null));

		assertSQLException(msg, () -> stmt.executeUpdate(""));
		assertSQLException(msg, () -> stmt.executeUpdate(null));

		assertSQLException(msg, () -> stmt.executeLargeUpdate(""));
		assertSQLException(msg, () -> stmt.executeLargeUpdate(null));

		assertSQLException(msg, () -> stmt.addBatch(""));
		assertSQLException(msg, () -> stmt.addBatch(null));

		assertSQLException(msg, () -> stmt.executeQuery(""));
		assertSQLException(msg, () -> stmt.executeQuery(null));

		assertSQLException(msg, () -> conn.prepareStatement(""));
		assertSQLException(msg, () -> conn.prepareStatement(null));

		assertSQLException(msg, () -> conn.prepareStatement("", Statement.RETURN_GENERATED_KEYS));
		assertSQLException(msg, () -> conn.prepareStatement(null, Statement.RETURN_GENERATED_KEYS));

		assertSQLException(msg, () -> conn.prepareCall(""));
		assertSQLException(msg, () -> conn.prepareCall(null));
	}

	@Test
	public void testFetchSize() throws SQLException {
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM tables")) {
			assertEquals(250, stmt.getFetchSize());
			assertEquals(250, rs.getFetchSize());
			stmt.setFetchSize(40);
			rs.setFetchSize(16384);
			assertEquals(40, stmt.getFetchSize());
			assertEquals(16384, rs.getFetchSize());
		}
	}
}
