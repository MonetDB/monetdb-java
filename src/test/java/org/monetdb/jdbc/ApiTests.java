package org.monetdb.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.monetdb.testinfra.CloseOnFailure;
import org.monetdb.testinfra.Config;
import org.monetdb.testinfra.MonetVersionNumber;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
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

	@Test
	public void testInt128() throws SQLException {
		// skip if server does not have huge
		boolean serverHasHuge = 1 == queryInt("SELECT COUNT(*) FROM sys.types where sqlname = 'hugeint'");
		assumeTrue(serverHasHuge);

		stmt.executeUpdate("DROP TABLE IF EXISTS test_huge_int");
		stmt.executeUpdate("CREATE TABLE test_huge_int (i HUGEINT)");
		stmt.executeUpdate("DROP TABLE IF EXISTS test_huge_dec");
		stmt.executeUpdate("CREATE TABLE test_huge_dec (d DECIMAL(38,19))");

		BigInteger bi = new BigInteger("123456789012345678909876543210987654321");
		BigDecimal bd = new BigDecimal("1234567890123456789.9876543210987654321");

		// Insert huge int using prepared statement
		String insertQuery = "INSERT INTO test_huge_int VALUES (?)";
		try (PreparedStatement ps = conn.prepareStatement(insertQuery)) {
			ps.setBigDecimal(1, new BigDecimal(bi));
			ps.executeUpdate();
		}

		// insert huge decimal using string interpolation
		stmt.executeUpdate("INSERT INTO test_huge_dec VALUES (" + bd + ");");

		// extract them
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_huge_int")) {
			rs.next();
			BigInteger i = rs.getBigDecimal(1).toBigInteger();
			assertEquals(bi, i);
		}
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_huge_dec")) {
			rs.next();
			BigDecimal d = rs.getBigDecimal(1);
			assertEquals(bd, d);
		}

		stmt.executeUpdate("DROP TABLE test_huge_int");
		stmt.executeUpdate("DROP TABLE test_huge_dec");
	}

	@TestFactory
	public ArrayList<DynamicTest> testIntervalTypes() {
		ArrayList<DynamicTest> tests = new ArrayList<>();

		tests.add(DynamicTest.dynamicTest("interval year",
				() -> verifyIntervalType("interval year", 10, 0, 4, 4, "java.lang.Integer")));
		tests.add(DynamicTest.dynamicTest("interval month",
				() -> verifyIntervalType("interval month", 10, 0, 6, 4, "java.lang.Integer")));
		tests.add(DynamicTest.dynamicTest("interval day",
				() -> verifyIntervalType("interval day", 9, 0, 9, 2, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval hour",
				() -> verifyIntervalType("interval hour", 11, 3, 11, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval minute",
				() -> verifyIntervalType("interval minute", 13, 3, 13, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval second",
				() -> verifyIntervalType("interval second", 15, 3, 15, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval year to month",
				() -> verifyIntervalType("interval year to month", 10, 0, 6, 4, "java.lang.Integer")));
		tests.add(DynamicTest.dynamicTest("interval day to hour",
				() -> verifyIntervalType("interval day to hour", 11, 3, 11, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval day to minute",
				() -> verifyIntervalType("interval day to minute", 13, 3, 13, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval day to second",
				() -> verifyIntervalType("interval day to second", 15, 3, 15, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval hour to minute",
				() -> verifyIntervalType("interval hour to minute", 13, 3, 13, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval hour to second",
				() -> verifyIntervalType("interval hour to second", 15, 3, 15, 3, "java.math.BigDecimal")));
		tests.add(DynamicTest.dynamicTest("interval minute to second",
				() -> verifyIntervalType("interval minute to second", 15, 3, 15, 3, "java.math.BigDecimal")));

		return tests;
	}

	private void verifyIntervalType(String tname, int prec, int scale, int width, int tnum, String className) throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS test_interval_type");
		stmt.executeUpdate("CREATE TABLE test_interval_type(c " + tname + ")");

		// verify regular statement result set metadata
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_interval_type")) {
			ResultSetMetaData md = rs.getMetaData();
			assertEquals(tname, md.getColumnTypeName(1));
			assertEquals(tnum, md.getColumnType(1));
			assertEquals(prec, md.getPrecision(1));
			assertEquals(scale, md.getScale(1));
			assertEquals(width, md.getColumnDisplaySize(1));
			assertEquals(className, md.getColumnClassName(1));
		}

		// verify prepared statement parameter- and result set metadata
		try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM test_interval_type WHERE c = ?")) {
			// Parameter
			ParameterMetaData pmd = ps.getParameterMetaData();
			assertEquals(tname, pmd.getParameterTypeName(1));
			assertEquals(tnum, pmd.getParameterType(1));
			assertEquals(prec, pmd.getPrecision(1));
			// Curiously, the parameter metadata always has scale 0
			assertEquals(0, pmd.getScale(1));
			assertEquals(className, pmd.getParameterClassName(1));
			// Result set
			ResultSetMetaData md = ps.getMetaData();
			assertEquals(tname, md.getColumnTypeName(1));
			assertEquals(tnum, md.getColumnType(1));
			assertEquals(prec, md.getPrecision(1));
			assertEquals(scale, md.getScale(1));
			assertEquals(width, md.getColumnDisplaySize(1));
			assertEquals(className, md.getColumnClassName(1));
		}
		stmt.executeUpdate("DROP TABLE test_interval_type");
	}

	@Test
	public void testLogicalPlan() throws SQLException {
		String keyword = monetVersion.planHasBecomeExplain() ? "EXPLAIN" : "PLAN";
		String query = keyword + " SELECT * FROM sys.generate_series(1, 10)";
		// EXPLAIN (PLAN) yields a one-column result set. One row describes a projection operation1
		try (ResultSet rs = stmt.executeQuery(query)) {
			ResultSetMetaData md = rs.getMetaData();
			assertEquals(1, md.getColumnCount());
			int colType = md.getColumnType(1);
			assertTrue(colType == Types.VARCHAR || colType == Types.CLOB, md.getColumnTypeName(1) + "=" + colType);
			while (rs.next()) {
				if (rs.getString(1).equals("project ("))
					return;
			}
			fail("Could not find substring <" + "project (" + "> in result set of query: " + query);
		}
	}

	@Test
	public void testExplain() throws SQLException {
		String keyword = monetVersion.planHasBecomeExplain() ? "EXPLAIN PHYSICAL" : "EXPLAIN";
		String query = keyword + " SELECT 42";
		// EXPLAIN PHYSICAL yields a one-column result set.
		// The first line is 'function user.main()', the last non-comment line is 'end user.main'
		try (ResultSet rs = stmt.executeQuery(query)) {
			ResultSetMetaData md = rs.getMetaData();
			assertEquals(1, md.getColumnCount());
			int colType = md.getColumnType(1);
			assertTrue(colType == Types.VARCHAR || colType == Types.CLOB, md.getColumnTypeName(1) + "=" + colType);
			String firstLine = null;
			String lastLine = null;
			while (rs.next()) {
				String line = rs.getString(1);
				if (line.startsWith("#"))
					continue;
				if (firstLine == null)
					firstLine = line;
				lastLine = line;
			}
			assertNotNull(lastLine); // implies same for firstLine
			assertTrue(firstLine.startsWith("function user.main"), firstLine);
			assertTrue(lastLine.startsWith("end user.main"), lastLine);
		}
	}

	@Test
	public void testTrace() throws SQLException {
		String query = "TRACE SELECT 42";
		ResultSet rs = null;  // used twice, once for the result set and once for the trace
		try {
			rs = stmt.executeQuery(query);

			// first the result set
			assertTrue(rs.next());
			assertEquals(42, rs.getInt(1));
			assertFalse(rs.next());

			if (monetVersion.planHasBecomeExplain()) {
				// Newer MonetDB's leave the trace in sys.tracelog
				assertFalse(stmt.getMoreResults());
				rs.close();    // is this necessary?
				rs = stmt.executeQuery("SELECT * FROM sys.tracelog");
			} else {
				// Older MonetDB's send the trace as a second result set
				assertTrue(stmt.getMoreResults());
				rs = stmt.getResultSet();
			}

			// Inspect the trace, is it really a trace?
			ResultSetMetaData md = rs.getMetaData();
			assertTrue(rs.next());
			String line = rs.getString(2);
			assertTrue(line.contains(":= querylog.define"), line);
			assertTrue(line.contains("select 42"), line);
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	@Test
	public void testDebug() throws SQLException {
		// DEBUG has not been supported by MonetDB for a while but the old jdbc test
		// suite had a test for it so we include it.
		//
		// The original test had the following comment:
		//     From Jun2023 we skip the comparison as it gives a different error msg on power8 platform: syntax error, unexpected IDENT in: "debug"
		// let's see what happens and adjust this test accordingly
		assertSQLException("unexpected IDENT", () -> stmt.executeQuery("DEBUG SELECT 42"));
	}

	@Test
	public void testGeneratedKeys() throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS psgenkey");
		stmt.executeUpdate("CREATE TABLE psgenkey (id SERIAL, val VARCHAR(20))");

		String ins = "INSERT INTO psgenkey(val) VALUES ('this is a test')";
		try (PreparedStatement ps = conn.prepareStatement(ins)) {
			ps.executeUpdate();
			ps.executeUpdate();
			ps.executeUpdate();
			int maxId = queryInt("SELECT MAX(id) FROM psgenkey");
			try (ResultSet rs = ps.getGeneratedKeys()) {
				// only yields the last one
				assertTrue(rs.next());
				int generatedKey = rs.getInt(1);
				assertFalse(rs.next());
				assertEquals(maxId, generatedKey);

				// While we're at it, test ResultSet#getStatement
				// (not sure why here, but that's ok)
				Statement parent = rs.getStatement();
				assertNotNull(parent);
				assertEquals(ps, parent);
			}
		}

		stmt.executeUpdate("DROP TABLE psgenkey");
	}

	@Test
	public void testGetIntObject() throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS test_getobject");
		stmt.executeUpdate("CREATE TABLE test_getobject (ti tinyint, si smallint, i int, bi bigint)");

		String ins = "INSERT INTO test_getobject(ti, si, i, bi) VALUES (?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(ins)) {
			ps.setShort(1, (short) 1);
			ps.setShort(2, (short) 1);
			ps.setInt(3, 1);
			ps.setLong(4, 1);
			ps.addBatch();

			ps.setShort(1, (short) 127);
			ps.setShort(2, (short) 12700);
			ps.setInt(3, 1270000);
			ps.setLong(4, 127000000);
			ps.addBatch();

			ps.setShort(1, (short) -127);
			ps.setShort(2, (short) -12700);
			ps.setInt(3, -1270000);
			ps.setLong(4, -127000000);
			ps.addBatch();

			ps.executeBatch();
		}

		try (ResultSet rs = stmt.executeQuery("SELECT ti, si, i, bi FROM test_getobject")) {
			assertTrue(rs.next());
			assertEquals((short) 1, (Short) rs.getObject(1));
			assertEquals((short) 1, (Short) rs.getObject(2));
			assertEquals(1, (Integer) rs.getObject(3));
			assertEquals((long) 1, (Long) rs.getObject(4));

			assertTrue(rs.next());
			assertEquals((short) 127, (Short) rs.getObject(1));
			assertEquals((short) 12700, (Short) rs.getObject(2));
			assertEquals(1270000, rs.getObject(3));
			assertEquals((long) 127000000, (Long) rs.getObject(4));

			assertTrue(rs.next());
			assertEquals((short) -127, (Short) rs.getObject(1));
			assertEquals((short) -12700, (Short) rs.getObject(2));
			assertEquals(-1270000, (Integer) rs.getObject(3));
			assertEquals((long) -127000000, (Long) rs.getObject(4));

			assertFalse(rs.next());
		}

		stmt.executeUpdate("DROP TABLE test_getobject");
	}

	@Test
	public void testLargeBatchValue() throws SQLException {
		/* test issue reported at https://github.com/MonetDB/MonetDB/issues/3470 */

		stmt.executeUpdate("DROP TABLE IF EXISTS test_largeval");
		stmt.executeUpdate("CREATE TABLE test_largeval(c INT, a CLOB, b DOUBLE)");

		// U+2027 Unicode name: HYPHENATION POINT
		byte[] errorBytes = new byte[]{(byte) 0xe2, (byte) 0x80, (byte) 0xa7};
		String errorStr = new String(errorBytes, UTF_8);
		StringBuilder repeatedErrorStr = new StringBuilder();
		for (int i = 0; i < 8170; i++) {
			repeatedErrorStr.append(errorStr);
		}
		String largeStr = repeatedErrorStr.toString();

		String ins = "INSERT INTO test_largeval VALUES (?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(ins)) {
			ps.setLong(1, 1L);
			ps.setString(2, largeStr);  // pass string directly
			ps.setDouble(3, 1.0);
			ps.addBatch();
			ps.executeBatch();

			ps.setLong(1, -2L);
			ps.setClob(2, new StringReader(largeStr));  // pass string as clob via reader
			ps.setDouble(3, -2.0);
			ps.addBatch();

			Clob myClob = conn.createClob();
			myClob.setString(1L, largeStr);
			ps.setLong(1, 123456789L);
			ps.setClob(2, myClob);
			ps.setDouble(3, 1245678901.98765);
			ps.addBatch();

			ps.executeBatch();
		}

		assertEquals(3, queryInt("SELECT COUNT(*) FROM test_largeval"));

		stmt.executeUpdate("DROP TABLE test_largeval");
	}

	@Test
	public void testManyConnectionsPrepared() throws SQLException {
		Connection[] conns = new Connection[60];
		PreparedStatement[] stmts = new PreparedStatement[conns.length];

		try {
			// connect them all
			for (int i = 0; i < conns.length; i++) {
				try {
					conns[i] = newConnection();
					stmts[i] = conns[i].prepareStatement("SELECT " + i);
				} catch (SQLException e) {
					fail("Caught exception while opening connection #" + i, e);
				}
			}

			// Check and disconnect them all
			// Occasionally force an error on another connection
			// to prove that doesn't affect other connections
			for (int i = 0; i < conns.length; i++) {
				try {
					try (ResultSet rs = stmts[i].executeQuery()) {
						assertTrue(rs.next());
						assertEquals(i, rs.getInt(1));
						assertFalse(rs.next());
					}
					stmts[i].close();
					conns[i].close();
					conns[i] = null;
				} catch (SQLException e) {
					fail("Caught exception while checking connection #" + i, e);
				}

				// Force an error on a throwaway connection.
				// Shouldn't affect ours.
				try (Connection c = newConnection(); Statement s = c.createStatement()) {
					s.execute("SELECT bad FROM FROM wrong");
					fail("expected the statement above to raise an exception");
				} catch (SQLException ignored) {}
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
	public void testPreparedStatementMetadata() throws SQLException {
		// Results depend on MonetDB version
		boolean supportsNestedTypes = false;
		String checkNested =
				"SELECT c.name\n" +
				"FROM sys.columns c, sys.tables t, sys.schemas s\n" +
				"WHERE c.name = 'multiset' AND t.name = '_columns' AND s.name = 'sys'\n" +
				"AND c.table_id = t.id AND t.schema_id = s.id\n";
		try (ResultSet rs = stmt.executeQuery(checkNested)) {
			supportsNestedTypes = rs.next();
		}

		// note the uppercase letters in the table name.
		// on retrieval they will be all lowercase.
		stmt.executeUpdate("DROP TABLE IF EXISTS table_Test_PSmetadata");
		stmt.executeUpdate("CREATE TABLE table_Test_PSmetadata ( myint int, mydouble double, mybool boolean, myvarchar varchar(15), myclob clob )");
		stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (NULL, NULL, NULL, NULL, NULL)");
		stmt.executeUpdate("INSERT INTO table_Test_PSmetadata VALUES (2 , 3.0, true, 'A string', 'bla bla bla')");

		String query = "SELECT CASE WHEN myint IS NULL THEN 0 ELSE 1 END AS intnull, * FROM table_Test_PSmetadata WHERE myint = ?";
		try (PreparedStatement ps = conn.prepareStatement(query)) {
			ResultSetMetaData md = ps.getMetaData();

			assertEquals("java.lang.Short", md.getColumnClassName(1));
			assertEquals("java.lang.Integer", md.getColumnClassName(2));
			assertEquals("java.lang.Double", md.getColumnClassName(3));
			assertEquals("java.lang.Boolean", md.getColumnClassName(4));
			assertEquals("java.lang.String", md.getColumnClassName(5));
			assertEquals("java.lang.String", md.getColumnClassName(6));

			assertEquals(3, md.getColumnDisplaySize(1));
			assertEquals(10, md.getColumnDisplaySize(2));
			assertEquals(15, md.getColumnDisplaySize(3));
			assertEquals(5, md.getColumnDisplaySize(4));
			assertEquals(15, md.getColumnDisplaySize(5));
			assertEquals(0, md.getColumnDisplaySize(6));

			assertEquals("intnull", md.getColumnLabel(1));
			assertEquals("myint", md.getColumnLabel(2));
			assertEquals("mydouble", md.getColumnLabel(3));
			assertEquals("mybool", md.getColumnLabel(4));
			assertEquals("myvarchar", md.getColumnLabel(5));
			assertEquals("myclob", md.getColumnLabel(6));

			assertEquals("intnull", md.getColumnName(1));
			assertEquals("myint", md.getColumnName(2));
			assertEquals("mydouble", md.getColumnName(3));
			assertEquals("mybool", md.getColumnName(4));
			assertEquals("myvarchar", md.getColumnName(5));
			assertEquals("myclob", md.getColumnName(6));

			assertEquals(-6, md.getColumnType(1));
			assertEquals(4, md.getColumnType(2));
			assertEquals(8, md.getColumnType(3));
			assertEquals(16, md.getColumnType(4));
			assertEquals(12, md.getColumnType(5));
			assertEquals(12, md.getColumnType(6));

			assertEquals("tinyint", md.getColumnTypeName(1));
			assertEquals("int", md.getColumnTypeName(2));
			assertEquals("double", md.getColumnTypeName(3));
			assertEquals("boolean", md.getColumnTypeName(4));
			assertEquals("varchar", md.getColumnTypeName(5));
			if (monetVersion.serverReturnsNewMetadata())
				assertEquals("varchar", md.getColumnTypeName(6));
			else
				assertEquals("clob", md.getColumnTypeName(6));

			assertEquals(3, md.getPrecision(1));
			assertEquals(10, md.getPrecision(2));
			assertEquals(15, md.getPrecision(3));
			assertEquals(1, md.getPrecision(4));
			assertEquals(15, md.getPrecision(5));
			assertEquals(0, md.getPrecision(6));

			assertEquals(0, md.getScale(1));
			assertEquals(0, md.getScale(2));
			assertEquals(0, md.getScale(3));
			assertEquals(0, md.getScale(4));
			assertEquals(0, md.getScale(5));
			assertEquals(0, md.getScale(6));

			assertNull(md.getCatalogName(1));
			assertNull(md.getCatalogName(2));
			assertNull(md.getCatalogName(3));
			assertNull(md.getCatalogName(4));
			assertNull(md.getCatalogName(5));
			assertNull(md.getCatalogName(6));

			String expectedSchemaName = supportsNestedTypes ? "sys" : "";
			assertEquals("", md.getSchemaName(1));
			assertEquals(expectedSchemaName, md.getSchemaName(2));
			assertEquals(expectedSchemaName, md.getSchemaName(3));
			assertEquals(expectedSchemaName, md.getSchemaName(4));
			assertEquals(expectedSchemaName, md.getSchemaName(5));
			assertEquals(expectedSchemaName, md.getSchemaName(6));

			assertEquals("", md.getTableName(1));
			assertEquals("table_test_psmetadata", md.getTableName(2));
			assertEquals("table_test_psmetadata", md.getTableName(3));
			assertEquals("table_test_psmetadata", md.getTableName(4));
			assertEquals("table_test_psmetadata", md.getTableName(5));
			assertEquals("table_test_psmetadata", md.getTableName(6));

			assertFalse(md.isAutoIncrement(1));
			assertFalse(md.isAutoIncrement(2));
			assertFalse(md.isAutoIncrement(3));
			assertFalse(md.isAutoIncrement(4));
			assertFalse(md.isAutoIncrement(5));
			assertFalse(md.isAutoIncrement(6));

			assertFalse(md.isCaseSensitive(1));
			assertFalse(md.isCaseSensitive(2));
			assertFalse(md.isCaseSensitive(3));
			assertFalse(md.isCaseSensitive(4));
			assertTrue(md.isCaseSensitive(5));
			assertTrue(md.isCaseSensitive(6));

			assertFalse(md.isCurrency(1));
			assertFalse(md.isCurrency(2));
			assertFalse(md.isCurrency(3));
			assertFalse(md.isCurrency(4));
			assertFalse(md.isCurrency(5));
			assertFalse(md.isCurrency(6));

			assertFalse(md.isDefinitelyWritable(1));
			assertFalse(md.isDefinitelyWritable(2));
			assertFalse(md.isDefinitelyWritable(3));
			assertFalse(md.isDefinitelyWritable(4));
			assertFalse(md.isDefinitelyWritable(5));
			assertFalse(md.isDefinitelyWritable(6));

			int expectedNullability = supportsNestedTypes
					? ResultSetMetaData.columnNullable /* == 1 */
					: ResultSetMetaData.columnNullableUnknown /* == 2 */
					;
			assertEquals(ResultSetMetaData.columnNullableUnknown, md.isNullable(1));
			assertEquals(expectedNullability, md.isNullable(2));
			assertEquals(expectedNullability, md.isNullable(3));
			assertEquals(expectedNullability, md.isNullable(4));
			assertEquals(expectedNullability, md.isNullable(5));
			assertEquals(expectedNullability, md.isNullable(6));

			assertTrue(md.isReadOnly(1));
			assertTrue(md.isReadOnly(2));
			assertTrue(md.isReadOnly(3));
			assertTrue(md.isReadOnly(4));
			assertTrue(md.isReadOnly(5));
			assertTrue(md.isReadOnly(6));

			assertTrue(md.isSearchable(1));
			assertTrue(md.isSearchable(2));
			assertTrue(md.isSearchable(3));
			assertTrue(md.isSearchable(4));
			assertTrue(md.isSearchable(5));
			assertTrue(md.isSearchable(6));

			assertTrue(md.isSigned(1));
			assertTrue(md.isSigned(2));
			assertTrue(md.isSigned(3));
			assertFalse(md.isSigned(4));
			assertFalse(md.isSigned(5));
			assertFalse(md.isSigned(6));

			assertFalse(md.isWritable(1));
			assertFalse(md.isWritable(2));
			assertFalse(md.isWritable(3));
			assertFalse(md.isWritable(4));
			assertFalse(md.isWritable(5));
			assertFalse(md.isWritable(6));

			// That was the result set metadata, now the parameter metadata

			ParameterMetaData pmd = ps.getParameterMetaData();
			assertEquals(ParameterMetaData.parameterNullableUnknown, pmd.isNullable(1));
			assertEquals(true, pmd.isSigned(1));
			assertEquals(10, pmd.getPrecision(1));
			assertEquals(0, pmd.getScale(1));
			assertEquals(Types.INTEGER, pmd.getParameterType(1));
			assertEquals("int", pmd.getParameterTypeName(1));
			assertEquals("java.lang.Integer", pmd.getParameterClassName(1));
			assertEquals(ParameterMetaData.parameterModeIn, pmd.getParameterMode(1));
		}

		stmt.executeUpdate("DROP TABLE table_Test_PSmetadata");
	}

	@Test
	public void testSetBytes() throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS test_setbytes");

		// Let's create with a prepared statement for a change
		String create = "CREATE TABLE test_setbytes(col1 CLOB, col2 BLOB, id SERIAL)";
		try (PreparedStatement ps = conn.prepareStatement(create)) {
			assertEquals(0, ps.getMetaData().getColumnCount());
			assertEquals(0, ps.getParameterMetaData().getParameterCount());
			ps.execute();
		}

		String[] testdata = {
				"0123456789abcdef",
				"~!@#$%^&*()_+`1-=][{}\\|';:,<.>/?",
				"\u00e0\u004f\u20f0\u0020\u00ea\u003a\u0069\u0010\u00a2\u00d8\u0008\u0001\u002b\u0030\u019c\u129e",
				"X\\Y"
		};

		// insert them
		String ins = "INSERT INTO test_setbytes(col1, col2) VALUES (?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(ins)) {
			assertEquals(0, ps.getMetaData().getColumnCount());
			assertEquals(2, ps.getParameterMetaData().getParameterCount());
			for (String val : testdata) {
				ps.setString(1, val);
				ps.setBytes(2, val.getBytes(UTF_8));
				ps.addBatch();
			}
			ps.executeBatch();
		}

		// retrieve and compare
		String sel = "SELECT col1, LENGTH(col1) AS lencol1, col2, LENGTH(col2) AS lencol2 FROM test_setbytes ORDER BY id";
		try (PreparedStatement ps = conn.prepareStatement(sel)) {
			assertEquals(4, ps.getMetaData().getColumnCount());
			assertEquals(0, ps.getParameterMetaData().getParameterCount());
			try (ResultSet rs = ps.executeQuery()) {
				assertEquals(4, rs.getMetaData().getColumnCount());

				for (String val : testdata) {
					assertTrue(rs.next());

					byte[] bin = val.getBytes(UTF_8);
					String hex = toUppercaseHexDigits(bin);
					assertEquals(val, rs.getString("col1"));
					assertEquals(val.length(), rs.getInt("lencol1"));
					assertArrayEquals(bin, rs.getBytes("col2"));
					assertEquals(hex, rs.getString("col2"));
					assertEquals(bin.length, rs.getInt("lencol2"));
				}
				assertFalse(rs.next());
			}
		}

		stmt.executeUpdate("DROP TABLE test_setbytes");
	}

	private String toUppercaseHexDigits(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		String digits = "0123456789ABCDEF";
		for (byte b : bytes) {
			int hi = b & 0xF0;
			int lo = b & 0x0F;
			sb.append(digits.charAt(hi >> 4));
			sb.append(digits.charAt(lo));
		}
		return sb.toString();
	}

	@Test
	public void testBackslashes() throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS test_backslashes");
		stmt.executeUpdate("CREATE TABLE test_backslashes(t VARCHAR(20))");


		// We have a string with a SINGLE backslash in the middle
		String val = "X\\Y";
		assertEquals(val.length(), 3);
		assertEquals(val.charAt(0), 'X');
		assertEquals(val.charAt(1), '\\');
		assertEquals(val.charAt(2), 'Y');

		// We insert it into our table
		String ins = "INSERT INTO test_backslashes VALUES (?)";
		try (PreparedStatement ps = conn.prepareStatement(ins)) {
			ps.setString(1, val);
			ps.execute();
		}

		// We expect to get it back with the backslash included
		String query = "SELECT t FROM test_backslashes";
		try (ResultSet rs = stmt.executeQuery(query)) {
			assertTrue(rs.next());
			assertEquals(val, rs.getString(1));
			assertFalse(rs.next());
		}

		stmt.executeUpdate("DROP TABLE test_backslashes");
	}

	// Converted from Test_PSsqldata() and Test_Rsqldata().
	// These also tested the inet types but those have their
	// own dedicated tests now.
	@Test
	public void testURLType() throws Exception {
		org.monetdb.jdbc.types.URL turl = new org.monetdb.jdbc.types.URL();
		// Note: the following method is declared to throw Exception
		turl.fromString("http://www.monetdb.org/");

		conn.setAutoCommit(false);
		try {
			stmt.execute("DROP TABLE IF EXISTS urltest");
			stmt.execute("CREATE TABLE urltest(myurl URL)");
			String insert = "INSERT INTO urltest VALUES (?)";
			try (PreparedStatement ps = conn.prepareStatement(insert)) {
				ParameterMetaData pmd = ps.getParameterMetaData();
				assertEquals(1, pmd.getParameterCount());
				assertEquals(Types.VARCHAR, pmd.getParameterType(1));
				assertEquals("url", pmd.getParameterTypeName(1));
				assertEquals("org.monetdb.jdbc.types.URL", pmd.getParameterClassName(1));

				ps.setObject(1, turl);
				ps.execute();
			}
			String select = "SELECT * FROM urltest";
			try (ResultSet rs = stmt.executeQuery(select)) {
				ResultSetMetaData rmd = rs.getMetaData();
				assertEquals("org.monetdb.jdbc.types.URL", rmd.getColumnClassName(1));
				assertEquals(null, rmd.getCatalogName(1));
				assertEquals("sys", rmd.getSchemaName(1));
				assertEquals("urltest", rmd.getTableName(1));
				assertEquals("myurl", rmd.getColumnName(1));

				assertTrue(rs.next());
				Object obj = rs.getObject(1);
				assertEquals("http://www.monetdb.org/", obj.toString());
				org.monetdb.jdbc.types.URL url = (org.monetdb.jdbc.types.URL) obj;
				assertEquals("http://www.monetdb.org/", url.toString());
			}
		} finally {
			conn.rollback();
			conn.setAutoCommit(true);
		}
	}

	@Test
	public void testBatching() throws SQLException {
		final int n1 = 3432;
		final int n2 = 3568;
		int i;
		stmt.execute("DROP TABLE IF EXISTS testbatching");
		stmt.execute("CREATE TABLE testbatching(id INT)");

		// Test regular statement with executeBatch
		for (i = 1; i <= n1; i++) {
			stmt.addBatch("INSERT INTO testbatching VALUES (" + i + ")");
			if (i % 1500 == 0)
				testBatching_execute(stmt, false, 1500);
		}
		testBatching_execute(stmt, false, n1 % 1500);
		stmt.clearBatch();

		// Test prepared statement with executeLargeBatch
		pstmt = conn.prepareStatement("INSERT INTO testbatching VALUES (?)");
		for (i = 1; i <= n2; i++) {
			pstmt.setInt(1, 1);
			pstmt.addBatch();
			if (i % 3000 == 0)
				testBatching_execute(pstmt, true, 3000);
		}
		testBatching_execute(pstmt, true, n2 % 3000);
		pstmt.clearBatch();

		// Check row count
		assertEquals(n1 + n2, queryInt("SELECT COUNT(*) FROM testbatching"));
	}

	private void testBatching_execute(Statement s, boolean large, int expected) throws SQLException {
		int[] ints = null;
		long[] longs = null;
		int resultCount;

		if (large) {
			longs = s.executeLargeBatch();
			resultCount = longs.length;
		} else {
			ints = s.executeBatch();
			resultCount = ints.length;
		}

		assertEquals(expected, resultCount);

		for (int i = 0; i < resultCount; i++)
			assertEquals(
					1L,
					large ? longs[i] : (long)ints[i],
					"result #" + i + " is wrong"
			);
	}

	@Test
	public void testPreparedLargeResponse() throws SQLException {
		// retrieve this to simulate a bug report
		String url = conn.getMetaData().getURL();

		try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM sys.columns")) {
			ps.execute();
			// The original test just ran execute.
			// Maybe we should actually retrieve the data as well
			int count = 0;
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next())
					count++;
			}
			assertNotEquals(0, count); // stop complaints about neccessary loop
		}
	}

	@Test
	public void testPreparedSomeAmount() throws SQLException {
		testManyPrepareStatements(120);
	}

	@Test
	@Tag("slow")
	@DisabledIf("org.monetdb.testinfra.Config#isSkipSlow")
	public void testPreparedLargeAmount() throws SQLException {
		testManyPrepareStatements(50001);
	}

	private void testManyPrepareStatements(int n) throws SQLException {
		for (int i = 0; i < n; i++) {
			String query = String.format("SELECT %d, %d = ?", i, i);
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setInt(1, i);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals(i, rs.getInt(1));
					assertTrue(rs.getBoolean(2));
					assertFalse(rs.next());
				}
			}
		}
	}

	@Test
	public void testTimeDatePrepared() throws SQLException {
		java.util.Date d = new java.util.Date();    // java.util.Date is basically millis since epoch
		long millis = d.getTime();
		java.sql.Time sqlTime = new Time(millis);
		java.sql.Timestamp sqlTimestamp = new Timestamp(millis);
		java.sql.Date sqlDate = new Date(millis);

		conn.setAutoCommit(false);
		stmt.execute("DROP TABLE IF EXISTS testtimedate");
		stmt.execute("CREATE TABLE testtimestamp(t TIME, ts TIMESTAMP, d DATE)");

		String insert = "INSERT INTO testtimestamp VALUES (?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(insert)) {
			ps.setTime(1, sqlTime);
			ps.setTimestamp(2, sqlTimestamp);
			ps.setDate(3, sqlDate);
			ps.executeUpdate();
		}

		String select = "SELECT * FROM testtimestamp";
		try (PreparedStatement ps = conn.prepareStatement(select); ResultSet rs = ps.executeQuery()) {
			assertTrue(rs.next());

			java.sql.Time rsTime = (Time) rs.getObject(1);
			java.sql.Timestamp rsTimestamp = (Timestamp) rs.getObject(2);
			java.sql.Date rsDate = (Date) rs.getObject(3);
			// We cannot directly assertEquals(sqlTime, rsTime) because
			// sqlTime is initialized from the millis of today and rsTime
			// from the millis of some moment of 1970-01-01.
			//
			// We do string comparisons instead
			assertEquals(sqlTime.toString(), rsTime.toString());
			assertEquals(sqlTimestamp.toString(), rsTimestamp.toString());
			assertEquals(sqlDate.toString(), rsDate.toString());

			assertFalse(rs.next());
		}

		conn.rollback();
		conn.setAutoCommit(true);
	}

	@Test
	public void testBug1757923() throws SQLException {
		// #1757923 is probably from the Sourceforge-era!

		conn.setAutoCommit(false);
		stmt.execute("" +
						"DROP TABLE IF EXISTS htmtest;\n" +
						"CREATE TABLE htmtest (\n" +
						"       htmid    bigint       NOT NULL,\n" +
						"       ra       double ,\n" +
						"       decl     double ,\n" +
						"       dra      double ,\n" +
						"       ddecl    double ,\n" +
						"       flux     double ,\n" +
						"       dflux    double ,\n" +
						"       freq     double ,\n" +
						"       bw       double ,\n" +
						"       type     decimal(1,0),\n" +
						"       imageurl url(100),\n" +
						"       comment  varchar(100),\n" +
						"       CONSTRAINT htmtest_htmid_pkey PRIMARY KEY (htmid)\n" +
						");\n" +
						"CREATE INDEX htmid ON htmtest (htmid);");

		String insert = "INSERT INTO HTMTEST (HTMID,RA,DECL,FLUX,COMMENT) VALUES (?,?,?,?,?)";
		String update = "UPDATE HTMTEST set COMMENT=?, TYPE=? WHERE HTMID=?";
		try (PreparedStatement ps1 = conn.prepareStatement(insert)) {
			ps1.setLong(1, 1L);
			ps1.setFloat(2, (float) 1.2);
			ps1.setDouble(3, 2.4);
			ps1.setDouble(4, 3.2);
			ps1.setString(5, "vlavbla");
			ps1.executeUpdate();

			try (PreparedStatement ps2 = conn.prepareStatement(update)) {
				ps2.setString(1, "some update");
				ps2.setObject(2, (float)3.2);
				ps2.setLong(3, 1L);
				ps2.executeUpdate();

				// Unfortunately the original test does not mention
				// what went wrong here
			}
		}

		conn.rollback();
		conn.setAutoCommit(true);
	}
}
