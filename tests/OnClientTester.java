import org.monetdb.jdbc.MonetConnection;
import org.monetdb.jdbc.MonetDownloadHandler;
import org.monetdb.jdbc.MonetUploadHandler;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public final class OnClientTester {
	private String jdbcUrl;
	boolean verbose = false;
	int testCount = 0;
	int failureCount = 0;
	private MonetConnection conn;
	private PrintWriter out;
	private Statement stmt;
	private StringWriter outBuffer;

	public static void main(String[] args) throws SQLException, NoSuchMethodException {
		String jdbcUrl;
		String specificTest = null;
		switch (args.length) {
			case 2:
				specificTest = args[1];
				/* fallthrough */
			case 1:
				jdbcUrl = args[0];
				break;
			default:
				throw new IllegalArgumentException("Usage: OnClientTester JDBC_URL [TESTNAME]");
		}

		OnClientTester tester = new OnClientTester(jdbcUrl);
		if (specificTest != null)
			tester.runTest(specificTest);
		else
			tester.runTests();

		System.out.println();
		System.out.println("Ran " + tester.testCount + " tests, " + tester.failureCount + " failed");
		if (tester.failureCount > 0) {
			System.exit(1);
		}
	}

	public OnClientTester(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	private void runTests() throws SQLException, NoSuchMethodException {
		for (Method method: this.getClass().getDeclaredMethods()) {
			String methodName = method.getName();
			if (methodName.startsWith("test_") && method.getParameterCount() == 0) {
				String testName = methodName.substring(5);
				runTest(testName, method);
			}
		}
	}

	private void runTest(String testName) throws SQLException, NoSuchMethodException {
		String methodName = "test_" + testName;
		Method method = this.getClass().getDeclaredMethod(methodName);
		runTest(testName, method);
	}

	private synchronized void runTest(String testName, Method method) throws SQLException {
		outBuffer = new StringWriter();
		out = new PrintWriter(outBuffer);

		Connection genericConnection = DriverManager.getConnection(jdbcUrl);
		conn = genericConnection.unwrap(MonetConnection.class);
		stmt = conn.createStatement();

		System.out.println();

		boolean failed = false;
		try {
			try {
				method.invoke(this);
			} catch (InvocationTargetException e) {
				Throwable cause = e.getCause();
				if (cause instanceof  Failure)
					throw (Failure)cause;
				else if (cause instanceof Exception) {
					throw (Exception)cause;
				} else {
					throw e;
				}
			}

			System.out.println("Test " + testName + " succeeded");
			if (verbose)
				dumpOutput(testName);
		} catch (Failure e) {
			failed = true;
			System.out.println("Test " + testName + " failed");
			dumpOutput(testName);
		} catch (Exception e) {
			failed = true;
			System.out.println("Test " + testName + " failed:");
			e.printStackTrace();
			dumpOutput(testName);
			// Show the inner bits of the exception again, they may have scrolled off screen
			Throwable t = e;
			while (t.getCause() != null) {
				t = t.getCause();
			}
			System.out.println();
			System.out.println("Innermost cause was " + t);
			if (t.getStackTrace().length > 0) {
				System.out.println("                 at " + t.getStackTrace()[0]);
			}
		} finally {
			testCount++;
			if (failed)
				failureCount++;
			stmt.close();
			conn.close();
		}
	}

	private void dumpOutput(String testName) {
		String output = outBuffer.getBuffer().toString();
		if (output.isEmpty()) {
			System.out.println("(Test did not produce any output)");
		} else {
			System.out.println("------ Accumulated output for " + testName + ":");
			boolean terminated = output.endsWith(System.lineSeparator());
			if (terminated) {
				System.out.print(output);
			} else {
				System.out.println(output);
			}
			System.out.println("------ End of accumulated output" + (terminated ? "" : " (no trailing newline)"));
		}
	}

	private void fail(String message) throws Failure {
		out.println("FAILURE: " + message);
		throw new Failure(message);
	}

	private void assertEq(String quantity, Object expected, Object actual) throws Failure {
		if (expected.equals(actual))
			return;
		fail("Expected '" + quantity + "' to be " + expected + ", got " + actual);
	}

	protected boolean execute(String query) throws SQLException {
		out.println("EXEC " + query);
		boolean result;
		result = stmt.execute(query);
		if (result) {
			out.println("  OK");
		} else {
			out.println("  OK, updated " + stmt.getUpdateCount() + " rows");
		}
		return result;
	}

	protected void update(String query, int expectedUpdateCount) throws SQLException, Failure {
		execute(query);
		int updateCount = stmt.getUpdateCount();
		assertEq("Update count", expectedUpdateCount, updateCount);
	}

	protected void expectError(String query, String expectedError) throws SQLException, Failure {
		try {
			execute(query);
		} catch (SQLException e) {
			if (e.getMessage().contains(expectedError)) {
				out.println("  GOT EXPECTED EXCEPTION: " + e.getMessage());
			} else {
				throw e;
			}
		}
	}

	protected void queryInt(String query, int expected) throws SQLException, Failure {
		if (execute(query) == false) {
			fail("Query does not return a result set");
		}
		ResultSet rs = stmt.getResultSet();
		ResultSetMetaData metaData = rs.getMetaData();
		assertEq("column count", 1, metaData.getColumnCount());
		if (!rs.next()) {
			fail("Result set is empty");
		}
		int result = rs.getInt(1);
		if (rs.next()) {
			String message = "Result set has more than one row";
			fail(message);
		}
		rs.close();
		assertEq("query result", expected, result);
	}

	protected void prepare() throws SQLException {
		execute("DROP TABLE IF EXISTS foo");
		execute("CREATE TABLE foo (i INT, t TEXT)");
	}

	static class MyUploadHandler implements MonetUploadHandler {

		private final int rows;
		private final int errorAt;
		private final String errorMessage;
		MyUploadHandler(int rows, int errorAt, String errorMessage) {
			this.rows = rows;
			this.errorAt = errorAt;
			this.errorMessage = errorMessage;
		}

		MyUploadHandler(int rows) {
			this(rows, -1, null);
		}

		MyUploadHandler(String errorMessage) {
			this(0, -1, errorMessage);
		}

		@Override
		public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, int offset) throws IOException {
			int toSkip = offset > 0 ? offset - 1 : 0;
			if (errorAt == -1 && errorMessage != null) {
				handle.sendError(errorMessage);
				return;
			}
			PrintStream stream = handle.getStream();
			for (int i = toSkip; i < rows; i++) {
				if (i == errorAt) {
					throw new IOException(errorMessage);
				}
				stream.printf("%d|%d%n", i + 1, i + 1);
			}
		}

	}

	static class MyDownloadHandler implements MonetDownloadHandler {
		private final int errorAtByte;
		private final String errorMessage;
		private int attempts = 0;
		private int bytesSeen = 0;
		private int lineEndingsSeen = 0;
		private int startOfLine = 0;

		MyDownloadHandler(int errorAtByte, String errorMessage) {
			this.errorAtByte = errorAtByte;
			this.errorMessage = errorMessage;
		}

		MyDownloadHandler(String errorMessage) {
			this(-1, errorMessage);
		}

		MyDownloadHandler() {
			this(-1, null);
		}

		@Override
		public void handleDownload(MonetConnection.Download handle, String name, boolean textMode) throws IOException {
			attempts++;
			bytesSeen = 0;
			lineEndingsSeen = 0;
			startOfLine = 0;

			if (errorMessage != null && errorAtByte < 0) {
				handle.sendError(errorMessage);
				return;
			}

			InputStream stream = handle.getStream();
			byte[] buffer = new byte[1024];
			while (true) {
				int toRead = buffer.length;
				if (errorMessage != null && errorAtByte >= 0) {
					if (bytesSeen == errorAtByte) {
						throw new IOException(errorMessage);
					}
					toRead = Integer.min(toRead, errorAtByte - bytesSeen);
				}
				int nread = stream.read(buffer, 0, toRead);
				if (nread < 0)
					break;
				for (int i = 0; i < nread; i++) {
					if (buffer[i] == '\n') {
						lineEndingsSeen += 1;
						startOfLine = bytesSeen + i + 1;
					}
				}
				bytesSeen += nread;
			}
		}

		public int countAttempts() {
			return attempts;
		}
		public int countBytes() {
			return bytesSeen;
		}

		public int lineCount() {
			int lines = lineEndingsSeen;
			if (startOfLine != bytesSeen)
				lines++;
			return lines;
		}
	}


	static class Failure extends Exception {

		public Failure(String message) {
			super(message);
		}
		public Failure(String message, Throwable cause) {
			super(message, cause);
		}

	}

	private void test_Upload() throws Exception {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY INTO foo FROM 'banana' ON CLIENT", 100);
		queryInt("SELECT COUNT(*) FROM foo", 100);
	}

	private void test_ClientRefuses() throws Exception {
		prepare();
		conn.setUploadHandler(new MyUploadHandler("immediate error"));
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "immediate error");
		queryInt("SELECT COUNT(*) FROM foo", 0);
	}

	private void test_Offset0() throws SQLException, Failure {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY OFFSET 0 INTO foo FROM 'banana' ON CLIENT", 100);
		queryInt("SELECT MIN(i) FROM foo", 1);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	private void test_Offset1() throws SQLException, Failure {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY OFFSET 1 INTO foo FROM 'banana' ON CLIENT", 100);
		queryInt("SELECT MIN(i) FROM foo", 1);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	private void test_Offset5() throws SQLException, Failure {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY OFFSET 5 INTO foo FROM 'banana' ON CLIENT", 96);
		queryInt("SELECT MIN(i) FROM foo", 5);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	private void testx_ServerCancels() throws SQLException, Failure {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT", 96);
		// Server stopped reading after 10 rows. Will we stay in sync?
		queryInt("SELECT COUNT(i) FROM foo", 10);
	}

	private void test_Download() throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler();
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		update("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", -1);
		assertEq("download attempts", 1, handler.countAttempts());
		assertEq("lines downloaded", 100, handler.lineCount());
	}

	private void test_CancelledDownload() throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler("download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		expectError("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", "download refused");
		// check if the connection still works
		queryInt("SELECT 42", 42);
	}


}