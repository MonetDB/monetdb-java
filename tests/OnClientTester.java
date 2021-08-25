import org.monetdb.jdbc.MonetConnection;
import org.monetdb.jdbc.MonetUploadHandler;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;

public final class OnClientTester {
	private String jdbcUrl;
	boolean verbose = false;
	boolean succeeded = true;
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
		boolean succeeded;
		if (specificTest != null)
			succeeded = tester.runTest(specificTest);
		else
			succeeded = tester.runTests();

		if (tester.verbosity >= VERBOSITY_ON || tester.failureCount > 0) {
			System.out.println();
			System.out.println("Ran " + tester.testCount + " tests, " + tester.failureCount + " failed");
		}
		if (tester.failureCount > 0) {
			System.exit(1);
		}
	}

	public OnClientTester(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	private boolean runTests() throws SQLException, NoSuchMethodException {
		for (Method method: this.getClass().getDeclaredMethods()) {
			String methodName = method.getName();
			if (methodName.startsWith("test_") && method.getParameterCount() == 0) {
				String testName = methodName.substring(5);
				runTest(testName, method);
			}
		}

		return succeeded;
	}

	private boolean runTest(String testName) throws SQLException, NoSuchMethodException {
		String methodName = "test_" + testName;
		Method method = this.getClass().getDeclaredMethod(methodName);

		return runTest(testName, method);
	}

	private synchronized boolean runTest(String testName, Method method) throws SQLException {
		outBuffer = new StringWriter();
		out = new PrintWriter(outBuffer);

		Connection genericConnection = DriverManager.getConnection(jdbcUrl);
		conn = genericConnection.unwrap(MonetConnection.class);
		stmt = conn.createStatement();

		System.out.println();

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
				dumpOutput();
		} catch (Failure e) {
			succeeded = false;
			System.out.println("Test " + testName + " failed");
			dumpOutput();
		} catch (Exception e) {
			succeeded = false;
			System.out.println("Test " + testName + " failed:");
			e.printStackTrace();
			System.out.println();
			dumpOutput();
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
			stmt.close();
			conn.close();
		}

		return succeeded;
	}

	private void dumpOutput() {
		String output = outBuffer.getBuffer().toString();
		if (output.isEmpty()) {
			System.out.println("(Test did not produce any output)");
		} else {
			System.out.println("------ Accumulated output:");
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
		if (updateCount != expectedUpdateCount) {
			fail("Query updated " + updateCount + "rows, expected " + expectedUpdateCount);
		}
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
		if (metaData.getColumnCount() != 1) {
			fail("Result should have exactly one column");
		}
		if (!rs.next()) {
			fail("Result set is empty");
		}
		int result = rs.getInt(1);
		if (rs.next()) {
			String message = "Result set has more than one row";
			fail(message);
		}
		rs.close();
		if (result != expected)
			fail("Query returned " + result + ", expected " + expected);
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

	private void test_ImmediateError() throws Exception {
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

	private void test_ServerCancels() throws SQLException, Failure {
		prepare();
		conn.setUploadHandler(new MyUploadHandler(100));
		update("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT", 96);
		queryInt("SELECT MIN(i) FROM foo", 5);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}


}