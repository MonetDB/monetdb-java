/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

import org.monetdb.jdbc.MonetConnection;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.util.ArrayList;

public class TestRunner {
	public static final int VERBOSITY_NONE = 0;
	public static final int VERBOSITY_ON = 1;
	public static final int VERBOSITY_SHOW_ALL = 2;

	protected final String jdbcUrl;
	private final int verbosity;
	protected String currentTestName;
	protected final WatchDog watchDog;
	protected MonetConnection conn;
	protected Statement stmt;
	private StringWriter outBuffer;
	protected PrintWriter out;
	private Path tmpDir;

	public TestRunner(String jdbcUrl, int verbosity, boolean watchDogEnabled) {
		this.jdbcUrl = jdbcUrl;
		this.verbosity = verbosity;
		watchDog = new WatchDog();
		if (watchDogEnabled)
			watchDog.enable();
		else
			watchDog.disable();
	}

	protected int runTests(String testPrefix) throws SQLException {
		int testCount = 0;
		int skippedCount = 0;
		final ArrayList<String> failures = new ArrayList<>();

		watchDog.stop();
		try {
			final String initialPrefix = "test_";
			if (testPrefix == null)
				testPrefix = "";
			final String methodPrefix = initialPrefix + testPrefix;

			for (Method method : this.getClass().getDeclaredMethods()) {
				if (method.getParameterCount() != 0) {
					continue;
				}
				if (!method.getName().startsWith(initialPrefix)) {
					continue;
				}
				testCount++;
				// so user can add $ to force full match
				final String augmentedMethodName = method.getName() + "$";
				if (!augmentedMethodName.startsWith(methodPrefix)) {
					skippedCount++;
					continue;
				}
				final String testName = method.getName().substring(initialPrefix.length());
				final boolean succeeded = runTest(testName, method);
				if (!succeeded)
					failures.add(testName);
			}
		} finally {
			watchDog.stop();
		}

		if (testCount > 0 && skippedCount == testCount && !testPrefix.isEmpty()) {
			System.err.printf("None of the %d tests matched prefix '%s'%n", testCount, testPrefix);
			return 1;
		}

		final int failureCount = failures.size();
		if (failureCount > 0) {
			System.out.println();
			System.out.printf("Ran %d out of %d %s tests, %d failed: %s%n",
					testCount - skippedCount, testCount,
					this.getClass().getSimpleName(),
					failureCount,
					String.join(", ", failures)
					);
		} else if (verbosity >= VERBOSITY_ON) {
			System.out.println();
			System.out.printf("Ran %d out of %d tests, none failed%n", testCount - skippedCount, testCount);
		}

		return failureCount;
	}

	private synchronized boolean runTest(final String testName, final Method method) throws SQLException {
		currentTestName = testName;
		watchDog.setContext("test " + testName);
		watchDog.setDuration(3_000);
		outBuffer = new StringWriter();
		out = new PrintWriter(outBuffer);

		final Connection genericConnection = DriverManager.getConnection(jdbcUrl);
		conn = genericConnection.unwrap(MonetConnection.class);
		stmt = conn.createStatement();

		boolean failed = true;
		try {
			long duration;
			try {
				long t0 = System.currentTimeMillis();
				method.invoke(this);
				long t1 = System.currentTimeMillis();
				duration = t1 - t0;
			} catch (InvocationTargetException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Failure)
					throw (Failure) cause;
				else if (cause instanceof Exception) {
					throw (Exception) cause;
				} else {
					throw e;
				}
			}

			failed = false;

			if (verbosity > VERBOSITY_ON)
				System.out.println();
			if (verbosity >= VERBOSITY_ON)
				System.out.println("Test " + testName + " succeeded in " + duration + "ms");
			if (verbosity >= VERBOSITY_SHOW_ALL)
				dumpOutput(testName);
		} catch (Failure e) {
			System.err.println();
			System.err.println("Test " + testName + " failed");
			dumpOutput(testName);
		} catch (Exception e) {
			System.err.println();
			System.err.println("Test " + testName + " failed:");
			e.printStackTrace(System.err);
			dumpOutput(testName);
			// Show the inner bits of the exception again, they may have scrolled off screen
			Throwable t = e;
			while (t.getCause() != null) {
				t = t.getCause();
			}
			System.out.println("Innermost cause was " + t);
			if (t.getStackTrace().length > 0) {
				System.err.println("                 at " + t.getStackTrace()[0]);
			}
		} finally {
			watchDog.setContext(null);
			if (failed && verbosity == VERBOSITY_ON) {
				// next test case will not print separator
				System.out.println();
			}
			stmt.close();
			conn.close();
		}

		return !failed;
	}

	private void dumpOutput(final String testName) {
		final String output = outBuffer.getBuffer().toString();
		if (output.isEmpty()) {
			System.out.println("(Test did not produce any output)");
		} else {
			System.out.println("------ Accumulated output for test " + testName + ":");
			final boolean terminated = output.endsWith(System.lineSeparator());
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

	private void checked(String quantity, Object actual) {
		out.println("  CHECKED: " + "<" + quantity + "> is " + actual + " as expected");
	}

	protected void assertEq(String quantity, Object expected, Object actual) throws Failure {
		if (expected.equals(actual)) {
			checked(quantity, actual);
		} else {
			fail("Expected <" + quantity + "> to be " + expected + " got " + actual);
		}
	}

	protected boolean execute(String query) throws SQLException {
		try {
			watchDog.start();
			out.println("EXECUTE: " + query);
			final boolean result = stmt.execute(query);
			if (result) {
				out.println("  OK");
			} else {
				out.println("  OK, updated " + stmt.getUpdateCount() + " rows");
			}
			return result;
		} finally {
			watchDog.stop();
		}
	}

	protected void update(String query) throws SQLException, Failure {
		execute(query);
	}

	protected void expectError(String query, String expectedError) throws SQLException {
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

	protected void assertQueryInt(String query, int expected) throws SQLException, Failure {
		if (execute(query) == false) {
			fail("Query does not return a result set");
		}
		final ResultSet rs = stmt.getResultSet();
		final ResultSetMetaData metaData = rs.getMetaData();
		assertEq("column count", 1, metaData.getColumnCount());
		if (!rs.next()) {
			fail("Result set is empty");
		}
		final int result = rs.getInt(1);
		if (rs.next()) {
			fail("Result set has more than one row");
		}
		rs.close();
		checked("row count", 1);
		assertEq("query result", expected, result);
	}

	protected void assertQueryString(String query, String expected) throws SQLException, Failure {
		if (execute(query) == false) {
			fail("Query does not return a result set");
		}
		final ResultSet rs = stmt.getResultSet();
		final ResultSetMetaData metaData = rs.getMetaData();
		assertEq("column count", 1, metaData.getColumnCount());
		if (!rs.next()) {
			fail("Result set is empty");
		}
		final String result = rs.getString(1);
		if (rs.next()) {
			fail("Result set has more than one row");
		}
		rs.close();
		checked("row count", 1);
		assertEq("query result", expected, result);
	}

	protected String queryString(String query) throws SQLException, Failure {
		if (execute(query) == false) {
			fail("Query does not return a result set");
		}
		final ResultSet rs = stmt.getResultSet();
		final ResultSetMetaData metaData = rs.getMetaData();
		assertEq("column count", 1, metaData.getColumnCount());
		if (!rs.next()) {
			fail("Result set is empty");
		}
		final String result = rs.getString(1);
		if (rs.next()) {
			fail("Result set has more than one row");
		}
		rs.close();
		checked("row count", 1);
		return result;
	}

	protected synchronized Path getTmpDir(String name) throws IOException {
		if (tmpDir == null) {
			tmpDir = Files.createTempDirectory("testMonetDB");
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					Files.walkFileTree(tmpDir, new SimpleFileVisitor<Path>() {
						@Override
						public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
							Files.delete(file);
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
							Files.delete(dir);
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException e) {
					// we do this on a best effort basis
				}
			}));
		}
		final Path p = tmpDir.resolve(name);
		Files.createDirectory(p);
		return p;
	}

	static class Failure extends Exception {
		static final long serialVersionUID = 3387516993124229948L;

		public Failure(String message) {
			super(message);
		}

		public Failure(String message, Throwable cause) {
			super(message, cause);
		}
	}

	static class WatchDog {
		private boolean enabled;
		private long duration = 1000;
		private long started = 0;
		private String context = "no context";

		WatchDog() {
			Thread watchDog = new Thread(this::work);
			watchDog.setName("watchdog_timer");
			watchDog.setDaemon(true);
			watchDog.start();
		}

		synchronized void enable() {
			this.enabled = true;
			this.notifyAll();
		}

		synchronized void disable() {
			this.enabled = false;
			this.notifyAll();
		}

		synchronized void setContext(String context) {
			this.context = context;
		}

		synchronized void setDuration(long duration) {
			if (duration <= 0)
				throw new IllegalArgumentException("duration should be > 0");
			this.duration = duration;
			this.notifyAll();
		}

		synchronized void start() {
			started = System.currentTimeMillis();
			this.notifyAll();
		}

		synchronized void stop() {
			started = 0;
			this.notifyAll();
		}

		private synchronized void work() {
			long now;
			try {
				while (true) {
					now = System.currentTimeMillis();
					final long sleepTime;
					if (started < 0) {
						// client asked us to go away
						// System.err.println("++ EXIT");
						return;
					} else if (!enabled || started == 0) {
						// wait for client to enable/start us
						sleepTime = 600_000;
					} else {
						sleepTime = started + duration - now;
					}
					// System.err.printf("++ now=%d, started=now%+d, duration=%d, sleep=%d%n",
					//		now, started - now, duration, sleepTime );
					if (sleepTime > 0) {
						this.wait(sleepTime);
					} else {
						trigger();
						return;
					}
				}
			} catch (InterruptedException e) {
				System.err.println("WATCHDOG TIMER INTERRUPTED, SHOULDN'T HAPPEN");
				System.exit(4);
			}
		}

		private void trigger() {
			final String c = context != null ? context : "no context";
			System.err.println();
			System.err.println();
			System.err.println("WATCHDOG TIMER EXPIRED [" + c + "], KILLING TESTS");
			System.exit(3);
		}
	}
}
