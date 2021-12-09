/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

import org.monetdb.jdbc.MonetConnection;
import org.monetdb.jdbc.MonetConnection.UploadHandler;
import org.monetdb.jdbc.MonetConnection.DownloadHandler;
import org.monetdb.util.FileTransferHandler;

import java.io.*;
import java.lang.Character.UnicodeBlock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;


/**
 * Program to test MonetDB JDBC Driver in combination with SQL:
 *   COPY ... INTO ... ON CLIENT
 * commands.
 * This allows Java programmers to locally (so ON CLIENT) stream csv data
 * to and from the MonetDB server for fast bulk data import / export.
 *
 * Specifically it tests the MonetDB specific extensions to register upload and download handlers
 * see {@link org.monetdb.jdbc.MonetConnection#setUploadHandler(UploadHandler)}
 * see {@link org.monetdb.jdbc.MonetConnection#setUploadHandler(DownloadHandler)}
 * and streaming of csv data to and from the MonetDB server using MAPI protocol.
 *
 * It also tests reading / writing data from / to a local file using
 * {@link org.monetdb.util.FileTransferHandler}
 *
 * @author JvR
 * @version 0.1
 */
public final class OnClientTester {
	public static final int VERBOSITY_NONE = 0;
	public static final int VERBOSITY_ON = 1;
	public static final int VERBOSITY_SHOW_ALL = 2;

	private final String jdbcUrl;
	private final int verbosity;
	private final ArrayList<String> selectedTests;
	private String currentTestName;
	private long startTime;
	private MonetConnection conn;
	private Statement stmt;
	private StringBuilder outBuffer;
	private Path tmpDir;

	public OnClientTester(String jdbcUrl, int verbosity) {
		this.jdbcUrl = jdbcUrl;
		this.verbosity = verbosity;
		this.selectedTests = null;
	}

	public OnClientTester(String jdbcUrl, int verbosity, ArrayList<String> selectedTests) {
		this.jdbcUrl = jdbcUrl;
		this.verbosity = verbosity;
		this.selectedTests = selectedTests;
	}

	public static void main(String[] args) {
		String jdbcUrl = null;
		int verbosity = 0;
		ArrayList<String> selectedTests = new ArrayList<>();

		for (String arg : args) {
			if (arg.equals("-v"))
				verbosity++;
			else if (arg.equals("-vv"))
				verbosity += 2;
			else if (jdbcUrl == null)
				jdbcUrl = arg;
			else if (arg.startsWith("-")){
				System.err.println("Unexpected argument " + arg);
				System.exit(2);
			} else {
				selectedTests.add(arg);
			}
		}
		if (jdbcUrl == null || jdbcUrl.isEmpty()) {
			System.err.println("Missing required startup argument: JDBC_connection_URL");
			System.exit(1);
		}

		OnClientTester tester = new OnClientTester(jdbcUrl, verbosity, selectedTests);
		int failures = tester.runTests();
		if (failures > 0)
			System.exit(-1);
	}

	boolean isSelected(String name) {
		return selectedTests == null || selectedTests.isEmpty() || selectedTests.contains(name);
	}

	public int runTests() {
		if (! openConnection())
			return 1;	// failed to open JDBC connection to MonetDB

		outBuffer = new StringBuilder(1024);

		int failures = 0;
		try {
			// all test methods start with test_ and have no arguments
			if (isSelected("BugFixLevel"))
				test_BugFixLevel();
			if (isSelected("Upload"))
				test_Upload();
			if (isSelected("ClientRefusesUpload"))
				test_ClientRefusesUpload();
			if (isSelected("Offset0"))
				test_Offset0();
			if (isSelected("Offset1"))
				test_Offset1();
			if (isSelected("Offset5"))
				test_Offset5();
			if (isSelected("ServerStopsReading"))
				test_ServerStopsReading();
			if (isSelected("Download"))
				test_Download();
			if (isSelected("ClientRefusesDownload"))
				test_ClientRefusesDownload();
			if (isSelected("LargeUpload"))
				test_LargeUpload();
			if (isSelected("LargeDownload"))
				test_LargeDownload();
			if (isSelected("UploadFromStream"))
				test_UploadFromStream();
			if (isSelected("UploadFromReader"))
				test_UploadFromReader();
			if (isSelected("UploadFromReaderOffset"))
				test_UploadFromReaderOffset();
			if (isSelected("FailUploadLate"))
				test_FailUploadLate();
			if (isSelected("FailUploadLate2"))
				test_FailUploadLate2();
			if (isSelected("FailDownloadLate"))
				test_FailDownloadLate();
			if (isSelected("FileTransferHandlerUploadUtf8"))
				test_FileTransferHandlerUploadUtf8();
			if (isSelected("FileTransferHandlerUploadLatin1"))
				test_FileTransferHandlerUploadLatin1();
			if (isSelected("FileTransferHandlerUploadNull"))
				test_FileTransferHandlerUploadNull();
			if (isSelected("FileTransferHandlerUploadRefused"))
				test_FileTransferHandlerUploadRefused();
			if (isSelected("FileTransferHandlerDownloadUtf8"))
				test_FileTransferHandlerDownloadUtf8();
			if (isSelected("FileTransferHandlerDownloadLatin1"))
				test_FileTransferHandlerDownloadLatin1();
			if (isSelected("FileTransferHandlerDownloadNull"))
				test_FileTransferHandlerDownloadNull();
			if (isSelected("test_FileTransferHandlerUploadNotCompressed"))
				test_FileTransferHandlerUploadNotCompressed();
			if (isSelected("test_FileTransferHandlerUploadNotCompressedSkip"))
				test_FileTransferHandlerUploadNotCompressedSkip();
			if (isSelected("test_FileTransferHandlerUploadCompressed"))
				test_FileTransferHandlerUploadCompressed();
			if (isSelected("test_FileTransferHandlerUploadCompressedSkip"))
				test_FileTransferHandlerUploadCompressedSkip();
			if (isSelected("FileTransferHandlerDownloadRefused"))
				test_FileTransferHandlerDownloadRefused();
		} catch (Failure e) {
			failures++;
			System.err.println();
			System.err.println("Test " + currentTestName + " failed");
			dumpOutput();
		} catch (Exception e) {
			failures++;
			System.err.println();
			System.err.println("Test " + currentTestName + " failed:");
			e.printStackTrace(System.err);
			dumpOutput();
			// Show the inner bits of the exception again, they may have scrolled off screen
			Throwable t = e;
			while (t.getCause() != null) {
				t = t.getCause();
			}
			System.err.println("Innermost cause was " + t);
			if (t.getStackTrace().length > 0) {
				System.err.println("                 at " + t.getStackTrace()[0]);
			}
		} finally {
			try {
				// cleanup created test table
				execute("DROP TABLE IF EXISTS foo");
			} catch (SQLException e) { /* ignore */ }
		}
		closeConnection();
		return failures;
	}

	private boolean openConnection() {
		try {
			// make a connection to MonetDB, its reused for all tests
			final Connection genericConnection = DriverManager.getConnection(jdbcUrl);
			conn = genericConnection.unwrap(MonetConnection.class);
			stmt = conn.createStatement();
			return true;
		} catch (SQLException e) {
			System.err.println("Failed to connect using JDBC URL: " + jdbcUrl);
			System.err.println(e);
		}
		return false;
	}

	private void closeConnection() {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) { /* ignore */ }
			stmt = null;
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) { /* ignore */ }
			conn = null;
		}
	}

	private void initTest(final String name) {
		currentTestName = name;
		outBuffer.setLength(0);		// clear the output log buffer
		startTime = System.currentTimeMillis();
	}

	private void exitTest() {
		if (verbosity > VERBOSITY_ON)
			System.err.println();
		if (verbosity >= VERBOSITY_ON) {
			final long duration = System.currentTimeMillis() - startTime;
			System.err.println("Test " + currentTestName + " succeeded in " + duration + "ms");
		}
		if (verbosity >= VERBOSITY_SHOW_ALL)
			dumpOutput();

		if (conn.isClosed())
			openConnection();	// restore connection for next test
	}

	private void dumpOutput() {
		final String output = outBuffer.toString();
		if (output.isEmpty()) {
			System.err.println("(Test " + currentTestName + " did not produce any output)");
		} else {
			System.err.println("------ Accumulated output for test " + currentTestName + ":");
			System.err.println(output);
			System.err.println("------ End of accumulated output");
		}
	}

	/// Some tests have to work limitations of the protocol or bugs in the server.
	/// This Enum is used to indicate the possibilities.
	private enum BugFixLevel {
		/// Only those tests that work with older MonetDB versions
		Baseline(0, 0, 0),

		/// Connection keeps working after download request has been refused by client
		CanRefuseDownload(11, 41, 12),

		;

		private final int major;
		private final int minor;
		private final int micro;

		BugFixLevel(int major, int minor, int micro) {
			this.major = major;
			this.minor = minor;
			this.micro = micro;
		}

		boolean includesVersion(int major, int minor, int micro) {
			if (major > this.major)
				return true;
			if (major < this.major)
				return false;
			if (minor > this.minor)
				return true;
			if (minor < this.minor)
				return false;
			return micro >= this.micro;
		}

		static BugFixLevel forVersion(String version) {
			String[] parts = version.split("[.]", 3);
			assert parts.length == 3;
			int major = Integer.parseInt(parts[0]);
			int minor = Integer.parseInt(parts[1]);
			int micro = Integer.parseInt(parts[2]);

			return BugFixLevel.forVersion(major, minor, micro);
		}

		static BugFixLevel forVersion(int major, int minor, int micro) {
			BugFixLevel lastValid = Baseline;
			for (BugFixLevel level : BugFixLevel.values()) {
				if (level.includesVersion(major, minor, micro))
					lastValid = level;
				else
					break;
			}
			return lastValid;
		}
	}

	private void prepare() throws SQLException {
		execute("DROP TABLE IF EXISTS foo");
		execute("CREATE TABLE foo (i INT, t CLOB)");
	}

	private BugFixLevel getLevel() throws SQLException, Failure {
		String version = queryString("SELECT value FROM environment WHERE name = 'monet_version'");
		BugFixLevel level = BugFixLevel.forVersion(version);
		outBuffer.append("  NOTE: version ").append(version).append(" means level = ").append(level).append("\n");
		return level;
	}

	private void test_BugFixLevel() throws Failure {
		initTest("test_BugFixLevel");

		assertEq("Baseline includes 0.0.0", true, BugFixLevel.Baseline.includesVersion(0, 0, 0));
		assertEq("Baseline includes 11.41.11", true, BugFixLevel.Baseline.includesVersion(11, 41, 11));
		assertEq("Baseline includes 11.41.12", true, BugFixLevel.Baseline.includesVersion(11, 41, 12));

		assertEq("CanRefuseDownload includes 0.0.0", false, BugFixLevel.CanRefuseDownload.includesVersion(0, 0, 0));

		assertEq("CanRefuseDownload includes 11.0.0", false, BugFixLevel.CanRefuseDownload.includesVersion(11, 0, 0));
		assertEq("CanRefuseDownload includes 12.0.0", true, BugFixLevel.CanRefuseDownload.includesVersion(12, 0, 0));

		assertEq("CanRefuseDownload includes 11.41.0", false, BugFixLevel.CanRefuseDownload.includesVersion(11, 41, 0));
		assertEq("CanRefuseDownload includes 11.42.0", true, BugFixLevel.CanRefuseDownload.includesVersion(11, 42, 0));

		assertEq("CanRefuseDownload includes 11.41.11", false, BugFixLevel.CanRefuseDownload.includesVersion(11, 41, 11));
		assertEq("CanRefuseDownload includes 11.41.12", true, BugFixLevel.CanRefuseDownload.includesVersion(11, 41, 12));

		assertEq("Level for 0.0.0", BugFixLevel.Baseline, BugFixLevel.forVersion(0, 0, 0));
		assertEq("Level for 11.0.0", BugFixLevel.Baseline, BugFixLevel.forVersion(11, 0, 0));
		assertEq("Level for 11.41.0", BugFixLevel.Baseline, BugFixLevel.forVersion(11, 41, 0));
		assertEq("Level for 11.41.11", BugFixLevel.Baseline, BugFixLevel.forVersion(11, 41, 11));
		assertEq("Level for 11.41.12", BugFixLevel.CanRefuseDownload, BugFixLevel.forVersion(11, 41, 12));
		assertEq("Level for 11.42.0", BugFixLevel.CanRefuseDownload, BugFixLevel.forVersion(11, 42, 0));
		assertEq("Level for 12.0.0", BugFixLevel.CanRefuseDownload, BugFixLevel.forVersion(12, 0, 0));

		assertEq("Level for \"11.41.11\"", BugFixLevel.Baseline, BugFixLevel.forVersion("11.41.11"));
		assertEq("Level for \"11.41.12\"", BugFixLevel.CanRefuseDownload, BugFixLevel.forVersion("11.41.12"));

		exitTest();
	}

	private void test_Upload() throws SQLException, Failure {
		initTest("test_Upload");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT COUNT(*) FROM foo", 100);
		exitTest();
	}

	private void test_ClientRefusesUpload() throws SQLException, Failure {
		initTest("test_ClientRefusesUpload");
		prepare();
		MyUploadHandler handler = new MyUploadHandler("immediate error");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "immediate error");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT COUNT(*) FROM foo", 0);
		exitTest();
	}

	private void test_Offset0() throws SQLException, Failure {
		initTest("test_Offset0");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 0 INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 1);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
		exitTest();
	}

	private void test_Offset1() throws SQLException, Failure {
		initTest("test_Offset1");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 1 INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 1);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
		exitTest();
	}

	private void test_Offset5() throws SQLException, Failure {
		initTest("test_Offset5");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 5 INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 5);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
		exitTest();
	}

	private void test_ServerStopsReading() throws SQLException, Failure {
		initTest("test_ServerStopsReading");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", true, handler.isCancelled());
		assertEq("handler encountered write error", true, handler.encounteredWriteError());
		// connection is still alive
		assertQueryInt("SELECT COUNT(i) FROM foo", 10);
		exitTest();
	}

	private void test_Download(int n) throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler();
		conn.setDownloadHandler(handler);
		String q = "INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, " + n + ")";
		update(q);
		update("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT");
		assertEq("download attempts", 1, handler.countAttempts());
		assertEq("lines downloaded", n, handler.lineCount());
		// connection is still alive
		assertQueryInt("SELECT COUNT(*) FROM foo", n);
	}

	private void test_Download() throws SQLException, Failure {
		initTest("test_Download");
		test_Download(100);
		exitTest();
	}

	private void test_ClientRefusesDownload() throws SQLException, Failure {
		initTest("test_ClientRefusesDownload");
		prepare();
		BugFixLevel level = getLevel();
		MyDownloadHandler handler = new MyDownloadHandler("download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)");
		expectError("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", "download refused");
		// Wish it were different but the server closes the connection
		expectError("SELECT 42 -- check if the connection still works", "Connection to server lost!");
		if (level.compareTo(BugFixLevel.CanRefuseDownload) >= 0) {
			// connection is still alive
			assertQueryInt("SELECT COUNT(*) FROM foo", 100);
		}
		exitTest();
	}

	private void test_LargeUpload() throws SQLException, Failure {
		initTest("test_LargeUpload");
		prepare();
		int n = 4_000_000;
		MyUploadHandler handler = new MyUploadHandler(n);
		conn.setUploadHandler(handler);
		handler.setChunkSize(1024 * 1024);
		update("COPY INTO foo FROM 'banana' ON CLIENT");
		assertEq("cancellation callback called", false, handler.isCancelled());
		// connection is still alive
		assertQueryInt("SELECT COUNT(DISTINCT i) FROM foo", n);
		exitTest();
	}

	private void test_LargeDownload() throws SQLException, Failure {
		initTest("test_LargeDownload");
		test_Download(4_000_000);
		exitTest();
	}

	private void test_UploadFromStream() throws SQLException, Failure {
		initTest("test_UploadFromStream");
		prepare();
		UploadHandler handler = new UploadHandler() {
			final String data = "1|one\n2|two\n3|three\n";

			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				// ignoring linesToSkip as it's not used in this test
				ByteArrayInputStream s = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
				handle.uploadFrom(s);
			}
		};
		conn.setUploadHandler(handler);
		update("COPY INTO foo FROM 'banana' ON CLIENT");
		// connection is still alive
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
		exitTest();
	}

	private void test_UploadFromReader() throws SQLException, Failure {
		initTest("test_UploadFromReader");
		prepare();
		UploadHandler handler = new UploadHandler() {
			final String data = "1|one\n2|two\n3|three\n";

			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				// ignoring linesToSkip as it's not used in this test
				StringReader r = new StringReader(data);
				handle.uploadFrom(r);
			}
		};
		conn.setUploadHandler(handler);
		update("COPY INTO foo FROM 'banana' ON CLIENT");
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
		exitTest();
	}

	private void test_UploadFromReaderOffset() throws SQLException, Failure {
		initTest("test_UploadFromReaderOffset");
		prepare();
		UploadHandler handler = new UploadHandler() {
			final String data = "1|one\n2|two\n3|three\n";

			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				BufferedReader r = new BufferedReader(new StringReader(data));
				handle.uploadFrom(r, linesToSkip);
			}
		};
		conn.setUploadHandler(handler);
		update("COPY OFFSET 2 INTO foo FROM 'banana' ON CLIENT");
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
		exitTest();
	}

	private void test_FailUploadLate() throws SQLException, Failure {
		initTest("test_FailUploadLate");
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100, 50, "i don't like line 50");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "i don't like");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertEq("connection is closed", true, conn.isClosed());
		exitTest();
	}

	private void test_FailUploadLate2() throws SQLException, Failure {
		initTest("test_FailUploadLate2");
		// Here we send empty lines only, to check if the server detects it properly instead
		// of simply complaining about an incomplete file.
		prepare();
		UploadHandler handler = new UploadHandler() {
			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				// ignoring linesToSkip as it's not used in this test
				PrintStream stream = handle.getStream();
				for (int i = 1; i <= 20_000; i++)
					stream.println();
				stream.flush();
				throw new IOException("exception after all");
			}
		};
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo(t) FROM 'banana'(t) ON CLIENT", "after all");
		assertEq("connection is closed", true, conn.isClosed());
		// Cannot check the server log, but at the time I checked, it said "prematurely stopped client", which is fine.
		exitTest();
	}

	private void test_FailDownloadLate() throws SQLException, Failure {
		initTest("test_FailDownloadLate");
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler(200, "download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)");
		expectError("COPY (SELECT * FROM sys.generate_series(0,200)) INTO 'banana' ON CLIENT", "download refused");
		// Exception closes the connection
		assertEq("connection is closed", conn.isClosed(), true);
		exitTest();
	}

	private void test_FileTransferHandlerUploadNotCompressed() throws IOException, SQLException, Failure {
		initTest("FileTransferHandlerUploadNotCompressed");
		testFileTransferHandlerUploadCompressed(StandardCharsets.UTF_8, false, 0);
		exitTest();
	}

	private void test_FileTransferHandlerUploadNotCompressedSkip() throws IOException, SQLException, Failure {
		initTest("FileTransferHandlerUploadNotCompressedSkip");
		testFileTransferHandlerUploadCompressed(StandardCharsets.UTF_8, false, 2);
		exitTest();
	}

	private void test_FileTransferHandlerUploadCompressed() throws IOException, SQLException, Failure {
		initTest("FileTransferHandlerUploadCompressed");
		testFileTransferHandlerUploadCompressed(StandardCharsets.UTF_8, true, 0);
		exitTest();
	}

	private void test_FileTransferHandlerUploadCompressedSkip() throws IOException, SQLException, Failure {
		initTest("FileTransferHandlerUploadCompressedSkip");
		testFileTransferHandlerUploadCompressed(StandardCharsets.UTF_8, true, 2);
		exitTest();
	}

	private void test_FileTransferHandlerUploadUtf8() throws IOException, SQLException, Failure {
		initTest("test_FileTransferHandlerUploadUtf8");
		testFileTransferHandlerUploadEncoding(StandardCharsets.UTF_8, "UTF-8");
		exitTest();
	}

	private void test_FileTransferHandlerUploadLatin1() throws IOException, SQLException, Failure {
		initTest("test_FileTransferHandlerUploadLatin1");
		testFileTransferHandlerUploadEncoding(Charset.forName("latin1"), "latin1");
		exitTest();
	}

	private void test_FileTransferHandlerUploadNull() throws IOException, SQLException, Failure {
		initTest("test_FileTransferHandlerUploadNull");
		testFileTransferHandlerUploadEncoding(null, Charset.defaultCharset().name());
		exitTest();
	}

	private String hexdump(String s)
	{
		StringBuilder buf = new StringBuilder();
		char[] chars = s.toCharArray();
		for (char c: chars) {
			buf.append(' ');
			buf.append((int)c);

//			UnicodeBlock b = UnicodeBlock.of(c);
//			if (!Character.isISOControl(c) && b != null) {
//				if (b != UnicodeBlock.HIGH_SURROGATES && b != UnicodeBlock.LOW_SURROGATES && b != UnicodeBlock.SPECIALS) {
//						buf.append("='");
//						buf.append(c);
//						buf.append("'");
//				}
//			}
		}

		return "<" + buf.toString().trim() + ">";
	}

	private void testFileTransferHandlerUploadEncoding(Charset handlerEncoding, String fileEncoding) throws IOException, SQLException, Failure {
		prepare();
		outBuffer.append("Default encoding is " + Charset.defaultCharset().displayName() + "\n");
		Path d = getTmpDir(currentTestName);
		Path f = d.resolve("data.txt");
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		PrintStream ps = new PrintStream(s, false, fileEncoding);
		ps.println("1|one");
		ps.println("2|twø");
		ps.println("3|three");
		ps.close();
		conn.setUploadHandler(new FileTransferHandler(d, handlerEncoding));
		update("COPY INTO foo FROM 'data.txt' ON CLIENT");
		assertQueryInt("SELECT SUM(i) FROM foo", 6);
		final String result = queryString("SELECT t FROM foo WHERE i = 2");
		String two = "twø";
		//
		String hexTwo = hexdump(two);
		String hexResult = hexdump(result);
		assertEq("query result hexdump", hexTwo, hexResult);
//		assertEq("query result", two, result);
	}

	private void testFileTransferHandlerUploadCompressed(Charset encoding, boolean compressed, int skipLines) throws IOException, SQLException, Failure {
		prepare();
		Path d = getTmpDir(currentTestName);
		String fileName = "data.txt";
		if (compressed)
			fileName += ".gz";
		Path f = d.resolve(fileName);
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		if (compressed) {
			s = new GZIPOutputStream(s);
		}
		Writer w = new OutputStreamWriter(s, encoding);
		PrintWriter ps = new PrintWriter(w);
		String[] words = { "one", "twø", "three" };
		int i = 0;
		int expectedSum = 0;
		for (String word: words) {
			int n = i + 1;
			ps.println("" + n + "|" + word);
			if (i >= skipLines) {
				expectedSum += n;
			}
			i += 1;
		}
		ps.close();
		conn.setUploadHandler(new FileTransferHandler(d, encoding));
		String query = "COPY OFFSET " + (skipLines + 1) + " INTO foo FROM '" + fileName + "' ON CLIENT";
		update(query);
		assertQueryInt("SELECT SUM(i) FROM foo", expectedSum);
	}

	private void test_FileTransferHandlerUploadRefused() throws IOException, SQLException, Failure {
		initTest("test_FileTransferHandlerUploadRefused");
		prepare();
		Path d = getTmpDir(currentTestName);
		Path f = d.resolve("data.txt");
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		PrintStream ps = new PrintStream(s, false, "UTF-8");
		ps.println("1|one");
		ps.println("2|two");
		ps.println("3|three");
		ps.close();

		Path d2 = getTmpDir(currentTestName + "2");
		conn.setUploadHandler(new FileTransferHandler(d2, StandardCharsets.UTF_8));
		String quoted = f.toAbsolutePath().toString().replaceAll("'", "''");
		expectError("COPY INTO foo FROM R'"+ quoted + "' ON CLIENT", "not in upload directory");
		// connection is still alive
		assertQueryInt("SELECT SUM(i) FROM foo", 0);
		exitTest();
	}

	private void test_FileTransferHandlerDownloadUtf8() throws SQLException, Failure, IOException {
		initTest("test_FileTransferHandlerDownloadUtf8");
		testFileTransferHandlerDownload(StandardCharsets.UTF_8, StandardCharsets.UTF_8);
		exitTest();
	}

	private void test_FileTransferHandlerDownloadLatin1() throws SQLException, Failure, IOException {
		initTest("test_FileTransferHandlerDownloadLatin1");
		Charset latin1 = Charset.forName("latin1");
		testFileTransferHandlerDownload(latin1, latin1);
		exitTest();
	}

	private void test_FileTransferHandlerDownloadNull() throws SQLException, Failure, IOException {
		initTest("test_FileTransferHandlerDownloadNull");
		testFileTransferHandlerDownload(null, Charset.defaultCharset());
		exitTest();
	}

	private void testFileTransferHandlerDownload(Charset handlerEncoding, Charset fileEncoding) throws SQLException, Failure, IOException {
		prepare();
		update("INSERT INTO foo VALUES (42, 'forty-twø')");
		Path d = getTmpDir(currentTestName);
		conn.setDownloadHandler(new FileTransferHandler(d, handlerEncoding));
		update("COPY SELECT * FROM foo INTO 'data.txt' ON CLIENT");
		List<String> lines = Files.readAllLines(d.resolve("data.txt"), fileEncoding);
		assertEq("lines written", lines.size(), 1);
		assertEq("line content", lines.get(0), "42|\"forty-twø\"");
		// connection is still alive
		assertQueryInt("SELECT SUM(i) FROM foo", 42);
	}

	private void test_FileTransferHandlerDownloadRefused() throws SQLException, Failure, IOException {
		initTest("test_FileTransferHandlerDownloadRefused");
		prepare();
		BugFixLevel level = getLevel();
		update("INSERT INTO foo VALUES (42, 'forty-two')");
		Path d = getTmpDir(currentTestName);
		Path d2 = getTmpDir(currentTestName + "2");
		conn.setDownloadHandler(new FileTransferHandler(d2, StandardCharsets.UTF_8));
		String quoted = d.resolve("data.txt").toAbsolutePath().toString().replaceAll("'", "''");
		expectError("COPY SELECT * FROM foo INTO R'" + quoted + "' ON CLIENT", "not in download directory");
		if (level.compareTo(BugFixLevel.CanRefuseDownload) >= 0) {
			// connection is still alive
			assertQueryInt("SELECT SUM(i) FROM foo", 42);
		}
		exitTest();
	}


	/* utility methods */
	private void fail(String message) throws Failure {
		outBuffer.append("FAILURE: ").append(message).append("\n");
		throw new Failure(message);
	}

	private void checked(String quantity, Object actual) {
		outBuffer.append("  CHECKED: <").append(quantity).append("> is ").append(actual).append(" as expected").append("\n");
	}

	private void assertEq(String quantity, Object expected, Object actual) throws Failure {
		if (expected.equals(actual)) {
			checked(quantity, actual);
		} else {
			fail("Expected <" + quantity + "> to be " + expected + " got " + actual);
		}
	}

	private boolean execute(String query) throws SQLException {
		outBuffer.append("EXECUTE: ").append(query).append("\n");
		final boolean result = stmt.execute(query);
		outBuffer.append("  OK");
		if (!result) {
			outBuffer.append(", updated ").append(stmt.getUpdateCount()).append(" rows");
		}
		outBuffer.append("\n");
		return result;
	}

	private void update(String query) throws SQLException, Failure {
		execute(query);
	}

	private void expectError(String query, String expectedError) throws SQLException {
		try {
			execute(query);
		} catch (SQLException e) {
			if (e.getMessage().contains(expectedError)) {
				outBuffer.append("  GOT EXPECTED EXCEPTION: ").append(e.getMessage()).append("\n");
			} else {
				throw e;
			}
		}
	}

	private void assertQueryInt(String query, int expected) throws SQLException, Failure {
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

	private void assertQueryString(String query, String expected) throws SQLException, Failure {
		final String result = queryString(query);
		assertEq("query result", expected, result);
	}

	private String queryString(String query) throws SQLException, Failure {
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

	private synchronized Path getTmpDir(String name) throws IOException {
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

	/**
	 * Implementation of an UploadHandler
	 */
	static class MyUploadHandler implements UploadHandler {
		private final long rows;
		private final long errorAt;
		private final String errorMessage;
		private boolean encounteredWriteError = false;
		private boolean cancelled = false;

		private int chunkSize = 100; // small number to trigger more bugs

		MyUploadHandler(long rows, long errorAt, String errorMessage) {
			this.rows = rows;
			this.errorAt = errorAt;
			this.errorMessage = errorMessage;
		}

		MyUploadHandler(long rows) {
			this(rows, -1, null);
		}

		MyUploadHandler(String errorMessage) {
			this(0, -1, errorMessage);
		}

		public void setChunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
		}

		@Override
		public void uploadCancelled() {
			cancelled = true;
		}

		@Override
		public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
			if (errorAt == -1 && errorMessage != null) {
				handle.sendError(errorMessage);
				return;
			}
			handle.setChunkSize(chunkSize);
			PrintStream stream = handle.getStream();
			for (long i = linesToSkip; i < rows; i++) {
				if (i == errorAt) {
					throw new IOException(errorMessage);
				}
				stream.printf("%d|%d%n", i + 1, i + 1);
				if (i % 25 == 0 && stream.checkError()) {
					encounteredWriteError = true;
					break;
				}
			}
		}

		public Object encounteredWriteError() {
			return encounteredWriteError;
		}

		public boolean isCancelled() {
			return cancelled;
		}
	}

	/**
	 * Implementation of a DownloadHandler
	 */
	static class MyDownloadHandler implements DownloadHandler {
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

		public int lineCount() {
			int lines = lineEndingsSeen;
			if (startOfLine != bytesSeen)
				lines++;
			return lines;
		}
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
}
