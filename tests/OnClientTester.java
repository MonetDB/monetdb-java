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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

public final class OnClientTester extends TestRunner {

	public OnClientTester(String jdbcUrl, int verbosity, boolean watchDogEnabled) {
		super(jdbcUrl, verbosity, watchDogEnabled);
	}

	public static void main(String[] args) throws SQLException, NoSuchMethodException, ClassNotFoundException {
		String jdbcUrl = null;
		String requiredPrefix = null;
		int verbosity = 0;
		boolean watchDogEnabled = true;

		// Don't know why I need this all of a sudden.. is it only on my system?
		Class.forName("org.monetdb.jdbc.MonetDriver");

		for (String arg : args) {
			if (arg.equals("-v"))
				verbosity++;
			else if (arg.equals("-vv"))
				verbosity += 2;
			else if (arg.equals("-w"))
				watchDogEnabled = false;
			else if (jdbcUrl == null)
				jdbcUrl = arg;
			else if (requiredPrefix == null)
				requiredPrefix = arg;
			else {
				System.err.println("Unexpected argument " + arg);
				System.exit(2);
			}
		}

		OnClientTester tester = new OnClientTester(jdbcUrl, verbosity, watchDogEnabled);
		int failures = tester.runTests(requiredPrefix);

		if (failures > 0)
			System.exit(1);
	}

	/// Some tests have to work limitations of the protocol or bugs in the server.
	/// This Enum is used to indicate the possibilities.
	public enum BugFixLevel {
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
			for (BugFixLevel level: BugFixLevel.values()) {
				if (level.includesVersion(major, minor, micro))
					lastValid = level;
				else
					break;
			}
			return lastValid;
		}
	}

	void prepare() throws SQLException {
		execute("DROP TABLE IF EXISTS foo");
		execute("CREATE TABLE foo (i INT, t TEXT)");
	}

	private BugFixLevel getLevel() throws SQLException, Failure {
		String version = queryString("SELECT value FROM environment WHERE name = 'monet_version'");
		BugFixLevel level = BugFixLevel.forVersion(version);
		out.println("  NOTE: version " + version + " means level = " + level);
		return level;
	}

	public void test_BugFixLevel() throws Failure {
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
	}

	public void test_Upload() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT COUNT(*) FROM foo", 100);
	}

	public void test_ClientRefusesUpload() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler("immediate error");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "immediate error");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT COUNT(*) FROM foo", 0);
	}

	public void test_Offset0() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 0 INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 1);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_Offset1() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 1 INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 1);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_Offset5() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 5 INTO foo FROM 'banana' ON CLIENT", 96);
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertQueryInt("SELECT MIN(i) FROM foo", 5);
		assertQueryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_ServerStopsReading() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT", 10);
		assertEq("cancellation callback called", true, handler.isCancelled());
		assertEq("handler encountered write error", true, handler.encounteredWriteError());
		// connection is still alive
		assertQueryInt("SELECT COUNT(i) FROM foo", 10);
	}

	public void test_Download(int n) throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler();
		conn.setDownloadHandler(handler);
		String q = "INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, " + n + ")";
		update(q, n);
		update("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", -1);
		assertEq("download attempts", 1, handler.countAttempts());
		assertEq("lines downloaded", n, handler.lineCount());
		// connection is still alive
		assertQueryInt("SELECT COUNT(*) FROM foo", n);
	}

	public void test_Download() throws SQLException, Failure {
		test_Download(100);
	}

	public void test_ClientRefusesDownload() throws SQLException, Failure {
		prepare();
		BugFixLevel level = getLevel();
		MyDownloadHandler handler = new MyDownloadHandler("download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		expectError("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", "download refused");
		// Wish it were different but the server closes the connection
		expectError("SELECT 42 -- check if the connection still works", "Connection to server lost!");
		if (level.compareTo(BugFixLevel.CanRefuseDownload) >= 0) {
			// connection is still alive
			assertQueryInt("SELECT COUNT(*) FROM foo", 100);
		}
	}

	public void test_LargeUpload() throws SQLException, Failure {
		watchDog.setDuration(25_000);
		prepare();
		int n = 4_000_000;
		MyUploadHandler handler = new MyUploadHandler(n);
		conn.setUploadHandler(handler);
		handler.setChunkSize(1024 * 1024);
		update("COPY INTO foo FROM 'banana' ON CLIENT", n);
		assertEq("cancellation callback called", false, handler.isCancelled());
		// connection is still alive
		assertQueryInt("SELECT COUNT(DISTINCT i) FROM foo", n);
	}

	public void test_LargeDownload() throws SQLException, Failure {
		watchDog.setDuration(25_000);
		test_Download(4_000_000);
	}

	public void test_UploadFromStream() throws SQLException, Failure {
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
		update("COPY INTO foo FROM 'banana' ON CLIENT", 3);
		// connection is still alive
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
	}

	public void test_UploadFromReader() throws SQLException, Failure {
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
		update("COPY INTO foo FROM 'banana' ON CLIENT", 3);
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
	}

	public void test_UploadFromReaderOffset() throws SQLException, Failure {
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
		update("COPY OFFSET 2 INTO foo FROM 'banana' ON CLIENT", 2);
		assertQueryInt("SELECT i FROM foo WHERE t = 'three'", 3);
	}

	public void test_FailUploadLate() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100, 50, "i don't like line 50");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "i don't like");
		assertEq("cancellation callback called", false, handler.isCancelled());
		assertEq("connection is closed", true, conn.isClosed());
	}


	public void test_FailUploadLate2() throws SQLException, Failure {
		// Here we send empty lines only, to check if the server detects is properly instead
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
	}

	public void test_FailDownloadLate() throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler(200, "download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		expectError("COPY (SELECT * FROM sys.generate_series(0,200)) INTO 'banana' ON CLIENT", "download refused");
		// Exception closes the connection
		assertEq("connection is closed", conn.isClosed(), true);
	}

	public void test_FileTransferHandlerUpload() throws IOException, SQLException, Failure {
		prepare();
		Path d = getTmpDir(currentTestName);
		Path f = d.resolve("data.txt");
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		PrintStream ps = new PrintStream(s, false, "UTF-8");
		ps.println("1|one");
		ps.println("2|two");
		ps.println("3|three");
		ps.close();
		conn.setUploadHandler(new FileTransferHandler(d, true));
		update("COPY INTO foo FROM 'data.txt' ON CLIENT", 3);
		assertQueryInt("SELECT SUM(i) FROM foo", 6);
	}

	public void test_FileTransferHandlerUploadRefused() throws IOException, SQLException, Failure {
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
		conn.setUploadHandler(new FileTransferHandler(d2, false));
		String quoted = f.toAbsolutePath().toString().replaceAll("'", "''");
		expectError("COPY INTO foo FROM R'"+ quoted + "' ON CLIENT", "not in upload directory");
		// connection is still alive
		assertQueryInt("SELECT SUM(i) FROM foo", 0);
	}

	public void test_FileTransferHandlerDownload() throws SQLException, Failure, IOException {
		prepare();
		update("INSERT INTO foo VALUES (42, 'forty-two')", 1);
		Path d = getTmpDir(currentTestName);
		conn.setDownloadHandler(new FileTransferHandler(d, false));
		update("COPY SELECT * FROM foo INTO 'data.txt' ON CLIENT", -1);
		List<String> lines = Files.readAllLines(d.resolve("data.txt"));
		assertEq("lines written", lines.size(), 1);
		assertEq("line content", lines.get(0), "42|\"forty-two\"");
		// connection is still alive
		assertQueryInt("SELECT SUM(i) FROM foo", 42);
	}

	public void test_FileTransferHandlerDownloadRefused() throws SQLException, Failure, IOException {
		prepare();
		BugFixLevel level = getLevel();
		update("INSERT INTO foo VALUES (42, 'forty-two')", 1);
		Path d = getTmpDir(currentTestName);
		Path d2 = getTmpDir(currentTestName + "2");
		conn.setDownloadHandler(new FileTransferHandler(d2, false));
		String quoted = d.resolve("data.txt").toAbsolutePath().toString().replaceAll("'", "''");
		expectError("COPY SELECT * FROM foo INTO R'" + quoted + "' ON CLIENT", "not in download directory");
		if (level.compareTo(BugFixLevel.CanRefuseDownload) >= 0) {
			// connection is still alive
			assertQueryInt("SELECT SUM(i) FROM foo", 42);
		}
	}

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

}
