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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

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

	protected void prepare() throws SQLException {
		execute("DROP TABLE IF EXISTS foo");
		execute("CREATE TABLE foo (i INT, t TEXT)");
	}

	public void test_Upload() throws Exception {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT COUNT(*) FROM foo", 100);
	}

	public void test_ClientRefusesUpload() throws Exception {
		prepare();
		MyUploadHandler handler = new MyUploadHandler("immediate error");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "immediate error");
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT COUNT(*) FROM foo", 0);
	}

	public void test_Offset0() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 0 INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT MIN(i) FROM foo", 1);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_Offset1() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 1 INTO foo FROM 'banana' ON CLIENT", 100);
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT MIN(i) FROM foo", 1);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_Offset5() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY OFFSET 5 INTO foo FROM 'banana' ON CLIENT", 96);
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT MIN(i) FROM foo", 5);
		queryInt("SELECT MAX(i) FROM foo", 100);
	}

	public void test_ServerStopsReading() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		update("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT", 10);
		assertEq("handler encountered write error", true, handler.encounteredWriteError());
		// Server stopped reading after 10 rows. Will we stay in sync?
		queryInt("SELECT COUNT(i) FROM foo", 10);
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
	}

	public void test_Download() throws SQLException, Failure {
		test_Download(100);
	}

	public void test_ClientRefusesDownload() throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler("download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		expectError("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT", "download refused");
		// Wish it were different but the server closes the connection
		expectError("SELECT 42 -- check if the connection still works", "Connection to server lost!");
	}

	public void test_LargeUpload() throws SQLException, Failure {
		watchDog.setDuration(25_000);
		prepare();
		int n = 4_000_000;
		MyUploadHandler handler = new MyUploadHandler(n);
		conn.setUploadHandler(handler);
		handler.setChunkSize(1024 * 1024);
		update("COPY INTO foo FROM 'banana' ON CLIENT", n);
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
		queryInt("SELECT COUNT(DISTINCT i) FROM foo", n);
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
		queryInt("SELECT i FROM foo WHERE t = 'three'", 3);
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
		queryInt("SELECT i FROM foo WHERE t = 'three'", 3);
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
		queryInt("SELECT i FROM foo WHERE t = 'three'", 3);
	}

	public void test_FailUploadLate() throws SQLException, Failure {
		prepare();
		MyUploadHandler handler = new MyUploadHandler(100, 50, "i don't like line 50");
		conn.setUploadHandler(handler);
		expectError("COPY INTO foo FROM 'banana' ON CLIENT", "i don't like");
		assertEq("handler encountered write error", false, handler.encounteredWriteError());
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

	// Disabled because it hangs, triggering the watchdog timer
	public void test_FailDownloadLate() throws SQLException, Failure {
		prepare();
		MyDownloadHandler handler = new MyDownloadHandler(200, "download refused");
		conn.setDownloadHandler(handler);
		update("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)", 100);
		expectError("COPY (SELECT * FROM sys.generate_series(0,200)) INTO 'banana' ON CLIENT", "download refused");
		// Exception closes the connection
		assertEq("connection is closed", conn.isClosed(), true);
	}

	static class MyUploadHandler implements UploadHandler {
		private final long rows;
		private final long errorAt;
		private final String errorMessage;
		private boolean encounteredWriteError;

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

}
