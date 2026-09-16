/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */
package org.monetdb.jdbc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.monetdb.jdbc.MonetConnection.UploadHandler;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.monetdb.testinfra.Assertions.assertSQLException;


@Tag("api")
public class OnClientTests extends OnClientTestsParent {
	@Test
	public void testUpload() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY INTO foo FROM 'banana' ON CLIENT");
		assertFalse(handler.isCancelled());
		assertEquals(100, queryInt("SELECT COUNT(*) FROM foo"));
	}

	@Test
	public void testUploadCrLf() throws SQLException {
		UploadHandler handler = new UploadHandler() {
			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				String contentText = "100|foo\r\n10|bar\r\n1|baz\r\n";
				byte[] contentBytes = contentText.getBytes(StandardCharsets.UTF_8);
				ByteArrayInputStream contentStream = new ByteArrayInputStream(contentBytes);
				handle.uploadFrom(contentStream);
			}
		};
		conn.setUploadHandler(handler);
		stmt.execute("COPY INTO foo FROM 'banana' ON CLIENT");
		assertEquals(333, queryInt("SELECT SUM(i * LENGTH(t)) FROM foo"));
	}

	@Test
	public void testNormalizeCrLf() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintStream details = new PrintStream(bos);
		boolean different = false;
		Exception err = null;

		try {
			different = testNormalizeCrLf_(details);
		} catch (Exception e) {
			err = e;
		}

		if (different || err != null) {
			System.err.write(bos.toByteArray());
		}

		if (different) {
			fail("Normalized text is different than expected");
		}

	}

	private boolean testNormalizeCrLf_(PrintStream stream) throws IOException {
		boolean different = false;
		String[] fragments = {
				/* does not end in pending cr */ "\r\naaa\n\n\r\n",
				/* ends in pending cr */ "\n\r\naaa\r",
				/* clears it */ "\n",
				/* means call the single-argument write(), cr now pending */ "13",
				/* again, should flush the pending one and remain pending */ "13",
				/* now the pending cr should be dropped */ "10",
				/* same as above, but with arrays */ "\r", "\r", "\n",
				/* empty write should not clear the pending */ "\r", "", "\n",
				/* trailing \r */ "\r",};

		ByteArrayOutputStream out0 = new ByteArrayOutputStream();
		MonetConnection.StripCrLfStream out = new MonetConnection.StripCrLfStream(out0);
		ByteArrayOutputStream ref = new ByteArrayOutputStream();
		ArrayList<Integer> fragmentPositions = new ArrayList<Integer>();
		ArrayList<Boolean> wasPending = new ArrayList<Boolean>();
		for (String f : fragments) {
			int pos = out0.toByteArray().length;
			boolean pending = out.pending();
			fragmentPositions.add(pos);
			wasPending.add(pending);
			if (!f.isEmpty() && Character.isDigit(f.charAt(0))) {
				int n = Integer.parseInt(f);
				ref.write(n);
				out.write(n);
			} else {
				byte[] bytes = f.getBytes(StandardCharsets.UTF_8);
				ref.write(bytes);
				out.write(bytes);
			}
		}
		out.close();

		String data = out0.toString();
		String refData = ref.toString().replaceAll("\r\n", "\n");


		stream.print("GOT\t\tEXPECTED\n");
		int fragNo = 0;
		for (int i = 0; i < data.length() || i < refData.length(); i++) {
			while (fragNo < fragmentPositions.size() && i == fragmentPositions.get(fragNo)) {
				stream.print("(Start of fragment ");
				stream.print(fragNo);
				if (wasPending.get(fragNo)) {
					stream.print(", cr pending");
				} else {
					stream.print(", cr not pending");
				}
				stream.print(':');
				String frag = fragments[fragNo];
				if (!frag.isEmpty() && Character.isDigit(frag.charAt(0))) {
					stream.print(Integer.parseInt(frag));
				} else {
					for (int k = 0; k < frag.length(); k++) {
						int c = frag.charAt(k);
						stream.print(' ');
						stream.print(c);
						if (c == '\n' && k != frag.length() - 1)
							stream.print("  ");
					}
				}
				stream.print(")\n");
				fragNo++;
			}
			int left = i < data.length() ? data.charAt(i) : 0;
			int right = i < refData.length() ? refData.charAt(i) : 0;
			stream.print(left);
			stream.print("\t\t");
			stream.print(right);
			if (!different && left != right) {
				stream.print("\t\t <---------------------- first difference found!");
				different = true;
			}
			stream.print('\n');
		}
		return different;
	}

	@Test
	public void testClientRefusesUpload() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler("immediate error");
		conn.setUploadHandler(handler);
		assertSQLException("immediate error", () -> stmt.executeUpdate("COPY INTO foo FROM 'banana' ON CLIENT"));
		assertFalse(handler.isCancelled());
		assertEquals(0, queryInt("SELECT COUNT(*) FROM foo"));
	}

	@Test
	public void testOffset0() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY OFFSET 0 INTO foo FROM 'banana' ON CLIENT");
		assertFalse(handler.isCancelled());
		assertEquals(1, queryInt("SELECT MIN(i) FROM foo"));
		assertEquals(100, queryInt("SELECT MAX(i) FROM foo"));
	}

	@Test
	public void testOffset1() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY OFFSET 1 INTO foo FROM 'banana' ON CLIENT");
		assertFalse(handler.isCancelled());
		assertEquals(1, queryInt("SELECT MIN(i) FROM foo"));
		assertEquals(100, queryInt("SELECT MAX(i) FROM foo"));
	}

	@Test
	public void testOffset5() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler(100);
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY OFFSET 5 INTO foo FROM 'banana' ON CLIENT");
		assertFalse(handler.isCancelled());
		assertEquals(5, queryInt("SELECT MIN(i) FROM foo"));
		assertEquals(100, queryInt("SELECT MAX(i) FROM foo"));
	}

	@Test
	public void testServerStopsReading() throws SQLException {
		long n = 2 * 1024 * 1024 / 10;
		MyUploadHandler handler = new MyUploadHandler(n);
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY 10 RECORDS INTO foo FROM 'banana' ON CLIENT");
		assertTrue(handler.isCancelled());
		assertTrue(handler.encounteredWriteError());
		// connection is still alive
		assertEquals(10, queryInt("SELECT COUNT(i) FROM foo"));
	}

	public void testDownload_(int n) throws SQLException {
		MyDownloadHandler handler = new MyDownloadHandler();
		conn.setDownloadHandler(handler);
		String q = "INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, " + n + ")";
		stmt.executeUpdate(q);
		stmt.executeUpdate("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT");
		assertEquals(1, handler.countAttempts());
		assertEquals(n, handler.lineCount());
		// connection is still alive
		assertEquals(n, queryInt("SELECT COUNT(*) FROM foo"));
	}

	@Test
	public void testDownload() throws SQLException {
		testDownload_(100);
	}

	@Test
	@Tag("slow")
	@DisabledIf("org.monetdb.testinfra.Config#isSkipSlow")
	public void testLargeDownload() throws SQLException {
		testDownload_(4_000_000);
	}

	@Test
	public void testClientRefusesDownload() throws SQLException {
		// retrieve the bugfixlevel while the connection still works
		MyDownloadHandler handler = new MyDownloadHandler("download refused");
		conn.setDownloadHandler(handler);
		stmt.executeUpdate("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)");
		assertSQLException("download refused", () -> stmt.executeUpdate("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT"));
		if (monetVersion.serverCanRefuseDownload())
			assertEquals(100, queryInt("SELECT COUNT(*) FROM foo"));
		else
			// Wish it were different but the server closes the connection
			assertSQLException("", () -> stmt.executeUpdate("SELECT 42 -- check if the connection still works"));
	}

	@Test
	@Tag("slow")
	@DisabledIf("org.monetdb.testinfra.Config#isSkipSlow")
	public void testLargeUpload() throws SQLException {
		int n = 4_000_000;
		MyUploadHandler handler = new MyUploadHandler(n);
		conn.setUploadHandler(handler);
		handler.setChunkSize(1024 * 1024);
		stmt.executeUpdate("COPY INTO foo FROM 'banana' ON CLIENT");
		assertFalse(handler.isCancelled());
		// connection is still alive
		assertEquals(n, queryInt("SELECT COUNT(DISTINCT i) FROM foo"));
	}

	@Test
	public void testDownloadCrLf() throws SQLException, IOException {

		// This tests forces line ending conversion and reads in small batches, hoping to trigger corner cases

		stmt.executeUpdate("ALTER TABLE foo DROP COLUMN t");
		stmt.executeUpdate("ALTER TABLE foo ADD COLUMN j INT");
		stmt.executeUpdate("INSERT INTO foo SELECT rand() % CASE WHEN value % 10 = 0 THEN 1000 ELSE 10 END AS i, 0 AS j FROM generate_series(0, 500000)");
		ByteArrayOutputStream target = new ByteArrayOutputStream();
		Random rng = new Random(42);
		MonetConnection.DownloadHandler handler = (handle, name, textMode) -> {
			handle.setLineSeparator("\r\n");
			InputStream s = handle.getStream();
			byte[] buf = new byte[10];
			boolean expectEof = false;
			for (; ; ) {
				int n = rng.nextInt(buf.length - 1) + 1;
				int nread = s.read(buf, 0, n);
				if (nread < 0) {
					break;
				}
				target.write(buf, 0, nread);
			}

		};
		conn.setDownloadHandler(handler);
		stmt.executeUpdate("COPY SELECT * FROM foo INTO 'banana' ON CLIENT");
		// go to String instead of byte[] because Strings have handy replace methods.
		String result = new String(target.toByteArray(), StandardCharsets.UTF_8);

		// It should contain only \r\n's, no lonely \r's or \n's.
		String replaced = result.replaceAll("\r\n", "XX");
		assertEquals(-1, replaced.indexOf('\r'));
		assertEquals(-1, replaced.indexOf('\n'));

		String withoutData = result.replaceAll("[0-9]", "");
		// Replacement leaves three characters per line: SEP CR LF
		assertEquals(0, withoutData.length() % 3);
		for (int i = 0; i < withoutData.length(); i += 3) {
			String sub = withoutData.substring(i, i + 3);
			if (!sub.equals("|\r\n")) {
				fail(String.format("At index %d out of %d in the skeleton (=digits removed) we find <%02x %02x %02x> instead of <7c 0d 0a>", i, withoutData.length(), (int) sub.charAt(0), (int) sub.charAt(1), (int) sub.charAt(2)));
			}
		}
	}

	@Test
	public void testUploadFromStream() throws SQLException {
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
		stmt.executeUpdate("COPY INTO foo FROM 'banana' ON CLIENT");
		// connection is still alive
		assertEquals(3, queryInt("SELECT i FROM foo WHERE t = 'three'"));
	}

	@Test
	public void testUploadFromReader() throws SQLException {
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
		stmt.executeUpdate("COPY INTO foo FROM 'banana' ON CLIENT");
		assertEquals(3, queryInt("SELECT i FROM foo WHERE t = 'three'"));
	}

	@Test
	public void testUploadFromReaderOffset() throws SQLException {
		UploadHandler handler = new UploadHandler() {
			final String data = "1|one\n2|two\n3|three\n";

			@Override
			public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
				BufferedReader r = new BufferedReader(new StringReader(data));
				handle.uploadFrom(r, linesToSkip);
			}
		};
		conn.setUploadHandler(handler);
		stmt.executeUpdate("COPY OFFSET 2 INTO foo FROM 'banana' ON CLIENT");
		assertEquals(2, queryInt("SELECT MIN(i) FROM foo"));
		assertEquals(3, queryInt("SELECT i FROM foo WHERE t = 'three'"));
	}

	@Test
	public void testFailUploadLate() throws SQLException {
		MyUploadHandler handler = new MyUploadHandler(100, 50, "i don't like line 50");
		conn.setUploadHandler(handler);
		assertSQLException("i don't like", () -> stmt.execute("COPY INTO foo FROM 'banana' ON CLIENT"));
		assertFalse(handler.isCancelled());
		assertTrue(conn.isClosed());
	}

	@Test
	public void testFailUploadLate2() throws SQLException {
		// Here we send empty lines only, to check if the server detects it properly instead
		// of simply complaining about an incomplete file.
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
		assertSQLException("after all", () -> stmt.execute("COPY INTO foo(t) FROM 'banana'(t) ON CLIENT"));
		assertTrue(conn.isClosed());
		// Cannot check the server log, but at the time I checked, it said "prematurely stopped client", which is fine.
	}

	@Test
	public void testFailDownloadLate() throws SQLException {
		MyDownloadHandler handler = new MyDownloadHandler(200, "download refused");
		conn.setDownloadHandler(handler);
		stmt.executeUpdate("INSERT INTO foo SELECT value as i, 'number' || value AS t FROM sys.generate_series(0, 100)");
		assertSQLException("download refused", () -> stmt.execute("COPY (SELECT * FROM sys.generate_series(0,200)) INTO 'banana' ON CLIENT"));
		// Exception closes the connection
		assertTrue(conn.isClosed());
	}
}
