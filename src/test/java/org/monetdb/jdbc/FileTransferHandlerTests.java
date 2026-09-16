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
import org.junit.jupiter.api.io.TempDir;
import org.monetdb.util.FileTransferHandler;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.monetdb.testinfra.Assertions.assertSQLException;

@Tag("api")
public class FileTransferHandlerTests extends OnClientTestsParent {

	@Test
	public void testUploadNotCompressed(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadCompressed(tempDir, StandardCharsets.UTF_8, false, "", 0, true);
	}

	@Test
	public void testUploadNotCompressedSkip(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadCompressed(tempDir, StandardCharsets.UTF_8, false, "", 2, true);
	}

	@Test
	public void testUploadCompressed(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadCompressed(tempDir, StandardCharsets.UTF_8, true, ".gz", 0, true);
	}

	@Test
	public void testUploadCompressedSkip(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadCompressed(tempDir, StandardCharsets.UTF_8, true, ".gz", 2, true);
	}

	@Test
	public void testUploadCompressionDisabled(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadCompressed(tempDir, StandardCharsets.UTF_8, false, ".gz", 0, false);
	}

	private void testFileTransferHandlerUploadCompressed(Path tempDir, Charset encoding, boolean compressData, String suffix, int skipLines, boolean compressionEnabled) throws IOException, SQLException {
		String fileName = "data.txt";
		fileName += suffix;
		Path f = tempDir.resolve(fileName);
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		if (compressData) {
			s = new GZIPOutputStream(s);
		}
		Writer w = new OutputStreamWriter(s, encoding);
		PrintWriter ps = new PrintWriter(w);
		String[] words = {"one", "twø", "three"};
		int i = 0;
		int expectedSum = 0;
		for (String word : words) {
			int n = i + 1;
			ps.println(n + "|" + word);
			if (i >= skipLines) {
				expectedSum += n;
			}
			i += 1;
		}
		ps.close();
		conn.setUploadHandler(new FileTransferHandler(tempDir, encoding, compressionEnabled));
		String query = "COPY OFFSET " + (skipLines + 1) + " INTO foo FROM '" + fileName + "' ON CLIENT";
		stmt.executeUpdate(query);
		assertEquals(expectedSum, queryInt("SELECT SUM(i) FROM foo"));
	}

	@Test
	public void testUploadUtf8(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadEncoding(tempDir, StandardCharsets.UTF_8, "UTF-8");
	}

	@Test
	public void testUploadLatin1(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadEncoding(tempDir, Charset.forName("latin1"), "latin1");
	}

	@Test
	public void testUploadNull(@TempDir Path tempDir) throws IOException, SQLException {
		testFileTransferHandlerUploadEncoding(tempDir, null, Charset.defaultCharset().name());
	}

	private String hexdump(String s) {
		StringWriter w = new StringWriter();
		PrintWriter pw = new PrintWriter(w);
		char[] chars = s.toCharArray();
		String sep = "";
		for (char c : chars) {
			pw.printf("%s%02X", sep, (int) c);
			sep = " ";
		}
		return "<" + w + ">";
	}

	private void testFileTransferHandlerUploadEncoding(Path tempDir, Charset handlerEncoding, String fileEncoding) throws IOException, SQLException {
		Path f = tempDir.resolve("data.txt");
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		PrintStream ps = new PrintStream(s, false, fileEncoding);
		ps.println("1|one");
		ps.println("2|twø");
		ps.println("3|three");
		ps.close();
		conn.setUploadHandler(new FileTransferHandler(tempDir, handlerEncoding));
		stmt.executeUpdate("COPY INTO foo FROM 'data.txt' ON CLIENT");
		assertEquals(6, queryInt("SELECT SUM(i) FROM foo"));
		final String result = queryString("SELECT t FROM foo WHERE i = 2");
		String expected = "twø";
		//
		String hexResult = hexdump(result);
		String hexExpected = hexdump(expected);
		assertEquals(hexExpected, hexResult);
	}

	@Test
	public void testUploadRefused(@TempDir Path tempDir1, @TempDir Path tempDir2) throws IOException, SQLException {
		Path f = tempDir1.resolve("data.txt");
		OutputStream s = Files.newOutputStream(f, CREATE_NEW);
		PrintStream ps = new PrintStream(s, false, "UTF-8");
		ps.println("1|one");
		ps.println("2|two");
		ps.println("3|three");
		ps.close();

		conn.setUploadHandler(new FileTransferHandler(tempDir2, StandardCharsets.UTF_8));
		String quotedAbsPath = f.toAbsolutePath().toString().replaceAll("'", "''");
		String query = "COPY INTO foo FROM R'" + quotedAbsPath + "' ON CLIENT";
		assertSQLException("not in upload directory", () -> stmt.execute(query));
		// connection is still alive
		assertEquals(0, queryInt("SELECT SUM(i) FROM foo"));
	}

	@Test
	public void testDownloadUtf8(@TempDir Path tempDir) throws SQLException, IOException {
		testFileTransferHandlerDownload(tempDir, StandardCharsets.UTF_8, StandardCharsets.UTF_8);
	}

	@Test
	public void testDownloadLatin1(@TempDir Path tempDir) throws SQLException, IOException {
		Charset latin1 = Charset.forName("latin1");
		testFileTransferHandlerDownload(tempDir, latin1, latin1);
	}

	@Test
	public void testDownloadNull(@TempDir Path tempDir) throws SQLException, IOException {
		testFileTransferHandlerDownload(tempDir, null, Charset.defaultCharset());
	}

	private void testFileTransferHandlerDownload(@TempDir Path tempDir, Charset handlerEncoding, Charset fileEncoding) throws SQLException, IOException {
		stmt.executeUpdate("INSERT INTO foo VALUES (42, 'forty-twø')");
		conn.setDownloadHandler(new FileTransferHandler(tempDir, handlerEncoding));
		stmt.executeUpdate("COPY SELECT * FROM foo INTO 'data.txt' ON CLIENT");
		List<String> lines = Files.readAllLines(tempDir.resolve("data.txt"), fileEncoding);
		assertEquals(1, lines.size());
		assertEquals("42|\"forty-twø\"", lines.get(0));
		// connection is still alive
		assertEquals(42, queryInt("SELECT SUM(i) FROM foo"));
	}

	@Test
	public void testDownloadRefused(@TempDir Path tempDir1, @TempDir Path tempDir2) throws SQLException, IOException {
		stmt.executeUpdate("INSERT INTO foo VALUES (42, 'forty-two')");
		conn.setDownloadHandler(new FileTransferHandler(tempDir2, StandardCharsets.UTF_8));
		String quoted = tempDir1.resolve("data.txt").toAbsolutePath().toString().replaceAll("'", "''");
		assertSQLException("not in download directory", () -> stmt.execute("COPY SELECT * FROM foo INTO R'" + quoted + "' ON CLIENT"));
		if (monetVersion.serverCanRefuseDownload()) {
			// connection is still alive
			assertEquals(42, queryInt("SELECT SUM(i) FROM foo"));
		}
	}


}
