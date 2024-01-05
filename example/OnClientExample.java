/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

import org.monetdb.jdbc.MonetConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;

public class OnClientExample {

	public static void main(String[] args) {
		int status;
		try {
			// Ideally this would not be hardcoded..
			final String dbUrl = "jdbc:monetdb://localhost:50000/demo";
			final String userName = "monetdb";
			final String password = "monetdb";
			final String uploadDir = "/home/jvr/mydata";
			final boolean filesAreUtf8 = false;
			String[] queries = {
					"DROP TABLE IF EXISTS mytable",
					"CREATE TABLE mytable(i INT, t TEXT)",

					// with this filename our handler will make up data by itself
					"COPY INTO mytable FROM 'generated.csv' ON CLIENT",

					// with this filename our handler will access the file system
					"COPY INTO mytable FROM 'data.csv' ON CLIENT",

					// with this statement, the server will ask the handler to stop halfway
					"COPY 20 RECORDS OFFSET 5 INTO mytable FROM 'generated.csv' ON CLIENT",

					// this demonstrates sending errors
					"COPY INTO mytable FROM 'nonexistentfilethatdoesnotexist.csv' ON CLIENT",

					// downloads but does not write to file, only counts the lines
					"COPY SELECT * FROM mytable INTO 'justcount.csv' ON CLIENT",

					// downloads to actual file
					"COPY SELECT * FROM mytable INTO 'download.csv' ON CLIENT",

					// demonstrate that the connection is still alive
					"SELECT COUNT(*) FROM mytable",
			};

			status = run(dbUrl, userName, password, uploadDir, filesAreUtf8, queries);

		} catch (Exception e) {
			status = 1;
			e.printStackTrace();
		}
		System.exit(status);
	}

	private static int run(String dbUrl, String userName, String password, String uploadDir, boolean filesAreUtf8, String[] queries) throws ClassNotFoundException, SQLException {
		int status = 0;

		// Connect
		Class.forName("org.monetdb.jdbc.MonetDriver");
		Connection conn = DriverManager.getConnection(dbUrl, userName, password);

		// Register upload- and download handler
		MyHandler handler = new MyHandler(uploadDir, filesAreUtf8);
		MonetConnection monetConnection = conn.unwrap(MonetConnection.class);
		monetConnection.setUploadHandler(handler);
		monetConnection.setDownloadHandler(handler);

		Statement stmt = conn.createStatement();
		for (String q : queries) {
			System.out.println(q);
			try {
				boolean hasResultSet = stmt.execute(q);
				if (hasResultSet) {
					ResultSet rs = stmt.getResultSet();
					long count = 0;
					while (rs.next()) {
						count++;
					}
					rs.close();
					System.out.printf("  OK, returned %d rows%n", count);
				} else {
					System.out.printf("  OK, updated %d rows%n", stmt.getUpdateCount());
				}
			} catch (SQLNonTransientException e) {
				throw e;
			} catch (SQLException e) {
				System.out.println("  => SQL ERROR " + e.getMessage());
				status = 1;
			}
		}

		return status;
	}


	private static class MyHandler implements MonetConnection.UploadHandler, MonetConnection.DownloadHandler {
		private final Path uploadDir;
		private final boolean filesAreUtf8;
		private boolean stopUploading = false;

		public MyHandler(String uploadDir, boolean filesAreUtf8) {
			this.uploadDir = FileSystems.getDefault().getPath(uploadDir).normalize();
			this.filesAreUtf8 = filesAreUtf8;
		}

		@Override
		public void uploadCancelled() {
			System.out.println("  CANCELLATION CALLBACK: server cancelled the upload");
			stopUploading = true;
		}

		@Override
		public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
			// We can upload data read from the file system but also make up our own data
			if (name.equals("generated.csv")) {
				uploadGeneratedData(handle, linesToSkip);
			} else {
				uploadFileData(handle, name, textMode, linesToSkip);
			}
		}

		@Override
		public void handleDownload(MonetConnection.Download handle, String name, boolean textMode) throws IOException {
			if (name.equals("justcount.csv")) {
				justCountLines(handle);
			} else {
				downloadFileData(handle, name, textMode);
			}
		}

		private Path securelyResolvePath(String name) {
			Path p = uploadDir.resolve(name).normalize();
			if (p.startsWith(uploadDir)) {
				return p;
			} else {
				return null;
			}
		}

		private void uploadGeneratedData(MonetConnection.Upload handle, long toSkip) throws IOException {
			// Set the chunk size to a tiny amount, so we can demonstrate
			// cancellation handling. The default chunk size is one megabyte.
			// DO NOT DO THIS IN PRODUCTION!
			handle.setChunkSize(50);

			// Make up some data and upload it.
			PrintStream stream = handle.getStream();
			long n = 100;
			System.out.printf("  HANDLER: uploading %d generated lines, numbered %d to %d%n", n - toSkip, toSkip + 1, n);
			long i;
			for (i = toSkip + 1; i <= n; i++) {
				if (stopUploading) {
					System.out.printf("  HANDLER: at line %d we noticed the server asked us to stop sending%n", i);
					break;
				}
				stream.printf("%d|the number is %d%n", i, i);
			}
			System.out.println("  HANDLER: done uploading");
			stream.close();
		}

		private void uploadFileData(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
			// Validate the path, demonstrating two ways of dealing with errors
			Path path = securelyResolvePath(name);
			if (path == null || !Files.exists(path)) {
				// This makes the COPY command fail but keeps the connection
				// alive. Can only be used if we haven't sent any data yet
				handle.sendError("Invalid path");
				return;
			}
			if (!Files.isReadable(path)) {
				// As opposed to handle.sendError(), we can throw an IOException
				// at any time. Unfortunately, the file upload protocol does not
				// provide a way to indicate to the server that the data sent so
				// far is incomplete, so for the time being throwing an
				// IOException from {@handleUpload} terminates the connection.
				throw new IOException("Unreadable: " + path);
			}

			boolean binary = !textMode;
			if (binary) {
				uploadAsBinary(handle, path);
			} else if (linesToSkip == 0 && filesAreUtf8) {
				// Avoid unnecessary UTF-8 -> Java String -> UTF-8 conversions
				// by pretending the data is binary.
				uploadAsBinary(handle, path);
			} else {
				// Charset and skip handling are really necessary
				uploadAsText(handle, path, linesToSkip);
			}
		}

		private void uploadAsText(MonetConnection.Upload handle, Path path, long toSkip) throws IOException {
			BufferedReader reader = Files.newBufferedReader(path);// Converts from system encoding to Java text
			for (long i = 0; i < toSkip; i++) {
				reader.readLine();
			}
			// This variant of uploadFrom takes a Reader
			handle.uploadFrom(reader); // Converts from Java text to UTF-8 as required by MonetDB
		}

		private void uploadAsBinary(MonetConnection.Upload handle, Path path) throws IOException {
			// No charset conversion whatsoever..
			// Use this for binary data or when you are certain the file is UTF-8 encoded.
			InputStream stream = Files.newInputStream(path);
			// This variant of uploadFrom takes a Stream
			handle.uploadFrom(stream);
		}

		private void justCountLines(MonetConnection.Download handle) throws IOException {
			System.out.println("  HANDLER: not writing the download to file, just counting the lines");
			InputStream stream = handle.getStream();
			InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8); // MonetDB always sends UTF-8
			BufferedReader bufreader = new BufferedReader(reader);
			long count = 0;
			while (bufreader.readLine() != null) {
				count++;
			}
			System.out.println("  HANDLER: file had " + count + " lines");
		}

		private void downloadFileData(MonetConnection.Download handle, String name, boolean textMode) throws IOException {
			Path path = securelyResolvePath(name);
			if (path == null) {
				handle.sendError("Illegal path");
				return;
			}

			OutputStream stream = Files.newOutputStream(path);
			if (!textMode || filesAreUtf8) {
				handle.downloadTo(stream);
				stream.close(); // do not forget this
			} else {
				OutputStreamWriter writer = new OutputStreamWriter(stream, Charset.defaultCharset()); // let system decide the encoding
				BufferedWriter bufwriter = new BufferedWriter(writer);
				handle.downloadTo(bufwriter);
				bufwriter.close(); // do not forget this
			}
		}
	}
}
