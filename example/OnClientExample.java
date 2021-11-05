/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

import org.monetdb.jdbc.MonetConnection;
import org.monetdb.jdbc.MonetConnection.UploadHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

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
					"COPY INTO mytable FROM 'generated.csv' ON CLIENT",
					"COPY 20 RECORDS OFFSET 5 INTO mytable FROM 'generated.csv' ON CLIENT",
					"COPY INTO mytable FROM 'nonexistentfilethatdoesnotexist.csv' ON CLIENT",
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

		// Register upload handler
		MyUploader handler = new MyUploader(uploadDir, filesAreUtf8);
		conn.unwrap(MonetConnection.class).setUploadHandler(handler);

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


	private static class MyUploader implements UploadHandler {
		private final Path uploadDir;
		private final boolean filesAreUtf8;
		private boolean stopUploading = false;

		public MyUploader(String uploadDir, boolean filesAreUtf8) {
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
				return;
			}

			// Validate the path, demonstrating two ways of dealing with errors
			Path path = securityCheck(name);
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
				// Charset and skip handling really necessary
				uploadAsText(handle, path, linesToSkip);
			}
		}

		private Path securityCheck(String name) {
			Path p = uploadDir.resolve(name).normalize();
			if (p.startsWith(uploadDir)) {
				return p;
			} else {
				return null;
			}
		}

		private void uploadGeneratedData(MonetConnection.Upload handle, long toSkip) throws IOException {
			// Set the chunk size to a tiny amount so we can demonstrate
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

		private void uploadAsText(MonetConnection.Upload handle, Path path, long toSkip) throws IOException {
			BufferedReader reader = Files.newBufferedReader(path);// Converts from system encoding to Java text
			for (long i = 0; i < toSkip; i++) {
				reader.readLine();
			}
			handle.uploadFrom(reader); // Converts from Java text to UTF-8 as required by MonetDB
		}

		private void uploadAsBinary(MonetConnection.Upload handle, Path path) throws IOException {
			// No charset conversion whatsoever..
			// Use this for binary data or when you are certain the file is UTF-8 encoded.
			InputStream stream = Files.newInputStream(path);
			handle.uploadFrom(stream);
		}
	}
}
