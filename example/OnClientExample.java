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
			final String dbUrl = "jdbc:monetdb://localhost:55000/banana";
			final String uploadDir = "/home/jvr/mydata";
			final boolean filesAreUtf8 = false;
			final String userName = "monetdb";
			final String password = "monetdb";

			status = run(dbUrl, userName, password, uploadDir, filesAreUtf8);

		} catch (Exception e) {
			status = 1;
			e.printStackTrace();
		}
		System.exit(status);
	}

	private static int run(String dbUrl, String userName, String password, String uploadDir, boolean filesAreUtf8) throws ClassNotFoundException, SQLException {
		int status = 0;

		// Connect
		Class.forName("org.monetdb.jdbc.MonetDriver");
		Connection conn = DriverManager.getConnection(dbUrl, userName, password);

		// Register upload handler
		MyUploader handler = new MyUploader(uploadDir, filesAreUtf8);
		conn.unwrap(MonetConnection.class).setUploadHandler(handler);

		// Run some SQL statements involving ON CLIENT
		String[] queries = {
				"DROP TABLE IF EXISTS bar",
				"CREATE TABLE bar(i INT, t TEXT)",
				"COPY INTO bar FROM 'generated.csv' ON CLIENT",
				"COPY INTO bar FROM 'file.csv' ON CLIENT",
				// following statement will run even if file.csv does not exist
				"SELECT COUNT(*) FROM bar",
		};
		Statement stmt = conn.createStatement();
		for (String q : queries) {
			System.out.println(q);
			try {
				stmt.execute(q);
				ResultSet rs = stmt.getResultSet();
				if (rs == null) {
					System.out.printf("  OK, %d rows updated%n", stmt.getUpdateCount());
				} else {
					long count = 0;
					while (rs.next()) {
						count++;
					}
					System.out.printf("  OK, returned %d rows%n", count);
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

		public MyUploader(String uploadDir, boolean filesAreUtf8) {
			this.uploadDir = FileSystems.getDefault().getPath(uploadDir).normalize();
			this.filesAreUtf8 = filesAreUtf8;
		}

		@Override
		public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {

			// COPY OFFSET line numbers are 1-based but 0 is also allowed.
			// Compute the number of lines to skip

			// We can upload data read from the file system but also make up our own data
			if (name.equals("generated.csv")) {
				uploadGenerated(handle, linesToSkip);
				return;
			}

			// Validate the path, demonstrating two ways of dealing with errors
			Path path = securityCheck(name);
			if (path == null || !Files.exists(path)) {
				// This makes the COPY command fail but keeps the connection alive.
				// Can only be used if we haven't sent any data yet
				handle.sendError("Invalid path");
				return;
			}
			if (!Files.isReadable(path)) {
				// As opposed to handle.sendError(), throwing an IOException ends the whole connection.
				throw new IOException("Unreadable: " + path);
			}

			boolean binary = !textMode;
			if (binary) {
				uploadBinary(handle, path);
			} else if (linesToSkip == 0 && filesAreUtf8) {
				// Avoid unnecessary character set conversions by pretending it's binary
				uploadBinary(handle, path);
			} else {
				// Charset and skip handling really necessary
				uploadTextFile(handle, path, linesToSkip);
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

		private void uploadGenerated(MonetConnection.Upload handle, long toSkip) throws IOException {
			PrintStream stream = handle.getStream();
			for (long i = toSkip + 1; i <= 100; i++) {
				stream.printf("%d|the number is %d%n", i, i);
			}
			stream.close();
		}

		private void uploadTextFile(MonetConnection.Upload handle, Path path, long toSkip) throws IOException {
			BufferedReader reader = Files.newBufferedReader(path);// Converts from system encoding to Java text
			for (long i = 0; i < toSkip; i++) {
				reader.readLine();
			}
			handle.uploadFrom(reader); // Converts from Java text to UTF-8 as required by MonetDB
		}

		private void uploadBinary(MonetConnection.Upload handle, Path path) throws IOException {
			// No charset conversion whatsoever..
			// Use this for binary data or when you are certain the file is UTF-8 encoded.
			InputStream stream = Files.newInputStream(path);
			handle.uploadFrom(stream);
		}
	}
}
