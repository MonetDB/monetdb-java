package org.monetdb.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.monetdb.testinfra.Config;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoggingTests {
	@TempDir
	Path tempDir;
	private Path logFile;

	@BeforeAll
	public static void checkConnection() throws SQLException {
		String url = Config.getServerURL();
		DriverManager.getConnection(url).close();
	}

	@BeforeEach
	public void setUp() {
		logFile = tempDir.resolve("log.txt");
	}

	private MonetConnection connect() throws SQLException {
		Properties props = new Properties();
		props.setProperty("debug", "true");
		props.setProperty("logfile", logFile.toString());
		Connection conn = DriverManager.getConnection(Config.getServerURL(), props);
		return conn.unwrap(MonetConnection.class);
	}

	@Test
	public void testBasicLogging() throws SQLException, IOException {
		try (Connection conn = connect()) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("SELECT 'banana'");
			}
		}

		List<String> content = Files.readAllLines(logFile);
		assertTrue(content.stream().anyMatch(s->s.contains("banana")));
	}

	@Test
	public void testCopyBinary() throws SQLException, IOException {
		byte[] dataBytes = new byte[40];
		for (int i = 1; i < dataBytes.length - 1; i++)
			dataBytes[i] = 42;
		String data = new String(dataBytes, StandardCharsets.UTF_8);

		try (MonetConnection conn = connect(); Statement stmt = conn.createStatement()) {
			conn.setUploadHandler(new MonetConnection.UploadHandler() {
				@Override
				public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
					OutputStream stream = handle.getStream();
					stream.write(dataBytes);
				}
			});

			stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo(x TINYINT)");
			stmt.execute("COPY LITTLE ENDIAN BINARY INTO foo FROM '' ON CLIENT");
		}

		byte[] contentBytes = Files.readAllBytes(logFile);
		String content = new String(contentBytes, StandardCharsets.UTF_8);
		assertTrue(content.contains(data));
	}


}
