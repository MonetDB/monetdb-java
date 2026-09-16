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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.monetdb.testinfra.Config;

import java.sql.*;
import java.util.Properties;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

@Tag("api")
public class ConnectionTests {

	@BeforeAll
	public static void checkConnection() throws SQLException {
		Connection conn = connect(null, null, null);
		conn.close();
	}

	@AfterAll
	public static void dropConntest() throws SQLException {
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS conntest");
		}
	}

	static Connection connect(String extraUrl, String extraPropKey, String extraPropValue) throws SQLException {
		String url = Config.getServerURL();

		// quick and dirty, intentionally do not use Target logic
		if (extraUrl != null) {
			url += url.contains("?") ? "&" : "?";
			url += extraUrl;
		}
		Properties props = null;
		if (extraPropKey != null) {
			props = new Properties();
			props.setProperty(extraPropKey, extraPropValue);
		}

		return DriverManager.getConnection(url, props);
	}

	static Connection connect() throws SQLException {
		return connect(null, null, null);
	}

	static int selectInt(Connection conn, String query) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(query)) {
				assertTrue(rs.next(), "Query <" + query + "> should return a single row");
				int result = rs.getInt(1);
				assertFalse(rs.next(), "Query <" + query + "> should return a single row");
				return result;
			}
		}
	}

	private static void expectAutocommit(Connection conn, boolean expectAutocommit) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			assertEquals(conn.getAutoCommit(), expectAutocommit);

			// The connection believes autocommit is on/off, but is it really?
			stmt.execute("DROP TABLE IF EXISTS conntest");
			stmt.execute("CREATE TABLE conntest(i INT)");
			stmt.execute("INSERT INTO conntest VALUES (1)");
			if (expectAutocommit) {
				assertThrows(SQLException.class, conn::commit);
			} else {
				conn.commit();
			}
			stmt.execute("INSERT INTO conntest VALUES (2)");
			if (expectAutocommit) {
				assertThrows(SQLException.class, conn::rollback);
			} else {
				conn.rollback();
			}
			int rowCount = selectInt(conn, "SELECT COUNT(*) FROM conntest");
			if (expectAutocommit) {
				// 1 and 2 have both been inserted, rollback has failed
				assertEquals(2, rowCount);
			} else {
				// 1 has been committed, 2 has been rolled back
				assertEquals(1, rowCount);
			}
		}
	}

	private static void expectTimezone(Connection conn, String suffix) throws SQLException {
		String query = "SELECT str_to_timestamp(100, '%s')";
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
			assertTrue(rs.next());
			String timestamp = rs.getString(1);
			assertTrue(timestamp.endsWith(suffix), "Expected <" + timestamp + "> to end with <" + suffix + ">");
			assertFalse(rs.next());
		}

	}

	@Test
	public void testAutocloseable() throws SQLException, InterruptedException {
		try (Connection conn1 = connect()) {
			int session1 = selectInt(conn1, "SELECT current_sessionid()");
			int session2;
			try (Connection conn2 = connect()) {
				session2 = selectInt(conn2, "SELECT current_sessionid()");
				assertNotEquals(session1, session2);
			}
			Thread.sleep(300);
			int session2count = selectInt(conn1, "SELECT COUNT(*) FROM sys.sessions WHERE sessionid = " + session2);
			assertEquals(0, session2count);
		}
	}

	@Test
	public void testAutocommitDefault() throws SQLException {
		try (Connection conn = connect(null, null, null)) {
			expectAutocommit(conn, true);
		}
	}

	@Test
	public void testAutocommitUrlTrue() throws SQLException {
		try (Connection conn = connect("autocommit=true", null, null)) {
			expectAutocommit(conn, true);
		}
	}

	@Test
	public void testAutocommitUrlFalse() throws SQLException {
		try (Connection conn = connect("autocommit=false", null, null)) {
			expectAutocommit(conn, false);
		}
	}

	@Test
	public void testAutocommitPropTrue() throws SQLException {
		try (Connection conn = connect(null, "autocommit", "Yes")) {
			expectAutocommit(conn, true);
		}
	}

	@Test
	public void testAutocommitPropFalse() throws SQLException {
		try (Connection conn = connect(null, "autocommit", "NO")) {
			expectAutocommit(conn, false);
		}
	}

	@Test
	public void testTimeZoneUrl0() throws SQLException {
		try (Connection conn = connect("timezone=0", null, null)) {
			expectTimezone(conn, "+00:00");
		}
	}

	@Test
	public void testTimeZoneUrl240() throws SQLException {
		try (Connection conn = connect("timezone=240", null, null)) {
			expectTimezone(conn, "+04:00");
		}
	}

	@Test
	public void testTimeZoneUrl270() throws SQLException {
		try (Connection conn = connect("timezone=-270", null, null)) {
			expectTimezone(conn, "04:30");
		}
	}

	@Test
	public void testTimeZoneProp0() throws SQLException {
		try (Connection conn = connect(null, "timezone", "0")) {
			expectTimezone(conn, "+00:00");
		}
	}

	@Test
	public void testTimeZoneProp240() throws SQLException {
		try (Connection conn = connect(null, "timezone", "240")) {
			expectTimezone(conn, "+04:00");
		}
	}

	@Test
	public void testTimeZoneProp270() throws SQLException {
		try (Connection conn = connect(null, "timezone", "-270")) {
			expectTimezone(conn, "04:30");
		}
	}

	@Test
	public void testTimeZoneDefault0() throws SQLException {
		try (TemporaryTimeZone ignored = new TemporaryTimeZone(0)) {
			try (Connection conn = connect()) {
				expectTimezone(conn, "+00:00");
			}
		}
	}

	@Test
	public void testTimeZoneDefault240() throws SQLException {
		try (TemporaryTimeZone ignored = new TemporaryTimeZone(240)) {
			try (Connection conn = connect()) {
				expectTimezone(conn, "+04:00");
			}
		}
	}

	@Test
	public void testTimeZoneDefault270() throws SQLException {
		try (TemporaryTimeZone ignored = new TemporaryTimeZone(-270)) {
			try (Connection conn = connect()) {
				expectTimezone(conn, "04:30");
			}
		}
	}

	private static class TemporaryTimeZone implements AutoCloseable {
		private final TimeZone restore;

		private TemporaryTimeZone(int offsetMinutes) {
			this(new SimpleTimeZone(offsetMinutes * 60 * 1000, "Custom " + offsetMinutes));
		}

		private TemporaryTimeZone(TimeZone tz) {
			restore = TimeZone.getDefault();
			TimeZone.setDefault(tz);
		}

		@Override
		public void close() {
			TimeZone.setDefault(restore);
		}
	}

}
