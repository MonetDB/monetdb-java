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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public final class ConnectionTests {

	private final String url;
	private final Properties connProps;
	private final TimeZone timeZone;

	public ConnectionTests(String url, Properties props, TimeZone timeZone) {
		this.url = url;
		Properties myProps = null;
		if (props != null) {
			myProps = new Properties();
			myProps.putAll(props);
		}
		this.connProps = myProps;
		this.timeZone = timeZone;
	}

	public ConnectionTests(String url) {
		this(url, null, null);
	}

	public ConnectionTests withSuffix(String suffix) {
		String newUrl = url;

		if (newUrl.contains("?")) {
			newUrl += "&";
		} else {
			newUrl += "?";
		}
		newUrl += suffix;

		return new ConnectionTests(newUrl, this.connProps, this.timeZone);
	}

	public ConnectionTests withProp(String key, String value) {
		ConnectionTests sub = new ConnectionTests(this.url, new Properties(), this.timeZone);
		if (this.connProps != null)
			sub.connProps.putAll(this.connProps);
		sub.connProps.setProperty(key, value);
		return sub;
	}

	public ConnectionTests withTimeZone(int offsetMinutes) {
		TimeZone tz = new SimpleTimeZone(offsetMinutes * 60 * 1000, "Custom" + offsetMinutes);
		return new ConnectionTests(this.url, this.connProps, tz);
	}

	public static void main(String[] args) throws SQLException, Failure {
		String url = args[0];
		runTests(url);
	}

	public static void runTests(String url) throws SQLException, Failure {
		ConnectionTests tester = new ConnectionTests(url);

		tester.checkAutoCommit(true);
		tester.withSuffix("autocommit=true").checkAutoCommit(true);
		tester.withSuffix("autocommit=false").checkAutoCommit(false);
		tester.withProp("autocommit", "true").checkAutoCommit(true);
		tester.withProp("autocommit", "false").checkAutoCommit(false);

		tester.testTimeZone();

		tester.cleanup();
	}

	Connection connect() throws SQLException {
		TimeZone restore = null;
		try {
			if (this.timeZone != null) {
				restore = TimeZone.getDefault();
				TimeZone.setDefault(this.timeZone);
			}
			if (connProps != null) {
				return DriverManager.getConnection(url, connProps);
			} else {
				return DriverManager.getConnection(url);
			}
		} finally {
			if (restore != null) {
				TimeZone.setDefault(restore);
			}
		}
	}

	private void checkAutoCommit(boolean expectAutocommit) throws SQLException, Failure {
		// Create and fill the table, leave one row uncommitted.
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			// Does the connection itself believe to be in the correct mode?
			boolean autocommitEnabled = conn.getAutoCommit();
			if (autocommitEnabled != expectAutocommit) {
				throw new Failure("Expected autocommit to start as " + expectAutocommit + ", got " + autocommitEnabled);
			}

			// Let's test how it works out in practice
			stmt.execute("DROP TABLE IF EXISTS connectiontests");
			stmt.execute("CREATE TABLE connectiontests(i INT)");
			stmt.execute("INSERT INTO connectiontests VALUES (42)");
			if (!expectAutocommit)
				conn.commit();
			// This will only be committed in autocommit mode
			stmt.execute("INSERT INTO connectiontests VALUES (99)");
		}

		// Check whether the uncommitted row is there or not
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery("SELECT COUNT(i) FROM connectiontests")) {
				rs.next();
				int n = rs.getInt(1);
				if (expectAutocommit) {
					if (n != 2) {
						throw new Failure("Expected 2 rows because autocommit should be on, got " + n);
					}
				} else {
					if (n != 1) {
						throw new Failure("Expected 1 row because autocommit should be off, got " + n);
					}
				}
			}
		}
	}

	private void testTimeZone() throws SQLException, Failure {
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS connectiontests_ts");
			stmt.execute("CREATE TABLE connectiontests_ts(ts TIMESTAMP WITH TIME ZONE)");
			stmt.execute("INSERT INTO connectiontests_ts VALUES (str_to_timestamp(100, '%s'))");
		}

		this.withTimeZone(0).verifyTimeZoneSuffix("+00:00");
		this.withTimeZone(240).verifyTimeZoneSuffix("+04:00");
		this.withTimeZone(270).verifyTimeZoneSuffix("+04:30");
	}

	private void verifyTimeZoneSuffix(String suffix) throws SQLException, Failure {
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery("SELECT * FROM connectiontests_ts");
			rs.next();
			String s = rs.getString(1);
			if (!s.endsWith(suffix)) {
				String msg = String.format("Expected suffix '%s', got timestamp '%s'", suffix, s);
				throw new Failure(msg);
			}
		}
	}

	private void cleanup() throws SQLException {
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS connectiontests");
			stmt.execute("DROP TABLE IF EXISTS connectiontests_ts");
		}
	}

	public class Failure extends Exception {
		private static final long serialVersionUID = 1L;

		public Failure(String msg) {
			super(msg);
		}

		@Override
		public String getMessage() {
			StringBuilder msg = new StringBuilder();
			msg.append("When connected to ");
			msg.append(url);
			if (timeZone != null) {
				msg.append(", in time zone ");
				msg.append(timeZone.getID());
				msg.append(" (");
				msg.append(timeZone.getDisplayName());
				msg.append(")");
			} else {
				msg.append(", in the default time zone");
			}
			if (connProps != null) {
				msg.append(", with ");
				msg.append(connProps.size());
				msg.append(" properties");
				connProps.forEach((k, v) -> {
					msg.append(", ");
					msg.append(k);
					msg.append("=");
					msg.append(v);
				});
			}
			msg.append(": ");
			msg.append(super.getMessage());
			return msg.toString();
		}
	}
}
