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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.monetdb.testinfra.Config;
import org.monetdb.testinfra.MonetVersionNumber;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common code between tthe {@link OnClientTests} and the {@link FileTransferHandlerTests}.
 */
public abstract class OnClientTestsParent {
	protected MonetVersionNumber monetVersion;
	protected MonetConnection conn;
	protected Statement stmt;

	@BeforeAll
	public static void checkConnection() throws SQLException {
		DriverManager.getConnection(Config.getServerURL()).close();
	}

	@BeforeEach
	public void setUp() throws SQLException {
		java.sql.Connection c = DriverManager.getConnection(Config.getServerURL());
		conn = c.unwrap(MonetConnection.class);
		stmt = conn.createStatement();
		monetVersion = MonetVersionNumber.retrieve(conn);
		stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo(i INT, t TEXT)");
	}

	@AfterEach
	public void tearDown() {
		try {
			stmt.close();
		} catch (SQLException ignored) {
		}
		conn.close();
	}

	protected String queryString(String query) throws SQLException {
		try (ResultSet rs = stmt.executeQuery(query)) {
			assertTrue(rs.next(), "query should return a single row");
			String result = rs.getString(1);
			assertFalse(rs.next(), "query should return a single row");
			return result;
		}
	}

	protected int queryInt(String query) throws SQLException {
		try (ResultSet rs = stmt.executeQuery(query)) {
			assertTrue(rs.next(), "query should return a single row");
			int result = rs.getInt(1);
			assertFalse(rs.next(), "query should return a single row");
			return result;
		}
	}
}
