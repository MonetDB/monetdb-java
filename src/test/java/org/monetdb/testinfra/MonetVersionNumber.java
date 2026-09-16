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
package org.monetdb.testinfra;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extract Monet version from a Monet connection and interpret it
 */
public class MonetVersionNumber {
	public final int serverMajor;
	public final int serverMinor;
	public final int serverPatch;

	public MonetVersionNumber(int major, int minor, int patch) {
		serverMajor = major;
		serverMinor = minor;
		serverPatch = patch;
	}

	public static MonetVersionNumber retrieve(Connection conn) throws SQLException {
		String query = "SELECT value FROM environment WHERE name = 'monet_version'";
		String version;
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
			if (!rs.next())
				throw new IllegalStateException("MonetDB did not return a version number");
			version = rs.getString(1);
			if (rs.next())
				throw new IllegalStateException("MonetDB returned more than one version number: '" + version + "' and '" + rs.getString(1) + "'");
		}
		String[] parts = version.split("[.]");
		if (parts.length != 3)
			throw new IllegalStateException("MonetDB returned invalid version number: " + version);
		int major = Integer.parseInt(parts[0]);
		int minor = Integer.parseInt(parts[1]);
		int patch = Integer.parseInt(parts[2]);
		return new MonetVersionNumber(major, minor, patch);
	}

	public boolean isAtLeast(int requiredMajor, int requiredMinor, int requiredPatch) {
		if (serverMajor > requiredMajor)
			return true;
		if (serverMajor < requiredMajor)
			return false;
		if (serverMinor > requiredMinor)
			return true;
		if (serverMinor < requiredMinor)
			return false;
		return serverPatch >= requiredPatch;
	}

	/**
	 * Equivalent to {@code isAtLeast(11, 41, 12)}
	 *
	 * @return
	 */
	public boolean serverCanRefuseDownload() {
		return isAtLeast(11, 41, 12);
	}

	/**
	 * Equivalent to {@code isAtLeast(11, 52, 0)}
	 *
	 * @return
	 */
	public boolean serverCanQueryUnclosedResultSets() {
		return isAtLeast(11, 52, 0);
	}

	/**
	 * Equivalent to {@code isAtLeast(11, 50, 0)}
	 *
	 * From version 11.50 on, the MonetDB server returns different metadata for
	 * integer digits (1 less) and for clob and char columns (now return varchar).
	 */
	public boolean serverReturnsNewMetadata() {
		return isAtLeast(11, 50, 0);
	}
}
