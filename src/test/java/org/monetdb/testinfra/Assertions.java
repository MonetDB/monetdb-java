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

import java.sql.SQLException;

import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.fail;

public class Assertions {
	/**
	 * Call {@code callable} and check that it throws a SQLException whose message contains {@code substring}.
	 *
	 * @param substring
	 * @param callable
	 * @throws SQLException
	 */
	public static void assertSQLException(String substring, SQLCode callable) throws SQLException {
		try {
			callable.execute();
			fail("Expected a SQLException containing '" + substring + "'");
		} catch (SQLException e) {
			if (e.getMessage().contains(substring))
				return;
			throw e;
		}
	}

	@FunctionalInterface
	public interface SQLCode {

		void execute() throws SQLException;

	}

	public static void assertContains(String needle, String haystack) {
		assertContains(needle, haystack, null);
	}

	public static void assertContains(String needle, String haystack, String message) {
		if (needle == null)
			assertionFailure().reason("substring to search must not be null").buildAndThrow();
		if (haystack != null && haystack.contains(needle))
			return;

		assertionFailure() //
				.reason("substring not found") //
				.message(message) //
				.expected(needle) //
				.actual(haystack) //
				.buildAndThrow();
	}

	public static void assertNotContains(String needle, String haystack) {
		assertNotContains(needle, haystack, null);
	}

	public static void assertNotContains(String needle, String haystack, String message) {
		if (needle == null)
			assertionFailure().reason("substring to search must not be null").buildAndThrow();
		if (haystack != null && !haystack.contains(needle))
			return;

		assertionFailure() //
				.reason("forbidden substring did occur") //
				.message(message) //
				.expected(needle) //
				.actual(haystack) //
				.buildAndThrow();
	}
}
