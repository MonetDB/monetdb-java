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
package org.monetdb.mcl.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class ScriptRunnerSelfTest {
	private void attempt(String expectedError, String... lines) {
		ScriptRunner.Failure err = null;
		try {
			ScriptRunner runner = new ScriptRunner("dummy", 1, "", lines);
			runner.execute();
		} catch (ScriptRunner.Failure e) {
			err = e;
		}

		if (expectedError != null) {
			if (err == null) {
				fail("Expected failure <" + expectedError + ">");
			} else if (!err.getMessage().contains(expectedError)) {
				fail("Expected failure <" + expectedError + ">, got: " + err.getMessage());
			}
		} else {
			if (err != null) {
				fail("Unexpected failure: " + err.getMessage());
			}
		}
	}

	@Test
	public void testParse() {
		attempt(null, "PARSE monetdb:///demo");
		attempt("scheme must be", "PARSE banana://demo");
		// sock + tls fails validation but it should parse
		attempt(null, "PARSE monetdbs:///demo?sock=lalala");
	}

	@Test
	public void testAccept() {
		attempt(null, "ACCEPT monetdb:///demo");
		attempt("scheme must be", "ACCEPT banana://demo");
		attempt("cannot be combined", "ACCEPT monetdbs:///demo?sock=lalala");
	}

	@Test
	public void testReject() {
		attempt(null, "REJECT banana:///demo");
		attempt(null, "REJECT monetdbs:///demo?sock=lalala");
		attempt("unexpectedly parsed and validated", "REJECT monetdb://demo");
	}

	@Test
	public void testLineno() {
		attempt(null, "PARSE monetdb://demo");
		attempt(null, "PARSE monetdb://demo", "PARSE monetdb://demo");

		attempt("dummy:1:", "PARSE banana://demo", "PARSE monetdb://demo");
		attempt("dummy:2:", "PARSE monetdb://demo", "PARSE banana://demo");


	}

}
