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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.fail;

@Tag("api")
public class UrlTests {

	private static ArrayList<ScriptRunner> loadScripts(String source, BufferedReader reader) throws IOException {
		ArrayList<ScriptRunner> runners = new ArrayList<>();
		ArrayList<String> lines = new ArrayList<>();
		int lineno = 0;
		int startLine = -1;
		String section = "";
		int nrInSection = 0;

		while (true) {
			String line = reader.readLine();
			if (line == null) {
				if (startLine >= 0) {
					throw new IllegalStateException("end of file reached in script block that started at line " + startLine);
				}
				break;
			}
			lineno++;
			line = line.trim();
			if (startLine >= 0) {
				if (!line.equals("```")) {
					lines.add(line);
				} else {
					String remark = !section.isEmpty() ? " (Test " + ++nrInSection + " of section '" + section + "')" : "";
					ScriptRunner runner = new ScriptRunner(source, startLine, remark, lines.toArray(new String[0]));
					runners.add(runner);
					startLine = -1;
				}
			} else {
				if (line.equals("```test")) {
					startLine = lineno;
					lines.clear();
				} else if (line.startsWith("#")) {
					do {
						line = line.substring(1);
					} while (line.startsWith("#"));
					section = line.trim();
					nrInSection = 0;
				}
			}
		}

		return runners;
	}

	ArrayList<ScriptRunner> loadScripts(String resourceName) throws IOException {
		InputStream stream = this.getClass().getResourceAsStream(resourceName);
		if (stream == null) {
			throw new IOException("Test script '" + resourceName + "' not found");
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		return loadScripts(resourceName, reader);
	}

	ArrayList<DynamicTest> loadTests(String resourceName) throws IOException {
		ArrayList<DynamicTest> tests = new ArrayList<>();
		for (ScriptRunner runner : loadScripts(resourceName)) {
			String name = runner.origin();
			DynamicTest test = DynamicTest.dynamicTest(name, () -> {
				try {
					runner.execute();
				} catch (ScriptRunner.Failure e) {
					fail(e.getMessage());
				}
			});
			tests.add(test);
		}
		return tests;
	}

	@TestFactory
	ArrayList<DynamicTest> testStandardTests() throws IOException {
		return loadTests("tests.md");
	}

	@TestFactory
	ArrayList<DynamicTest> testJavaSpecificTests() throws IOException {
		return loadTests("javaspecific.md");
	}
}
