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

import org.monetdb.mcl.net.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;

public final class UrlTester {
	final String filename;
	final int verbose;
	final BufferedReader reader;
	int lineno = 0;
	int testCount = 0;
	Target target = null;
	Target.Validated validated = null;

	public UrlTester(String filename, BufferedReader reader, int verbose) {
		this.filename = filename;
		this.verbose = verbose;
		this.reader = reader;
	}

	public UrlTester(String filename, int verbose) throws IOException {
		this.filename = filename;
		this.verbose = verbose;
		this.reader = new BufferedReader(new FileReader(filename));
	}

	public static void main(String[] args) throws IOException {
		ArrayList<String> filenames = new ArrayList<>();
		int verbose = 0;
		for (String arg : args) {
			switch (arg) {
				case "-vvv":
					verbose++;
				case "-vv":
					verbose++;
				case "-v":
					verbose++;
					break;
				case "-h":
				case "--help":
					exitUsage(null);
					break;
				default:
					if (!arg.startsWith("-")) {
						filenames.add(arg);
					} else {
						exitUsage("Unexpected argument: " + arg);
					}
					break;
			}
		}

		runUnitTests();

		try {
			if (filenames.isEmpty()) {
				runAllTests();
			} else {
				for (String filename : filenames) {
					new UrlTester(filename, verbose).run();
				}
			}
		} catch (Failure e) {
			System.err.println("Test failed: " + e.getMessage());
			System.exit(1);
		}
	}

	private static void exitUsage(String message) {
		if (message != null) {
			System.err.println(message);
		}
		System.err.println("Usage: UrlTester OPTIONS [FILENAME..]");
		System.err.println("Options:");
		System.err.println("   -v        Be more verbose");
		System.err.println("   -h --help Show this help");
		int status = message == null ? 0 : 1;
		System.exit(status);
	}

	public static UrlTester forResource(String resourceName, int verbose) throws FileNotFoundException {
		InputStream stream = UrlTester.class.getResourceAsStream(resourceName);
		if (stream == null) {
			throw new FileNotFoundException("Resource " + resourceName);
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		return new UrlTester(resourceName, reader, verbose);
	}

	public static void runAllTests() throws IOException, Failure {
		runUnitTests();
		UrlTester.forResource("/tests.md", 0).run();
		UrlTester.forResource("/javaspecific.md", 0).run();
	}

	public static void runUnitTests() {
		testDefaults();
		testParameters();
	}

	private static void testDefaults() {
		Target target = new Target();

		for (Parameter parm : Parameter.values()) {
			Object expected = parm.getDefault();
			if (expected == null)
				continue;
			Object actual = target.getObject(parm);
			if (!expected.equals(actual)) {
				throw new RuntimeException("Default for " + parm.name + " expected to be <" + expected + "> but is <" + actual + ">");
			}
		}
	}

	private static void testParameters() {
		for (Parameter parm : Parameter.values()) {
			Parameter found = Parameter.forName(parm.name);
			if (parm != found) {
				String foundStr = found != null ? found.name : "null";
				throw new RuntimeException("Looking up <" + parm.name + ">, found <" + foundStr);
			}
		}
	}

	public void run() throws Failure, IOException {
		try {
			processFile();
		} catch (Failure e) {
			if (e.getFilename() == null) {
				e.setFilename(filename);
				e.setLineno(lineno);
				throw e;
			}
		}
	}

	private void processFile() throws IOException, Failure {
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			lineno++;
			processLine(line);
		}
		if (verbose >= 1) {
			System.out.println();
			System.out.println("Ran " + testCount + " tests in " + lineno + " lines");
		}
	}

	private void processLine(String line) throws Failure {
		line = line.replaceFirst("\\s+$", ""); // remove trailing
		if (target == null && line.equals("```test")) {
			if (verbose >= 2) {
				if (testCount > 0) {
					System.out.println();
				}
				System.out.println("\u25B6 " + filename + ":" + lineno);
			}
			target = new Target();
			testCount++;
			return;
		}
		if (target != null) {
			if (line.equals("```")) {
				stopProcessing();
				return;
			}
			handleCommand(line);
		}
	}

	private void stopProcessing() {
		target = null;
		validated = null;
	}

	private void handleCommand(String line) throws Failure {
		if (verbose >= 3) {
			System.out.println(line);
		}
		if (line.isEmpty())
			return;

		String[] parts = line.split("\\s+", 2);
		String command = parts[0];
		switch (command.toUpperCase()) {
			case "ONLY":
				handleOnly(true, parts[1]);
				return;
			case "NOT":
				handleOnly(false, parts[1]);
				return;
			case "PARSE":
				handleParse(parts[1], null);
				return;
			case "ACCEPT":
				handleParse(parts[1], true);
				return;
			case "REJECT":
				handleParse(parts[1], false);
				return;
			case "SET":
				handleSet(parts[1]);
				return;
			case "EXPECT":
				handleExpect(parts[1]);
				return;
			default:
				throw new Failure("Unexpected command: " + command);
		}

	}

	private void handleOnly(boolean mustBePresent, String rest) throws Failure {
		boolean found = false;
		for (String part : rest.split("\\s+")) {
			if (part.equals("jdbc")) {
				found = true;
				break;
			}
		}
		if (found != mustBePresent) {
			// do not further process this block
			stopProcessing();
		}
	}

	private int findEqualSign(String rest) throws Failure {
		int index = rest.indexOf('=');
		if (index < -1)
			throw new Failure("Expected to find a '='");
		return index;
	}

	private String splitKey(String rest) throws Failure {
		int index = findEqualSign(rest);
		return rest.substring(0, index);
	}

	private String splitValue(String rest) throws Failure {
		int index = findEqualSign(rest);
		return rest.substring(index + 1);
	}

	private void handleSet(String rest) throws Failure {
		validated = null;
		String key = splitKey(rest);
		String value = splitValue(rest);

		try {
			target.setString(key, value);
		} catch (ValidationError e) {
			throw new Failure(e.getMessage());
		}
	}

	private void handleParse(String rest, Boolean shouldSucceed) throws Failure {
		URISyntaxException parseError = null;
		ValidationError validationError = null;

		validated = null;
		try {
			target.barrier();
			MonetUrlParser.parse(target, rest);
		} catch (URISyntaxException e) {
			parseError = e;
		} catch (ValidationError e) {
			validationError = e;
		}

		if (parseError == null && validationError == null) {
			try {
				tryValidate();
			} catch (ValidationError e) {
				validationError = e;
			}
		}

		if (shouldSucceed == Boolean.FALSE) {
			if (parseError != null || validationError != null)
				return; // happy
			else
				throw new Failure("URL unexpectedly parsed and validated");
		}

		if (parseError != null)
			throw new Failure("Parse error: " + parseError);
		if (validationError != null && shouldSucceed == Boolean.TRUE)
			throw new Failure("Validation error: " + validationError);
	}

	private void handleExpect(String rest) throws Failure {
		String key = splitKey(rest);
		String expectedString = splitValue(rest);

		Object actual = null;
		try {
			actual = extract(key);
		} catch (ValidationError e) {
			throw new Failure(e.getMessage());
		}

		Object expected;
		try {
			if (actual instanceof Boolean)
				expected = ParameterType.Bool.parse(key, expectedString);
			else if (actual instanceof Integer)
				expected = ParameterType.Int.parse(key, expectedString);
			else
				expected = expectedString;
		} catch (ValidationError e) {
			String typ = actual.getClass().getName();
			throw new Failure("Cannot convert expected value <" + expectedString + "> to " + typ + ": " + e.getMessage());
		}

		if (actual.equals(expected))
			return;
		throw new Failure("Expected " + key + "=<" + expectedString + ">, found <" + actual + ">");
	}

	private Target.Validated tryValidate() throws ValidationError {
		if (validated == null)
			validated = target.validate();
		return validated;
	}

	private Object extract(String key) throws ValidationError, Failure {
		switch (key) {
			case "valid":
				try {
					tryValidate();
				} catch (ValidationError e) {
					return Boolean.FALSE;
				}
				return Boolean.TRUE;

			case "connect_scan":
				return tryValidate().connectScan();
			case "connect_port":
				return tryValidate().connectPort();
			case "connect_unix":
				return tryValidate().connectUnix();
			case "connect_tcp":
				return tryValidate().connectTcp();
			case "connect_tls_verify":
				switch (tryValidate().connectVerify()) {
					case None:
						return "";
					case Cert:
						return "cert";
					case Hash:
						return "hash";
					case System:
						return "system";
					default:
						throw new IllegalStateException("unreachable");
				}
			case "connect_certhash_digits":
				return tryValidate().connectCertHashDigits();
			case "connect_binary":
				return tryValidate().connectBinary();
			case "connect_clientkey":
				return tryValidate().connectClientKey();
			case "connect_clientcert":
				return tryValidate().connectClientCert();

			default:
				Parameter parm = Parameter.forName(key);
				if (parm != null)
					return target.getObject(parm);
				else
					throw new Failure("Unknown attribute: " + key);
		}
	}

	@SuppressWarnings("serial")
	public static class Failure extends Exception {
		private String filename = null;
		private int lineno = -1;

		public Failure(String message) {
			super(message);
		}

		@Override
		public String getMessage() {
			StringBuilder buffer = new StringBuilder();
			if (filename != null) {
				buffer.append(filename).append(":");
				if (lineno >= 0)
					buffer.append(lineno).append(":");
			}
			buffer.append(super.getMessage());
			return buffer.toString();
		}

		public String getFilename() {
			return filename;
		}

		public void setFilename(String filename) {
			this.filename = filename;
		}

		public int getLineno() {
			return lineno;
		}

		public void setLineno(int lineno) {
			this.lineno = lineno;
		}
	}
}
