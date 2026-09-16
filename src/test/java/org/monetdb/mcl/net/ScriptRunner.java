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

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.fail;

public class ScriptRunner {
	private final static Pattern pattern = Pattern.compile("([A-Z]+)\\s+((.*?)(=(.*))?)");
	private final String source;
	private final int sourceLineOffset;
	private final String section;
	private final String[] lines;
	int lineno = 0;
	Target target = null;
	Target.Validated validated = null;

	public ScriptRunner(String source, int sourceLineOffset, String section, String[] lines) {
		this.source = source;
		this.sourceLineOffset = sourceLineOffset;
		this.section = section;
		this.lines = lines;
	}

	public String origin() {
		return source + ":" + (sourceLineOffset + 1) + section;
	}

	public void reset() {
		lineno = 0;
		target = new Target();
		validated = null;
	}

	public void execute() throws Failure {
		reset();

		for (String line : lines) {
			line = line.trim();
			if (!line.isEmpty()) {
				Matcher m = pattern.matcher(line);
				if (!m.matches()) {
					throw new Failure("Invalid command: " + line);
				}
				String command = m.group(1);
				String argument = m.group(2);
				String key = m.group(3);
				String value = m.group(5);
				switch (command) {
					case "ONLY":
						if (!haveFeature(argument))
							return;
						break;
					case "NOT":
						if (haveFeature(argument))
							return;
						break;
					case "PARSE":
						handleParse(argument, null);
						break;
					case "ACCEPT":
						handleParse(argument, true);
						break;
					case "REJECT":
						handleParse(argument, false);
						break;
					case "SET":
						if (value == null)
							throw new Failure("expected KEY=VALUE");
						handleSet(key, value);
						break;
					case "EXPECT":
						if (value == null)
							throw new Failure("expected KEY=VALUE");
						handleExpect(key, value);
				}
			}

			lineno++;
		}
	}

	private boolean haveFeature(String features) throws Failure {
		for (String part : features.split("\\s+")) {
			if (part.equals("jdbc"))
				return true;
		}
		return false;
	}

	private void handleParse(String url, Boolean shouldSucceed) throws Failure {
		URISyntaxException parseError = null;
		ValidationError validationError = null;

		validated = null;
		try {
			target.barrier();
			MonetUrlParser.parse(target, url);
			target.validate();
		} catch (URISyntaxException e) {
			parseError = e;
		} catch (ValidationError e) {
			validationError = e;
		}

		// REJECT
		if (shouldSucceed == Boolean.FALSE) {
			if (parseError != null || validationError != null)
				return; // happy
			else
				throw new Failure("URL unexpectedly parsed and validated");
		}

		// PARSE AND ACCEPT
		if (parseError != null)
			throw new Failure(parseError.getMessage());
		// ACCEPT
		if (shouldSucceed == Boolean.TRUE && validationError != null)
			throw new Failure(validationError.getMessage());
	}

	private void handleSet(String key, String value) throws Failure {
		validated = null;

		try {
			target.setString(key, value);
		} catch (ValidationError e) {
			throw new Failure(e.getMessage());
		}
	}

	private void handleExpect(String key, String expectedString) throws Failure {
		Object actual;
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

	private Target.Validated validated() {
		try {
			return tryValidate();
		} catch (ValidationError e) {
			return fail(e.getMessage());
		}
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
				return validated().connectScan();
			case "connect_port":
				return validated().connectPort();
			case "connect_unix":
				return validated().connectUnix();
			case "connect_tcp":
				return validated().connectTcp();
			case "connect_tls_verify":
				switch (validated().connectVerify()) {
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
				return validated().connectCertHashDigits();
			case "connect_binary":
				return validated().connectBinary();
			case "connect_clientkey":
				return validated().connectClientKey();
			case "connect_clientcert":
				return validated().connectClientCert();

			default:
				Parameter parm = Parameter.forName(key);
				if (parm != null)
					return target.getObject(parm);
				else
					throw new Failure("Unknown attribute: " + key);
		}
	}


	public class Failure extends Exception {
		public Failure(String message) {
			super(source + ":" + (lineno + sourceLineOffset) + ": " + message);
		}
	}
}
