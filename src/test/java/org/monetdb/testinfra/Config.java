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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Centralize the names of the system properties and environment variables used in the tests.
 *
 * This class also provides getters.
 */
public class Config {
	public static final String SERVER_URL_PROPERTY = "test.url";
	public static final String SERVER_URL_ENVVAR = "MONETDB_TEST_URL";
	public static final String SERVER_URL_DEFAULT = "jdbc:monetdb:///testjdbc";

	public static final String TLSTESTER_PROPERTY = "test.tlstester";
	public static final String TLSTESTER_ENVVAR = "MONETDB_TLSTESTER";

	public static final String TLSTEST_KNOWNGOOD_PROPERTY = "test.tls.goodurl";
	public static final String TLSTEST_KNOWNGOOD_ENVVAR = "MONETDB_TLS_GOODURL";
	public static final String TLSTEST_KNOWNGOOD_DEFAULT = "jdbc:monetdbs://monetdb.ergates.nl/demo";

	public static final String TLSTEST_ALIAS_PROPERTY = "test.tls.alturl";
	public static final String TLSTEST_ALIAS_ENVVAR = "MONETDB_TLS_ALTURL";
	public static final String TLSTEST_ALIAS_DEFAULT = "jdbc:monetdbs://monetdbxyz.ergates.nl/demo";

	public static final String SKIP_MALOUTPUT_PROPERTY = "test.skipmaloutput";
	public static final String SKIP_MALOUTPUT_ENVVAR = "MONETDB_TEST_SKIP_MAL_OUTPUT";

	public static final String SKIP_SLOW_PROPERTY = "test.skipslow";
	public static final String SKIP_SLOW_ENVVAR = "MONETDB_TEST_SKIP_SLOW";

	private static String lookup(String propName, String envName, String defaultValue) {
		String value = null;
		try {
			if (propName != null)
				value = System.getProperty(propName);
			if (value != null && !value.equals(""))
				return value;
		} catch (SecurityException ignored) {
		}

		try {
			if (envName != null)
				value = System.getenv(envName);
			if (value != null)
				return value;
		} catch (SecurityException ignored) {
		}

		if (defaultValue != null)
			return defaultValue;

		if (propName != null && envName != null)
			return fail("Neither property '" + propName + "' nor environment variable '" + envName + "' is set");
		else if (propName != null)
			return fail("Property '" + propName + "' is not set");
		else if (envName != null)
			return fail("Environment variable '" + envName + "' is not set");
		else
			return fail("propName and envName cannot both be null");
	}

	public static String getRawServerURL() {
		return lookup(SERVER_URL_PROPERTY, SERVER_URL_ENVVAR, SERVER_URL_DEFAULT);
	}

	public static String getServerURL() {
		String raw = getRawServerURL();
		String fixed = addDefaultCredentials(raw);
		return fixed;
	}

	public static boolean isSkipMalOutput() {
		return Boolean.parseBoolean(lookup(SKIP_MALOUTPUT_PROPERTY, SKIP_MALOUTPUT_ENVVAR, "false"));
	}

	public static boolean isSkipSlow() {
		return Boolean.parseBoolean(lookup(SKIP_SLOW_PROPERTY, SKIP_SLOW_ENVVAR, "false"));
	}

	public static boolean isTLSTesterConfigured() {
		String addr = getTLSTester();
		return addr != null && !addr.isEmpty();
	}

	private static String getTLSTester() {
		return lookup(TLSTESTER_PROPERTY, TLSTESTER_ENVVAR, null);
	}

	public static String getKnownGoodUrl() {
		return prependJdbc(lookup(TLSTEST_KNOWNGOOD_PROPERTY, TLSTEST_KNOWNGOOD_ENVVAR, TLSTEST_KNOWNGOOD_DEFAULT));
	}

	public static String getAliasKnownGoodUrl() {
		return prependJdbc(lookup(TLSTEST_ALIAS_PROPERTY, TLSTEST_ALIAS_ENVVAR, TLSTEST_ALIAS_DEFAULT));
	}

	private static String prependJdbc(String url) {
		if (url.startsWith("monetdb://") || url.startsWith("monetdbs://"))
			return "jdbc:" + url;
		return url;
	}

	private static String parseTLSTester(String addr, int group) {
		Pattern testerPattern = Pattern.compile("(?:(.*):)?(\\d+)");
		Matcher matcher = testerPattern.matcher(addr);
		if (!matcher.matches())
			fail("TLSTester address '" + addr + "' does not match PORT or HOST:PORT");
		return matcher.group(group);
	}

	public static int getTesterPort() {
		String addr = getTLSTester();
		String port = parseTLSTester(addr, 2);
		int portNr = Integer.parseInt(port);
		assertTrue(portNr > 0, "" + portNr);
		assertTrue(portNr <= 65535, "" + portNr);
		return portNr;
	}

	public static String getTesterHost() {
		String addr = getTLSTester();
		String host = parseTLSTester(addr, 1);
		return host != null ? host : "localhost";
	}

	private static String addDefaultCredentials(String rawURL) {
		try {
			assertTrue(rawURL.startsWith("jdbc:"));
			URI oldUri = null;
			oldUri = new URI(rawURL.substring(5));
			String oldQuery = oldUri.getRawQuery();
			if (oldQuery != null && (oldQuery.contains("&user=") || oldQuery.contains("&password="))) {
				// do not mess with existing credentials
				return rawURL;
			}
			String creds = "user=monetdb&password=monetdb";
			String newQuery = oldQuery != null ? oldQuery + "&" + creds : creds;
			String authority = oldUri.getAuthority();
			authority = authority != null ? authority : "";
			URI newUri = new URI(oldUri.getScheme(), authority, oldUri.getPath(), newQuery, null);
			return "jdbc:" + newUri;
		} catch (URISyntaxException e) {
			return rawURL;
		}
	}
}
