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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;
import org.monetdb.testinfra.Config;
import org.monetdb.mcl.net.Parameter;
import org.monetdb.mcl.net.Target;
import org.monetdb.mcl.net.ValidationError;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.monetdb.testinfra.Assertions.assertSQLException;

@Tag("tls")
@EnabledIf("org.monetdb.testinfra.Config#isTLSTesterConfigured")
public class TLSTests {
	@TempDir
	File tmpDir;

	/**
	 * Connection parameters used by {@link #attempt()}.
	 * Tests configure it using {@link #target(String)}.
	 */
	Target parms = null;
	/**
	 * Query string attached to all http requests, contains the test name.
	 * This makes it easier to read the tlstester log.
	 */
	private String query;

	@BeforeAll
	public static void checkConfig() throws IOException {
		Config.getTesterHost();
		Config.getTesterPort();
		TLSTests me = new TLSTests();
		me.lookupPort("base");
	}

	@BeforeEach
	public void setQuery(TestInfo info) {
		query = "test=" + info.getDisplayName();
	}

	/**
	 * Fetch TLSTester resource {@code name} and write it to {@code wr}.
	 *
	 * @param name
	 * @param wr
	 * @throws IOException
	 */
	private void fetch(String name, OutputStream wr) throws IOException {
		String authority = Config.getTesterHost() + ":" + Config.getTesterPort();
		URL url = null;
		try {
			URI uri = new URI("http", authority, name, query, null);
			url = uri.toURL();
			URLConnection conn = url.openConnection();
			conn.connect();
			byte[] buf = new byte[1024];
			try (InputStream rd = conn.getInputStream()) {
				while (true) {
					int nread = rd.read(buf);
					if (nread < 0)
						break;
					wr.write(buf, 0, nread);
				}
			}
		} catch (URISyntaxException | IOException e) {
			String message = "Cannot reach TLS Tester"; // fetch resource " + name;
			if (url != null)
				message += " at " + url;
			else
				message += " to retrieve " + name;
			throw new IOException(message, e);
		}
	}

	/**
	 * Download TLSTester resource {@code name} to a temporary directory and return its path.
	 *
	 * @param name
	 * @return
	 * @throws IOException
	 */
	private String download(String name) throws IOException {
		assertTrue(name.startsWith("/"));
		assertFalse(name.substring(1).contains("/"));
		File filename = new File(tmpDir, name.substring(1));
		try (FileOutputStream out = new FileOutputStream(filename)) {
			fetch(name, out);
		}
		return filename.toString();
	}

	/**
	 * The TLSTester exposes a number of ports to which we can connect.
	 * Look up the port number corresponding to name {@code portName}.
	 *
	 * @param portName
	 * @return
	 * @throws IOException
	 */
	private int lookupPort(String portName) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		fetch("/", out);
		String portMapData = out.toString("UTF-8");
		int portNr = -1;
		for (String line : portMapData.split("\\n")) {
			String[] parts = line.trim().split(":");
			assertEquals(2, parts.length, "invalid portmap line: " + line);
			if (parts[0].equals(portName)) {
				portNr = Integer.parseInt(parts[1]);
				break;
			}
		}
		assertNotEquals(-1, portNr, "port '" + portName + "' not found");
		return portNr;
	}

	/**
	 * Configure {@code target} to connect to the given named port of the TLS Tester.
	 * {@code target} can be modified further before {@link #attempt()} is called.
	 *
	 * @param portName
	 * @throws IOException
	 */
	private void target(String portName) throws IOException {
		parms = new Target();
		parms.setTls(true);
		parms.setHost(Config.getTesterHost());
		parms.setSoTimeout(3000);
		if (portName != null) {
			int portNr = lookupPort(portName);
			parms.setPort(portNr);
		}
	}

	/**
	 * Download TLSTester resource {@code name} and assign its path to {@code target} parameter {@code parm}.
	 *
	 * @param parm
	 * @param name
	 * @throws IOException
	 * @throws ValidationError
	 */
	private void setFile(Parameter parm, String name) throws IOException, ValidationError {
		String path = download(name);
		parms.setString(parm, path);
	}

	/**
	 * Make a connection attempt using the parameters configured in {@code target}.
	 * <p>
	 * Asserts that an exception is thrown.
	 * Returns succesfully if the exception contains a known, recognizable error message.
	 * This indicates that any requested TLS handshake has succeeded.
	 * Any other exception is passed on.
	 *
	 * @throws SQLException
	 */
	private void attempt() throws SQLException {
		if (parms == null) {
			fail("all tests must first call port()");
		}
		final String EXPECTED_MESSAGE = "Sorry, this is not a real MonetDB instance";
		try {
			MonetConnection conn = new MonetConnection(parms);
			conn.close();
			fail("TLS Tester was supposed to return the error message '" + EXPECTED_MESSAGE + "' but the connection succeeded");
		} catch (SQLException e) {
			if (!e.getMessage().startsWith(EXPECTED_MESSAGE)) {
				// something real went wrong
				throw e;
			}
			// the attempt was succesful
		}
	}

	@Test
	public void test_connect_plain() throws IOException, SQLException {
		target("plain");
		parms.setTls(false);
		attempt();
	}

	@Test
	public void test_connect_tls() throws IOException, SQLException, ValidationError {
		target("server1");
		setFile(Parameter.CERT, "/ca1.crt");
		attempt();
	}

	@Test
	public void test_refuse_no_cert() throws IOException, SQLException {
		target("server1");
		assertSQLException("PKIX path building failed", this::attempt);
	}

	@Test
	public void test_refuse_wrong_cert() throws IOException, SQLException, ValidationError {
		target("server1");
		setFile(Parameter.CERT, "/ca2.crt");
		assertSQLException("PKIX path building failed", this::attempt);
	}

	@Test
	public void test_refuse_wrong_host() throws IOException, SQLException, ValidationError {
		String host = Config.getTesterHost();
		Assumptions.assumeTrue(host.equals("localhost"));
		String altHost = "localhost.localdomain";
		target("server1");
		parms.setHost(altHost);
		setFile(Parameter.CERT, "/ca1.crt");
		assertSQLException("No subject alternative DNS name", this::attempt);
	}

	@Test
	public void test_refuse_tlsv12() throws IOException, SQLException, ValidationError {
		target("tls12");
		setFile(Parameter.CERT, "/ca1.crt");
		assertSQLException("protocol_version", this::attempt);
	}

	@Test
	public void test_refuse_expired() throws IOException, SQLException, ValidationError {
		target("expiredcert");
		setFile(Parameter.CERT, "/ca1.crt");
		assertSQLException("PKIX path validation failed", this::attempt);
	}

	@Test
	@Disabled("client auth not yet supported")
	public void test_connect_client_auth1() throws IOException, SQLException, ValidationError {
		target("clientauth");
		setFile(Parameter.CERT, "/ca1.crt");
		setFile(Parameter.CLIENTKEY, "/client2.keycrt");
		attempt();
	}

	@Test
	@Disabled("client auth not yet supported")
	public void test_connect_client_auth2() throws IOException, SQLException, ValidationError {
		target("clientauth");
		setFile(Parameter.CERT, "/ca1.crt");
		setFile(Parameter.CLIENTKEY, "/client2.key");
		setFile(Parameter.CLIENTCERT, "/client2.crt");
		attempt();
	}

	@Test
	public void test_fail_tls_to_plain() throws IOException, SQLException, ValidationError {
		target("plain");
		setFile(Parameter.CERT, "/ca1.crt");
		assertSQLException("", this::attempt);

	}

	@Test
	public void test_fail_plain_to_tls() throws IOException, SQLException {
		target("server1");
		parms.setTls(false);
		assertSQLException("Could not connect", this::attempt);
	}

	@Test
	public void test_connect_server_name() throws IOException, SQLException, ValidationError {
		target("sni");
		setFile(Parameter.CERT, "/ca1.crt");
		attempt();
	}

	@Test
	public void test_connect_alpn_mapi9() throws IOException, SQLException, ValidationError {
		target("alpn_mapi9");
		setFile(Parameter.CERT, "/ca1.crt");
		attempt();
	}

	@Test
	public void test_connect_trusted() throws IOException, SQLException, URISyntaxException, ValidationError {
		target(null);
		String url = Config.getKnownGoodUrl();
		parms.parseUrl(url);
		attempt();
	}

	@Test
	public void test_refuse_trusted_wrong_host() throws IOException, SQLException, URISyntaxException, ValidationError {
		target(null);
		String url = Config.getAliasKnownGoodUrl();
		parms.parseUrl(url);
		assertSQLException("No subject alternative DNS name", this::attempt);
	}
}
