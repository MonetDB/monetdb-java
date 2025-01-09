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

import org.monetdb.mcl.net.Parameter;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public final class TLSTester {
	final HashMap<String, File> fileCache = new HashMap<>();
	int verbose = 0;
	String serverHost = null;
	String altHost = null;
	int serverPort = -1;
	boolean enableTrusted = false;
	File tempDir = null;
	private final HashSet<String> preparedButNotRun = new HashSet<>();

	public TLSTester(String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-v")) {
				verbose = 1;
			} else if (arg.equals("-a")) {
				altHost = args[++i];
			} else if (arg.equals("-t")) {
				enableTrusted = true;
			} else if (!arg.startsWith("-") && serverHost == null) {
				int idx = arg.indexOf(':');
				if (idx > 0) {
					serverHost = arg.substring(0, idx);
					try {
						serverPort = Integer.parseInt(arg.substring(idx + 1));
						if (serverPort > 0 && serverPort < 65536)
							continue;
					} catch (NumberFormatException ignored) {
					}
				}
				// if we get here it wasn't very valid
				throw new IllegalArgumentException("Invalid argument: " + arg);
			} else {
				throw new IllegalArgumentException("Unexpected argument: " + arg);
			}
		}
	}

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		Class.forName("org.monetdb.jdbc.MonetDriver");
		TLSTester main = new TLSTester(args);
		main.run();
	}

	private HashMap<String, Integer> loadPortMap(String testName) throws IOException {
		HashMap<String, Integer> portMap = new HashMap<>();
		InputStream in = fetchData("/?test=" + testName);
		BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			int idx = line.indexOf(':');
			String service = line.substring(0, idx);
			int port;
			try {
				port = Integer.parseInt(line.substring(idx + 1));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid port map line: " + line);
			}
			portMap.put(service, port);
		}
		return portMap;
	}

	private File resource(String resource) throws IOException {
		if (!fileCache.containsKey(resource))
			fetchResource(resource);
		return fileCache.get(resource);
	}

	private void fetchResource(String resource) throws IOException {
		if (!resource.startsWith("/")) {
			throw new IllegalArgumentException("Resource must start with slash: " + resource);
		}
		if (tempDir == null) {
			tempDir = Files.createTempDirectory("tlstest").toFile();
			tempDir.deleteOnExit();
		}
		File outPath = new File(tempDir, resource.substring(1));
		try (InputStream in = fetchData(resource); FileOutputStream out = new FileOutputStream(outPath)) {
			byte[] buffer = new byte[12];
			while (true) {
				int n = in.read(buffer);
				if (n <= 0)
					break;
				out.write(buffer, 0, n);
			}
		}
		fileCache.put(resource, outPath);
	}

	private InputStream fetchData(String resource) throws IOException {
		String urlText = "http://" + serverHost + ":" + serverPort + resource;
		if (verbose > 0) {
			System.out.println("Fetching " + resource + " from " + urlText);
		}
		URL url = null;
		try {
			url = new java.net.URI(urlText).toURL();
			URLConnection conn = url.openConnection();
			conn.connect();
			return conn.getInputStream();
		} catch (URISyntaxException | IOException e) {
			throw new IOException("Cannot fetch resource " + resource + " from " + urlText + ": " + e, e);
		}
	}

	private void run() throws IOException, SQLException {
		test_connect_plain();
		test_connect_tls();
		test_refuse_no_cert();
		test_refuse_wrong_cert();
		test_refuse_wrong_host();
		test_refuse_tlsv12();
		test_refuse_expired();
//		test_connect_client_auth1();
//		test_connect_client_auth2();
		test_fail_tls_to_plain();
		test_fail_plain_to_tls();
		test_connect_server_name();
		test_connect_alpn_mapi9();
		test_connect_trusted();
		test_refuse_trusted_wrong_host();

		// did we forget to call expectSucceed and expectFailure somewhere?
		if (!preparedButNotRun.isEmpty()) {
			String names = String.join(", ", preparedButNotRun);
			throw new RuntimeException("Not all tests called expectSuccess/expectFailure: " + names);
		}
	}

	private void test_connect_plain() throws IOException, SQLException {
		attempt("connect_plain", "plain").with(Parameter.TLS, false).expectSuccess();
	}

	private void test_connect_tls() throws IOException, SQLException {
		Attempt attempt = attempt("connect_tls", "server1");
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectSuccess();
	}

	private void test_refuse_no_cert() throws IOException, SQLException {
		attempt("refuse_no_cert", "server1").expectFailure("PKIX path building failed");
	}

	private void test_refuse_wrong_cert() throws IOException, SQLException {
		Attempt attempt = attempt("refuse_wrong_cert", "server1");
		attempt.withFile(Parameter.CERT, "/ca2.crt").expectFailure("PKIX path building failed");
	}

	private void test_refuse_wrong_host() throws IOException, SQLException {
		if (altHost == null)
			return;
		Attempt attempt = attempt("refuse_wrong_host", "server1").with(Parameter.HOST, altHost);
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectFailure("No subject alternative DNS name");
	}

	private void test_refuse_tlsv12() throws IOException, SQLException {
		Attempt attempt = attempt("refuse_tlsv12", "tls12");
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectFailure("protocol_version");
	}

	private void test_refuse_expired() throws IOException, SQLException {
		Attempt attempt = attempt("refuse_expired", "expiredcert");
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectFailure("PKIX path validation failed");
	}

	private void test_connect_client_auth1() throws IOException, SQLException {
		attempt("connect_client_auth1", "clientauth")
				.withFile(Parameter.CERT, "/ca1.crt")
				.withFile(Parameter.CLIENTKEY, "/client2.keycrt")
				.expectSuccess();
	}

	private void test_connect_client_auth2() throws IOException, SQLException {
		attempt("connect_client_auth2", "clientauth")
				.withFile(Parameter.CERT, "/ca1.crt")
				.withFile(Parameter.CLIENTKEY, "/client2.key")
				.withFile(Parameter.CLIENTCERT, "/client2.crt")
				.expectSuccess();
	}

	private void test_fail_tls_to_plain() throws IOException, SQLException {
		Attempt attempt = attempt("fail_tls_to_plain", "plain");
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectFailure("");

	}

	private void test_fail_plain_to_tls() throws IOException, SQLException {
		attempt("fail_plain_to_tls", "server1").with(Parameter.TLS, false).expectFailure("Cannot connect", "Could not connect");
	}

	private void test_connect_server_name() throws IOException, SQLException {
		Attempt attempt = attempt("connect_server_name", "sni");
		attempt.withFile(Parameter.CERT, "/ca1.crt").expectSuccess();
	}

	private void test_connect_alpn_mapi9() throws IOException, SQLException {
		attempt("connect_alpn_mapi9", "alpn_mapi9").withFile(Parameter.CERT, "/ca1.crt").expectSuccess();
	}

	private void test_connect_trusted() throws IOException, SQLException {
		attempt("connect_trusted", null)
				.with(Parameter.HOST, "monetdb.ergates.nl")
				.with(Parameter.PORT, 50000)
				.expectSuccess();
	}

	private void test_refuse_trusted_wrong_host() throws IOException, SQLException {
		attempt("test_refuse_trusted_wrong_host", null)
				.with(Parameter.HOST, "monetdbxyz.ergates.nl")
				.with(Parameter.PORT, 50000)
				.expectFailure("No subject alternative DNS name");
	}

	private Attempt attempt(String testName, String portName) throws IOException {
		preparedButNotRun.add(testName);
		return new Attempt(testName, portName);
	}

	private class Attempt {
		private final String testName;
		private final Properties props = new Properties();
		boolean disabled = false;

		public Attempt(String testName, String portName) throws IOException {
			HashMap<String, Integer> portMap = loadPortMap(testName);

			this.testName = testName;
			with(Parameter.TLS, true);
			with(Parameter.HOST, serverHost);
			with(Parameter.SO_TIMEOUT, 3000);
			if (portName != null) {
				Integer port = portMap.get(portName);
				if (port != null) {
					with(Parameter.PORT, port);
				} else {
					throw new RuntimeException("Unknown port name: " + portName);
				}
			}
		}

		private Attempt with(Parameter parm, String value) {
			props.setProperty(parm.name, value);
			return this;
		}

		private Attempt with(Parameter parm, int value) {
			props.setProperty(parm.name, Integer.toString(value));
			return this;
		}

		private Attempt with(Parameter parm, boolean value) {
			props.setProperty(parm.name, value ? "true" : "false");
			return this;
		}

		private Attempt withFile(Parameter parm, String certResource) throws IOException {
			File certFile = resource(certResource);
			String path = certFile.getPath();
			with(parm, path);
			return this;
		}

		public void expectSuccess() throws SQLException {
			preparedButNotRun.remove(testName);
			if (disabled)
				return;
			startVerbose();
			try {
				Connection conn = DriverManager.getConnection("jdbc:monetdb:", props);
				conn.close();
				throw new RuntimeException("Test " + testName + " was supposed to throw an Exception saying 'Sorry, this is not a real MonetDB instance'");
			} catch (SQLException e) {
				if (e.getMessage().startsWith("Sorry, this is not a real MonetDB instance")) {
					// it looks like a failure but this is actually our success scenario
					// because this is what the TLS Tester does when the connection succeeds.
					endVerbose("successful MAPI handshake, as expected");
					return;
				}
				// other exceptions ARE errors and should be reported.
				throw e;
			}
		}

		public void expectFailure(String... expectedMessages) throws SQLException {
			preparedButNotRun.remove(testName);
			if (disabled)
				return;
			startVerbose();
			try {
				Connection conn = DriverManager.getConnection("jdbc:monetdb:", props);
				conn.close();
				throw new RuntimeException("Expected test " + testName + " to throw an exception but it didn't");
			} catch (SQLException e) {
				for (String expected : expectedMessages) {
					if (e.getMessage().contains(expected)) {
						endVerbose("connection failed as expected, message: " + e.getMessage());
						return;
					}
				}
				String message = "Test " + testName + " threw the wrong exception: " + e.getMessage() + '\n' + "Expected:\n        <" + String.join(">\n        <", expectedMessages) + ">";
				throw new RuntimeException(message, e);
			}
		}

		private void startVerbose() {
			if (verbose == 0)
				return;

			System.out.println("Test " + testName + ":");
			for (String key: props.stringPropertyNames()) {
				Object value = props.get(key);
				if (value == null)
					System.out.println("    " + key + " is null");
				else
					System.out.println("    " + key + " = " + value.toString());
			}
		}

		private void endVerbose(String message) {
			if (verbose > 0) {
				System.out.println("    -> " + message);
				System.out.println();
			}
		}
	}
}
