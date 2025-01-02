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

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;

public final class SecureSocket {
	private static final String[] ENABLED_PROTOCOLS = {"TLSv1.3"};
	private static final String[] APPLICATION_PROTOCOLS = {"mapi/9"};

	// Cache for the default SSL factory. It must load all trust roots
	// so it's worthwhile to cache.
	// Only access this through #getDefaultSocketFactory()
	private static SSLSocketFactory vanillaFactory = null;

	private static synchronized SSLSocketFactory getDefaultSocketFactory() {
		if (vanillaFactory == null) {
			vanillaFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
		}
		return vanillaFactory;
	}

	public static Socket wrap(Target.Validated validated, Socket inner) throws IOException {
		Target.Verify verify = validated.connectVerify();
		SSLSocketFactory socketFactory;
		boolean checkName = true;
		try {
			switch (verify) {
				case System:
					socketFactory = getDefaultSocketFactory();
					break;
				case Cert:
					KeyStore keyStore = keyStoreForCert(validated.getCert());
					socketFactory = certBasedSocketFactory(keyStore);
					break;
				case Hash:
					socketFactory = hashBasedSocketFactory(validated.connectCertHashDigits());
					checkName = false;
					break;
				default:
					throw new RuntimeException("unreachable: unexpected verification strategy " + verify.name());
			}
			return wrapSocket(inner, validated, socketFactory, checkName);
		} catch (CertificateException e) {
			throw new SSLException("TLS certificate rejected", e);
		}
	}

	private static SSLSocket wrapSocket(Socket inner, Target.Validated validated, SSLSocketFactory socketFactory, boolean checkName) throws IOException {
		SSLSocket sock = (SSLSocket) socketFactory.createSocket(inner, validated.connectTcp(), validated.connectPort(), true);
		sock.setUseClientMode(true);
		SSLParameters parameters = sock.getSSLParameters();

		parameters.setProtocols(ENABLED_PROTOCOLS);

		parameters.setServerNames(Collections.singletonList(new SNIHostName(validated.connectTcp())));

		if (checkName) {
			parameters.setEndpointIdentificationAlgorithm("HTTPS");
		}

		// Unfortunately, SSLParameters.setApplicationProtocols is only available
		// since language level 9 and currently we're on 8.
		// Still call it if it happens to be available.
		try {
			Method setApplicationProtocols = SSLParameters.class.getMethod("setApplicationProtocols", String[].class);
			setApplicationProtocols.invoke(parameters, (Object) APPLICATION_PROTOCOLS);
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException ignored) {
		}

		sock.setSSLParameters(parameters);
		sock.startHandshake();
		return sock;
	}

	private static X509Certificate loadCertificate(String path) throws CertificateException, IOException {
		CertificateFactory factory = CertificateFactory.getInstance("X509");
		try (FileInputStream s = new FileInputStream(path)) {
			return (X509Certificate) factory.generateCertificate(s);
		}
	}

	private static SSLSocketFactory certBasedSocketFactory(KeyStore store) throws IOException, CertificateException {
		TrustManagerFactory trustManagerFactory;
		try {
			trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(store);
		} catch (NoSuchAlgorithmException | KeyStoreException e) {
			throw new RuntimeException("Could not create TrustManagerFactory", e);
		}

		SSLContext context;
		try {
			context = SSLContext.getInstance("TLS");
			context.init(null, trustManagerFactory.getTrustManagers(), null);
		} catch (NoSuchAlgorithmException | KeyManagementException e) {
			throw new RuntimeException("Could not create SSLContext", e);
		}

		return context.getSocketFactory();
	}

	private static KeyStore keyStoreForCert(String path) throws IOException, CertificateException {
		try {
			X509Certificate cert = loadCertificate(path);
			KeyStore store = emptyKeyStore();
			store.setCertificateEntry("root", cert);
			return store;
		} catch (KeyStoreException e) {
			throw new RuntimeException("Could not create KeyStore for certificate", e);
		}
	}

	private static KeyStore emptyKeyStore() throws IOException, CertificateException {
		KeyStore store;
		try {
			store = KeyStore.getInstance("PKCS12");
			store.load(null, null);
			return store;
		} catch (KeyStoreException | NoSuchAlgorithmException e) {
			throw new RuntimeException("Could not create KeyStore for certificate", e);
		}
	}

	private static SSLSocketFactory hashBasedSocketFactory(String hashDigits) {
		TrustManager trustManager = new HashBasedTrustManager(hashDigits);
		try {
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(null, new TrustManager[]{trustManager}, null);
			return context.getSocketFactory();
		} catch (NoSuchAlgorithmException | KeyManagementException e) {
			throw new RuntimeException("Could not create SSLContext", e);
		}

	}

	private static class HashBasedTrustManager implements X509TrustManager {
		private static final char[] HEXDIGITS = "0123456789abcdef".toCharArray();
		private final String hashDigits;

		public HashBasedTrustManager(String hashDigits) {
			this.hashDigits = hashDigits;
		}


		@Override
		public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
			throw new RuntimeException("this TrustManager is only suitable for client side connections");
		}

		@Override
		public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
			X509Certificate cert = x509Certificates[0];
			byte[] certBytes = cert.getEncoded();

			// for now it's always SHA256.
			byte[] hashBytes;
			try {
				MessageDigest hasher = MessageDigest.getInstance("SHA-256");
				hasher.update(certBytes);
				hashBytes = hasher.digest();
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException("failed to instantiate hash digest");
			}

			// convert to hex digits
			StringBuilder buffer = new StringBuilder(2 * hashBytes.length);
			for (byte b : hashBytes) {
				int hi = (b & 0xF0) >> 4;
				int lo = b & 0x0F;
				buffer.append(HEXDIGITS[hi]);
				buffer.append(HEXDIGITS[lo]);
			}
			String certDigits = buffer.toString();

			if (!certDigits.startsWith(hashDigits)) {
				throw new CertificateException("Certificate hash does not start with '" + hashDigits + "': " + certDigits);
			}


		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[0];
		}
	}
}
