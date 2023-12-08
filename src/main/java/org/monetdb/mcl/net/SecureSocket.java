package org.monetdb.mcl.net;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class SecureSocket {
    private static final String[] ENABLED_PROTOCOLS = {"TLSv1.3"};
    final String[] APPLICATION_PROTOCOLS = {"mapi/9"};

    public static Socket wrap(Target.Validated validated, Socket inner) throws IOException {
        Target.Verify verify = validated.connectVerify();
        SSLSocketFactory socketFactory;
        try {
            switch (verify) {
                case System:
                    socketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                    return wrapSocket(inner, validated, socketFactory, true);
                case Cert:
                        KeyStore keyStore = keyStoreForCert(validated.getCert());
                        socketFactory = certBasedSocketFactory(keyStore);
                    return wrapSocket(inner, validated, socketFactory, true);
                case Hash:
                    return wrapHash(validated, inner);
                default:
                    throw new RuntimeException("unreachable: unexpected verification strategy " + verify.name());
            }
        } catch (CertificateException e) {
            throw new SSLException(e.getMessage(), e);
        }
    }

    private static Socket wrapHash(Target.Validated validated, Socket inner) throws IOException, CertificateException {
        SSLSocketFactory socketFactory = hashBasedSocketFactory(validated.connectCertHashDigits());
        SSLSocket sock = wrapSocket(inner, validated, socketFactory, false);

        return sock;
    }

    private static SSLSocket wrapSocket(Socket inner, Target.Validated validated, SSLSocketFactory socketFactory, boolean checkName) throws IOException {
        SSLSocket sock = (SSLSocket) socketFactory.createSocket(inner, validated.connectTcp(), validated.connectPort(), true);

        sock.setUseClientMode(true);
        sock.setEnabledProtocols(ENABLED_PROTOCOLS);

        if (checkName) {
            SSLParameters parameters = sock.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            sock.setSSLParameters(parameters);
        }
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
            context.init(null, new TrustManager[]{ trustManager}, null);
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
            for (byte b: hashBytes) {
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
