package org.monetdb.mcl.net;

import org.monetdb.mcl.MCLException;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;

public class SecureSocket {
    public static Socket wrap(Target.Validated validated, Socket inner) throws IOException {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        String host = validated.connectTcp();
        int port = validated.connectPort();
        boolean autoclose = true;
        SSLSocket sock = (SSLSocket) factory.createSocket(inner, host, port, autoclose);
        sock.setUseClientMode(true);

        sock.startHandshake();
        return sock;
    }
}
