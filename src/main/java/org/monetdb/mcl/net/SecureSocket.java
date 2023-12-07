package org.monetdb.mcl.net;

import java.net.Socket;

public class SecureSocket {
    public static Socket wrap(Target.Validated validated, Socket sock) {
        throw new MCLException("TLS connections (monetdbs://) are not supported yet");
    }
}
