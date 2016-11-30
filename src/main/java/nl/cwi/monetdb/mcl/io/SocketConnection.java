package nl.cwi.monetdb.mcl.io;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by ferreira on 11/29/16.
 */
public class SocketConnection implements Closeable {

    /** The blocksize (hardcoded in compliance with stream.mx) */
    private static final int BLOCK = 8 * 1024 - 2;

    /** The socket channel */
    private SocketChannel connection;

    public SocketConnection(String hostname, int port) throws IOException {
        this.connection = SocketChannel.open(new InetSocketAddress(hostname, port));
        this.connection.configureBlocking(true);
    }

    public int getSoTimeout() throws SocketException {
        return connection.socket().getSoTimeout();
    }

    public void setSoTimeout(int s) throws SocketException {
        connection.socket().setSoTimeout(s);
    }

    public int getBlockSize() {
        return BLOCK;
    }

    public int readMore(ByteBuffer dst) throws IOException {
        return connection.read(dst);
    }

    public int writeMore(ByteBuffer src) throws IOException {
        return connection.write(src);
    }

    public void setTcpNoDelay(boolean on) throws SocketException {
        this.connection.socket().setTcpNoDelay(on);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
