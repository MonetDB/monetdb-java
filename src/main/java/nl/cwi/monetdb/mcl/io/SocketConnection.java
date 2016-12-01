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

    private static final int CHAR_SIZE = Character.SIZE / Byte.SIZE;

    private static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;

    private static final int INTEGER_SIZE = Integer.SIZE / Byte.SIZE;

    private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

    private static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;

    private static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;

    private static final int INTERMEDIATE_BUFFER_SIZE = 1024;

    /* Local variables */
    private boolean hasFinished;

    /** Bytebuffers */
    private final ByteBuffer bufferIn = ByteBuffer.allocateDirect(INTERMEDIATE_BUFFER_SIZE);

    private final ByteBuffer bufferOut = ByteBuffer.allocateDirect(INTERMEDIATE_BUFFER_SIZE);

    /** The socket channel */
    private final SocketChannel connection;

    public SocketConnection(String hostname, int port) throws IOException {
        this.connection = SocketChannel.open(new InetSocketAddress(hostname, port));
        this.connection.configureBlocking(true);
    }

    /* Socket Channel methods */

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
        this.hasFinished = true;
        this.connection.close();
    }

    /* Byte buffer methods */

    private void refillBufferIn() throws IOException {
        bufferIn.compact();
        if(!hasFinished) {
            try {
                connection.read(this.bufferIn);
                bufferIn.flip();
            } catch (IOException ex) {
                hasFinished = true;
            }
        } else {
            throw new IOException("Done!");
        }
    }


    public byte readNextByte() throws IOException {
        if(this.bufferIn.remaining() < Byte.SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.get();
    }

    public char readNextChar() throws IOException {
        if(this.bufferIn.remaining() < CHAR_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getChar();
    }

    public short readNextShort() throws IOException {
        if(this.bufferIn.remaining() < SHORT_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getShort();
    }

    public int readNextInt() throws IOException {
        if(this.bufferIn.remaining() < INTEGER_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getInt();
    }

    public long readNextLong() throws IOException {
        if(this.bufferIn.remaining() < LONG_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getLong();
    }

    public float readNextFloat() throws IOException {
        if(this.bufferIn.remaining() < FLOAT_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getFloat();
    }

    public double readNextDouble() throws IOException {
        if(this.bufferIn.remaining() < DOUBLE_SIZE) {
            this.refillBufferIn();
        }
        return this.bufferIn.getDouble();
    }

    public int readUntilChar(StringBuilder builder, char limit) throws IOException {
        builder.setLength(0);
        boolean found = false;

        while(!found) {
            if (this.bufferIn.remaining() < CHAR_SIZE) {
                this.refillBufferIn();
            }
            char next = this.bufferIn.getChar();
            builder.append(next);
            if(next == limit) {
                found = true;
            }
        }
        return builder.length();
    }

    public void writeNextLine(byte[] line) throws IOException {
        bufferOut.clear();
        this.writeNextBlock(line);
        if (bufferOut.hasRemaining()) {
            bufferOut.flip();
            connection.write(this.bufferOut);
        }
    }

    public void writeNextLine(byte[] prefix, String line, byte[] suffix) throws IOException {
        bufferOut.clear();
        this.writeNextBlock(prefix);
        this.writeNextBlock(line.getBytes());
        this.writeNextBlock(suffix);
        if (bufferOut.hasRemaining()) {
            bufferOut.flip();
            connection.write(this.bufferOut);
        }
    }

    private void writeNextBlock(byte[] block) throws IOException {
        for (byte aBlock : block) {
            if (!bufferOut.hasRemaining()) {
                bufferOut.flip();
                connection.write(this.bufferOut);
                bufferOut.clear();
            }
            bufferOut.put(aBlock);
        }
    }
}
