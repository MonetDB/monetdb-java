package nl.cwi.monetdb.mcl.connection.socket;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Created by ferreira on 12/9/16.
 */
public abstract class AbstractSocket implements Closeable {

    protected final Socket socket;

    protected final MapiConnection connection;

    private final ByteBuffer bufferIn;

    private final ByteBuffer bufferOut;

    private final CharBuffer stringsEncoded;

    private final CharBuffer stringsDecoded;

    private final CharsetEncoder asciiEncoder = StandardCharsets.UTF_8.newEncoder();

    private final CharsetDecoder asciiDecoder = StandardCharsets.UTF_8.newDecoder();

    private boolean hasFinished;

    public AbstractSocket(String hostname, int port, MapiConnection connection) throws IOException {
        this.socket = new Socket(hostname, port);
        this.connection = connection;
        this.bufferIn = ByteBuffer.wrap(new byte[getBlockSize()]);
        this.bufferOut = ByteBuffer.wrap(new byte[getBlockSize()]);
        this.stringsEncoded = CharBuffer.allocate(getBlockSize());
        this.stringsDecoded = CharBuffer.allocate(getBlockSize());
        this.stringsDecoded.flip();
    }

    public int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }

    public void setSoTimeout(int s) throws SocketException {
        socket.setSoTimeout(s);
    }

    public void setTcpNoDelay(boolean on) throws SocketException {
        socket.setTcpNoDelay(on);
    }

    public void setSocketChannelEndianness(ByteOrder bo) {
        this.bufferIn.order(bo);
        this.bufferOut.order(bo);
    }

    public abstract int getBlockSize();

    abstract int readToBufferIn(ByteBuffer bufferIn) throws IOException;

    abstract int writeFromBufferOut(ByteBuffer bufferOut) throws IOException;

    abstract void flush() throws IOException;

    private void readToBuffer() throws IOException {
        int read = this.readToBufferIn(this.bufferIn);
        if(read == 0) {
            this.hasFinished = true;
            throw new IOException("Done!");
        }
        this.stringsDecoded.clear();
        this.asciiDecoder.reset();
        this.asciiDecoder.decode(this.bufferIn, this.stringsDecoded,true);
        this.asciiDecoder.flush(this.stringsDecoded);
        this.stringsDecoded.flip();
    }

    public int readLine(StringBuilder builder) throws IOException {
        builder.setLength(0);
        boolean found = false;

        while(!found) {
            if(!this.stringsDecoded.hasRemaining()) {
                this.readToBuffer();
            }
            char c = this.stringsDecoded.get();
            if(c == '\n') {
                found = true;
            } else {
                builder.append(c);
            }
        }
        return builder.length();
    }

    private void flushOutputCharBuffer() throws IOException {
        this.stringsEncoded.flip();
        this.asciiEncoder.reset();
        this.asciiEncoder.encode(this.stringsEncoded, this.bufferOut, true);
        this.asciiEncoder.flush(this.bufferOut);
        this.stringsEncoded.clear();
        int written = this.writeFromBufferOut(this.bufferOut);
        if(written == 0) {
            this.hasFinished = true;
            throw new IOException("Done!");
        } else {
            this.flush();
        }
    }

    private void writeNextBlock(String line) throws IOException {
        int limit = line.length();
        for (int i = 0; i < limit; i++) {
            if (!this.stringsEncoded.hasRemaining()) {
                this.flushOutputCharBuffer();
            }
            this.stringsEncoded.put(line.charAt(i));
        }
    }

    public void writeNextLine(String prefix, String line, String suffix) throws IOException {
        if(prefix != null) {
            this.writeNextBlock(prefix);
        }
        this.writeNextBlock(line);
        if(suffix != null) {
            this.writeNextBlock(suffix);
        }
        this.writeNextBlock("\n");
        if (this.stringsEncoded.hasRemaining()) {
            this.flushOutputCharBuffer();
        }
    }
}
