package nl.cwi.monetdb.mcl.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by ferreira on 11/29/16.
 */
public class SocketIOHandler {

    private static final int CHAR_SIZE = Character.SIZE / Byte.SIZE;

    private static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;

    private static final int INTEGER_SIZE = Integer.SIZE / Byte.SIZE;

    private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

    private static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;

    private static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;

    private static final int INTERMEDIATE_BUFFER_SIZE = 1024;

    private boolean hasFinished;

    private ByteBuffer bufferIn = ByteBuffer.allocateDirect(INTERMEDIATE_BUFFER_SIZE);

    private ByteBuffer bufferOut = ByteBuffer.allocateDirect(INTERMEDIATE_BUFFER_SIZE);

    private final SocketConnection connection;

    public SocketIOHandler(SocketConnection connection) {
        this.connection = connection;
    }

    public SocketConnection getConnection() {
        return connection;
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

    public void readUntilChar(StringBuilder builder, char limit) throws IOException {
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
    }

    private void refillBufferIn() throws IOException {
        bufferIn.compact();
        if(!hasFinished) {
            try {
                connection.readMore(this.bufferIn);
                bufferIn.flip();
            } catch (IOException ex) {
                hasFinished = true;
            }
        } else {
            throw new IOException("Done!");
        }
    }

    public void writeNextLine(byte[] prefix, String line, byte[] suffix) throws IOException {
        bufferOut.clear();
        this.writeNextBlock(prefix);
        this.writeNextBlock(line.getBytes());
        this.writeNextBlock(suffix);
        if (bufferOut.hasRemaining()) {
            bufferOut.flip();
            connection.writeMore(this.bufferOut);
        }
    }

    private void writeNextBlock(byte[] block) throws IOException {
        for (byte aBlock : block) {
            if (!bufferOut.hasRemaining()) {
                bufferOut.flip();
                connection.writeMore(this.bufferOut);
                bufferOut.clear();
            }
            bufferOut.put(aBlock);
        }
    }
}
