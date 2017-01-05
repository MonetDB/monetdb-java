/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.mapi;

import nl.cwi.monetdb.mcl.connection.helpers.BufferReallocator;

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

public abstract class AbstractSocket implements Closeable {

    protected final Socket socket;

    protected final MapiConnection connection;

    private final ByteBuffer bufferIn;

    private final ByteBuffer bufferOut;

    private final CharBuffer stringsEncoded;

    private final CharBuffer stringsDecoded;

    private final CharsetEncoder asciiEncoder = StandardCharsets.UTF_8.newEncoder();

    private final CharsetDecoder asciiDecoder = StandardCharsets.UTF_8.newDecoder();

    AbstractSocket(String hostname, int port, MapiConnection connection) throws IOException {
        this.socket = new Socket(hostname, port);
        this.connection = connection;
        this.bufferIn = ByteBuffer.wrap(new byte[getBlockSize()]);
        this.bufferOut = ByteBuffer.wrap(new byte[getBlockSize()]);
        this.stringsEncoded = CharBuffer.allocate(getBlockSize());
        this.stringsDecoded = CharBuffer.allocate(getBlockSize());
        this.stringsDecoded.flip();
    }

    int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }

    void setSoTimeout(int s) throws SocketException {
        socket.setSoTimeout(s);
    }

    void setTcpNoDelay(boolean on) throws SocketException {
        socket.setTcpNoDelay(on);
    }

    void setSocketChannelEndianness(ByteOrder bo) {
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
            throw new IOException("The server has reached EOF!");
        }
        this.stringsDecoded.clear();
        this.asciiDecoder.reset();
        this.asciiDecoder.decode(this.bufferIn, this.stringsDecoded,true);
        this.asciiDecoder.flush(this.stringsDecoded);
        this.stringsDecoded.flip();
    }

    public CharBuffer readLine(CharBuffer lineBuffer) throws IOException {
        lineBuffer.clear();
        boolean found = false;
        char[] sourceArray = this.stringsDecoded.array();
        int sourcePosition = this.stringsDecoded.position();
        char[] destinationArray = lineBuffer.array();
        int destinationPosition = 0;
        int destinationLimit = lineBuffer.limit();

        while(!found) {
            if(!this.stringsDecoded.hasRemaining()) {
                this.readToBuffer();
                sourceArray = this.stringsDecoded.array();
                sourcePosition = 0;
            }
            char c = sourceArray[sourcePosition++];
            if(c == '\n') {
                found = true;
            } else {
                if(destinationPosition + 1 >= destinationLimit) {
                    lineBuffer = BufferReallocator.ReallocateBuffer(lineBuffer);
                    destinationArray = lineBuffer.array();
                    destinationLimit = lineBuffer.limit();
                }
                destinationArray[destinationPosition++] = c;
            }
        }
        this.stringsDecoded.position(sourcePosition);
        lineBuffer.position(destinationPosition);
        lineBuffer.flip();
        return lineBuffer;
    }

    private void flushOutputCharBuffer() throws IOException {
        this.stringsEncoded.flip();
        this.asciiEncoder.reset();
        this.asciiEncoder.encode(this.stringsEncoded, this.bufferOut, true);
        this.asciiEncoder.flush(this.bufferOut);
        this.stringsEncoded.clear();
        int written = this.writeFromBufferOut(this.bufferOut);
        if(written == 0) {
            throw new IOException("The query could not be sent to the server!");
        } else {
            this.flush();
        }
    }

    private void writeNextBlock(String line) throws IOException {
        int limit = line.length();
        int destinationPosition = this.stringsEncoded.position();
        char[] destinationArray = this.stringsEncoded.array();

        for (int i = 0; i < limit; i++) {
            if (!this.stringsEncoded.hasRemaining()) {
                this.flushOutputCharBuffer();
                destinationArray = this.stringsEncoded.array();
                destinationPosition = 0;
            }
            destinationArray[destinationPosition++] = line.charAt(i);
        }
        this.stringsEncoded.position(destinationPosition);
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
