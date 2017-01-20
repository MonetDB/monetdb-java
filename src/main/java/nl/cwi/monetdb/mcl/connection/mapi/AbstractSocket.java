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

/**
 * An abstract class to be extended by a JDBC socket connection. The base idea of this class is to allow easy
 * integrations with future versions of the MAPI protocol. With new versions of the protocol, the way the data is
 * fetched will be different hence this class should be sub-classed according to the protocol itself.
 * <br/>
 * Meanwhile the implementation of this class uses Java ByteBuffers which allows memory re-usage for more performance.
 * Also MonetDB uses UTF-8 as its character encoding, hence its required to convert into UTF-16 (JVM encoding).
 *
 * @author Pedro Ferreira
 */
public abstract class AbstractSocket implements Closeable {
    /** The TCP Socket to mserver */
    protected final Socket socket;
    /** The MAPI connection this socket belong to */
    protected final MapiConnection connection;
    /** ByteBuffer to read from the underlying socket InputStream */
    private final ByteBuffer bufferIn;
    /** ByteBuffer to write into the underlying socket OutputStream */
    private final ByteBuffer bufferOut;
    /** The bytes read from the bufferIn decoded into UTF-16 */
    private final CharBuffer stringsDecoded;
    /** The bytes to write into the bufferOut encoded into UTF-8 */
    private final CharBuffer stringsEncoded;
    /** UTF-8 encoder */
    private final CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();
    /** UTF-8 decoder */
    private final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();

    AbstractSocket(String hostname, int port, MapiConnection connection) throws IOException {
        this.socket = new Socket(hostname, port);
        this.connection = connection;
        this.bufferIn = ByteBuffer.wrap(new byte[getFullBlockSize()]);
        this.bufferOut = ByteBuffer.wrap(new byte[getFullBlockSize()]);
        this.stringsDecoded = CharBuffer.allocate(getFullBlockSize());
        this.stringsDecoded.flip();
        this.stringsEncoded = CharBuffer.allocate(getFullBlockSize());
    }

    /**
     * Get the socket timeout in milliseconds.
     *
     * @return The currently in use socket timeout in milliseconds
     * @throws SocketException If an error in the underlying connection happened
     */
    int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }

    /**
     * Sets the socket timeout in milliseconds.
     *
     * @param s The socket timeout in milliseconds
     * @throws SocketException If an error in the underlying connection happened
     */
    void setSoTimeout(int s) throws SocketException {
        socket.setSoTimeout(s);
    }

    /**
     * Sets the TCP no delay feature in the underlying socket.
     *
     * @param on A true or false value
     * @throws SocketException If an error in the underlying connection happened
     */
    void setTcpNoDelay(boolean on) throws SocketException {
        socket.setTcpNoDelay(on);
    }

    /**
     * Sets the underlying socket Endianness.
     *
     * @param bo A ByteOrder order value either Little-endian or Big-endian
     */
    void setSocketChannelEndianness(ByteOrder bo) {
        this.bufferIn.order(bo);
        this.bufferOut.order(bo);
    }

    /**
     * Gets the underlying socket full block size.
     *
     * @return The underlying socket full block size
     */
    public abstract int getFullBlockSize();

    /**
     * Gets the underlying socket block size.
     *
     * @return The underlying socket block size
     */
    public abstract int getBlockSize();

    /**
     * Reads from the underlying socket into the bufferIn.
     *
     * @return The number off bytes read
     * @throws IOException If an error in the underlying connection happened
     */
    abstract int readToBufferIn(ByteBuffer bufferIn) throws IOException;

    /**
     * Writes from bufferOut into the underlying socket.
     *
     * @return The number off bytes written
     * @throws IOException If an error in the underlying connection happened
     */
    abstract int writeFromBufferOut(ByteBuffer bufferOut) throws IOException;

    /**
     * Flushes the output.
     *
     * @throws IOException If an error in the underlying connection happened
     */
    abstract void flush() throws IOException;

    /**
     * Helper method to read and decode UTF-8 data.
     *
     * @throws IOException If an error in the underlying connection happened
     */
    private void readToInputBuffer() throws IOException {
        int read = this.readToBufferIn(this.bufferIn);
        if(read == 0) {
            throw new IOException("The server has reached EOF!");
        }
        this.stringsDecoded.clear();
        this.utf8Decoder.reset();
        this.utf8Decoder.decode(this.bufferIn, this.stringsDecoded,true);
        this.utf8Decoder.flush(this.stringsDecoded);
        this.stringsDecoded.flip();
    }

    /**
     * Reads a line into the input lineBuffer, reallocating it if necessary.
     *
     * @param lineBuffer The buffer the data will be read into
     * @return The input lineBuffer
     * @throws IOException If an error in the underlying connection happened
     */
    public CharBuffer readLine(CharBuffer lineBuffer) throws IOException {
        lineBuffer.clear();
        boolean found = false;
        char[] sourceArray = this.stringsDecoded.array();
        int sourcePosition = this.stringsDecoded.position();
        int sourceLimit = this.stringsDecoded.limit();
        char[] destinationArray = lineBuffer.array();
        int destinationPosition = 0;
        int destinationLimit = lineBuffer.limit();

        while(!found) {
            if(sourcePosition >= sourceLimit) {
                this.stringsDecoded.position(sourcePosition);
                this.readToInputBuffer();
                sourceArray = this.stringsDecoded.array();
                sourcePosition = 0;
                sourceLimit = this.stringsDecoded.limit();
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

    /**
     * Helper method to write, encode into UTF-8 and flush.
     *
     * @param toFlush A boolean indicating to flush the underlying stream or not
     * @throws IOException If an error in the underlying connection happened
     */
    private void writeToOutputBuffer(boolean toFlush) throws IOException {
        this.stringsEncoded.flip();
        this.utf8Encoder.reset();
        this.utf8Encoder.encode(this.stringsEncoded, this.bufferOut, true);
        this.utf8Encoder.flush(this.bufferOut);
        this.stringsEncoded.clear();
        int written = this.writeFromBufferOut(this.bufferOut);
        if(written == 0) {
            throw new IOException("The query could not be sent to the server!");
        } else {
            if(toFlush) {
                this.flush();
            }
        }
    }

    /**
     * Writes a String line into the underlying socket.
     *
     * @param line The line to write in the socket
     * @throws IOException If an error in the underlying connection happened
     */
    private void writeNextBlock(String line) throws IOException {
        int limit = line.length();
        int destinationPosition = this.stringsEncoded.position();
        int destinationCapacity = this.stringsEncoded.capacity();
        char[] destinationArray = this.stringsEncoded.array();

        for (int i = 0; i < limit; i++) {
            if (destinationPosition >= destinationCapacity) {
                this.stringsEncoded.position(destinationPosition);
                this.writeToOutputBuffer(false);
                destinationArray = this.stringsEncoded.array();
                destinationPosition = 0;
            }
            destinationArray[destinationPosition++] = line.charAt(i);
        }
        this.stringsEncoded.position(destinationPosition);
    }

    /**
     * Writes a String line as well a String prefix and suffix if supplied.
     *
     * @param prefix The prefix to write before the line if provided
     * @param line The line to write into the socket
     * @param suffix The suffix to write after the line if provided
     * @throws IOException If an error in the underlying connection happened
     */
    public void writeNextLine(String prefix, String line, String suffix) throws IOException {
        if(prefix != null) {
            this.writeNextBlock(prefix);
        }
        this.writeNextBlock(line);
        if(suffix != null) {
            this.writeNextBlock(suffix);
        }
        this.writeToOutputBuffer(true);
    }
}
