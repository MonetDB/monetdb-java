package nl.cwi.monetdb.mcl.connection.mapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by ferreira on 12/9/16.
 */
public class OldMapiSocket extends AbstractSocket {

    /** The blocksize (hardcoded in compliance with stream.mx) */
    public final static int BLOCK = 8 * 1024 - 2;

    /**
     * A short in two bytes for holding the block size in bytes
     */
    private final byte[] blklen = new byte[2];

    private final OldMapiBlockInputStream inStream;

    private final OldMapiBlockOutputStream outStream;

    OldMapiSocket(String hostname, int port, MapiConnection connection) throws IOException {
        super(hostname, port, connection);
        this.inStream = new OldMapiBlockInputStream(socket.getInputStream());
        this.outStream = new OldMapiBlockOutputStream(socket.getOutputStream());
    }

    @Override
    public int getBlockSize() {
        return BLOCK;
    }

    @Override
    int readToBufferIn(ByteBuffer bufferIn) throws IOException {
        return this.inStream.read(bufferIn);
    }

    @Override
    int writeFromBufferOut(ByteBuffer bufferOut) throws IOException {
        return this.outStream.write(bufferOut);
    }

    @Override
    void flush() throws IOException {
        this.outStream.flush();
    }

    @Override
    public void close() throws IOException {
        this.socket.close();
    }

    private class OldMapiBlockInputStream {

        private final InputStream inStream;

        private int readPos = 0;

        private int blockLen = 0;

        private final byte[] block = new byte[BLOCK];

        /**
         * Constructs this BlockInputStream, backed by the given InputStream. A BufferedInputStream is internally used.
         */
        OldMapiBlockInputStream(InputStream in) {
            this.inStream = in;
        }

        public int available() {
            return blockLen - readPos;
        }

        /**
         * Small wrapper to get a blocking variant of the read() method
         * on the BufferedInputStream.  We want to benefit from the
         * Buffered pre-fetching, but not dealing with half blocks.
         * Changing this class to be able to use the partially received
         * data will greatly complicate matters, while a performance
         * improvement is debatable given the relatively small size of
         * our blocks.  Maybe it does speed up on slower links, then
         * consider this method a quick bug fix/workaround.
         *
         * @return false if reading the block failed due to EOF
         */
        private boolean _read(byte[] b, int len) throws IOException {
            int s;
            int off = 0;

            while (len > 0) {
                s = inStream.read(b, off, len);
                if (s == -1) {
                    // if we have read something before, we should have been
                    // able to read the whole, so make this fatal
                    if (off > 0) {
                        throw new IOException("Read from " + connection.getHostname() + ":" +
                                connection.getPort() + ": Incomplete block read from stream");
                    }
                    return false;
                }
                len -= s;
                off += s;
            }
            return true;
        }

        /**
         * Reads the next block on the stream into the internal buffer,
         * or writes the prompt in the buffer.
         * <p>
         * The blocked stream protocol consists of first a two byte
         * integer indicating the length of the block, then the
         * block, followed by another length + block.  The end of
         * such sequence is put in the last bit of the length, and
         * hence this length should be shifted to the right to
         * obtain the real length value first.  We simply fetch
         * blocks here as soon as they are needed for the stream's
         * read methods.
         * <p>
         * The user-flush, which is an implicit effect of the end of
         * a block sequence, is communicated beyond the stream by
         * inserting a prompt sequence on the stream after the last
         * block.  This method makes sure that a final block ends with a
         * newline, if it doesn't already, in order to facilitate a
         * Reader that is possibly chained to this InputStream.
         * <p>
         * If the stream is not positioned correctly, hell will break
         * loose.
         */
        private int readBlock() throws IOException {
            // read next two bytes (short)
            if (!_read(blklen, 2))
                return -1;

            // Get the short-value and store its value in blockLen.
            blockLen = (short) ((blklen[0] & 0xFF) >> 1 | (blklen[1] & 0xFF) << 7);
            readPos = 0;

            // sanity check to avoid bad servers make us do an ugly stack trace
            if (blockLen > block.length)
                throw new AssertionError("Server sent a block larger than BLOCKsize: " +
                        blockLen + " > " + block.length);
            if (!_read(block, blockLen))
                return -1;

            // if this is the last block, make it end with a newline and prompt
            if ((blklen[0] & 0x1) == 1) {
                if (blockLen > 0 && block[blockLen - 1] != '\n') {
                    // to terminate the block in a Reader
                    block[blockLen++] = '\n';
                }
                // insert 'fake' flush
                block[blockLen++] = MapiConnection.PROMPT_CHAR;
                block[blockLen++] = '\n';
            }

            return blockLen;
        }

        public int read() throws IOException {
            if (available() == 0) {
                if (readBlock() == -1)
                    return -1;
            }
            return (int) block[readPos++];
        }

        public int read(ByteBuffer b) throws IOException {
            return read(b, 0, b.capacity());
        }

        public int read(ByteBuffer b, int off, int len) throws IOException {
            b.clear();
            int t;
            int size = 0;
            while (size < len) {
                t = available();
                if (t == 0) {
                    if (size != 0)
                        break;
                    if (readBlock() == -1) {
                        size = -1;
                        break;
                    }
                    t = available();
                }
                if (len > t) {
                    System.arraycopy(block, readPos, b.array(), off, t);
                    off += t;
                    len -= t;
                    readPos += t;
                    size += t;
                } else {
                    System.arraycopy(block, readPos, b.array(), off, len);
                    readPos += len;
                    size += len;
                    break;
                }
            }
            b.position(size);
            b.flip();
            return size;
        }

        public long skip(long n) throws IOException {
            long skip = n;
            int t = 0;
            while (skip > 0) {
                t = available();
                if (skip > t) {
                    skip -= t;
                    readPos += t;
                    readBlock();
                } else {
                    readPos += skip;
                    break;
                }
            }
            return n;
        }
    }

    class OldMapiBlockOutputStream {

        private final OutputStream outStream;

        private int writePos = 0;

        private byte[] block = new byte[BLOCK];

        private int blocksize = 0;

        /**
         * Constructs this BlockOutputStream, backed by the given OutputStream. A BufferedOutputStream is internally
         * used.
         */
        OldMapiBlockOutputStream(OutputStream out) {
            this.outStream = out;
        }

        void flush() throws IOException {
            // write the block (as final) then flush.
            writeBlock(true);
            outStream.flush();
        }

        /**
         * writeBlock puts the data in the block on the stream.  The
         * boolean last controls whether the block is sent with an
         * indicator to note it is the last block of a sequence or not.
         *
         * @param last whether this is the last block
         * @throws IOException if writing to the stream failed
         */
        void writeBlock(boolean last) throws IOException {
            if (last) {
                // always fits, because of BLOCK's size
                blocksize = (short) writePos;
                // this is the last block, so encode least
                // significant bit in the first byte (little-endian)
                blklen[0] = (byte) (blocksize << 1 & 0xFF | 1);
                blklen[1] = (byte) (blocksize >> 7);
            } else {
                // always fits, because of BLOCK's size
                blocksize = (short) BLOCK;
                // another block will follow, encode least
                // significant bit in the first byte (little-endian)
                blklen[0] = (byte) (blocksize << 1 & 0xFF);
                blklen[1] = (byte) (blocksize >> 7);
            }
            outStream.write(blklen);
            // write the actual block
            outStream.write(block, 0, writePos);
            writePos = 0;
        }

        void write(int b) throws IOException {
            if (writePos == BLOCK) {
                writeBlock(false);
            }
            block[writePos++] = (byte) b;
        }

        int write(ByteBuffer b) throws IOException {
            return write(b, 0, b.position());
        }

        int write(ByteBuffer b, int off, int len) throws IOException {
            int t, written = 0;
            b.flip();
            while (len > 0) {
                t = BLOCK - writePos;
                if (len > t) {
                    System.arraycopy(b.array(), off, block, writePos, t);
                    off += t;
                    len -= t;
                    writePos += t;
                    written += t;
                    writeBlock(false);
                } else {
                    System.arraycopy(b.array(), off, block, writePos, len);
                    writePos += len;
                    written += len;
                    break;
                }
            }
            b.clear();
            return written;
        }

        public void close() throws IOException {
            // we don't want the flush() method to be called (default of the FilterOutputStream), so we close manually
            // here
            outStream.close();
        }
    }
}
