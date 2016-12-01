package nl.cwi.monetdb.mcl.connection;

import java.io.*;

/**
 * Created by ferreira on 12/1/16.
 */
public class Debugger implements Closeable {

    /** The Writer for the debug log-file */
    private final Writer log;

    /**
     * Enables logging to a stream what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param log to write the log to
     */
    public Debugger(Writer log) {
        this.log = log;
    }

    /**
     * Enables logging to a file what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param filename the name of the file to write to
     * @throws IOException if the file could not be opened for writing
     */
    public Debugger(String filename) throws IOException {
        this.log = new FileWriter(filename);
    }

    /**
     * Enables logging to a stream what is read and written from and to
     * the server.  Logging can be enabled at any time.  However, it is
     * encouraged to start debugging before actually connecting the
     * socket.
     *
     * @param out to write the log to
     */
    public Debugger(PrintStream out) {
        this.log = new PrintWriter(out);
    }

    /**
     * Writes a logline tagged with a timestamp using the given string.
     * Used for debugging purposes only and represents a message that is
     * connected to writing to the socket.  A logline might look like:
     * TX 152545124: Hello MonetDB!
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logTx(String message) throws IOException {
        log.write("TX " + System.currentTimeMillis() + ": " + message + "\n");
    }

    /**
     * Writes a logline tagged with a timestamp using the given string.
     * Lines written using this log method are tagged as "added
     * metadata" which is not strictly part of the data sent.
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logTd(String message) throws IOException {
        log.write("TD " + System.currentTimeMillis() + ": " + message + "\n");
    }

    /**
     * Writes a logline tagged with a timestamp using the given string,
     * and flushes afterwards.  Used for debugging purposes only and
     * represents a message that is connected to reading from the
     * socket.  The log is flushed after writing the line.  A logline
     * might look like:
     * RX 152545124: Hi JDBC!
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logRx(String message) throws IOException {
        log.write("RX " + System.currentTimeMillis() + ": " + message + "\n");
        log.flush();
    }

    /**
     * Writes a logline tagged with a timestamp using the given string,
     * and flushes afterwards.  Lines written using this log method are
     * tagged as "added metadata" which is not strictly part of the data
     * received.
     *
     * @param message the message to log
     * @throws IOException if an IO error occurs while writing to the logfile
     */
    private void logRd(String message) throws IOException {
        log.write("RD " + System.currentTimeMillis() + ": " + message + "\n");
        log.flush();
    }

    @Override
    public void close() throws IOException {
        if (log instanceof FileWriter) {
            log.close();
        }
    }
}
