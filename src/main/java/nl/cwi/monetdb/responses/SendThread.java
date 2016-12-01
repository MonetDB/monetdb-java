package nl.cwi.monetdb.responses;

import nl.cwi.monetdb.mcl.io.AbstractMCLWriter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread to send a query to the server.  When sending large
 * amounts of data to a server, the output buffer of the underlying
 * communication socket may overflow.  In such case the sending
 * process blocks.  In order to prevent deadlock, it might be
 * desirable that the driver as a whole does not block.  This thread
 * facilitates the prevention of such 'full block', because this
 * separate thread only will block.<br />
 * This thread is designed for reuse, as thread creation costs are
 * high.
 */
public class SendThread extends Thread {
    /** The state WAIT represents this thread to be waiting for
     *  something to do */
    private final static int WAIT = 0;
    /** The state QUERY represents this thread to be executing a query */
    private final static int QUERY = 1;
    /** The state SHUTDOWN is the final state that ends this thread */
    private final static int SHUTDOWN = -1;

    private String[] templ;
    private String query;
    private AbstractMCLWriter out;
    private String error;
    private int state = WAIT;

    private final Lock sendLock = new ReentrantLock();
    private final Condition queryAvailable = sendLock.newCondition();
    private final Condition waiting = sendLock.newCondition();

    /**
     * Constructor which immediately starts this thread and sets it
     * into daemon mode.
     *
     * @param out the socket to write to
     */
    public SendThread(AbstractMCLWriter out) {
        super("SendThread");
        this.setDaemon(true);
        this.out = out;
        this.start();
    }

    @Override
    public void run() {
        sendLock.lock();
        try {
            while (true) {
                while (state == WAIT) {
                    try {
                        queryAvailable.await();
                    } catch (InterruptedException e) {
                        // woken up, eh?
                    }
                }
                if (state == SHUTDOWN)
                    break;

                // state is QUERY here
                try {
                    out.writeLine((templ[0] == null ? "" : templ[0]) + query + (templ[1] == null ? "" : templ[1]));
                } catch (IOException e) {
                    error = e.getMessage();
                }

                // update our state, and notify, maybe someone is waiting
                // for us in throwErrors
                state = WAIT;
                waiting.signal();
            }
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * Starts sending the given query over the given socket.  Beware
     * that the thread should be finished (can be assured by calling
     * throwErrors()) before this method is called!
     *
     * @param templ the query template
     * @param query the query itself
     * @throws SQLException if this SendThread is already in use
     */
    public void runQuery(String[] templ, String query) throws SQLException {
        sendLock.lock();
        try {
            if (state != WAIT)
                throw new SQLException("SendThread already in use or shutting down!", "M0M03");

            this.templ = templ;
            this.query = query;

            // let the thread know there is some work to do
            state = QUERY;
            queryAvailable.signal();
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * Returns errors encountered during the sending process.
     *
     * @return the errors or null if none
     */
    public String getErrors() {
        sendLock.lock();
        try {
            // make sure the thread is in WAIT state, not QUERY
            while (state == QUERY) {
                try {
                    waiting.await();
                } catch (InterruptedException e) {
                    // just try again
                }
            }
            if (state == SHUTDOWN)
                error = "SendThread is shutting down";
        } finally {
            sendLock.unlock();
        }
        return error;
    }

    /**
     * Requests this SendThread to stop.
     */
    public void shutdown() {
        sendLock.lock();
        state = SHUTDOWN;
        sendLock.unlock();
        this.interrupt();  // break any wait conditions
    }
}
