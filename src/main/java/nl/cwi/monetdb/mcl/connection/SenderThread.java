/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection;

import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread to send a query to the server.  When sending large amounts of data to a server, the output buffer of the
 * underlying communication socket may overflow.  In such case the sending process blocks. In order to prevent deadlock,
 * it might be desirable that the driver as a whole does not block. This thread facilitates the prevention of such
 * 'full block', because this separate thread only will block.<br /> This thread is designed for reuse, as thread
 * creation costs are high.
 */
public class SenderThread extends Thread {

    /** The state WAIT represents this thread to be waiting for something to do */
    private static final int WAIT = 1;
    /** The state QUERY represents this thread to be executing a query */
    private static final int QUERY = 2;
    /** The state SHUTDOWN is the final state that ends this thread */
    private static final int SHUTDOWN = 3;

    private String[] templ;
    private String query;
    private AbstractProtocol protocol;
    private String error;
    private int state = SenderThread.WAIT;
    private final Lock sendLock = new ReentrantLock();
    private final Condition queryAvailable = sendLock.newCondition();
    private final Condition waiting = sendLock.newCondition();

    /**
     * Constructor which immediately starts this thread and sets it into daemon mode.
     *
     * @param out the socket to write to
     */
    public SenderThread(AbstractProtocol out) {
        super("SendThread");
        this.setDaemon(true);
        this.protocol = out;
        this.start();
    }

    @Override
    public void run() {
        this.sendLock.lock();
        try {
            while (true) {
                while (this.state == SenderThread.WAIT) {
                    try {
                        this.queryAvailable.await();
                    } catch (InterruptedException e) {
                        // woken up, eh?
                    }
                }
                if (this.state == SenderThread.SHUTDOWN)
                    break;

                // state is QUERY here
                try {
                    this.protocol.writeNextQuery((templ[0] == null ? "" : templ[0]), query,
                            (templ[1] == null ? "" : templ[1]));
                } catch (IOException e) {
                    this.error = e.getMessage();
                }

                // update our state, and notify, maybe someone is waiting
                // for us in throwErrors
                this.state = SenderThread.WAIT;
                this.waiting.signal();
            }
        } finally {
            this.sendLock.unlock();
        }
    }

    /**
     * Starts sending the given query over the given socket. Beware that the thread should be finished (can be assured
     * by calling throwErrors()) before this method is called!
     *
     * @param templ the query template
     * @param query the query itself
     * @throws SQLException if this SendThread is already in use
     */
    public void runQuery(String[] templ, String query) throws SQLException {
        this.sendLock.lock();
        try {
            if (this.state != SenderThread.WAIT) {
                throw new SQLException("Sender Thread already in use or shutting down!", "M0M03");
            }
            this.templ = templ;
            this.query = query;
            // let the thread know there is some work to do
            this.state = SenderThread.QUERY;
            this.queryAvailable.signal();
        } finally {
            this.sendLock.unlock();
        }
    }

    /**
     * Returns errors encountered during the sending process.
     *
     * @return the errors or null if none
     */
    public String getErrors() {
        this.sendLock.lock();
        try {
            // make sure the thread is in WAIT state, not QUERY
            while (this.state == SenderThread.QUERY) {
                try {
                    this.waiting.await();
                } catch (InterruptedException e) {
                    // just try again
                }
            }
            if (this.state == SenderThread.SHUTDOWN)
                this.error = "SendThread is shutting down";
        } finally {
            this.sendLock.unlock();
        }
        return error;
    }

    /**
     * Requests this SendThread to stop.
     */
    public void shutdown() {
        sendLock.lock();
        state = SenderThread.SHUTDOWN;
        sendLock.unlock();
        this.interrupt();  // break any wait conditions
    }
}
