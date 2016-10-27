/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Embedded version of MonetDB.
 * Communication between Java and native C is done via JNI.
 * <br/>
 * <strong>Note</strong>: You can have only one nl.cwi.monetdb.embedded MonetDB database running per JVM process.
 */
public class MonetDBEmbeddedDatabase {

    public static MonetDBEmbeddedDatabase StartDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag) throws SQLException {
        if(MonetDBEmbeddedInstance.IsEmbeddedInstanceInitialized() == false) {
            throw new SQLException("The embedded instance has not been loaded yet!");
        } else {
            return StartDatabaseInternal(dbDirectory, silentFlag, sequentialFlag);
        }
    }

    private static native MonetDBEmbeddedDatabase StartDatabaseInternal(String dbDirectory, boolean silentFlag, boolean sequentialFlag) throws SQLException;

    private final File databaseDirectory;

    private final boolean silentFlag;

    private final boolean sequentialFlag;

    private boolean isRunning;

    private final List<MonetDBEmbeddedConnection> connections = new ArrayList<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MonetDBEmbeddedDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag, boolean isRunning) {
        this.databaseDirectory = new File(dbDirectory);
        this.silentFlag = silentFlag;
        this.sequentialFlag = sequentialFlag;
        this.isRunning = isRunning;
    }

    public File getDatabaseDirectory() {
        return databaseDirectory;
    }

    public boolean isSilentFlagSet() {
        return silentFlag;
    }

    public boolean isSequentialFlagSet() {
        return sequentialFlag;
    }

    public boolean isRunning() {
        boolean result;
        lock.readLock().lock();
        result = isRunning;
        lock.readLock().unlock();
        return result;
    }

    private void setRunning(boolean running) {
        lock.writeLock().lock();
        isRunning = running;
        lock.writeLock().unlock();
    }

    public void stopDatabase() throws SQLException {
        lock.writeLock().lock();
        try {
            if(this.isRunning) {
                for(MonetDBEmbeddedConnection mdbec : connections) {
                    this.shutdownConnection(mdbec);
                }
                this.connections.clear();
                this.stopDatabaseInternal();
                this.isRunning = false;
            } else {
                throw new SQLException("The database is not running!");
            }
        } catch (SQLException ex) {
            lock.writeLock().unlock();
            throw ex;
        }
        lock.writeLock().unlock();
    }

    private native void stopDatabaseInternal();

    public MonetDBEmbeddedConnection createConnection() throws SQLException {
        MonetDBEmbeddedConnection mdbec;
        lock.writeLock().lock();
        try {
            if(this.isRunning) {
                mdbec = this.createConnectionInternal();
                connections.add(mdbec);
                lock.writeLock().unlock();
                return mdbec;
            } else {
                throw new SQLException("The database is not running!");
            }
        } catch (SQLException ex) {
            lock.writeLock().unlock();
            throw ex;
        }
    }

    private native MonetDBEmbeddedConnection createConnectionInternal() throws SQLException;

    public void shutdownConnection(MonetDBEmbeddedConnection mdbec) throws SQLException {
        lock.writeLock().lock();
        try {
            if(this.isRunning) {
                this.shutdownConnectionInternal(mdbec.getConnectionPointer());
                this.connections.remove(mdbec);
            } else {
                throw new SQLException("The database is not running!");
            }
        } catch (SQLException ex) {
            lock.writeLock().unlock();
            throw ex;
        }
    }

    private native void shutdownConnectionInternal(long connectionPointer);
}
