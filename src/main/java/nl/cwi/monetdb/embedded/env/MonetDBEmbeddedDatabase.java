/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import java.util.HashSet;
import java.util.Set;

/**
 * An embedded version of a MonetDB database.
 * Communication between Java and native C is done via JNI.
 * <br/>
 * <strong>Note</strong>: You can have only one Embedded MonetDB database running per JVM process.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBEmbeddedDatabase {

    /**
     * Starts a MonetDB database on the given farm.
     *
     * @param dbDirectory The full path of the farm
     * @param silentFlag A boolean if silent mode will be turned on or not
     * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
     * @return A MonetDBEmbeddedDatabase instance
     * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
     */
    public static MonetDBEmbeddedDatabase StartDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag)
            throws MonetDBEmbeddedException {
        if(!MonetDBEmbeddedInstance.IsEmbeddedInstanceInitialized()) {
            throw new MonetDBEmbeddedException("The embedded instance has not been loaded yet!");
        } else {
            return StartDatabaseInternal(dbDirectory, silentFlag, sequentialFlag);
        }
    }

    /**
     * Starts a MonetDB database on the given farm asynchronously.
     *
     * @param dbDirectory The full path of the farm
     * @param silentFlag A boolean if silent mode will be turned on or not
     * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
     * @return A MonetDBEmbeddedDatabase instance
     * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
     */
    /*public static MonetDBEmbeddedDatabase StartDatabaseAsync(String dbDirectory, boolean silentFlag,
                                                             boolean sequentialFlag) throws MonetDBEmbeddedException {
        CompletableFuture.supplyAsync(() -> StartDatabase(dbDirectory, silentFlag, sequentialFlag));
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }*/

    private final String databaseDirectory;

    private final boolean silentFlag;

    private final boolean sequentialFlag;

    private boolean isRunning = true;

    private final Set<MonetDBEmbeddedConnection> connections = new HashSet<>();

    private MonetDBEmbeddedDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag) {
        this.databaseDirectory = dbDirectory;
        this.silentFlag = silentFlag;
        this.sequentialFlag = sequentialFlag;
    }

    /**
     * Get the database farm directory.
     *
     * @return A String representing the farm directory
     */
    public String getDatabaseDirectory() {
        return databaseDirectory;
    }

    /**
     * Check if the Silent Flag was set while creating the database.
     *
     * @return The Silent Flag
     */
    public boolean isSilentFlagSet() {
        return silentFlag;
    }

    /**
     * Check if the Sequential Flag was set while creating the database.
     *
     * @return The Sequential Flag
     */
    public boolean isSequentialFlagSet() {
        return sequentialFlag;
    }

    /**
     * Check if the database is still running or not.
     *
     * @return A boolean indicating if the database is running
     */
    public boolean isRunning() { return isRunning; }

    /**
     * Stops the database. All the pending connections will be shut down as well.
     *
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    public void stopDatabase() throws MonetDBEmbeddedException {
        if(this.isRunning) {
            for(MonetDBEmbeddedConnection mdbec : connections) {
                mdbec.closeConnectionImplementation();
            }
            this.connections.clear();
            this.stopDatabaseInternal();
            this.isRunning = false;
        } else {
            throw new MonetDBEmbeddedException("The database is not running!");
        }
    }

    /**
     * Stops the database asynchronously. All the pending connections will be shut down as well.
     *
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    /*public void stopDatabaseAsync() throws MonetDBEmbeddedException {
        CompletableFuture.supplyAsync(() -> this.stopDatabase());
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }*/

    /**
     * Creates a connection on the database, set on the default schema.
     *
     * @return A MonetDBEmbeddedConnection instance
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    public MonetDBEmbeddedConnection createConnection() throws MonetDBEmbeddedException {
        return this.createConnectionInternal();
    }

    /**
     * Creates a connection on the database, set on the default schema asynchronously.
     *
     * @return A MonetDBEmbeddedConnection instance
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    /*public MonetDBEmbeddedConnection createConnectionAsync() throws MonetDBEmbeddedException {
        CompletableFuture.supplyAsync(() -> this.createConnectionInternal());
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }*/

    /**
     * Removes a connection from this database.
     */
    protected void removeConnection(MonetDBEmbeddedConnection con) {
        this.connections.remove(con);
    }

    private static native MonetDBEmbeddedDatabase StartDatabaseInternal(String dbDirectory, boolean silentFlag,
                                                                        boolean sequentialFlag)
            throws MonetDBEmbeddedException;

    private native void stopDatabaseInternal();

    private native MonetDBEmbeddedConnection createConnectionInternal() throws MonetDBEmbeddedException;
}
