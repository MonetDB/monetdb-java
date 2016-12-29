/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import java.util.concurrent.ConcurrentHashMap;

/**
 * An embedded version of a MonetDB database.
 * Communication between Java and native C is done via JNI. The MonetDB's JNI library must be successfully loaded in
 * order to the other methods work.
 * <br/>
 * <strong>Note</strong>: You can have only one Embedded MonetDB database running per JVM process.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class MonetDBEmbeddedDatabase {

    private static MonetDBEmbeddedDatabase MonetDBEmbeddedDatabase = null;

    /**
     * Check if the database is still running or not.
     *
     * @return A boolean indicating if the database is running
     */
    public static boolean IsDatabaseRunning() { return MonetDBEmbeddedDatabase != null; }

    /**
     * Starts a MonetDB database on the given farm.
     *
     * @param dbDirectory The full path of the farm
     * @param silentFlag A boolean if silent mode will be turned on or not
     * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
     * @return Returns true if the load was successful.
     * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
     */
    public static boolean StartDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag)
            throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase != null) {
            throw new MonetDBEmbeddedException("The database is still running!");
        } else {
            System.loadLibrary("monetdb5");
            MonetDBEmbeddedDatabase = StartDatabaseInternal(dbDirectory, silentFlag, sequentialFlag);
        }
        return true;
    }

    /**
     * Starts a MonetDB database on the given farm asynchronously.
     *
     * @param dbDirectory The full path of the farm
     * @param silentFlag A boolean if silent mode will be turned on or not
     * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
     * @return Returns true if the load was successful
     * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
     */
    /*public static CompletableFuture<MonetDBEmbeddedDatabase> StartDatabaseAsync(String dbDirectory, boolean silentFlag,
                                                             boolean sequentialFlag) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> StartDatabase(dbDirectory, silentFlag, sequentialFlag));
    }*/

    /**
     * Get the database farm directory.
     *
     * @throws MonetDBEmbeddedException If the database is not running
     * @return A String representing the farm directory
     */
    public static String GetDatabaseDirectory() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        }
        return MonetDBEmbeddedDatabase.databaseDirectory;
    }

    /**
     * Check if the Silent Flag was set while creating the database.
     *
     * @throws MonetDBEmbeddedException If the database is not running
     * @return The Silent Flag
     */
    public static boolean IsSilentFlagSet() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        }
        return MonetDBEmbeddedDatabase.silentFlag;
    }

    /**
     * Check if the Sequential Flag was set while creating the database.
     *
     * @throws MonetDBEmbeddedException If the database is not running
     * @return The Sequential Flag
     */
    public static boolean IsSequentialFlagSet() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        }
        return MonetDBEmbeddedDatabase.sequentialFlag;
    }

    /**
     * Stops the database. All the pending connections will be shut down as well.
     *
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    public static void StopDatabase() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        } else {
            for(MonetDBEmbeddedConnection mdbec : MonetDBEmbeddedDatabase.connections.values()) {
                mdbec.closeConnectionImplementation();
            }
            MonetDBEmbeddedDatabase.connections.clear();
            MonetDBEmbeddedDatabase.stopDatabaseInternal();
            MonetDBEmbeddedDatabase = null;
        }
    }

    /*
     * Stops the database asynchronously. All the pending connections will be shut down as well.
     *
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    /*public static CompletableFuture<Void> StopDatabaseAsync() throws MonetDBEmbeddedException {
        return CompletableFuture.runAsync(() -> this.stopDatabase());
    }*/

    private final String databaseDirectory;

    private final boolean silentFlag;

    private final boolean sequentialFlag;

    private final ConcurrentHashMap<Long, MonetDBEmbeddedConnection> connections = new ConcurrentHashMap<>();

    private MonetDBEmbeddedDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag) {
        this.databaseDirectory = dbDirectory;
        this.silentFlag = silentFlag;
        this.sequentialFlag = sequentialFlag;
    }

    /**
     * Creates a connection on the database, set on the default schema.
     *
     * @return A MonetDBEmbeddedConnection instance
     * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
     */
    public static MonetDBEmbeddedConnection CreateConnection() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        } else {
            MonetDBEmbeddedConnection con = MonetDBEmbeddedDatabase.createConnectionInternal();
            MonetDBEmbeddedDatabase.connections.put(con.getConnectionPointer(), con);
            return con;
        }
    }

    public static JDBCEmbeddedConnection CreateJDBCEmbeddedConnection() throws MonetDBEmbeddedException {
        if(MonetDBEmbeddedDatabase == null) {
            throw new MonetDBEmbeddedException("The database is not running!");
        } else {
            JDBCEmbeddedConnection con = MonetDBEmbeddedDatabase.createJDBCEmbeddedConnectionInternal();
            MonetDBEmbeddedDatabase.connections.put(con.getConnectionPointer(), con);
            return con;
        }
    }

    /**
     * Removes a connection from this database.
     */
    static void RemoveConnection(MonetDBEmbeddedConnection con) {
        MonetDBEmbeddedDatabase.connections.remove(con.getConnectionPointer());
    }

    /**
     * Internal implementation to start a database.
     */
    private static native MonetDBEmbeddedDatabase StartDatabaseInternal(String dbDirectory, boolean silentFlag,
                                                                        boolean sequentialFlag)
            throws MonetDBEmbeddedException;

    /**
     * Internal implementation to stop a database.
     */
    private native void stopDatabaseInternal();

    /**
     * Internal implementation to create a connection on this database.
     */
    private native MonetDBEmbeddedConnection createConnectionInternal() throws MonetDBEmbeddedException;

    /**
     * Internal implementation to create a JDBC embeddded connection on this database.
     */
    private native JDBCEmbeddedConnection createJDBCEmbeddedConnectionInternal() throws MonetDBEmbeddedException;
}
