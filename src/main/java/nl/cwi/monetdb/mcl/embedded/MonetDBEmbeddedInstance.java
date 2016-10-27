/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded;

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
public class MonetDBEmbeddedInstance {

    private final static String NATIVE_LIB_PATH_IN_JAR = "src" + File.separatorChar + "main" +
            File.separatorChar + "resources";
    private final static String NATIVE_LIB_NAME = "libmonetdb5.so";

    /**
     * The native nl.cwi.monetdb.embedded MonetDB library.
     */
    static {
        try {
            // Try load the nl.cwi.monetdb.embedded library
            System.loadLibrary("monetdb5");
        } catch (UnsatisfiedLinkError e) {
            // Still no, then get the resources.lib bundled in the jar
            loadLibFromJar(NATIVE_LIB_NAME);
        }
    }

    private static void loadLibFromJar(String fileName) {
        String pathToLib = NATIVE_LIB_PATH_IN_JAR + File.separatorChar + fileName;
        try {
            InputStream in = MonetDBEmbeddedInstance.class.getResourceAsStream(File.separatorChar + pathToLib);
            if (in == null) {
                // OK, the input stream is null, hence no .jar
                // This was probably a test and/or in an IDE
                // Just read the files from the src/main/resources dir
                in = new FileInputStream(new File(pathToLib));
            }
            // Set a temp location to extract (and load from later)
            final Path tempLibsDir = Files.createTempDirectory("nl.cwi.monetdb.embedded");
            File fileOut = new File(tempLibsDir.toString() + File.separatorChar + fileName);
            try (OutputStream out = new FileOutputStream(fileOut)) {
                byte[] buffer = new byte[in.available()];
                while (in.read(buffer) != -1) {
                    out.write(buffer);
                }
                out.flush();
                in.close();
                // Load the resources.lib from the extracted file
                System.load(fileOut.toString());
            }
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Unable to extract native library from JAR:" + e.getMessage());
        }
    }

    public static native MonetDBEmbeddedInstance StartDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag) throws SQLException;

    private final File databaseDirectory;

    private final boolean silentFlag;

    private final boolean sequentialFlag;

    private boolean isRunning;

    private final List<MonetDBEmbeddedConnection> connections = new ArrayList<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MonetDBEmbeddedInstance(String dbDirectory, boolean silentFlag, boolean sequentialFlag, boolean isRunning) {
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
