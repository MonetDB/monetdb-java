/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.io.Closeable;

/**
 * The base class for a query result.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractStatementResult implements Closeable {

    /**
     * The corresponding connection of this result.
     */
    private final MonetDBEmbeddedConnection connection;

    /**
     * Pointer to the native result set.
     * We need to keep it around for getting columns.
     * The native result set is kept until the {@link super.close()} is called.
     */
    protected long resultPointer;

    protected AbstractStatementResult(MonetDBEmbeddedConnection connection, long resultPointer) {
        this.resultPointer = resultPointer;
        this.connection = connection;
    }

    /**
     * Get the corresponding connection to this statement result
     *
     * @return A MonetDBEmbeddedConnection instance
     */
    public MonetDBEmbeddedConnection getConnection() { return connection;}

    /**
     * Tells if the connection of this statement result has been closed or not
     *
     * @return A boolean indicating if the statement result has been cleaned or not
     */
    public boolean isStatementClosed() {
        return this.resultPointer == 0;
    }

    /**
     * Close the query data so no more new results can be retrieved
     */
    @Override
    public void close() {
        this.cleanupResult(this.resultPointer);
        this.resultPointer = 0;
        this.connection.removeQueryResult(this);
    }

    private native void cleanupResult(long resultPointer);
}
