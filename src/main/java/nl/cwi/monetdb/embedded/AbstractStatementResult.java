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

    protected AbstractStatementResult(MonetDBEmbeddedConnection connection) {
        this.connection = connection;
    }

    /**
     * Get the corresponding connection to this statement result.
     *
     * @return A MonetDBEmbeddedConnection instance
     */
    public MonetDBEmbeddedConnection getConnection() { return connection; }

    /**
     * To be called by the connection when is closing, to avoid concurrency problems on the iteration.
     */
    protected abstract void closeImplementation();

    @Override
    public void close() {
        this.closeImplementation();
        this.connection.removeQueryResult(this);
    }
}
