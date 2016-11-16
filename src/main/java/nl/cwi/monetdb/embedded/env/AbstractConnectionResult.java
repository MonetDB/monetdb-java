/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import java.io.Closeable;

/**
 * The base class for a pending statement to a connection.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractConnectionResult implements Closeable {

    /**
     * The corresponding connection of this result.
     */
    private final MonetDBEmbeddedConnection connection;

    protected AbstractConnectionResult(MonetDBEmbeddedConnection connection) { this.connection = connection; }

    /**
     * Get the corresponding connection to this statement result.
     *
     * @return A MonetDBEmbeddedConnection instance
     */
    public MonetDBEmbeddedConnection getConnection() { return connection; }

    protected long getConnectionPointer() { return connection.connectionPointer; }

    /**
     * To be called by the connection when is closing.
     */
    protected abstract void closeImplementation();

    @Override
    public void close() {
        this.closeImplementation();
        this.connection.removeQueryResult(this);
    }
}
