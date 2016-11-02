/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

/**
 * The result set from a sendUpdate method from a connection.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class UpdateResultSet extends AbstractStatementResult {

    protected UpdateResultSet(MonetDBEmbeddedConnection connection) {
        super(connection);
    }
}
