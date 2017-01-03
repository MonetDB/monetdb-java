/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

/**
 * The AutoCommitResponse represents a transaction message.  It
 * stores (a change in) the server side auto commit mode.<br />
 * <tt>&amp;4 (t|f)</tt>
 */
public class AutoCommitResponse extends SchemaResponse {

    private final boolean autocommit;

    public AutoCommitResponse(boolean ac) {
        // fill the blank final
        this.autocommit = ac;
    }

    public boolean isAutocommit() {
        return autocommit;
    }
}
