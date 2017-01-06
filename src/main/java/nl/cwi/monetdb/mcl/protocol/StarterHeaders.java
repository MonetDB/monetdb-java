/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

/**
 * This enum lists the possible responses of a query by the server. Notice that Q_PARSE is not used by neither a MAPI or
 * embedded connection, so it's here for completeness.
 */
public enum StarterHeaders {

    /* Please don't change the order */

    /** A parse response (not handled) */
    Q_PARSE,
    /** A tabular response (typical ResultSet) */
    Q_TABLE,
    /** A response to an update statement, contains number of affected rows and generated key-id */
    Q_UPDATE,
    /** A response to a schema update */
    Q_SCHEMA,
    /** A response to a transaction statement (start, rollback, abort, commit) */
    Q_TRANS,
    /** A tabular response in response to a PREPARE statement containing information about the wildcard values that
     * need to be supplied */
    Q_PREPARE,
    /** A tabular continuation response (for a ResultSet) */
    Q_BLOCK,
    /** An unknown and unsupported response */
    Q_UNKNOWN
}