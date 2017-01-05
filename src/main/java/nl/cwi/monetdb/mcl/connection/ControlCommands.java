/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection;

public enum ControlCommands {

    /** Send autocommit statement */
    AUTO_COMMIT,
    /** Set reply size for the server */
    REPLY_SIZE,
    /** Release a prepared statement data */
    RELEASE,
    /** Close a query */
    CLOSE
}
