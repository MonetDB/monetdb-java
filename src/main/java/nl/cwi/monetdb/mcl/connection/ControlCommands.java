/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection;

/**
 * The listening of the MonetDB's control commands sent by the client during a JDBC connection.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class ControlCommands {

	/* Please don't change the order or the values */

	/** Send autocommit statement */
	public static final int AUTO_COMMIT = 1;
	/** Set reply size for the server (for the maxrows specification) */
	public static final int REPLY_SIZE = 2;
	/** Release a prepared statement data */
	public static final int RELEASE = 3;
	/** Close a query */
	public static final int CLOSE = 4;
}
