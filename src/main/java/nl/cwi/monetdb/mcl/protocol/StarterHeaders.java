/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

/**
 * This class lists the possible responses of a query by the server. Notice that Q_PARSE is not used by neither a MAPI
 * or an embedded connection, so it's here for completeness.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class StarterHeaders {

	private StarterHeaders() {}

	/* Please don't change the order or the values */

	/** A parse response (not handled) */
	public static final int Q_PARSE = 0;
	/** A tabular response (typical ResultSet) */
	public static final int Q_TABLE = 1;
	/** A response to an update statement, contains number of affected rows and generated key-id */
	public static final int Q_UPDATE = 2;
	/** A response to a schema update */
	public static final int Q_SCHEMA = 3;
	/** A response to a transaction statement (start, rollback, abort, commit) */
	public static final int Q_TRANS = 4;
	/** A tabular response in response to a PREPARE statement containing information about the wildcard values that
	 * need to be supplied */
	public static final int Q_PREPARE = 5;
	/** A tabular continuation response (for a ResultSet) */
	public static final int Q_BLOCK = 6;
	/** An unknown and unsupported response */
	public static final int Q_UNKNOWN = 7;
}
