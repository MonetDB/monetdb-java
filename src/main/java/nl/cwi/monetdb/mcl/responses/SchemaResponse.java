/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import java.sql.Statement;

/**
 * The SchemaResponse represents an schema modification response. It is issued on statements like CREATE, DROP or
 * ALTER TABLE. This response keeps a field that represents the success state, as defined by JDBC, which is currently in
 * MonetDB's case always SUCCESS_NO_INFO. Note that this state is not sent by the server.
 * <br />
 * <tt>&amp;3</tt>
 *
 * @author Fabian Groffen
 */
public class SchemaResponse implements IResponse {

	public int getState() {
		return Statement.SUCCESS_NO_INFO;
	}

	@Override
	public void close() {
		// nothing to do here...
	}
}
