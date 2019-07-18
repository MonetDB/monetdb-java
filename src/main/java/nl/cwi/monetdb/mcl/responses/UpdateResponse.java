/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

/**
 * The UpdateResponse represents an update statement response. It is issued on an UPDATE, INSERT or DELETE SQL
 * statement. This response keeps a count field that represents the affected rows and a field that contains the last
 * inserted auto-generated ID, or -1 if not applicable.
 * <br />
 * <tt>&amp;2 0 -1</tt>
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public class UpdateResponse implements IResponse {

	/**
	 * The id of the response before this one.
	 */
	private final int lastid;

	/**
	 * The number of rows affected by the update statement.
	 */
	private final int count;

	public UpdateResponse(int lastid, int count) {
		// fill the blank finals
		this.lastid = lastid;
		this.count = count;
	}

	/**
	 * The id of the response before this one.
	 *
	 * @return The id of the response before this one
	 */
	public int getLastid() {
		return lastid;
	}

	/**
	 * The number of rows affected by the update statement.
	 *
	 * @return The number of rows affected by the update statement
	 */
	public int getCount() {
		return count;
	}

	@Override
	public void close() {
		// nothing to do here...
	}
}
