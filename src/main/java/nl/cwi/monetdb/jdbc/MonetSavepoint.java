/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The representation of a savepoint, which is a point within the current
 * transaction that can be referenced from the Connection.rollback method.
 * When a transaction is rolled back to a savepoint all changes made after
 * that savepoint are undone.
 * Savepoints can be either named or unnamed. Unnamed savepoints are
 * identified by an ID generated by the underlying data source.
 *
 * This little class is nothing more than a container for a name and/or
 * an id. Each instance of this class always has an id, which is used for
 * internal representation of the save point.
 *
 * Because the IDs which get generated are a logical sequence, application
 * wide, two concurrent transactions are guaranteed to not to have the same
 * save point identifiers. In this implementation the validaty of save points
 * is determined by the server, which makes this a light implementation.
 *
 * @author Fabian Groffen
 * @version 1.0
 */
public class MonetSavepoint implements Savepoint {
	/** The id of the last created Savepoint */
	private static AtomicInteger highestId = new AtomicInteger(0);

	/** The name of this Savepoint */
	private final String name;
	/** The id of this Savepoint */
	private final int id;

	public MonetSavepoint(String name) throws IllegalArgumentException {
		if (name == null)
		    throw new IllegalArgumentException("Null name not allowed");
		this.id = getNextId();
		this.name = name;
	}

	public MonetSavepoint() {
		this.id = getNextId();
		this.name = null;
	}

	/**
	 * Retrieves the generated ID for the savepoint that this Savepoint object represents.
	 *
	 * @return the numeric ID of this savepoint
	 * @throws SQLException if this is a named savepoint
	 */
	@Override
	public int getSavepointId() throws SQLException {
		if (name != null)
			throw new SQLException("Cannot getID for named savepoint", "3B000");
		return getId();
	}

	/**
	 * Retrieves the name of the savepoint that this Savepoint object represents.
	 *
	 * @return the name of this savepoint
	 * @throws SQLException if this is an un-named savepoint
	 */
	@Override
	public String getSavepointName() throws SQLException {
		if (name == null)
			throw new SQLException("Unable to retrieve name of unnamed savepoint", "3B000");
		return name;
	}

	//== end of methods from Savepoint interface

	/**
	 * Retrieves the savepoint id, like the getSavepointId method with the only difference that this method will always
	 * return the id, regardless of whether it is named or not.
	 *
	 * @return the numeric ID of this savepoint
	 */
	int getId() {
		return id;
	}

	/**
	 * Returns the name to use when referencing this save point to the MonetDB database. The returned value is
	 * guaranteed to be unique and consists of a prefix string and a sequence number.
	 *
	 * @return the unique savepoint name
	 */
	String getName() {
		return "MonetDBSP" + id;
	}


	/**
	 * Returns the next id, which is larger than the last returned id. This
	 * method is synchronized to prevent race conditions. Since this is static
	 * code, this method is shared by requests from multiple Connection objects.
	 * Therefore two successive calls to this method need not to have a
	 * difference of 1.
	 *
	 * @return the next int which is guaranteed to be higher than the one at the last call to this method.
	 */
	private static int getNextId() {
		return highestId.incrementAndGet();
	}

	/**
	 * Returns the highest id returned by getNextId(). This method is also
	 * synchronized to prevent race conditions and thus guaranteed to be
	 * thread-safe.
	 *
	 * @return the highest id returned by a call to getNextId()
	 */
	private static int getHighestId() {
		return highestId.get();
	}
}
