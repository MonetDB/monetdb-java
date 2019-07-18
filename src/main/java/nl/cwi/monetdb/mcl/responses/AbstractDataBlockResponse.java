/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.jdbc.MonetResultSet;
import nl.cwi.monetdb.mcl.connection.helpers.GregorianCalendarParser;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * The DataBlockResponse is tabular data belonging to a ResultSetResponse. On a MAPI connection, tabular data from the
 * server typically looks like:
 * <pre>
 * [ "value",	56	]
 * </pre>
 * where each column is separated by ",\t" and each tuple surrounded by brackets ("[" and "]"). A DataBlockResponse
 * object holds the raw data as read from the server, in a parsed manner, ready for easy retrieval. Meanwhile on an
 * Embedded connection, the data is automatically parsed.
 *
 * This object is not intended to be queried by multiple threads synchronously. It is designed to work for one thread
 * retrieving rows from it. When multiple threads will retrieve rows from this object, it is possible for threads to
 * get the same data.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public abstract class AbstractDataBlockResponse implements IIncompleteResponse {

	/** The connection protocol to parse the tuple lines */
	public final AbstractProtocol protocol;
	/** A 'pointer' to the current line */
	public int blockLine;
	/** The number of rows in the block */
	public final int rowcount;
	/** The JdbcSQLTypes mapping */
	public final int[] jdbcSQLTypes;
	/** The types mapping */
	public final String[] types;
	/** whether the last read field (via some getXyz() method) was NULL */
	public boolean lastReadWasNull = true;

	/**
	 * Constructs an AbstractDataBlockResponse object.
	 *
	 * @param rowcount the number of rows
	 * @param protocol the underlying protocol
	 * @param JdbcSQLTypes an array of the JDBC mappings of the columns
	 */
	public AbstractDataBlockResponse(int rowcount, AbstractProtocol protocol, int[] JdbcSQLTypes, String[] types) {
		this.rowcount = rowcount;
		this.protocol = protocol;
		this.jdbcSQLTypes = JdbcSQLTypes;
		this.types = types;
	}

	/**
	 * Returns whether this Response expects more lines to be added to it.
	 *
	 * @return true if a next line should be added, false otherwise
	 */
	@Override
	public abstract boolean wantsMore();

	/**
	 * addLines adds a batch of rows to the block. Before adding the first line, the column arrays are allocated.
	 *
	 * @param protocol The connection's protocol to fetch data from
	 * @throws ProtocolException If the result line is not expected
	 */
	@Override
	public abstract void addLines(AbstractProtocol protocol) throws ProtocolException;

	/* Methods to be called after the block construction has been completed */

	/**
	 * Sets the current line number on the block.
	 *
	 * @param blockLine the block line number
	 */
	void setBlockLine(int blockLine) {
		this.blockLine = blockLine;
	}

	/**
	 * Returns if the last value read was null or not.
	 *
	 * @return If the last value read was null or not
	 */
	public boolean isLastReadWasNull() {
		return lastReadWasNull;
	}

	/**
	 * Gets the current row value as a Java Boolean.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Boolean if the column is a boolean, otherwise a ClassCastException is thrown
	 */
	public abstract boolean getBooleanValue(int column);

	/**
	 * Gets the current row value as a Java Byte.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Byte if the column is a tinyint, otherwise a ClassCastException is thrown
	 */
	public abstract byte getByteValue(int column);

	/**
	 * Gets the current row value as a Java Short.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Short if the column is a smallint, otherwise a ClassCastException is thrown
	 */
	public abstract short getShortValue(int column);

	/**
	 * Gets the current row value as a Java Integer.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Integer if the column is an integer or month_interval, otherwise a ClassCastException is thrown
	 */
	public abstract int getIntValue(int column);

	/**
	 * Gets the current row value as a Java Long.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Long if the column is a bigint or second_interval, otherwise a ClassCastException is thrown
	 */
	public abstract long getLongValue(int column);

	/**
	 * Gets the current row value as a Java Float.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Float if the column is a real, otherwise a ClassCastException is thrown
	 */
	public abstract float getFloatValue(int column);

	/**
	 * Gets the current row value as a Java Double.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Double if the column is a double, otherwise a ClassCastException is thrown
	 */
	public abstract double getDoubleValue(int column);

	/**
	 * Gets the current row value as a Java Object.
	 *
	 * @param column The column index starting from 0
	 * @return A Java Object if the column is not a primitive type, otherwise a ClassCastException is thrown
	 */
	public abstract Object getObjectValue(int column) throws ProtocolException;

	/**
	 * Gets the current row value as a Java String.
	 *
	 * @param column The column index starting from 0
	 * @return The String representation of the data type
	 */
	public abstract String getValueAsString(int column) throws ProtocolException;

	/**
	 * Gets the current row value as a Java Object.
	 *
	 * @param column The column index starting from 0
	 * @return The Object representation of the data type
	 */
	public abstract Object getValueAsObject(int column) throws ProtocolException;

	/**
	 * To parse a String column in a date, time or timestamp instance, this method is used.
	 *
	 * @param mrs A MonetResultSet instance where warning can be added
	 * @param column The column index starting from 0
	 * @param jdbcType The JDBC type of the column desired to convert
	 * @return A {@link Calendar} instance of the parsed date
	 * @throws SQLException If the conversation cannot be performed
	 */
	public Calendar getDateValueFromString(MonetResultSet mrs, int column, int jdbcType) throws SQLException {
		try {
			String value = this.getObjectValue(column).toString();
			SimpleDateFormat aux;
			switch (jdbcType) {
				case Types.DATE:
					aux = protocol.getMonetDate();
					break;
				case Types.TIME:
				case 2013: //Types.TIME_WITH_TIMEZONE:
					aux = protocol.getMonetTimePrinter();
					break;
				case Types.TIMESTAMP:
				case 2014: //Types.TIMESTAMP_WITH_TIMEZONE:
					aux = protocol.getMonetTimestampPrinter();
					break;
				default:
					throw new SQLException("Internal error!", "M1M05");
			}
			return GregorianCalendarParser.parseDateString(mrs, value, protocol.getMonetParserPosition(), aux, jdbcType);
		} catch (ProtocolException e) {
			throw new SQLException(e);
		}
	}

	/**
	 * Get the last parsed nanoseconds value.
	 *
	 * @return The last parsed nanoseconds value
	 */
	public abstract int getLastNanos();
}
