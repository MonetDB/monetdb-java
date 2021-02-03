/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.jdbc;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.CharBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLOutput;
import java.sql.SQLType;	// new as of Java 1.8
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@link PreparedStatement} suitable for the MonetDB database.
 *
 * This implementation of the PreparedStatement interface uses the
 * capabilities of the MonetDB/SQL backend to prepare and execute
 * statements.  The backend takes care of finding the '?'s in the input and
 * returns the types it expects for them.
 *
 * An example of a server response on a prepare query is:
 * <pre>
 * % prepare select name from tables where id &gt; ? and id &lt; ?;
 * &amp;5 0 2 3 2
 * # prepare,      prepare,        prepare # table_name
 * # type, digits, scale # name
 * # varchar,      int,    int # type
 * # 0,    0,      0 # length
 * [ "int",        9,      0       ]
 * [ "int",        9,      0       ]
 * </pre>
 *
 * @author Fabian Groffen
 * @author Martin van Dinther
 * @version 0.6
 */
public class MonetPreparedStatement
	extends MonetStatement
	implements PreparedStatement, AutoCloseable
{
	private final String[] monetdbType;
	private final int[] javaType;
	private final int[] digits;
	private final int[] scale;
	private final String[] schema;
	private final String[] table;
	private final String[] column;
	private final int id;
	private final int size;
	private final int rscolcnt;

	private final String[] values;

	/* placeholders for date/time pattern formats created once (only when needed), used multiple times */
	/** Format of a timestamp with RFC822 time zone */
	private SimpleDateFormat mTimestampZ;
	/** Format of a timestamp */
	private SimpleDateFormat mTimestamp;
	/** Format of a time with RFC822 time zone */
	private SimpleDateFormat mTimeZ;
	/** Format of a time */
	private SimpleDateFormat mTime;
	/** Format of a date used by mserver */
	private SimpleDateFormat mDate;

	/**
	 * MonetPreparedStatement constructor which checks the arguments for
	 * validity.  A MonetPreparedStatement is backed by a
	 * {@link MonetStatement}, which deals with most of the required stuff of
	 * this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of {@link ResultSet} to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @param prepareQuery the query string to prepare
	 * @throws SQLException if an error occurs during login
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetPreparedStatement(
			final MonetConnection connection,
			final int resultSetType,
			final int resultSetConcurrency,
			final int resultSetHoldability,
			final String prepareQuery)
		throws SQLException, IllegalArgumentException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability
		);

		if (!super.execute("PREPARE " + prepareQuery))
			throw new SQLException("Unexpected server response", "M0M10");

		// cheat a bit to get the ID and the number of columns
		id = ((MonetConnection.ResultSetResponse)header).id;
		size = (int)((MonetConnection.ResultSetResponse)header).tuplecount;
		rscolcnt = ((MonetConnection.ResultSetResponse)header).columncount;

		// initialise blank finals
		monetdbType = new String[size];
		javaType = new int[size];
		digits = new int[size];
		scale = new int[size];
		schema = new String[size];
		table = new String[size];
		column = new String[size];
		values = new String[size];

		// fill the arrays
		final ResultSet rs = super.getResultSet();
		if (rs != null) {
			// System.out.println("After super.getResultSet();");
			final int type_colnr = rs.findColumn("type");
			final int digits_colnr = rs.findColumn("digits");
			final int scale_colnr = rs.findColumn("scale");
			final int schema_colnr = rs.findColumn("schema");
			final int table_colnr = rs.findColumn("table");
			final int column_colnr = rs.findColumn("column");
			for (int i = 0; rs.next(); i++) {
				monetdbType[i] = rs.getString(type_colnr);
				javaType[i] = MonetDriver.getJdbcSQLType(monetdbType[i]);
				if (javaType[i] == Types.CLOB) {
					if (connection.mapClobAsVarChar())
						javaType[i] = Types.VARCHAR;
				} else
				if (javaType[i] == Types.BLOB) {
					if (connection.mapBlobAsVarBinary())
						javaType[i] = Types.VARBINARY;
				}
				digits[i] = rs.getInt(digits_colnr);
				scale[i] = rs.getInt(scale_colnr);
				if (rscolcnt == 3)
					continue;
				schema[i] = rs.getString(schema_colnr);
				table[i] = rs.getString(table_colnr);
				column[i] = rs.getString(column_colnr);
				/* when column[i] != null it is a result column of the prepared query, see getColumnIdx(int),
				   when column[i] == null it is a parameter for the prepared statement, see getParamIdx(int). */
				// System.out.println("column " + i + " has value: " + column[i]);
			}
			rs.close();
		}

		// PreparedStatements are by default poolable
		poolable = true;
	}

	/**
	 * Constructs an empty MonetPreparedStatement.  This constructor is
	 * in particular useful for extensions of this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of ResultSet to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @throws SQLException if an error occurs during login
	 */
	/* Disabled this constructor code as it is not part of the JDBC interface
	   It may be enabled again when a subclass is constructed which needs it.
	MonetPreparedStatement(
			MonetConnection connection,
			int resultSetType,
			int resultSetConcurrency,
			int resultSetHoldability)
		throws SQLException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability
		);
		// initialise blank finals
		monetdbType = null;
		javaType = null;
		digits = null;
		scale = null;
		schema = null;
		table = null;
		column = null;
		values = null;
		id = -1;
		size = -1;
		rscolcnt = -1;
	}
	*/

	//== methods interface PreparedStatement

	/**
	 * Adds a set of parameters to this PreparedStatement object's batch
	 * of commands.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void addBatch() throws SQLException {
		super.addBatch(transform());
	}

	/** override the addBatch from the Statement to throw an SQLException */
	@Override
	public void addBatch(final String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Clears the current parameter values immediately.
	 *
	 * In general, parameter values remain in force for repeated use of a
	 * statement. Setting a parameter value automatically clears its previous
	 * value. However, in some cases it is useful to immediately release the
	 * resources used by the current parameter values; this can be done by
	 * calling the method clearParameters.
	 */
	@Override
	public void clearParameters() {
		for (int i = 0; i < size; i++) {
			values[i] = null;
		}
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object,
	 * which may be any kind of SQL statement.  Some prepared statements
	 * return multiple results; the execute method handles these complex
	 * statements as well as the simpler form of statements handled by
	 * the methods executeQuery and executeUpdate.
	 *
	 * The execute method returns a boolean to indicate the form of the
	 * first result.  You must call either the method getResultSet or
	 * getUpdateCount to retrieve the result; you must call
	 * getMoreResults to move to any subsequent result(s).
	 *
	 * @return true if the first result is a ResultSet object; false if the
	 *              first result is an update count or there is no result
	 * @throws SQLException if a database access error occurs or an argument
	 *                      is supplied to this method
	 */
	@Override
	public boolean execute() throws SQLException {
		return super.execute(transform());
	}

	/** override the execute from the Statement to throw an SQLException */
	@Override
	public boolean execute(final String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Executes the SQL query in this PreparedStatement object and returns the
	 * ResultSet object generated by the query.
	 *
	 * @return a ResultSet object that contains the data produced by the query;
	 *         never null
	 * @throws SQLException if a database access error occurs or the SQL
	 *                      statement does not return a ResultSet object
	 */
	@Override
	public ResultSet executeQuery() throws SQLException {
		if (execute() != true)
			throw new SQLException("Query did not produce a result set", "M1M19");

		return getResultSet();
	}

	/** override the executeQuery from the Statement to throw an SQLException */
	@Override
	public ResultSet executeQuery(final String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which must
	 * be an SQL INSERT, UPDATE or DELETE statement; or an SQL statement that
	 * returns nothing, such as a DDL statement.
	 *
	 * @return either (1) the row count for INSERT, UPDATE, or DELETE
	 *         statements or (2) 0 for SQL statements that return nothing
	 * @throws SQLException if a database access error occurs or the SQL
	 *                     statement returns a ResultSet object
	 */
	@Override
	public int executeUpdate() throws SQLException {
		if (execute() != false)
			throw new SQLException("Query produced a result set", "M1M17");

		return getUpdateCount();
	}

	/** override the executeUpdate from the Statement to throw an SQLException */
	@Override
	public int executeUpdate(final String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Returns the index (0..size-1) in the backing arrays for the given
	 * resultset column number or an SQLException when not found
	 */
	private final int getColumnIdx(final int colnr) throws SQLException {
		int curcol = 0;
		for (int i = 0; i < size; i++) {
			/* when column[i] == null it is a parameter, when column[i] != null it is a result column of the prepared query */
			if (column[i] == null)
				continue;
			curcol++;
			if (curcol == colnr)
				return i;
		}
		throw new SQLException("No such column with index: " + colnr, "M1M05");
	}
	/**
	 * Returns the index (0..size-1) in the backing arrays for the given
	 * parameter number or an SQLException when not found
	 */
	private final int getParamIdx(final int paramnr) throws SQLException {
		int curparam = 0;
		for (int i = 0; i < size; i++) {
			/* when column[i] == null it is a parameter, when column[i] != null it is a result column of the prepared query */
			if (column[i] != null)
				continue;
			curparam++;
			if (curparam == paramnr)
				return i;
		}
		throw new SQLException("No such parameter with index: " + paramnr, "M1M05");
	}


	/* helper for the anonymous class inside getMetaData */
	private abstract class rsmdw extends MonetWrapper implements ResultSetMetaData {}
	/**
	 * Retrieves a ResultSetMetaData object that contains information
	 * about the columns of the ResultSet object that will be returned
	 * when this PreparedStatement object is executed.
	 *
	 * Because a PreparedStatement object is precompiled, it is possible
	 * to know about the ResultSet object that it will return without
	 * having to execute it.  Consequently, it is possible to invoke the
	 * method getMetaData on a PreparedStatement object rather than
	 * waiting to execute it and then invoking the ResultSet.getMetaData
	 * method on the ResultSet object that is returned.
	 *
	 * @return the description of a ResultSet object's columns or null if the
	 *         driver cannot return a ResultSetMetaData object
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if (rscolcnt == 3)
			return null; // not sufficient data with pre-Dec2011 PREPARE

		// return inner class which implements the ResultSetMetaData interface
		return new rsmdw() {
			/**
			 * Returns the number of columns in this ResultSet object.
			 *
			 * @return the number of columns
			 */
			@Override
			public int getColumnCount() {
				int cnt = 0;

				for (int i = 0; i < size; i++) {
					if (column[i] != null)
						cnt++;
				}
				return cnt;
			}

			/**
			 * Indicates whether the designated column is automatically numbered.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public boolean isAutoIncrement(final int column) throws SQLException {
				/* TODO: in MonetDB only numeric (int, decimal) columns could be autoincrement/serial
				 * This however requires an expensive dbmd.getColumns(null, schema, table, column)
				 * query call to pull the IS_AUTOINCREMENT value for this column.
				 * See also ResultSetMetaData.isAutoIncrement()
				 */
				// For now we simply allways return false.
				return false;
			}

			/**
			 * Indicates whether a column's case matters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return false
			 */
			@Override
			public boolean isCaseSensitive(final int column) throws SQLException {
				switch (getColumnType(column)) {
					case Types.CHAR:
					case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR, it's here for completeness
					case Types.CLOB:
						return true;
					case Types.VARCHAR:
						final String monettype = getColumnTypeName(column);
						if (monettype != null) {
							// data of type inet or uuid is not case sensitive
							if ("inet".equals(monettype)
							 || "uuid".equals(monettype))
								return false;
						}
						return true;
				}

				return false;
			}

			/**
			 * Indicates whether the designated column can be used in a
			 * where clause.
			 *
			 * Returning true for all here, even for CLOB, BLOB.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true
			 */
			@Override
			public boolean isSearchable(final int column) {
				return true;
			}

			/**
			 * Indicates whether the designated column is a cash value.
			 * From the MonetDB database perspective it is by definition
			 * unknown whether the value is a currency, because there are
			 * no currency datatypes such as MONEY.  With this knowledge
			 * we can always return false here.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return false
			 */
			@Override
			public boolean isCurrency(final int column) {
				return false;
			}

			/**
			 * Indicates whether values in the designated column are signed
			 * numbers.
			 * Within MonetDB all numeric types (except oid and ptr) are signed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isSigned(final int column) throws SQLException {
				// we can hardcode this, based on the colum type
				switch (getColumnType(column)) {
					case Types.NUMERIC:
					case Types.DECIMAL:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.REAL:
					case Types.FLOAT:
					case Types.DOUBLE:
						return true;
					case Types.BIGINT:
						final String monettype = getColumnTypeName(column);
						if (monettype != null) {
							if ("oid".equals(monettype)
							 || "ptr".equals(monettype))
								return false;
						}
						return true;
				//	All other types should return false
				//	case Types.BOOLEAN:
				//	case Types.DATE:
				//	case Types.TIME:
				//	case Types.TIME_WITH_TIMEZONE:
				//	case Types.TIMESTAMP:
				//	case Types.TIMESTAMP_WITH_TIMEZONE:
					default:
						return false;
				}
			}

			/**
			 * Indicates the designated column's normal maximum width in
			 * characters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the normal maximum number of characters allowed as the
			 *         width of the designated column
			 * @throws SQLException if there is no such column
			 */
			@Override
			public int getColumnDisplaySize(final int column) throws SQLException {
				try {
					return digits[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Get the designated column's table's schema.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return schema name or "" if not applicable
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getSchemaName(final int column) throws SQLException {
				try {
					return schema[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Gets the designated column's table name.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return table name or "" if not applicable
			 */
			@Override
			public String getTableName(final int column) throws SQLException {
				try {
					return table[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Get the designated column's number of decimal digits.
			 * This method is currently very expensive as it needs to
			 * retrieve the information from the database using an SQL
			 * query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getPrecision(final int column) throws SQLException {
				try {
					return digits[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Gets the designated column's number of digits to right of
			 * the decimal point.  This method is currently very
			 * expensive as it needs to retrieve the information from
			 * the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return scale
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getScale(final int column) throws SQLException {
				try {
					return scale[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Indicates the nullability of values in the designated
			 * column.  This method is currently very expensive as it
			 * needs to retrieve the information from the database using
			 * an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return nullability
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(final int column) throws SQLException {
				return columnNullableUnknown;
			}

			/**
			 * Gets the designated column's table's catalog name.
			 * MonetDB does not support the catalog naming concept as in: catalog.schema.table naming scheme
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the name of the catalog for the table in which the given
			 *         column appears or "" if not applicable
			 */
			@Override
			public String getCatalogName(final int column) throws SQLException {
				return null;	// MonetDB does NOT support catalogs
			}

			/**
			 * Indicates whether the designated column is definitely not
			 * writable.  MonetDB does not support cursor updates, so
			 * nothing is writable.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isReadOnly(final int column) {
				return true;
			}

			/**
			 * Indicates whether it is possible for a write on the
			 * designated column to succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isWritable(final int column) {
				return false;
			}

			/**
			 * Indicates whether a write on the designated column will
			 * definitely succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isDefinitelyWritable(final int column) {
				return false;
			}

			/**
			 * Returns the fully-qualified name of the Java class whose
			 * instances are manufactured if the method
			 * ResultSet.getObject is called to retrieve a value from
			 * the column.  ResultSet.getObject may return a subclass of
			 * the class returned by this method.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the fully-qualified name of the class in the Java
			 *         programming language that would be used by the method
			 *         ResultSet.getObject to retrieve the value in the
			 *         specified column. This is the class name used for custom
			 *         mapping.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnClassName(final int column) throws SQLException {
				final String typeName = getColumnTypeName(column);
				final Map<String,Class<?>> map = getConnection().getTypeMap();
				final Class<?> c;
				if (map.containsKey(typeName)) {
					c = (Class)map.get(typeName);
				} else {
					c = MonetResultSet.getClassForType(getColumnType(column));
				}
				return c.getName();
			}

			/**
			 * Gets the designated column's suggested title for use in
			 * printouts and displays. This is currently equal to
			 * getColumnName().
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the suggested column title
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnLabel(final int column) throws SQLException {
				return getColumnName(column);
			}

			/**
			 * Gets the designated column's name
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the column name
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnName(final int colnr) throws SQLException {
				try {
					return column[getColumnIdx(colnr)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(colnr);
				}
			}

			/**
			 * Retrieves the designated column's SQL type.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return SQL type from java.sql.Types
			 * @throws SQLException if there is no such column
			 */
			@Override
			public int getColumnType(final int column) throws SQLException {
				try {
					return javaType[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Retrieves the designated column's database-specific type name.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return type name used by the database. If the column type is a
			 *         user-defined type, then a fully-qualified type name is
			 *         returned.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnTypeName(final int column) throws SQLException {
				try {
					return monetdbType[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}
		};
	}

	/* helper class for the anonymous class in getParameterMetaData */
	private abstract class pmdw extends MonetWrapper implements ParameterMetaData {}
	/**
	 * Retrieves the number, types and properties of this
	 * PreparedStatement object's parameters.
	 *
	 * @return a ParameterMetaData object that contains information
	 *         about the number, types and properties of this
	 *         PreparedStatement object's parameters
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return new pmdw() {
			/**
			 * Retrieves the number of parameters in the
			 * PreparedStatement object for which this ParameterMetaData
			 * object contains information.
			 *
			 * @return the number of parameters
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterCount() throws SQLException {
				int cnt = 0;

				for (int i = 0; i < size; i++) {
					if (column[i] == null)
						cnt++;
				}
				return cnt;
			}

			/**
			 * Retrieves whether null values are allowed in the
			 * designated parameter.
			 *
			 * This is currently always unknown for MonetDB/SQL.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return the nullability status of the given parameter;
			 *         one of ParameterMetaData.parameterNoNulls,
			 *         ParameterMetaData.parameterNullable, or
			 *         ParameterMetaData.parameterNullableUnknown
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(final int param) throws SQLException {
				return ParameterMetaData.parameterNullableUnknown;
			}

			/**
			 * Retrieves whether values for the designated parameter can
			 * be signed numbers.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public boolean isSigned(final int param) throws SQLException {
				// we can hardcode this, based on the colum type
				switch (getParameterType(param)) {
					case Types.NUMERIC:
					case Types.DECIMAL:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.REAL:
					case Types.FLOAT:
					case Types.DOUBLE:
						return true;
					case Types.BIGINT:
						final String monettype = getParameterTypeName(param);
						if (monettype != null) {
							if ("oid".equals(monettype)
							 || "ptr".equals(monettype))
								return false;
						}
						return true;
				//	All other types should return false
				//	case Types.BOOLEAN:
				//	case Types.DATE:
				//	case Types.TIME:
				//	case Types.TIME_WITH_TIMEZONE:
				//	case Types.TIMESTAMP:
				//	case Types.TIMESTAMP_WITH_TIMEZONE:
					default:
						return false;
				}
			}

			/**
			 * Retrieves the designated parameter's number of decimal
			 * digits.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getPrecision(final int param) throws SQLException {
				try {
					return digits[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			}

			/**
			 * Retrieves the designated parameter's number of digits to
			 * right of the decimal point.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return scale
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getScale(final int param) throws SQLException {
				try {
					return scale[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			}

			/**
			 * Retrieves the designated parameter's SQL type.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return SQL type from java.sql.Types
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterType(final int param) throws SQLException {
				try {
					return javaType[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			}

			/**
			 * Retrieves the designated parameter's database-specific
			 * type name.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return type the name used by the database.  If the
			 *         parameter type is a user-defined type, then a
			 *         fully-qualified type name is returned.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getParameterTypeName(final int param) throws SQLException {
				try {
					return monetdbType[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			}

			/**
			 * Retrieves the fully-qualified name of the Java class
			 * whose instances should be passed to the method
			 * PreparedStatement.setObject.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return the fully-qualified name of the class in the Java
			 *         programming language that would be used by the
			 *         method PreparedStatement.setObject to set the
			 *         value in the specified parameter. This is the
			 *         class name used for custom mapping.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getParameterClassName(final int param) throws SQLException {
				final String typeName = getParameterTypeName(param);
				final Map<String,Class<?>> map = getConnection().getTypeMap();
				final Class<?> c;
				if (map.containsKey(typeName)) {
					c = (Class)map.get(typeName);
				} else {
					c = MonetResultSet.getClassForType(getParameterType(param));
				}
				return c.getName();
			}

			/**
			 * Retrieves the designated parameter's mode.
			 * For MonetDB/SQL we currently only support INput parameters.
			 *
			 * @param param - the first parameter is 1, the second is 2, ...
			 * @return mode of the parameter; one of
			 *         ParameterMetaData.parameterModeIn,
			 *         ParameterMetaData.parameterModeOut, or
			 *         ParameterMetaData.parameterModeInOut
			 *         ParameterMetaData.parameterModeUnknown.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterMode(final int param) throws SQLException {
				return ParameterMetaData.parameterModeIn;
			}
		};
	}

	/**
	 * Sets the designated parameter to the given Array object.  The
	 * driver converts this to an SQL ARRAY value when it sends it to
	 * the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x an Array object that maps an SQL ARRAY value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setArray(final int parameterIndex, final Array x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setArray");
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large ASCII value is input to
	 * a LONGVARCHAR parameter, it may be more practical to send it via a
	 * java.io.InputStream. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion
	 * from ASCII to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the Java input stream that contains the ASCII parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setAsciiStream");
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large ASCII value is input to
	 * a LONGVARCHAR parameter, it may be more practical to send it via a
	 * java.io.InputStream. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion
	 * from ASCII to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the Java input stream that contains the ASCII parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x, final int length)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setAsciiStream");
	}

	/**
	 * Sets the designated parameter to the given input stream, which
	 * will have the specified number of bytes. When a very large ASCII
	 * value is input to a LONGVARCHAR parameter, it may be more
	 * practical to send it via a java.io.InputStream. Data will be read
	 * from the stream as needed until end-of-file is reached. The JDBC
	 * driver will do any necessary conversion from ASCII to the
	 * database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the Java input stream that contains the ASCII parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x, final long length)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setAsciiStream");
	}

	/**
	 * Sets the designated parameter to the given java.math.BigDecimal value.
	 * The driver converts this to an SQL NUMERIC value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBigDecimal(final int parameterIndex, BigDecimal x) throws SQLException {
		// get array position
		final int i = getParamIdx(parameterIndex);

		// round to the scale of the DB:
		x = x.setScale(scale[i], java.math.RoundingMode.HALF_UP);

		// if precision is now greater than that of the db, throw an error:
		if (x.precision() > digits[i]) {
			throw new SQLDataException("DECIMAL value exceeds allowed digits/scale: " + x.toPlainString() + " (" + digits[i] + "/" + scale[i] + ")", "22003");
		}

		// MonetDB doesn't like leading 0's, since it counts them as part of
		// the precision, so let's strip them off. (But be careful not to do
		// this to the exact number "0".)  Also strip off trailing
		// numbers that are inherent to the double representation.
		String xStr = x.toPlainString();
		final int dot = xStr.indexOf('.');
		if (dot >= 0)
			xStr = xStr.substring(0, Math.min(xStr.length(), dot + 1 + scale[i]));
		while (xStr.startsWith("0") && xStr.length() > 1)
			xStr = xStr.substring(1);
		setValue(parameterIndex, xStr);
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large binary value is input
	 * to a LONGVARBINARY parameter, it may be more practical to send it via a
	 * java.io.InputStream object. The data will be read from the stream as
	 * needed until end-of-file is reached.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java input stream which contains the binary parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setBinaryStream");
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large binary value is input
	 * to a LONGVARBINARY parameter, it may be more practical to send it via a
	 * java.io.InputStream object. The data will be read from the stream as
	 * needed until end-of-file is reached.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java input stream which contains the binary parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x, final int length)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setBinaryStream");
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large binary value is input
	 * to a LONGVARBINARY parameter, it may be more practical to send it via a
	 * java.io.InputStream object. The data will be read from the stream as
	 * needed until end-of-file is reached.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java input stream which contains the binary parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x, final long length)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setBinaryStream");
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver
	 * converts this to an SQL BLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a Blob object that maps an SQL BLOB value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBlob(final int parameterIndex, final InputStream x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setBlob");
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver
	 * converts this to an SQL BLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a Blob object that maps an SQL BLOB value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setBlob");
	}

	/**
	 * Sets the designated parameter to a InputStream object. The
	 * inputstream must contain the number of characters specified by
	 * length otherwise a SQLException will be generated when the
	 * PreparedStatement is executed. This method differs from the
	 * setBinaryStream (int, InputStream, int) method because it informs
	 * the driver that the parameter value should be sent to the server
	 * as a BLOB. When the setBinaryStream method is used, the driver
	 * may have to do extra work to determine whether the parameter data
	 * should be sent to the server as a LONGVARBINARY or a BLOB.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param is an object that contains the data to set the parameter
	 *           value to
	 * @param length the number of bytes in the parameter data
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setBlob(final int parameterIndex, final InputStream is, final long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setBlob");
	}

	/**
	 * Sets the designated parameter to the given Java boolean value. The
	 * driver converts this to an SQL BIT value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
		setValue(parameterIndex, Boolean.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java byte value. The driver
	 * converts this to an SQL TINYINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setByte(final int parameterIndex, final byte x) throws SQLException {
		setValue(parameterIndex, Byte.toString(x));
	}

	static final String HEXES = "0123456789ABCDEF";
	/**
	 * Sets the designated parameter to the given Java array of bytes. The
	 * driver converts this to an SQL VARBINARY or LONGVARBINARY (depending
	 * on the argument's size relative to the driver's limits on VARBINARY
	 * values) when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		final int len = x.length;
		final StringBuilder hex = new StringBuilder(8 + (len * 2));
		hex.append("blob '");	// add a casting prefix
		// convert the bytes into hex codes
		for (int i = 0; i < len; i++) {
			hex.append(HEXES.charAt((x[i] & 0xF0) >> 4))
			   .append(HEXES.charAt((x[i] & 0x0F)));
		}
		hex.append("'");	// end of hex string value
		setValue(parameterIndex, hex.toString());
	}

	/**
	 * Sets the designated parameter to the given Reader object, which is the
	 * given number of characters long. When a very large UNICODE value is
	 * input to a LONGVARCHAR parameter, it may be more practical to send it
	 * via a java.io.Reader object. The data will be read from the stream as
	 * needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader the java.io.Reader object that contains the Unicode data
	 * @param length the number of characters in the stream
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
		throws SQLException
	{
		setClob(parameterIndex, reader, (long)length);
	}

	/**
	 * Sets the designated parameter to the given Reader object, which is the
	 * given number of characters long. When a very large UNICODE value is
	 * input to a LONGVARCHAR parameter, it may be more practical to send it
	 * via a java.io.Reader object. The data will be read from the stream as
	 * needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader the java.io.Reader object that contains the Unicode data
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader)
		throws SQLException
	{
		setClob(parameterIndex, reader);
	}

	/**
	 * Sets the designated parameter to the given Reader object, which is the
	 * given number of characters long. When a very large UNICODE value is
	 * input to a LONGVARCHAR parameter, it may be more practical to send it
	 * via a java.io.Reader object. The data will be read from the stream as
	 * needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader the java.io.Reader object that contains the Unicode data
	 * @param length the number of characters in the stream
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader, final long length)
		throws SQLException
	{
		setClob(parameterIndex, reader, length);
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver
	 * converts this to an SQL CLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a Clob object that maps an SQL CLOB value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(final int parameterIndex, final Clob x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		// simply serialise the CLOB into a String for now... far from
		// efficient, but might work for a few cases...
		// be on your marks: we have to cast the length down!
		setString(parameterIndex, x.getSubString(1L, (int)(x.length())));
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver
	 * converts this to an SQL CLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader an object that contains the data to set the parameter value to
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
		if (reader == null) {
			setNull(parameterIndex, -1);
			return;
		}

		// Some buffer. Size of 8192 is default for BufferedReader, so...
		final int size = 8192;
		final char[] arr = new char[size];
		final StringBuilder buf = new StringBuilder(size * 32);
		try {
			int numChars;
			while ((numChars = reader.read(arr, 0, size)) > 0) {
				buf.append(arr, 0, numChars);
			}
			setString(parameterIndex, buf.toString());
		} catch (IOException e) {
			throw new SQLException("failed to read from stream: " + e.getMessage(), "M1M25");
		}
	}

	/**
	 * Sets the designated parameter to a Reader object. The reader must
	 * contain the number of characters specified by length otherwise a
	 * SQLException will be generated when the PreparedStatement is
	 * executed. This method differs from the setCharacterStream (int,
	 * Reader, int) method because it informs the driver that the
	 * parameter value should be sent to the server as a CLOB. When the
	 * setCharacterStream method is used, the driver may have to do
	 * extra work to determine whether the parameter data should be sent
	 * to the server as a LONGVARCHAR or a CLOB.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader An object that contains the data to set the parameter value to.
	 * @param length the number of characters in the parameter data.
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
		if (reader == null) {
			setNull(parameterIndex, -1);
			return;
		}
		if (length < 0 || length > Integer.MAX_VALUE) {
			throw new SQLException("Invalid length value: " + length, "M1M05");
		}

		// simply serialise the Reader data into a large buffer
		final CharBuffer buf = CharBuffer.allocate((int)length); // have to down cast
		try {
			reader.read(buf);
			// We have to rewind the buffer, because otherwise toString() returns "".
			buf.rewind();
			setString(parameterIndex, buf.toString());
		} catch (IOException e) {
			throw new SQLException("failed to read from stream: " + e.getMessage(), "M1M25");
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value. The
	 * driver converts this to an SQL DATE value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setDate(final int parameterIndex, final java.sql.Date x)
		throws SQLException
	{
		setDate(parameterIndex, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value, using
	 * the given Calendar object. The driver uses the Calendar object to
	 * construct an SQL DATE value, which the driver then sends to the
	 * database. With a Calendar object, the driver can calculate the date
	 * taking into account a custom timezone. If no Calendar object is
	 * specified, the driver uses the default timezone, which is that of the
	 * virtual machine running the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the date
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setDate(final int parameterIndex, final java.sql.Date x, final Calendar cal)
		throws SQLException
	{
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		if (cal == null) {
			setValue(parameterIndex, "date '" + x.toString() + "'");
		} else {
			if (mDate == null) {
				// first time usage, create and keep the mDate object for next usage
				mDate = new SimpleDateFormat("yyyy-MM-dd");
			}
			mDate.setTimeZone(cal.getTimeZone());
			setValue(parameterIndex, "date '" + mDate.format(x) + "'");
		}
	}

	/**
	 * Sets the designated parameter to the given Java double value. The driver
	 * converts this to an SQL DOUBLE value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setDouble(final int parameterIndex, final double x) throws SQLException {
		setValue(parameterIndex, Double.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java float value. The driver
	 * converts this to an SQL FLOAT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setFloat(final int parameterIndex, final float x) throws SQLException {
		setValue(parameterIndex, Float.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java int value. The driver
	 * converts this to an SQL INTEGER value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setInt(final int parameterIndex, final int x) throws SQLException {
		setValue(parameterIndex, Integer.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java long value. The driver
	 * converts this to an SQL BIGINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setLong(final int parameterIndex, final long x) throws SQLException {
		setValue(parameterIndex, Long.toString(x));
	}

	/**
	 * Sets the designated parameter to a Reader object. The Reader
	 * reads the data till end-of-file is reached. The driver does the
	 * necessary conversion from Java character format to the national
	 * character set in the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException {
		setCharacterStream(parameterIndex, value);
	}

	/**
	 * Sets the designated parameter to a Reader object. The Reader
	 * reads the data till end-of-file is reached. The driver does the
	 * necessary conversion from Java character format to the national
	 * character set in the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @param length the number of characters in the parameter data.
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNCharacterStream(final int parameterIndex, final Reader value, final long length)
		throws SQLException
	{
		setCharacterStream(parameterIndex, value, length);
	}

	/**
	 * Sets the designated parameter to a java.sql.NClob object. The
	 * driver converts this to a SQL NCLOB value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(final int parameterIndex, final Reader value) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNClob");
	}

	/**
	 * Sets the designated parameter to a java.sql.NClob object. The
	 * driver converts this to a SQL NCLOB value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(final int parameterIndex, final NClob value) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNClob");
	}

	/**
	 * Sets the designated parameter to a Reader object. The reader must
	 * contain the number of characters specified by length otherwise a
	 * SQLException will be generated when the PreparedStatement is
	 * executed. This method differs from the setCharacterStream (int,
	 * Reader, int) method because it informs the driver that the
	 * parameter value should be sent to the server as a NCLOB. When the
	 * setCharacterStream method is used, the driver may have to do
	 * extra work to determine whether the parameter data should be sent
	 * to the server as a LONGNVARCHAR or a NCLOB.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param r An object that contains the data to set the parameter
	 *          value to
	 * @param length the number of characters in the parameter data
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(final int parameterIndex, final Reader r, final long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNClob");
	}

	/**
	 * Sets the designated paramter to the given String object. The
	 * driver converts this to a SQL NCHAR or NVARCHAR or LONGNVARCHAR
	 * value (depending on the argument's size relative to the driver's
	 * limits on NVARCHAR values) when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNString(final int parameterIndex, final String value) throws SQLException {
		setString(parameterIndex, value);
	}

	/**
	 * Sets the designated parameter to SQL NULL.
	 *
	 * Note: You must specify the parameter's SQL type.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType the SQL type code defined in java.sql.Types
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
		// we discard the given type here, the backend converts the
		// value NULL to whatever it needs for the column
		setValue(parameterIndex, "NULL");
	}

	/**
	 * Sets the designated parameter to SQL NULL. This version of the method
	 * setNull should be used for user-defined types and REF type parameters.
	 * Examples of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT,
	 * and named array types.
	 *
	 * Note: To be portable, applications must give the SQL type code and the
	 * fully-qualified SQL type name when specifying a NULL user-defined or REF
	 * parameter. In the case of a user-defined type the name is the type name
	 * of the parameter itself. For a REF parameter, the name is the type name
	 * of the referenced type. If a JDBC driver does not need the type code or
	 * type name information, it may ignore it. Although it is intended for
	 * user-defined and Ref parameters, this method may be used to set a null
	 * parameter of any JDBC type. If the parameter does not have a
	 * user-defined or REF type, the given typeName is ignored.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType a value from java.sql.Types
	 * @param typeName the fully-qualified name of an SQL user-defined type;
	 *                 ignored if the parameter is not a user-defined type or
	 *                 REF
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNull(final int parameterIndex, final int sqlType, final String typeName)
		throws SQLException
	{
		// MonetDB/SQL's NULL needs no type
		setNull(parameterIndex, sqlType);
	}

	/**
	 * Sets the value of the designated parameter using the given
	 * object.  The second parameter must be of type Object; therefore,
	 * the java.lang equivalent objects should be used for built-in
	 * types.
	 *
	 * The JDBC specification specifies a standard mapping from Java
	 * Object types to SQL types. The given argument will be converted
	 * to the corresponding SQL type before being sent to the database.
	 *
	 * Note that this method may be used to pass datatabase-specific
	 * abstract data types, by using a driver-specific Java type. If the
	 * object is of a class implementing the interface SQLData, the JDBC
	 * driver should call the method SQLData.writeSQL to write it to the
	 * SQL data stream. If, on the other hand, the object is of a class
	 * implementing Ref, Blob, Clob, Struct, or Array, the driver should
	 * pass it to the database as a value of the corresponding SQL type.
	 *
	 * This method throws an exception if there is an ambiguity, for
	 * example, if the object is of a class implementing more than one
	 * of the interfaces named above.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @throws SQLException if a database access error occurs or the type of
	 *                      the given object is ambiguous
	 */
	@Override
	public void setObject(final int parameterIndex, final Object x) throws SQLException {
		setObject(parameterIndex, x, javaType[getParamIdx(parameterIndex)], 0);
	}

	/**
	 * Sets the value of the designated parameter with the given object. This
	 * method is like the method setObject below, except that it assumes a scale
	 * of zero.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
	 *                      sent to the database
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setObject(final int parameterIndex, final Object x, final int targetSqlType)
		throws SQLException
	{
		setObject(parameterIndex, x, targetSqlType, 0);
	}

	/**
	 * Sets the value of the designated parameter with the given object. The
	 * second argument must be an object type; for integral values, the
	 * java.lang equivalent objects should be used.
	 *
	 * The given Java object will be converted to the given targetSqlType
	 * before being sent to the database. If the object has a custom mapping
	 * (is of a class implementing the interface SQLData), the JDBC driver
	 * should call the method SQLData.writeSQL to write it to the SQL data
	 * stream. If, on the other hand, the object is of a class implementing
	 * Ref, Blob, Clob, Struct, or Array, the driver should pass it to the
	 * database as a value of the corresponding SQL type.
	 *
	 * Note that this method may be used to pass database-specific abstract
	 * data types.
	 *
	 * To meet the requirements of this interface, the Java object is
	 * converted in the driver, instead of using a SQL CAST construct.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to
	 *                      be sent to the database. The scale argument may
	 *                      further qualify this type.
	 * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
	 *              this is the number of digits after the decimal
	 *              point. For Java Object types InputStream and Reader,
	 *              this is the length of the data in the stream or
	 *              reader.  For all other types, this value will be
	 *              ignored.
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 * @see Types
	 */
	@Override
	public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scale)
		throws SQLException
	{
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		// this is according to table B-5
		if (x instanceof String) {
			setString(parameterIndex, (String)x);
		} else if (x instanceof BigDecimal ||
				x instanceof Byte ||
				x instanceof Short ||
				x instanceof Integer ||
				x instanceof Long ||
				x instanceof Float ||
				x instanceof Double)
		{
			final Number num = (Number)x;
			switch (targetSqlType) {
				case Types.TINYINT:
					setByte(parameterIndex, num.byteValue());
				break;
				case Types.SMALLINT:
					setShort(parameterIndex, num.shortValue());
				break;
				case Types.INTEGER:
					setInt(parameterIndex, num.intValue());
				break;
				case Types.BIGINT:
					if (x instanceof BigDecimal) {
						BigDecimal bd = (BigDecimal)x;
						setLong(parameterIndex, bd.setScale(scale, java.math.RoundingMode.HALF_UP).longValue());
					} else {
						setLong(parameterIndex, num.longValue());
					}
				break;
				case Types.REAL:
					setFloat(parameterIndex, num.floatValue());
				break;
				case Types.FLOAT:
				case Types.DOUBLE:
					setDouble(parameterIndex, num.doubleValue());
				break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					if (x instanceof BigDecimal) {
						setBigDecimal(parameterIndex, (BigDecimal)x);
					} else {
						setBigDecimal(parameterIndex, new BigDecimal(num.doubleValue()));
					}
				break;
				case Types.BOOLEAN:
					if (num.doubleValue() != 0.0) {
						setBoolean(parameterIndex, true);
					} else {
						setBoolean(parameterIndex, false);
					}
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					setString(parameterIndex, x.toString());
				break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof Boolean) {
			final boolean val = ((Boolean)x).booleanValue();
			switch (targetSqlType) {
				case Types.TINYINT:
					setByte(parameterIndex, (byte)(val ? 1 : 0));
				break;
				case Types.SMALLINT:
					setShort(parameterIndex, (short)(val ? 1 : 0));
				break;
				case Types.INTEGER:
					setInt(parameterIndex, (val ? 1 : 0));  // do not cast to (int) as it generates a compiler warning
				break;
				case Types.BIGINT:
					setLong(parameterIndex, (long)(val ? 1 : 0));
				break;
				case Types.REAL:
					setFloat(parameterIndex, (float)(val ? 1.0 : 0.0));
				break;
				case Types.FLOAT:
				case Types.DOUBLE:
					setDouble(parameterIndex, (val ? 1.0 : 0.0));  // do no cast to (double) as it generates a compiler warning
				break;
				case Types.DECIMAL:
				case Types.NUMERIC:
				{
					final BigDecimal dec;
					try {
						dec = new BigDecimal(val ? 1.0 : 0.0);
					} catch (NumberFormatException e) {
						throw new SQLException("Internal error: unable to create template BigDecimal: " + e.getMessage(), "M0M03");
					}
					setBigDecimal(parameterIndex, dec);
				} break;
				case Types.BOOLEAN:
					setBoolean(parameterIndex, val);
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					setString(parameterIndex, x.toString());
				break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof BigInteger) {
			final BigInteger num = (BigInteger)x;
			switch (targetSqlType) {
				case Types.BIGINT:
					setLong(parameterIndex, num.longValue());
				break;
				case Types.DECIMAL:
				case Types.NUMERIC:
				{
					final BigDecimal dec;
					try {
						dec = new BigDecimal(num);
					} catch (NumberFormatException e) {
						throw new SQLException("Internal error: unable to create template BigDecimal: " + e.getMessage(), "M0M03");
					}
					setBigDecimal(parameterIndex, dec);
				} break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					setString(parameterIndex, x.toString());
				break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof byte[]) {
			switch (targetSqlType) {
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
					setBytes(parameterIndex, (byte[])x);
				break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof java.sql.Date ||
				x instanceof Timestamp ||
				x instanceof Time ||
				x instanceof Calendar ||
				x instanceof java.util.Date)
		{
			switch (targetSqlType) {
				case Types.DATE:
					if (x instanceof java.sql.Date) {
						setDate(parameterIndex, (java.sql.Date)x);
					} else if (x instanceof Timestamp) {
						setDate(parameterIndex, new java.sql.Date(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setDate(parameterIndex, new java.sql.Date(
									((java.util.Date)x).getTime()));
					} else if (x instanceof Calendar) {
						setDate(parameterIndex, new java.sql.Date(
									((Calendar)x).getTimeInMillis()));
					} else {
						throw new SQLException("Conversion not allowed", "M1M05");
					}
				break;
				case Types.TIME:
				case Types.TIME_WITH_TIMEZONE:
					if (x instanceof Time) {
						setTime(parameterIndex, (Time)x);
					} else if (x instanceof Timestamp) {
						setTime(parameterIndex, new Time(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setTime(parameterIndex, new java.sql.Time(
									((java.util.Date)x).getTime()));
					} else if (x instanceof Calendar) {
						setTime(parameterIndex, new java.sql.Time(
									((Calendar)x).getTimeInMillis()));
					} else {
						throw new SQLException("Conversion not allowed", "M1M05");
					}
				break;
				case Types.TIMESTAMP:
				case Types.TIMESTAMP_WITH_TIMEZONE:
					if (x instanceof Timestamp) {
						setTimestamp(parameterIndex, (Timestamp)x);
					} else if (x instanceof java.sql.Date) {
						setTimestamp(parameterIndex, new Timestamp(((java.sql.Date)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setTimestamp(parameterIndex, new java.sql.Timestamp(
									((java.util.Date)x).getTime()));
					} else if (x instanceof Calendar) {
						setTimestamp(parameterIndex, new java.sql.Timestamp(
									((Calendar)x).getTimeInMillis()));
					} else {
						throw new SQLException("Conversion not allowed", "M1M05");
					}
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					setString(parameterIndex, x.toString());
				break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof Array) {
			setArray(parameterIndex, (Array)x);
		} else if (x instanceof MonetBlob || x instanceof Blob) {
			setBlob(parameterIndex, (Blob)x);
		} else if (x instanceof MonetClob || x instanceof Clob) {
			setClob(parameterIndex, (Clob)x);
		} else if (x instanceof Struct) {
			// I have no idea how to do this...
			throw newSQLFeatureNotSupportedException("setObject() with object of type Struct");
		} else if (x instanceof Ref) {
			setRef(parameterIndex, (Ref)x);
		} else if (x instanceof java.net.URL) {
			setURL(parameterIndex, (java.net.URL)x);
		} else if (x instanceof java.util.UUID) {
			setString(parameterIndex, x.toString());
		} else if (x instanceof RowId) {
			setRowId(parameterIndex, (RowId)x);
		} else if (x instanceof NClob) {
			setNClob(parameterIndex, (NClob)x);
		} else if (x instanceof SQLXML) {
			setSQLXML(parameterIndex, (SQLXML)x);
		} else if (x instanceof SQLData) { // not in JDBC4.1???
			final SQLData sx = (SQLData)x;
			final int paramnr = parameterIndex;
			final String sqltype = sx.getSQLTypeName();
			final SQLOutput out = new SQLOutput() {
				@Override
				public void writeString(String x) throws SQLException {
					// special situation, this is when a string
					// representation is given, but we need to prefix it
					// with the actual sqltype the server expects, or we
					// will get an error back
					setValue(paramnr, sqltype + " " + MonetWrapper.sq(x));
				}

				@Override
				public void writeBoolean(boolean x) throws SQLException {
					setBoolean(paramnr, x);
				}

				@Override
				public void writeByte(byte x) throws SQLException {
					setByte(paramnr, x);
				}

				@Override
				public void writeShort(short x) throws SQLException {
					setShort(paramnr, x);
				}

				@Override
				public void writeInt(int x) throws SQLException {
					setInt(paramnr, x);
				}

				@Override
				public void writeLong(long x) throws SQLException {
					setLong(paramnr, x);
				}

				@Override
				public void writeFloat(float x) throws SQLException {
					setFloat(paramnr, x);
				}

				@Override
				public void writeDouble(double x) throws SQLException {
					setDouble(paramnr, x);
				}

				@Override
				public void writeBigDecimal(BigDecimal x) throws SQLException {
					setBigDecimal(paramnr, x);
				}

				@Override
				public void writeBytes(byte[] x) throws SQLException {
					setBytes(paramnr, x);
				}

				@Override
				public void writeDate(java.sql.Date x) throws SQLException {
					setDate(paramnr, x);
				}

				@Override
				public void writeTime(java.sql.Time x) throws SQLException {
					setTime(paramnr, x);
				}

				@Override
				public void writeTimestamp(Timestamp x) throws SQLException {
					setTimestamp(paramnr, x);
				}

				@Override
				public void writeCharacterStream(Reader x) throws SQLException {
					setCharacterStream(paramnr, x);
				}

				@Override
				public void writeAsciiStream(InputStream x) throws SQLException {
					setAsciiStream(paramnr, x);
				}

				@Override
				public void writeBinaryStream(InputStream x) throws SQLException {
					setBinaryStream(paramnr, x);
				}

				@Override
				public void writeObject(SQLData x) throws SQLException {
					setObject(paramnr, x);
				}

				@Override
				public void writeRef(Ref x) throws SQLException {
					setRef(paramnr, x);
				}

				@Override
				public void writeBlob(Blob x) throws SQLException {
					setBlob(paramnr, x);
				}

				@Override
				public void writeClob(Clob x) throws SQLException {
					setClob(paramnr, x);
				}

				@Override
				public void writeStruct(Struct x) throws SQLException {
					setObject(paramnr, x);
				}

				@Override
				public void writeArray(Array x) throws SQLException {
					setArray(paramnr, x);
				}

				@Override
				public void writeURL(URL x) throws SQLException {
					setURL(paramnr, x);
				}

				@Override
				public void writeNString(String x) throws SQLException {
					setNString(paramnr, x);
				}

				@Override
				public void writeNClob(NClob x) throws SQLException {
					setNClob(paramnr, x);
				}

				@Override
				public void writeRowId(RowId x) throws SQLException {
					setRowId(paramnr, x);
				}

				@Override
				public void writeSQLXML(SQLXML x) throws SQLException {
					setSQLXML(paramnr, x);
				}
			};
			sx.writeSQL(out);
		} else {	// java Class
			throw newSQLFeatureNotSupportedException("setObject() with object of type Class " + x.getClass().getName());
		}
	}

	/**
	 * Sets the designated parameter to the given REF(&lt;structured-type&gt;) value.
	 * The driver converts this to an SQL REF value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x an SQL REF value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setRef(final int parameterIndex, final Ref x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setRef");
	}

	/**
	 * Sets the designated parameter to the given java.sql.RowId object.
	 * The driver converts this to a SQL ROWID value when it sends it to
	 * the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setRowId");
	}

	/**
	 * Sets the designated parameter to the given Java short value. The driver
	 * converts this to an SQL SMALLINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setShort(final int parameterIndex, final short x) throws SQLException {
		setValue(parameterIndex, Short.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java String value. The driver
	 * converts this to an SQL VARCHAR or LONGVARCHAR value (depending on the
	 * argument's size relative to the driver's limits on VARCHAR values) when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setString(final int parameterIndex, final String x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		final int paramIdx = getParamIdx(parameterIndex);	// this will throw a SQLException if parameter can not be found

		/* depending on the parameter data type (as expected by MonetDB) we
		   may need to add the data type as cast prefix to the parameter value */
		final int paramJdbcType = javaType[paramIdx];
		final String paramMonetdbType = monetdbType[paramIdx];

		switch (paramJdbcType) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CLOB:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			{
				String castprefix = "";
				switch (paramMonetdbType) {
					// some MonetDB specific data types require a cast prefix
					case "inet":
						try {
							// check if x represents a valid inet string to prevent
							// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
							org.monetdb.jdbc.types.INET inet_obj = new org.monetdb.jdbc.types.INET();
							inet_obj.fromString(x);
						} catch (SQLException se) {
							throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + se.getMessage(), "22M29");
						}
						castprefix = "inet";
						break;
					case "json":
						// There is no support for JSON in standard java class libraries.
						// Possibly we could use org.json.simple.JSONObject or other/faster libs
						// javax.json.Json is not released yet (see https://json-processing-spec.java.net/)
						// see also https://github.com/fabienrenaud/java-json-benchmark
						// Note that it would make our JDBC driver dependent of an external jar
						// and we don't want that.

						// do simplistic check if x represents a valid json string to prevent
						// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
						if (x.isEmpty() ||
							(x.startsWith("{") && !x.endsWith("}")) ||
							(x.startsWith("[") && !x.endsWith("]"))
							// TODO check completely if x represents a valid json string
						   )
							throw new SQLDataException("Invalid json string. It does not start with { or [ and end with } or ]", "22M32");

						// TODO check completely if x represents a valid json string

						castprefix = "json";
						break;
					case "url":
						try {
							// also check if x represents a valid url string to prevent
							// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
							java.net.URL url_obj = new java.net.URL(x);
						} catch (java.net.MalformedURLException mue) {
							throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + mue.getMessage(), "22M30");
						}
						castprefix = "url";
						break;
					case "uuid":
						try {
							// also check if x represents a valid uuid string to prevent
							// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
							java.util.UUID uuid_obj = java.util.UUID.fromString(x);
						} catch (IllegalArgumentException iae) {
							throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + iae.getMessage(), "22M31");
						}
						castprefix = "uuid";
						break;
				}
				/* in specific cases prefix the string with: inet or json or url or uuid */
				setValue(parameterIndex, castprefix + " " + MonetWrapper.sq(x));

				break;
			}
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.DECIMAL:
			case Types.NUMERIC:
				try {
					// check (by calling parse) if the string represents a valid number to prevent
					// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
					if (paramJdbcType == Types.INTEGER || paramJdbcType == Types.SMALLINT || paramJdbcType == Types.TINYINT) {
						int number = Integer.parseInt(x);
					} else
					if (paramJdbcType == Types.BIGINT) {
						long number = Long.parseLong(x);
					} else
					if (paramJdbcType == Types.REAL || paramJdbcType == Types.DOUBLE || paramJdbcType == Types.FLOAT) {
						double number = Double.parseDouble(x);
					} else
					if (paramJdbcType == Types.DECIMAL || paramJdbcType == Types.NUMERIC) {
						BigDecimal number = new BigDecimal(x);
					}
				} catch (NumberFormatException nfe) {
					throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + nfe.getMessage(), "22003");
				}
				setValue(parameterIndex, x);
				break;
			case Types.BOOLEAN:
				if  (x.equalsIgnoreCase("false") || x.equalsIgnoreCase("true") || x.equals("0") || x.equals("1")) {
					setValue(parameterIndex, x);
				} else {
					throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed", "22000");
				}
				break;
			case Types.DATE:
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				try {
					// check if the string represents a valid calendar date or time or timestamp to prevent
					// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
					if (paramJdbcType == Types.DATE) {
						java.sql.Date datum = java.sql.Date.valueOf(x);
					} else
					if (paramJdbcType == Types.TIME || paramJdbcType == Types.TIME_WITH_TIMEZONE) {
						Time tijdstip = Time.valueOf(x);
					} else
					if (paramJdbcType == Types.TIMESTAMP || paramJdbcType == Types.TIMESTAMP_WITH_TIMEZONE) {
						Timestamp tijdstip = Timestamp.valueOf(x);
					}
				} catch (IllegalArgumentException iae) {
					throw new SQLDataException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + iae.getMessage(), "22007");
				}
				/* prefix the string with: date or time or timetz or timestamp or timestamptz */
				setValue(parameterIndex, paramMonetdbType + " '" + x + "'");
				break;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
				// check if the string x contains pairs of hex chars to prevent
				// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
				final int xlen = x.length();
				for (int i = 0; i < xlen; i++) {
					char c = x.charAt(i);
					if (c < '0' || c > '9') {
						if (c < 'A' || c > 'F') {
							if (c < 'a' || c > 'f') {
								throw new SQLDataException("Invalid string for parameter data type " + paramMonetdbType + ". The string may contain only hex chars", "22M28");
							}
						}
					}
				}
				/* prefix the string with: blob */
				setValue(parameterIndex, "blob '" + x + "'");
				break;
			default:
				throw new SQLException("Conversion of string to parameter data type " + paramMonetdbType + " is not (yet) supported", "M1M05");
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.SQLXML
	 * object. The driver converts this to an SQL XML value when it
	 * sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a SQLXML object that maps an SQL XML value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setSQLXML(final int parameterIndex, final SQLXML x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setSQLXML");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value.
	 * The driver converts this to an SQL TIME value when it sends it to
	 * the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTime(final int parameterIndex, final Time x) throws SQLException {
		setTime(parameterIndex, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value,
	 * using the given Calendar object.  The driver uses the Calendar
	 * object to construct an SQL TIME value, which the driver then
	 * sends to the database.  With a Calendar object, the driver can
	 * calculate the time taking into account a custom timezone.  If no
	 * Calendar object is specified, the driver uses the default
	 * timezone, which is that of the virtual machine running the
	 * application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the time
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTime(final int parameterIndex, final Time x, final Calendar cal)
		throws SQLException
	{
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		final String MonetDBType = monetdbType[getParamIdx(parameterIndex)];
		final boolean hasTimeZone = ("timetz".equals(MonetDBType) || "timestamptz".equals(MonetDBType));
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			if (mTimeZ == null) {
				// first time usage, create and keep the mTimeZ object for next usage
				mTimeZ = new SimpleDateFormat("HH:mm:ss.SSSZ");
			}
			final String RFC822 = mTimeZ.format(x);
			setValue(parameterIndex, "timetz '" + RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
		} else {
			// server is not timezone aware for this field, and no
			// calendar given, since we told the server our timezone at
			// connection creation, we can just write a plain timestamp
			// here
			if (cal == null) {
				setValue(parameterIndex, "time '" + x.toString() + "'");
			} else {
				if (mTime == null) {
					// first time usage, create and keep the mTime object for next usage
					mTime = new SimpleDateFormat("HH:mm:ss.SSS");
				}
				mTime.setTimeZone(cal.getTimeZone());
				setValue(parameterIndex, "time '" + mTime.format(x) + "'");
			}
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value.  The driver converts this to an SQL TIMESTAMP value when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x)
		throws SQLException
	{
		setTimestamp(parameterIndex, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value, using the given Calendar object.  The driver uses the
	 * Calendar object to construct an SQL TIMESTAMP value, which the
	 * driver then sends to the database.  With a Calendar object, the
	 * driver can calculate the timestamp taking into account a custom
	 * timezone.  If no Calendar object is specified, the driver uses the
	 * default timezone, which is that of the virtual machine running
	 * the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the
	 *            timestamp
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal)
		throws SQLException
	{
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		final String MonetDBType = monetdbType[getParamIdx(parameterIndex)];
		final boolean hasTimeZone = ("timestamptz".equals(MonetDBType) || "timetz".equals(MonetDBType));
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			if (mTimestampZ == null) {
				// first time usage, create and keep the mTimestampZ object for next usage
				mTimestampZ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
			}
			final String RFC822 = mTimestampZ.format(x);
			setValue(parameterIndex, "timestamptz '" + RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
		} else {
			// server is not timezone aware for this field, and no
			// calendar given, since we told the server our timezone at
			// connection creation, we can just write a plain timestamp here
			if (cal == null) {
				setValue(parameterIndex, "timestamp '" + x.toString() + "'");
			} else {
				if (mTimestamp == null) {
					// first time usage, create and keep the mTimestamp object for next usage
					mTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				}
				mTimestamp.setTimeZone(cal.getTimeZone());
				setValue(parameterIndex, "timestamp '" + mTimestamp.format(x) + "'");
			}
		}
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. A Unicode character has two bytes, with
	 * the first byte being the high byte, and the second being the low byte.
	 * When a very large Unicode value is input to a LONGVARCHAR parameter, it
	 * may be more practical to send it via a java.io.InputStream object. The
	 * data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from Unicode
	 * to the database char format.
	 *
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @deprecated Use setCharacterStream
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a java.io.InputStream object that contains the Unicode
	 *          parameter value as two-byte Unicode characters
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	@Deprecated
	public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length)
		throws SQLException
	{
		throw newSQLFeatureNotSupportedException("setUnicodeStream");
	}

	/**
	 * Sets the designated parameter to the given java.net.URL value. The
	 * driver converts this to an SQL DATALINK value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java.net.URL object to be set
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setURL(final int parameterIndex, final URL x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		setValue(parameterIndex, "url " + MonetWrapper.sq(x.toString()));
	}

	/**
	 * Releases this PreparedStatement object's database and JDBC
	 * resources immediately instead of waiting for this to happen when
	 * it is automatically closed.  It is generally good practice to
	 * release resources as soon as you are finished with them to avoid
	 * tying up database resources.
	 *
	 * Calling the method close on a PreparedStatement object that is
	 * already closed has no effect.
	 *
	 * <b>Note:</b> A PreparedStatement object is automatically closed
	 * when it is garbage collected. When a Statement object is closed,
	 * its current ResultSet object, if one exists, is also closed.
	 */
	@Override
	public void close() {
		try {
			if (!closed && id != -1)
				connection.sendControlCommand("release " + id);
		} catch (SQLException e) {
			// probably server closed connection
		}
		super.close();
	}

	/**
	 * Call close to release the server-sided handle for this
	 * PreparedStatement.
	 *
	 * @deprecated (since="9")
	 */
	@Override
	@Deprecated
	protected void finalize() {
		close();
	}

	//== Java 1.8 methods (JDBC 4.2)

	@Override
	public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType, final int scaleOrLength) throws SQLException {
		// setObject(parameterIndex, x, convertSQLType(targetSqlType), scaleOrLength);	// TODO implement convertSQLType(targetSqlType)
		throw newSQLFeatureNotSupportedException("setObject");
	}

	@Override
	public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType) throws SQLException {
		// setObject(parameterIndex, x, convertSQLType(targetSqlType));	// TODO implement convertSQLType(targetSqlType)
		throw newSQLFeatureNotSupportedException("setObject");
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which must be
	 * an SQL Data Manipulation Language (DML) statement, such as INSERT, UPDATE or DELETE statement;
	 * or an SQL statement that returns nothing, such as a DDL statement.
	 *
	 * This method should be used when the returned row count may exceed Integer.MAX_VALUE.
	 * The default implementation will throw UnsupportedOperationException
	 *
	 * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
	 *         or (2) 0 for SQL statements that return nothing
	 * @throws SQLException if a database access error occurs; this method is called on a closed PreparedStatement
	 *		or the SQL statement returns a ResultSet object
	 */
	@Override
	public long executeLargeUpdate() throws SQLException {
		if (execute() != false)
			throw new SQLException("Query produced a result set", "M1M17");

		return getLargeUpdateCount();
	}

	//== end methods interface PreparedStatement


	//== internal helper methods which do not belong to the JDBC interface

	/**
	 * Sets the given index with the supplied value. If the given index is
	 * out of bounds, and SQLException is thrown.  The given value should
	 * never be null.
	 *
	 * @param parameterIndex the parameter index
	 * @param val the exact String representation to set
	 * @throws SQLException if the given index is out of bounds
	 */
	private final void setValue(final int parameterIndex, final String val) throws SQLException {
		values[getParamIdx(parameterIndex)] = (val == null ? "NULL" : val);
	}

	/**
	 * Transforms the prepare query into a simple SQL query by replacing
	 * the ?'s with the given column contents.
	 * Mind that the JDBC specs allow `reuse' of a value for a column over
	 * multiple executes.
	 *
	 * @return the simple SQL string for the prepare query
	 * @throws SQLException if not all columns are set
	 */
	private final String transform() throws SQLException {
		final StringBuilder buf = new StringBuilder(8 + 12 * size);
		buf.append("exec ").append(id).append('(');
		// check if all columns are set and do a replace
		int col = 0;
		for (int i = 0; i < size; i++) {
			if (column[i] != null)
				continue;
			col++;
			if (col > 1)
				buf.append(',');
			if (values[i] == null)
				throw new SQLException("Cannot execute, parameter " + col + " is missing.", "M1M05");

			buf.append(values[i]);
		}
		buf.append(')');

		return buf.toString();
	}

	/**
	 * Small helper method that formats the "Invalid Parameter Index number ..." message
	 * and creates a new SQLDataException object whose SQLState is set
	 * to "22010": invalid indicator parameter value.
	 *
	 * @param paramIdx the parameter index number
	 * @return a new created SQLDataException object with SQLState 22010
	 */
	private static final SQLDataException newSQLInvalidParameterIndexException(final int paramIdx) {
		return new SQLDataException("Invalid Parameter Index number: " + paramIdx, "22010");
	}
}
