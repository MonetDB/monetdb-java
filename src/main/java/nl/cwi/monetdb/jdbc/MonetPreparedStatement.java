/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import nl.cwi.monetdb.mcl.connection.ControlCommands;
import nl.cwi.monetdb.mcl.responses.ResultSetResponse;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.CharBuffer;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@link PreparedStatement} suitable for the MonetDB database.
 *
 * This implementation of the PreparedStatement interface uses the
 * capabilities of the MonetDB/SQL backend to prepare and execute
 * queries.  The backend takes care of finding the '?'s in the input and
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
 * @author Fabian Groffen, Martin van Dinther
 * @version 0.4
 */
public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {

	/* only parse the date patterns once, use multiple times */
	/** Format of a timestamp with RFC822 time zone */
	private static final SimpleDateFormat MTimestampZ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	/** Format of a timestamp */
	private static final SimpleDateFormat MTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	/** Format of a time with RFC822 time zone */
	private static final SimpleDateFormat MTimeZ = new SimpleDateFormat("HH:mm:ss.SSSZ");
	/** Format of a time */
	private static final SimpleDateFormat MTime = new SimpleDateFormat("HH:mm:ss.SSS");
	/** Format of a date used by mserver */
	private static final SimpleDateFormat MDate = new SimpleDateFormat("yyyy-MM-dd");

	private final MonetConnection connection;
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

	/**
	 * MonetPreparedStatement constructor which checks the arguments for validity. A MonetPreparedStatement is backed
	 * by a {@link MonetStatement}, which deals with most of the required stuff of this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of {@link ResultSet} to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @param prepareQuery the query string to prepare
	 * @throws SQLException if an error occurs during login
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetPreparedStatement(MonetConnection connection, int resultSetType, int resultSetConcurrency,
						   int resultSetHoldability, String prepareQuery) throws SQLException, IllegalArgumentException {
		super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);

		if (!super.execute("PREPARE " + prepareQuery))
			throw new SQLException("Unexpected server response", "M0M10");

		// cheat a bit to get the ID and the number of columns
		id = ((ResultSetResponse)header).getId();
		size = ((ResultSetResponse)header).getTuplecount();
		rscolcnt = ((ResultSetResponse)header).getColumncount();

		// initialise blank finals
		monetdbType = new String[size];
		javaType = new int[size];
		digits = new int[size];
		scale = new int[size];
		schema = new String[size];
		table = new String[size];
		column = new String[size];
		values = new String[size];

		this.connection = connection;

		// fill the arrays
		ResultSet rs = super.getResultSet();
		for (int i = 0; rs.next(); i++) {
			monetdbType[i] = rs.getString("type");
			javaType[i] = MonetDriver.getJavaType(monetdbType[i]);
			digits[i] = rs.getInt("digits");
			scale[i] = rs.getInt("scale");
			if (rscolcnt == 3)
				continue;
			schema[i] = rs.getString("schema");
			table[i] = rs.getString("table");
			column[i] = rs.getString("column");
		}
		rs.close();

		// PreparedStatements are by default poolable
		poolable = true;
	}

	//== methods interface PreparedStatement

	/**
	 * Adds a set of parameters to this PreparedStatement object's batch of commands.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void addBatch() throws SQLException {
		super.addBatch(transform());
	}

	/** override the addBatch from the Statement to throw an SQLException */
	@Override
	public void addBatch(String q) throws SQLException {
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
		for (int i = 0; i < values.length; i++) {
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
	 * @return true if the first result is a ResultSet object; false if the first result is an update count or there is
	 * no result
	 * @throws SQLException if a database access error occurs or an argument is supplied to this method
	 */
	@Override
	public boolean execute() throws SQLException {
		return super.execute(transform());
	}

	/** override the execute from the Statement to throw an SQLException */
	@Override
	public boolean execute(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Executes the SQL query in this PreparedStatement object and returns the ResultSet object generated by the query.
	 *
	 * @return a ResultSet object that contains the data produced by the query never null
	 * @throws SQLException if a database access error occurs or the SQL statement does not return a ResultSet object
	 */
	@Override
	public ResultSet executeQuery() throws SQLException {
		if (!execute())
			throw new SQLException("Query did not produce a result set", "M1M19");

		return getResultSet();
	}

	/** override the executeQuery from the Statement to throw an SQLException */
	@Override
	public ResultSet executeQuery(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which must be an SQL INSERT, UPDATE or DELETE
	 * statement; or an SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @return either (1) the row count for INSERT, UPDATE, or DELETE
	 *         statements or (2) 0 for SQL statements that return nothing
	 * @throws SQLException if a database access error occurs or the SQL statement returns a ResultSet object
	 */
	@Override
	public int executeUpdate() throws SQLException {
		if (execute())
			throw new SQLException("Query produced a result set", "M1M17");
		return getUpdateCount();
	}

	/** override the executeUpdate from the Statement to throw an SQLException */
	@Override
	public int executeUpdate(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
	}

	/**
	 * Returns the index (0..size-1) in the backing arrays for the given resultset column number or an SQLException
	 * when not found
	 */
	private int getColumnIdx(int colnr) throws SQLException {
		int curcol = 0;
		for (int i = 0; i < size; i++) {
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
	private int getParamIdx(int paramnr) throws SQLException {
		int curparam = 0;
		for (int i = 0; i < size; i++) {
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
	 */
	@Override
	public ResultSetMetaData getMetaData() {
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
			public boolean isAutoIncrement(int column) throws SQLException {
				/* TODO: in MonetDB only numeric (int, decimal) columns could be autoincrement/serial
				 * This however requires an expensive dbmd.getColumns(null, schema, table, column)
				 * query call to pull the IS_AUTOINCREMENT value for this column.
				 * See also ResultSetMetaData.isAutoIncrement()
				 */
				// For now we simply always return false.
				return false;
			}

			/**
			 * Indicates whether a column's case matters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return if the column is case sensitive
			 */
			@Override
			public boolean isCaseSensitive(int column) throws SQLException {
				switch (getColumnType(column)) {
					case Types.CLOB:
					case Types.CHAR:
					case Types.VARCHAR:
					case Types.LONGVARCHAR:
						return true;
					default:
						return true;
				}
			}

			/**
			 * Indicates whether the designated column can be used in a where clause.
			 *
			 * Returning true for all here, even for CLOB, BLOB.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true
			 */
			@Override
			public boolean isSearchable(int column) {
				return true;
			}

			/**
			 * Indicates whether the designated column is a cash value. From the MonetDB database perspective it is by
			 * definition unknown whether the value is a currency, because there are no currency datatypes such as
			 * MONEY. With this knowledge we can always return false here.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return false
			 */
			@Override
			public boolean isCurrency(int column) {
				return false;
			}
			
			/**
			 * Indicates whether values in the designated column are signed numbers.
			 * Within MonetDB all numeric types (except oid and ptr) are signed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isSigned(int column) throws SQLException {
				// we can hardcode this, based on the colum type
				switch (getColumnType(column)) {
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.REAL:
					case Types.DOUBLE:
					case Types.BIGINT:
					case Types.NUMERIC:
					case Types.DECIMAL:
						return true;
					default:
						return false;
				}
			}

			/**
			 * Indicates the designated column's normal maximum width in characters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the normal maximum number of characters allowed as the width of the designated column
			 * @throws SQLException if there is no such column
			 */
			@Override
			public int getColumnDisplaySize(int column) throws SQLException {
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
			public String getSchemaName(int column) throws SQLException {
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
			public String getTableName(int column) throws SQLException {
				try {
					return table[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Get the designated column's number of decimal digits. This method is currently very expensive as it
			 * needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getPrecision(int column) throws SQLException {
				try {
					return digits[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Gets the designated column's number of digits to right of the decimal point. This method is currently
			 * very expensive as it needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return scale
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getScale(int column) throws SQLException {
				try {
					return scale[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
			}

			/**
			 * Indicates the nullability of values in the designated column. This method is currently very expensive as
			 * it needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return nullability
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(int column) throws SQLException {
				return columnNullableUnknown;
			}

			/**
			 * Gets the designated column's table's catalog name.
			 * MonetDB does not support the catalog naming concept as in: catalog.schema.table naming scheme
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the name of the catalog for the table in which the given column appears or "" if not applicable
			 */
			@Override
			public String getCatalogName(int column) throws SQLException {
				return null;	// MonetDB does NOT support catalogs
			}

			/**
			 * Indicates whether the designated column is definitely not writable.  MonetDB does not support cursor
			 * updates, so nothing is writable.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isReadOnly(int column) {
				return true;
			}

			/**
			 * Indicates whether it is possible for a write on the designated column to succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isWritable(int column) {
				return false;
			}

			/**
			 * Indicates whether a write on the designated column will definitely succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isDefinitelyWritable(int column) {
				return false;
			}

			/**
			 * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
			 * ResultSet.getObject is called to retrieve a value from the column.  ResultSet.getObject may return a
			 * subclass of the class returned by this method.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the fully-qualified name of the class in the Java programming language that would be used by the
			 * method ResultSet.getObject to retrieve the value in the specified column. This is the class name used
			 * for custom mapping.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnClassName(int column) throws SQLException {
				return MonetResultSet.getClassForType(getColumnType(column)).getName();
			}

			/**
			 * Gets the designated column's suggested title for use in printouts and displays. This is currently equal
			 * to getColumnName().
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the suggested column title
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnLabel(int column) throws SQLException {
				return getColumnName(column);
			}

			/**
			 * Gets the designated column's name
			 *
			 * @param colnr the first column is 1, the second is 2, ...
			 * @return the column name
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnName(int colnr) throws SQLException {
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
			public int getColumnType(int column) throws SQLException {
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
			 * @return type name used by the database. If the column type is a user-defined type, then a
			 * fully-qualified type name is returned.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnTypeName(int column) throws SQLException {
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
	 * Retrieves the number, types and properties of this PreparedStatement object's parameters.
	 *
	 * @return a ParameterMetaData object that contains information about the number, types and properties of this
	 * PreparedStatement object's parameters
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return new pmdw() {
			/**
			 * Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData
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
			 * Retrieves whether null values are allowed in the designated parameter.
			 *
			 * This is currently always unknown for MonetDB/SQL.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return the nullability status of the given parameter; one of ParameterMetaData.parameterNoNulls,
			 * ParameterMetaData.parameterNullable, or ParameterMetaData.parameterNullableUnknown
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(int param) throws SQLException {
				return ParameterMetaData.parameterNullableUnknown;
			}

			/**
			 * Retrieves whether values for the designated parameter can be signed numbers.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public boolean isSigned(int param) throws SQLException {
				// we can hardcode this, based on the column type
				switch (getParameterType(param)) {
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.REAL:
					case Types.DOUBLE:
					case Types.BIGINT:
					case Types.NUMERIC:
					case Types.DECIMAL:
						return true;
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
			public int getPrecision(int param) throws SQLException {
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
			public int getScale(int param) throws SQLException {
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
			public int getParameterType(int param) throws SQLException {
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
			public String getParameterTypeName(int param) throws SQLException {
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
			public String getParameterClassName(int param) throws SQLException {
				String typeName = getParameterTypeName(param);
				Map<String,Class<?>> map = getConnection().getTypeMap();
				Class<?> c;
				if (map.containsKey(typeName)) {
					c = map.get(typeName);
				} else {
					c = MonetResultSet.getClassForType(getParameterType(param));
				}
				return c.getName();
			}

			/**
			 * Retrieves the designated parameter's mode.
			 * For MonetDB/SQL this is currently always unknown.
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
			public int getParameterMode(int param) throws SQLException {
				return ParameterMetaData.parameterModeUnknown;
			}
		};
	}

	/**
	 * Sets the designated parameter to the given Array object.  The
	 * driver converts this to an SQL ARRAY value when it sends it to
	 * the database.
     *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x an Array object that maps an SQL ARRAY value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setArray(int i, Array x) throws SQLException {
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
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
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
	 */
	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
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
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setAsciiStream");
	}

	/**
	 * Sets the designated parameter to the given java.math.BigDecimal value.
	 * The driver converts this to an SQL NUMERIC value when it sends it to the
	 * database.
	 *
	 * @param idx the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBigDecimal(int idx, BigDecimal x) throws SQLException {
		// get array position
		int i = getParamIdx(idx);

		// round to the scale of the DB:
		x = x.setScale(scale[i], RoundingMode.HALF_UP);

		// if precision is now greater than that of the db, throw an error:
		if (x.precision() > digits[i]) {
			throw new SQLDataException("DECIMAL value exceeds allowed digits/scale: " + x.toPlainString() +
					" (" + digits[i] + "/" + scale[i] + ")", "22003");
		}

		// MonetDB doesn't like leading 0's, since it counts them as part of
		// the precision, so let's strip them off. (But be careful not to do
		// this to the exact number "0".)  Also strip off trailing
		// numbers that are inherent to the double representation.
		String xStr = x.toPlainString();
		int dot = xStr.indexOf('.');
		if (dot >= 0)
			xStr = xStr.substring(0, Math.min(xStr.length(), dot + 1 + scale[i]));
		while (xStr.startsWith("0") && xStr.length() > 1)
			xStr = xStr.substring(1);
		setValue(idx, xStr);
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
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
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
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
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
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setBinaryStream");
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver
	 * converts this to an SQL BLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param stream an object that contains the data to set the parameter value to
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBlob(int parameterIndex, InputStream stream) throws SQLException {
		if (stream == null) {
			setNull(parameterIndex, -1);
			return;
		}
		// Some buffer. Size of 8192 is default for BufferedReader, so...
		byte[] arr = new byte[8192];
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		int numChars;
		try {
			while ((numChars = stream.read(arr, 0, arr.length)) > 0) {
				buf.write(arr, 0, numChars);
			}
			setBytes(parameterIndex, buf.toByteArray());
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver
	 * converts this to an SQL BLOB value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a Blob object that maps an SQL BLOB value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		setBytes(parameterIndex, x.getBytes(0, (int) x.length()));
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
	 * @param stream an object that contains the data to set the parameter value to
	 * @param length the number of bytes in the parameter data
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setBlob(int parameterIndex, InputStream stream, long length) throws SQLException {
		if (stream == null) {
			setNull(parameterIndex, -1);
			return;
		}
		try {
			byte[] arr = new byte[(int) length];
			ByteArrayOutputStream buf = new ByteArrayOutputStream((int) length);

			int numChars = stream.read(arr, 0, (int) length);
			buf.write(arr, 0, numChars);
			setBytes(parameterIndex, buf.toByteArray());
		} catch (IOException e) {
			throw new SQLException(e);
		}
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
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setValue(parameterIndex, "" + x);
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
	public void setByte(int parameterIndex, byte x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	private static final String HEXES = "0123456789ABCDEF";
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
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		StringBuilder hex = new StringBuilder(x.length * 2);
		for (byte aX : x) {
			hex.append(HEXES.charAt((aX & 0xF0) >> 4)).append(HEXES.charAt((aX & 0x0F)));
		}
		setValue(parameterIndex, "blob '" + hex.toString() + "'");
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
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		if (reader == null) {
			setNull(parameterIndex, -1);
			return;
		}

		CharBuffer tmp = CharBuffer.allocate(length);
		try {
			reader.read(tmp);
		} catch (IOException e) {
			throw new SQLException(e.getMessage(), "M1M25");
		}
		setString(parameterIndex, tmp.toString());
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
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader, 0);
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
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		// given the implementation of the int-version, downcast is ok
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver
	 * converts this to an SQL CLOB value when it sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x a Clob object that maps an SQL CLOB value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(int i, Clob x) throws SQLException {
		if (x == null) {
			setNull(i, -1);
			return;
		}

		// simply serialise the CLOB into a variable for now... far from
		// efficient, but might work for a few cases...
		// be on your marks: we have to cast the length down!
		setString(i, x.getSubString(1L, (int)(x.length())));
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver
	 * converts this to an SQL CLOB value when it sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param reader an object that contains the data to set the parameter
	 *          value to
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(int i, Reader reader) throws SQLException {
		if (reader == null) {
			setNull(i, -1);
			return;
		}
		// Some buffer. Size of 8192 is default for BufferedReader, so...
		char[] arr = new char[8192];
		StringBuilder buf = new StringBuilder(8192 * 8);
		int numChars;
		try {
			while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
				buf.append(arr, 0, numChars);
			}
			setString(i, buf.toString());
		} catch (IOException e) {
			throw new SQLException(e);
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
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param reader An object that contains the data to set the
	 *        parameter value to.
	 * @param length the number of characters in the parameter data.
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setClob(int i, Reader reader, long length) throws SQLException {
		if (reader == null || length < 0) {
			setNull(i, -1);
			return;
		}
		// simply serialise the CLOB into a variable for now... far from
		// efficient, but might work for a few cases...
		CharBuffer buf = CharBuffer.allocate((int) length); // have to down cast :(
		try {
			reader.read(buf);
		} catch (IOException e) {
			throw new SQLException("failed to read from stream: " +
					e.getMessage(), "M1M25");
		}
		// We have to rewind the buffer, because otherwise toString() returns "".
		buf.rewind();
		setString(i, buf.toString());
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
	public void setDate(int parameterIndex, Date x) throws SQLException {
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
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		if (cal == null) {
			setValue(parameterIndex, "date '" + x.toString() + "'");
		} else {
			MDate.setTimeZone(cal.getTimeZone());
			setValue(parameterIndex, "date '" + MDate.format(x) + "'");
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
	public void setDouble(int parameterIndex, double x) throws SQLException {
		setValue(parameterIndex, "" + x);
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
	public void setFloat(int parameterIndex, float x) throws SQLException {
		setValue(parameterIndex, "" + x);
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
	public void setInt(int parameterIndex, int x) throws SQLException {
		setValue(parameterIndex, "" + x);
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
	public void setLong(int parameterIndex, long x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to a Reader object. The Reader
	 * reads the data till end-of-file is reached. The driver does the
	 * necessary conversion from Java character format to the national
	 * character set in the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNCharacterStream(int i, Reader value) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNCharacterStream");
	}

	/**
	 * Sets the designated parameter to a Reader object. The Reader
	 * reads the data till end-of-file is reached. The driver does the
	 * necessary conversion from Java character format to the national
	 * character set in the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @param length the number of characters in the parameter data.
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNCharacterStream(int i, Reader value, long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNCharacterStream");
	}

	/**
	 * Sets the designated parameter to a java.sql.NClob object. The
	 * driver converts this to a SQL NCLOB value when it sends it to the
	 * database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(int i, Reader value) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNClob");
	}

	/**
	 * Sets the designated parameter to a java.sql.NClob object. The
	 * driver converts this to a SQL NCLOB value when it sends it to the
	 * database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(int i, NClob value) throws SQLException {
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
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param r An object that contains the data to set the parameter
	 *          value to
	 * @param length the number of characters in the parameter data
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNClob(int i, Reader r, long length) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNClob");
	}

	/**
	 * Sets the designated paramter to the given String object. The
	 * driver converts this to a SQL NCHAR or NVARCHAR or LONGNVARCHAR
	 * value (depending on the argument's size relative to the driver's
	 * limits on NVARCHAR values) when it sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param value the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setNString(int i, String value) throws SQLException {
		throw newSQLFeatureNotSupportedException("setNString");
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
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
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
	 * @param paramIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType a value from java.sql.Types
	 * @param typeName the fully-qualified name of an SQL user-defined type;
	 *                 ignored if the parameter is not a user-defined type or
	 *                 REF
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
		// MonetDB/SQL's NULL needs no type
		setNull(paramIndex, sqlType);
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
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @throws SQLException if a database access error occurs or the type of
	 *                      the given object is ambiguous
	 */
	@Override
	public void setObject(int index, Object x) throws SQLException {
		setObject(index, x, javaType[getParamIdx(index)]);
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
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
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
	 * @see Types
	 */
	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
		// this is according to table B-5
		if (x instanceof String) {
			switch (targetSqlType) {
				case Types.TINYINT:
					byte val1;
					try {
						val1 = Byte.parseByte((String)x);
					} catch (NumberFormatException e) {
						val1 = 0;
					}
					setByte(parameterIndex, val1);
					break;
				case Types.SMALLINT:
					short val2;
					try {
						val2 = Short.parseShort((String)x);
					} catch (NumberFormatException e) {
						val2 = 0;
					}
					setShort(parameterIndex, val2);
					break;
				case Types.INTEGER:
					int val3;
					try {
						val3 = Integer.parseInt((String)x);
					} catch (NumberFormatException e) {
						val3 = 0;
					}
					setInt(parameterIndex, val3);
					break;
				case Types.BIGINT:
					long val4;
					try {
						val4 = Long.parseLong((String)x);
					} catch (NumberFormatException e) {
						val4 = 0;
					}
					setLong(parameterIndex, val4);
					break;
				case Types.REAL:
					float val5;
					try {
						val5 = Float.parseFloat((String)x);
					} catch (NumberFormatException e) {
						val5 = 0;
					}
					setFloat(parameterIndex, val5);
					break;
				case Types.DOUBLE:
					double val6;
					try {
						val6 = Double.parseDouble((String)x);
					} catch (NumberFormatException e) {
						val6 = 0;
					}
					setDouble(parameterIndex, val6);
					break;
				case Types.NUMERIC:
					BigInteger val7;
					try {
						val7 = new BigInteger((String)x);
					} catch (NumberFormatException e) {
						try {
							val7 = BigInteger.ZERO;
						} catch (NumberFormatException ex) {
							throw new SQLException("Internal error: unable to create template BigInteger: " + ex.getMessage(), "M0M03");
						}
					}
					setObject(parameterIndex, val7);
					break;
				case Types.DECIMAL:
					BigDecimal val8;
					try {
						val8 = new BigDecimal((String)x);
					} catch (NumberFormatException e) {
						try {
							val8 = BigDecimal.ZERO;
						} catch (NumberFormatException ex) {
							throw new SQLException("Internal error: unable to create template BigDecimal: " + ex.getMessage(), "M0M03");
						}
					}
					val8 = val8.setScale(scale, BigDecimal.ROUND_HALF_UP);
					setBigDecimal(parameterIndex, val8);
					break;
				case Types.BOOLEAN:
					setBoolean(parameterIndex, Boolean.valueOf((String) x));
					break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, (String)x);
					break;
				case Types.LONGVARBINARY:
					setBytes(parameterIndex, ((String)x).getBytes());
					break;
				case Types.BLOB:
					setBlob(parameterIndex, new MonetBlob(((String)x).getBytes()));
					break;
				case Types.CLOB:
					setClob(parameterIndex, new MonetClob((String)x));
					break;
				case Types.DATE:
					Date val9;
					try {
						val9 = Date.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val9 = new Date(0L);
					}
					setDate(parameterIndex, val9);
					break;
				case Types.TIME:
					Time val10;
					try {
						val10 = Time.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val10 = new Time(0L);
					}
					setTime(parameterIndex, val10);
					break;
				case Types.TIMESTAMP:
					Timestamp val11;
					try {
						val11 = Timestamp.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val11 = new Timestamp(0L);
					}
					setTimestamp(parameterIndex, val11);
					break;
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					throw newSQLFeatureNotSupportedException("setObject() with targetType N[VAR]CHAR");
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof BigDecimal || x instanceof BigInteger || x instanceof Byte || x instanceof Short || x instanceof Integer || x instanceof Long || x instanceof Float || x instanceof Double) {
			Number num = (Number)x;
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
					setLong(parameterIndex, num.longValue());
					break;
				case Types.REAL:
					setFloat(parameterIndex, num.floatValue());
					break;
				case Types.DOUBLE:
					setDouble(parameterIndex, num.doubleValue());
					break;
				case Types.NUMERIC:
					if (x instanceof BigInteger) {
						setObject(parameterIndex, x);
					} else {
						setObject(parameterIndex, new BigInteger(Integer.toString(num.intValue())));
					}
					break;
				case Types.DECIMAL:
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
					setString(parameterIndex, x.toString());
					break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof Boolean) {
			boolean val = (Boolean) x;
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
				case Types.DOUBLE:
					setDouble(parameterIndex, (val ? 1.0 : 0.0));  // do no cast to (double) as it generates a compiler warning
					break;
				case Types.NUMERIC:
					setObject(parameterIndex, val ? BigInteger.ONE : BigInteger.ZERO);
					break;
				case Types.DECIMAL:
					setBigDecimal(parameterIndex, val ? BigDecimal.ONE : BigDecimal.ZERO);
				 	break;
				case Types.BIT:
				case Types.BOOLEAN:
					setBoolean(parameterIndex, val);
					break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, "" + val);
					break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof byte[]) {
			switch (targetSqlType) {
				case Types.LONGVARBINARY:
					setBytes(parameterIndex, (byte[]) x);
					break;
				case Types.BLOB:
					setBlob(parameterIndex, new MonetBlob((byte[]) x));
					break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof Date || x instanceof Timestamp || x instanceof Time || x instanceof Calendar || x instanceof java.util.Date) {
			switch (targetSqlType) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, x.toString());
					break;
				case Types.DATE:
					if (x instanceof Time) {
						throw new SQLException("Conversion not allowed", "M1M05");
					} else if (x instanceof Date) {
						setDate(parameterIndex, (Date)x);
					} else if (x instanceof Timestamp) {
						setDate(parameterIndex, new Date(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setDate(parameterIndex, new Date(((java.util.Date)x).getTime()));
					} else { //Calendar
						setDate(parameterIndex, new Date(((Calendar)x).getTimeInMillis()));
					}
					break;
				case Types.TIME:
					if (x instanceof Time) {
						setTime(parameterIndex, (Time)x);
					} else if (x instanceof Date) {
						throw new SQLException("Conversion not allowed", "M1M05");
					} else if (x instanceof Timestamp) {
						setTime(parameterIndex, new Time(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setTime(parameterIndex, new Time(((java.util.Date)x).getTime()));
					} else { //Calendar
						setTime(parameterIndex, new Time(((Calendar)x).getTimeInMillis()));
					}
					break;
				case Types.TIMESTAMP:
					if (x instanceof Time) {
						throw new SQLException("Conversion not allowed", "M1M05");
					} else if (x instanceof Date) {
						setTimestamp(parameterIndex, new Timestamp(((Date)x).getTime()));
					} else if (x instanceof Timestamp) {
						setTimestamp(parameterIndex, (Timestamp)x);
					} else if (x instanceof java.util.Date) {
						setTimestamp(parameterIndex, new Timestamp(((java.util.Date)x).getTime()));
					} else { //Calendar
						setTimestamp(parameterIndex, new Timestamp(((Calendar)x).getTimeInMillis()));
					}
					break;
				default:
					throw new SQLException("Conversion not allowed", "M1M05");
			}
		} else if (x instanceof Array) {
			setArray(parameterIndex, (Array)x);
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob)x);
		} else if (x instanceof Clob) {
			setClob(parameterIndex, (Clob)x);
		} else if (x instanceof Struct) {
			// I have no idea how to do this...
			throw newSQLFeatureNotSupportedException("setObject() with object of type Struct");
		} else if (x instanceof Ref) {
			setRef(parameterIndex, (Ref)x);
		} else if (x instanceof java.net.URL) {
			setURL(parameterIndex, (java.net.URL)x);
		} else if (x instanceof RowId) {
			setRowId(parameterIndex, (RowId)x);
		} else if (x instanceof SQLXML) {
			throw newSQLFeatureNotSupportedException("setObject() with object of type SQLXML");
		} else if (x instanceof SQLData) { // not in JDBC4.1???
			SQLData sx = (SQLData)x;
			final int paramnr = parameterIndex;
			final String sqltype = sx.getSQLTypeName();
			SQLOutput out = new SQLOutput() {
				@Override
				public void writeString(String x) throws SQLException {
					// special situation, this is when a string
					// representation is given, but we need to prefix it
					// with the actual sqltype the server expects, or we
					// will get an error back
					setValue(paramnr,
						sqltype + " '" + x.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'");
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
				public void writeDate(Date x) throws SQLException {
					setDate(paramnr, x);
				}

				@Override
				public void writeTime(Time x) throws SQLException {
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
			throw newSQLFeatureNotSupportedException("setObject() with object of type Class");
		}
	}

	/**
	 * Sets the designated parameter to the given REF(<structured-type>) value.
	 * The driver converts this to an SQL REF value when it sends it to the
	 * database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x an SQL REF value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setRef(int i, Ref x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setRef");
	}

	/**
	 * Sets the designated parameter to the given java.sql.RowId object.
	 * The driver converts this to a SQL ROWID value when it sends it to
	 * the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void setRowId(int i, RowId x) throws SQLException {
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
	public void setShort(int parameterIndex, short x) throws SQLException {
		setValue(parameterIndex, "" + x);
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
	public void setString(int parameterIndex, String x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		setValue(parameterIndex, "'" + x.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'");
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
	public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
		throw newSQLFeatureNotSupportedException("setSQLXML");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value.
	 * The driver converts this to an SQL TIME value when it sends it to
	 * the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTime(int index, Time x) throws SQLException {
		setTime(index, x, null);
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
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the time
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTime(int index, Time x, Calendar cal) throws SQLException {
		if (x == null) {
			setNull(index, -1);
			return;
		}

		boolean hasTimeZone = monetdbType[getParamIdx(index)].endsWith("tz");
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			String RFC822 = MTimeZ.format(x);
			setValue(index, "timetz '" + RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
		} else {
			// server is not timezone aware for this field, and no
			// calendar given, since we told the server our timezone at
			// connection creation, we can just write a plain timestamp
			// here
			if (cal == null) {
				setValue(index, "time '" + x.toString() + "'");
			} else {
				MTime.setTimeZone(cal.getTimeZone());
				setValue(index, "time '" + MTime.format(x) + "'");
			}
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value.  The driver converts this to an SQL TIMESTAMP value when
	 * it sends it to the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTimestamp(int index, Timestamp x) throws SQLException {
		setTimestamp(index, x, null);
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
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the
	 *            timestamp
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public void setTimestamp(int index, Timestamp x, Calendar cal) throws SQLException {
		if (x == null) {
			setNull(index, -1);
			return;
		}

		boolean hasTimeZone = monetdbType[getParamIdx(index)].endsWith("tz");
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			String RFC822 = MTimestampZ.format(x);
			setValue(index, "timestamptz '" + RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
		} else {
			// server is not timezone aware for this field, and no
			// calendar given, since we told the server our timezone at
			// connection creation, we can just write a plain timestamp
			// here
			if (cal == null) {
				setValue(index, "timestamp '" + x.toString() + "'");
			} else {
				MTimestamp.setTimeZone(cal.getTimeZone());
				setValue(index, "timestamp '" + MTimestamp.format(x) + "'");
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
	 * @deprecated
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a java.io.InputStream object that contains the Unicode
	 *          parameter value as two-byte Unicode characters
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	@Deprecated
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
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
	public void setURL(int parameterIndex, URL x) throws SQLException {
		setString(parameterIndex, x.toString());
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
				connection.sendControlCommand(ControlCommands.RELEASE, id);
		} catch (SQLException e) {
			// probably server closed connection
		}
		super.close();
	}

	/**
	 * Call close to release the server-sided handle for this PreparedStatement.
	 */
	@Override
	protected void finalize() {
		close();
	}

	//== end methods interface PreparedStatement

	/**
	 * Sets the given index with the supplied value. If the given index is out of bounds, and SQLException is thrown.
	 * The given value should never be null.
	 *
	 * @param index the parameter index
	 * @param val the exact String representation to set
	 * @throws SQLException if the given index is out of bounds
	 */
	private void setValue(int index, String val) throws SQLException {
		values[getParamIdx(index)] = val;
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
	private String transform() throws SQLException {
		StringBuilder buf = new StringBuilder(8 + 12 * size);
		buf.append("exec ");
		buf.append(id);
		buf.append('(');
		// check if all columns are set and do a replace
		int col = 0;
		for (int i = 0; i < size; i++) {
			if (column[i] != null)
				continue;
			col++;
			if (col > 1)
				buf.append(',');
			if (values[i] == null) throw
				new SQLException("Cannot execute, parameter " + col + " is missing.", "M1M05");

			buf.append(values[i]);
		}
		buf.append(')');

		return buf.toString();
	}

	/**
	 * Small helper method that formats the "Invalid Parameter Index number ..." message
	 * and creates a new SQLException object whose SQLState is set to "M1M05".
	 *
	 * @param paramIdx the parameter index number
	 * @return a new created SQLException object with SQLState M1M05
	 */
	private static SQLException newSQLInvalidParameterIndexException(int paramIdx) {
		return new SQLException("Invalid Parameter Index number: " + paramIdx, "M1M05");
	}

	/**
	 * Small helper method that formats the "Method ... not implemented" message
	 * and creates a new SQLFeatureNotSupportedException object
	 * whose SQLState is set to "0A000".
	 *
	 * @param name the method name
	 * @return a new created SQLFeatureNotSupportedException object with SQLState 0A000
	 */
	private static SQLFeatureNotSupportedException newSQLFeatureNotSupportedException(String name) {
		return new SQLFeatureNotSupportedException("Method " + name + " not implemented", "0A000");
	}
}
