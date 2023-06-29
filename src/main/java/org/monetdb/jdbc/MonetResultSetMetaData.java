/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 *<pre>
 * A {@link ResultSetMetaData} suitable for the MonetDB database.
 *
 * An object that can be used to get information about the types and
 * properties of the columns in a ResultSet object.
 *</pre>
 *
 * @author Martin van Dinther
 * @version 1.0
 */
final class MonetResultSetMetaData
	extends MonetWrapper
	implements ResultSetMetaData
{
	/** The parental Connection object */
	private final MonetConnection conn;

	/** The number of columns, it can be zero !! */
	private final int colCount;

	/** The schema names of the columns in this ResultSet */
	private final String[] schemas;
	/** The table names of the columns in this ResultSet */
	private final String[] tables;
	/** The names of the columns in this ResultSet */
	private final String[] columns;
	/** The MonetDB type names of the columns in this ResultSet */
	private final String[] types;
	/** The JDBC SQL type codes of the columns in this ResultSet.
	  * The content will be derived once from the MonetDB String[] types */
	private final int[] JdbcSQLTypes;
	/** The lengths of the columns in this ResultSet */
	private final int[] lengths;
	/** The precisions of the columns in this ResultSet */
	private final int[] precisions;
	/** The scales of the columns in this ResultSet */
	private final int[] scales;

	/**
	 * For the methods: isNullable() and isAutoIncrement(), we need to query the server.
	 * To do this efficiently we query many columns combined in one SELECT
	 * query and cache the results in following arrays.
	 */
	private final int array_size;
	/** Whether info for a column is already queried or not */
	private final boolean[] _is_queried;
	/** Whether info for a column is already fetched or not */
	private final boolean[] _is_fetched;
	/** The nullability of the columns in this ResultSet */
	private final int[] _isNullable;
	/** The auto increment property of the columns in this ResultSet */
	private final boolean[] _isAutoincrement;
	/** an upper bound value to calculate the range of columns to query */
	private int nextUpperbound;


	/**
	 * Main constructor backed by the given connection and header.
	 *
	 * @param connection the parent connection
	 * @param header a ResultSetResponse containing the metadata
	 * @throws IllegalArgumentException if called with null for one of the arguments
	 */
	MonetResultSetMetaData(
		final MonetConnection connection,
		final MonetConnection.ResultSetResponse header)
		throws IllegalArgumentException
	{
		if (connection == null) {
			throw new IllegalArgumentException("Connection may not be null!");
		}
		if (header == null) {
			throw new IllegalArgumentException("Header may not be null!");
		}
		this.conn = connection;
		schemas = header.getSchemaNames();
		tables = header.getTableNames();
		columns = header.getNames();
		lengths = header.getColumnLengths();
		types = header.getTypes();
		precisions = header.getColumnPrecisions();
		scales = header.getColumnScales();

		colCount = columns.length;
		if (columns.length != tables.length || columns.length != types.length ) {
			throw new IllegalArgumentException("Inconsistent Header metadata");
		}

		// derive the JDBC SQL type codes from the types[] names once
		JdbcSQLTypes = new int[types.length];
		for (int i = 0; i < types.length; i++) {
			int javaSQLtype = MonetDriver.getJdbcSQLType(types[i]);
			if (javaSQLtype == Types.CLOB) {
				if (connection.mapClobAsVarChar())
					javaSQLtype = Types.VARCHAR;
			} else
			if (javaSQLtype == Types.BLOB) {
				if (connection.mapBlobAsVarBinary())
					javaSQLtype = Types.VARBINARY;
			}
			JdbcSQLTypes[i] = javaSQLtype;
		}

		// initialize structures for storing columns info on nullability and autoincrement
		array_size = colCount + 1;  // add 1 as in JDBC columns start from 1 (array from 0).
		_is_queried = new boolean[array_size];
		_is_fetched = new boolean[array_size];
		_isNullable = new int[array_size];
		_isAutoincrement = new boolean[array_size];
		nextUpperbound = array_size;
	}

	/**
	 * Alternative constructor backed by the given connection and metadata arrays.
	 * It is used by MonetPreparedStatement.
	 *
	 * @param connection the parent connection
	 * @param colcount the number of result columns, it can be zero !!
	 * @param schemas the schema names
	 * @param tables the table names
	 * @param columns the column names
	 * @param types the MonetDB type names
	 * @param jdbcTypes the JDBC SQL type codes
	 * @param lengths the maximum display length for each column
	 * @param precisions the precision for each column
	 * @param scales the scale for each column
	 * @throws IllegalArgumentException if called with null for one of the arguments
	 */
	MonetResultSetMetaData(
		final MonetConnection connection,
		final int colcount,
		final String[] schemas,
		final String[] tables,
		final String[] columns,
		final String[] types,
		final int[] jdbcTypes,
		final int[] lengths,
		final int[] precisions,
		final int[] scales)
		throws IllegalArgumentException
	{
		if (connection == null) {
			throw new IllegalArgumentException("Connection may not be null!");
		}
		if (schemas == null) {
			throw new IllegalArgumentException("Schemas may not be null!");
		}
		if (tables == null) {
			throw new IllegalArgumentException("Tables may not be null!");
		}
		if (columns == null) {
			throw new IllegalArgumentException("Columns may not be null!");
		}
		if (types == null) {
			throw new IllegalArgumentException("MonetDB Types may not be null!");
		}
		if (jdbcTypes == null) {
			throw new IllegalArgumentException("JDBC Types may not be null!");
		}
		if (lengths == null) {
			throw new IllegalArgumentException("Lengths may not be null!");
		}
		if (precisions == null) {
			throw new IllegalArgumentException("Precisions may not be null!");
		}
		if (scales == null) {
			throw new IllegalArgumentException("Scales may not be null!");
		}
		if (columns.length != tables.length || columns.length != types.length ) {
			throw new IllegalArgumentException("Inconsistent Header metadata");
		}
		this.conn = connection;
		this.colCount = colcount;
		this.schemas = schemas;
		this.tables = tables;
		this.columns = columns;
		this.lengths = lengths;
		this.types = types;
		this.JdbcSQLTypes = jdbcTypes;
		this.precisions = precisions;
		this.scales = scales;

		// initialize structures for storing columns info on nullability and autoincrement
		array_size = colCount + 1;  // add 1 as in JDBC columns start from 1 (array from 0).
		_is_queried = new boolean[array_size];
		_is_fetched = new boolean[array_size];
		_isNullable = new int[array_size];
		_isAutoincrement = new boolean[array_size];
		nextUpperbound = array_size;
	}

	/**
	 * Returns the number of columns in this ResultSet object.
	 *
	 * @return the number of columns
	 */
	@Override
	public int getColumnCount() {
		// for debug: System.out.println("In rsmd.getColumnCount() = " + colCount + ". this rsmd object = " + this.toString());
		return colCount;
	}

	/**
	 * Indicates whether the designated column is automatically numbered.
	 *
	 * This method is currently very expensive for BIGINT,
	 * INTEGER, SMALLINT and TINYINT result column types
	 * as it needs to retrieve the information from the
	 * database using an SQL meta data query.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if there is no such column or a database access error occurs
	 */
	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		// only few integer types can be auto incrementable in MonetDB
		// see: https://www.monetdb.org/Documentation/SQLReference/DataTypes/SerialDatatypes
		switch (getColumnType(column)) {
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
				try {
					if (_is_fetched[column] != true) {
						fetchColumnInfo(column);
					}
					return _isAutoincrement[column];
				} catch (IndexOutOfBoundsException e) {
					throw MonetResultSet.newSQLInvalidColumnIndexException(column);
				}
		}

		return false;
	}

	/**
	 * Indicates whether a column's case matters.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true for all character string columns else false
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		switch (getColumnType(column)) {
			case Types.CHAR:
		/*	case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR */
			case Types.CLOB:
				return true;
			case Types.VARCHAR:
				final String monettype = getColumnTypeName(column);
				if (monettype != null && monettype.length() == 4) {
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
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isSearchable(final int column) throws SQLException {
		checkColumnIndexValidity(column);
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
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isCurrency(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		return false;
	}

	/**
	 * Indicates the nullability of values in the designated column.
	 *
	 * This method is currently very expensive as it needs to
	 * retrieve the information from the database using an SQL
	 * meta data query.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the nullability status of the given column; one of
	 *	columnNoNulls, columnNullable or columnNullableUnknown
	 * @throws SQLException if there is no such column or a database access error occurs
	 */
	@Override
	public int isNullable(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		try {
			if (_is_fetched[column] != true) {
				fetchColumnInfo(column);
			}
			return _isNullable[column];
		} catch (IndexOutOfBoundsException e) {
			throw MonetResultSet.newSQLInvalidColumnIndexException(column);
		}
	}

	/**
	 * Indicates whether values in the designated column are signed
	 * numbers.
	 * Within MonetDB all numeric types (except oid and ptr) are signed.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isSigned(final int column) throws SQLException {
		// we can hardcode this, based on the colum type
		switch (getColumnType(column)) {
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.DECIMAL:
			case Types.NUMERIC:
				return true;
			case Types.BIGINT:
				final String monettype = getColumnTypeName(column);
				if (monettype != null && monettype.length() == 3) {
					// data of type oid or ptr is not signed
					if ("oid".equals(monettype)
					 || "ptr".equals(monettype))
						return false;
				}
				return true;
		//	All other types should return false
		//	case Types.BOOLEAN:
		//	case Types.DATE:	// can year be negative?
		//	case Types.TIME:	// can time be negative?
		//	case Types.TIME_WITH_TIMEZONE:
		//	case Types.TIMESTAMP:	// can year be negative?
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
		checkColumnIndexValidity(column);
		if (lengths != null) {
			try {
				int len = lengths[column - 1];
				if (len == 0) {
					final String monettype = getColumnTypeName(column);
					// in case of inet it always has 0 as length. we need to correct it.
					if (monettype != null && "inet".equals(monettype)) {
						len = 18;	// 128.127.126.125/24
					}
				}
				return len;
			} catch (IndexOutOfBoundsException e) {
				throw MonetResultSet.newSQLInvalidColumnIndexException(column);
			}
		}
		return 1;
	}

	/**
	 * Gets the designated column's suggested title for use in
	 * printouts and displays. The suggested title is usually
	 * specified by the SQL AS clause. If a SQL AS is not specified,
	 * the value returned from getColumnLabel will be the same as
	 * the value returned by the getColumnName method.
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
	public String getColumnName(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		try {
			return columns[column - 1];
		} catch (IndexOutOfBoundsException e) {
			throw MonetResultSet.newSQLInvalidColumnIndexException(column);
		}
	}

	/**
	 * Gets the designated column's table's catalog name.
	 * MonetDB does not support the catalog naming concept as in: catalog.schema.table naming scheme
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return null or the name of the catalog for the table in which the given
	 *         column appears or "" if not applicable
	 * @throws SQLException if there is no such column
	 */
	@Override
	public String getCatalogName(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		return null;	// MonetDB does NOT support catalog qualifiers

	}

	/**
	 * Get the designated column's schema name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return schema name or "" if not applicable
	 * @throws SQLException if there is no such column
	 */
	@Override
	public String getSchemaName(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		if (schemas != null) {
			try {
				return schemas[column - 1];
			} catch (IndexOutOfBoundsException e) {
				throw MonetResultSet.newSQLInvalidColumnIndexException(column);
			}
		}
		return "";
	}

	/**
	 * Gets the designated column's table name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return table name or "" if not applicable
	 * @throws SQLException if there is no such column
	 */
	@Override
	public String getTableName(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		if (tables != null) {
			try {
				return tables[column - 1];
			} catch (IndexOutOfBoundsException e) {
				throw MonetResultSet.newSQLInvalidColumnIndexException(column);
			}
		}
		return "";
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
		checkColumnIndexValidity(column);
		try {
			return JdbcSQLTypes[column - 1];
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
		checkColumnIndexValidity(column);
		try {
			final String monettype = types[column - 1];
			if (monettype != null && monettype.endsWith("_interval")) {
				/* convert the interval type names to valid SQL data type names,
				 * such that generic applications can use them in create table statements
				 */
				if ("day_interval".equals(monettype))
					return "interval day";
				if ("month_interval".equals(monettype))
					return "interval month";
				if ("sec_interval".equals(monettype))
					return "interval second";
			}
			return monettype;
		} catch (IndexOutOfBoundsException e) {
			throw MonetResultSet.newSQLInvalidColumnIndexException(column);
		}
	}

	/**
	 * Get the designated column's specified column size.
	 * For numeric data, this is the maximum precision.
	 * For character data, this is the length in characters.
	 * For datetime datatypes, this is the length in characters
	 * of the String representation (assuming the maximum
	 * allowed precision of the fractional seconds component).
	 * For binary data, this is the length in bytes.
	 * For the ROWID datatype, this is the length in bytes.
	 * 0 is returned for data types where the column size is not applicable.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return maximum precision or length in characters
	 * @throws SQLException if there is no such column
	 */
	@Override
	public int getPrecision(final int column) throws SQLException {
		switch (getColumnType(column)) {
			case Types.BIGINT:
				return 19;
			case Types.INTEGER:
				return 10;
			case Types.SMALLINT:
				return 5;
			case Types.TINYINT:
				return 3;
			case Types.REAL:
				return 7;
			case Types.FLOAT:
			case Types.DOUBLE:
				return 15;
			case Types.DECIMAL:
			case Types.NUMERIC:
				// these data types have a variable precision (max precision is 38)
				if (precisions != null) {
					try {
						return precisions[column - 1];
					} catch (IndexOutOfBoundsException e) {
						throw MonetResultSet.newSQLInvalidColumnIndexException(column);
					}
				}
				return 18;
			case Types.CHAR:
			case Types.VARCHAR:
		/*	case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR */
			case Types.CLOB:
				// these data types have a variable length
				if (precisions != null) {
					try {
						int prec = precisions[column - 1];
						if (prec <= 0) {
							// apparently no positive precision or max length could be fetched
							// use columnDisplaySize() value as fallback
							prec = getColumnDisplaySize(column);
							precisions[column - 1] = prec;
						}
						return prec;
					} catch (IndexOutOfBoundsException e) {
						throw MonetResultSet.newSQLInvalidColumnIndexException(column);
					}
				}
				// apparently no precisions array is available
				// use columnDisplaySize() value as alternative
				return getColumnDisplaySize(column);
			case Types.BINARY:
			case Types.VARBINARY:
		/*	case Types.LONGVARBINARY: // MonetDB doesn't use type LONGVARBINARY */
			case Types.BLOB:
				// these data types have a variable length
				if (precisions != null) {
					try {
						int prec = precisions[column - 1];
						if (prec <= 0) {
							// apparently no positive precision or max length could be fetched
							// use columnDisplaySize() value as fallback
							// It expect number of bytes, not number of hex chars
							prec = (getColumnDisplaySize(column) / 2) +1;
							precisions[column - 1] = prec;
						}
						return prec;
					} catch (IndexOutOfBoundsException e) {
						throw MonetResultSet.newSQLInvalidColumnIndexException(column);
					}
				}
				// apparently no precisions array is available
				// use columnDisplaySize() value as alternative
				// It expect number of bytes, not number of hex chars
				return (getColumnDisplaySize(column) / 2) +1;
			case Types.DATE:
				return 10;	// 2020-10-08
			case Types.TIME:
				return 15;	// 21:51:34.399753
			case Types.TIME_WITH_TIMEZONE:
				return 21;	// 21:51:34.399753+02:00
			case Types.TIMESTAMP:
				return 26;	// 2020-10-08 21:51:34.399753
			case Types.TIMESTAMP_WITH_TIMEZONE:
				return 32;	// 2020-10-08 21:51:34.399753+02:00
			case Types.BOOLEAN:
				return 1;
			default:
				// All other types should return 0
				return 0;
		}
	}

	/**
	 * Gets the designated column's number of digits to right of
	 * the decimal point.
	 * 0 is returned for data types where the scale is not applicable.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return scale
	 * @throws SQLException if there is no such column
	 */
	@Override
	public int getScale(final int column) throws SQLException {
		switch (getColumnType(column)) {
			case Types.DECIMAL:
			case Types.NUMERIC:
			{
				// these data types may have a variable scale, max scale is 38

				// Special handling for: day_interval and sec_interval as they are
				// mapped to Types.NUMERIC and Types.DECIMAL types (see MonetDriver typeMap)
				// They appear to have a fixed scale (tested against Oct2020)
				final String monettype = getColumnTypeName(column);
				if ("interval day".equals(monettype))
					return 0;
				if ("interval second".equals(monettype))
					return 3;

				if (scales != null) {
					try {
						return scales[column - 1];
					} catch (IndexOutOfBoundsException e) {
						throw MonetResultSet.newSQLInvalidColumnIndexException(column);
					}
				}
				return 0;
			}
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				if (scales != null) {
					try {
						return scales[column - 1];
					} catch (IndexOutOfBoundsException e) {
						throw MonetResultSet.newSQLInvalidColumnIndexException(column);
					}
				}
				// support microseconds, so scale 6
				return 6;	// 21:51:34.399753

			// All other types should return 0
		//	case Types.BIGINT:
		//	case Types.INTEGER:
		//	case Types.SMALLINT:
		//	case Types.TINYINT:
		//	case Types.REAL:
		//	case Types.FLOAT:
		//	case Types.DOUBLE:
		//	case Types.CHAR:
		//	case Types.VARCHAR:
		//	case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR
		//	case Types.CLOB:
		//	case Types.BINARY:
		//	case Types.VARBINARY:
		//	case Types.LONGVARBINARY: // MonetDB doesn't use type LONGVARBINARY
		//	case Types.BLOB:
		//	case Types.DATE:
		//	case Types.BOOLEAN:
			default:
				return 0;
		}
	}

	/**
	 * Indicates whether the designated column is definitely not
	 * writable.  MonetDB does not support cursor updates, so
	 * nothing is writable.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isReadOnly(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		return true;
	}

	/**
	 * Indicates whether it is possible for a write on the
	 * designated column to succeed.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isWritable(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		return false;
	}

	/**
	 * Indicates whether a write on the designated column will
	 * definitely succeed.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if there is no such column
	 */
	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		return false;
	}

	/**
	 * Returns the fully-qualified name of the Java class whose instances
	 * are manufactured if the method ResultSet.getObject is called to
	 * retrieve a value from the column.
	 * ResultSet.getObject may return a subclass of the class returned by
	 * this method.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the fully-qualified name of the class in the Java programming
	 *	language that would be used by the method ResultSet.getObject
	 *	to retrieve the value in the specified column. This is the
	 *	class name used for custom mapping.
	 * @throws SQLException if there is no such column
	 */
	@Override
	public String getColumnClassName(final int column) throws SQLException {
		checkColumnIndexValidity(column);
		try {
			final String MonetDBType = types[column - 1];
			Class<?> type = null;
			if (conn != null) {
				final java.util.Map<String,Class<?>> map = conn.getTypeMap();
				if (map != null && map.containsKey(MonetDBType)) {
					type = (Class)map.get(MonetDBType);
				}
			}
			if (type == null) {
				// fallback to the standard SQL type Class mappings
				type = MonetDriver.getClassForType(JdbcSQLTypes[column - 1]);
			}
			if (type != null) {
				return type.getCanonicalName();
			}
			throw new SQLException("column type mapping null: " + MonetDBType, "M0M03");
		} catch (IndexOutOfBoundsException e) {
			throw MonetResultSet.newSQLInvalidColumnIndexException(column);
		}
	}


	/**
	 * A private utility method to check validity of column index number
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @throws java.sql.SQLDataException when invalid column index number
	 */
	private final void checkColumnIndexValidity(final int column) throws java.sql.SQLDataException {
		if (column < 1 || column > colCount)
			throw MonetResultSet.newSQLInvalidColumnIndexException(column);
	}

	/**
	 * A private method to fetch the isNullable and isAutoincrement values
	 * combined for a specific column.
	 * The fetched values are stored in the array caches.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @throws SQLException if there is no such column
	 */
	private final void fetchColumnInfo(final int column) throws SQLException {
		// for debug: System.out.println("fetchColumnInfo(" + column + ")");
		checkColumnIndexValidity(column);
		if (_is_fetched[column] != true) {
			// fetch column info for multiple columns combined in one go
			fetchManyColumnsInfo(column);
		}
		if (_is_fetched[column])
			return;

		// apparently no data could be fetched for this resultset column, fall back to defaults
		_isNullable[column] = columnNullableUnknown;
		_isAutoincrement[column] = false;
	}

	/**
	 * A private method to fetch the isNullable and isAutoincrement values
	 * for many fully qualified columns combined in one SQL query to reduce the number of queries sent.
	 * As fetching this meta information from the server per column is costly we combine the querying of
	 * the isNullable and isAutoincrement values and cache it in internal arrays.
	 * We also do this for many columns combined in one query to reduce
	 * the number of queries needed for fetching this metadata for all resultset columns.
	 * Many generic JDBC database tools (e.g. SQuirreL, DBeaver) request this meta data for each
	 * column of each resultset, so these optimisations reduces the number of meta data queries significantly.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @throws SQLException if a database access error occurs
	 */
	private final void fetchManyColumnsInfo(final int column) throws SQLException {
		// for debug: System.out.println("fetchManyColumnsInfo(" + column + ")");

		// Most queries have less than 80 resultset columns
		// So 80 is a good balance between speedup (up to 79x) and size of generated query sent to server
		final int MAX_COLUMNS_PER_QUERY = 80;

		// Determine the optimal startcol to make use of fetching up to 80 columns in one query.
		int startcol = column;
		if ((startcol > 1) && (startcol + MAX_COLUMNS_PER_QUERY >= nextUpperbound)) {
			// we can fetch info from more columns in one query if we start with a lower startcol
			startcol = nextUpperbound - MAX_COLUMNS_PER_QUERY;
			if (startcol < 1) {
				startcol = 1;
			} else
			if (startcol > column) {
				startcol = column;
			}
			nextUpperbound = startcol;	// next time this nextUpperbound value will be used
			// for debug: System.out.println("fetchManyColumnsInfo(" + column + ")" + (startcol != column ? " changed into startcol: " + startcol : "") + " nextUpperbound: " + nextUpperbound);
		}

		final StringBuilder query = new StringBuilder(410 + (MAX_COLUMNS_PER_QUERY * 150));
		/* next SQL query is a simplified version of query in MonetDatabaseMetaData.getColumns(), to fetch only the needed attributes of a column */
		query.append("SELECT " +
			"s.\"name\" AS schnm, " +
			"t.\"name\" AS tblnm, " +
			"c.\"name\" AS colnm, " +
			"cast(CASE c.\"null\" WHEN true THEN " + ResultSetMetaData.columnNullable +
				" WHEN false THEN " + ResultSetMetaData.columnNoNulls +
				" ELSE " + ResultSetMetaData.columnNullableUnknown +
				" END AS int) AS nullable, " +
			"cast(CASE WHEN c.\"default\" IS NOT NULL AND c.\"default\" LIKE 'next value for %' THEN true ELSE false END AS boolean) AS isautoincrement " +
		"FROM \"sys\".\"columns\" c " +
		"JOIN \"sys\".\"tables\" t ON c.\"table_id\" = t.\"id\" " +
		"JOIN \"sys\".\"schemas\" s ON t.\"schema_id\" = s.\"id\" " +
		"WHERE ");

		/* combine the conditions for multiple (up to 80) columns into the WHERE-clause */
		String schName = null;
		String tblName = null;
		String colName = null;
		int queriedcolcount = 0;
		for (int col = startcol; col < array_size && queriedcolcount < MAX_COLUMNS_PER_QUERY; col++) {
			if (_is_fetched[col] != true) {
				if (_is_queried[col] != true) {
					_isNullable[col] = columnNullableUnknown;
					_isAutoincrement[col] = false;
					schName = getSchemaName(col);
					if (schName != null && !schName.isEmpty()) {
						tblName = getTableName(col);
						if (tblName != null && !tblName.isEmpty()) {
							colName = getColumnName(col);
							if (colName != null && !colName.isEmpty()) {
								if (queriedcolcount > 0)
									query.append(" OR ");
								query.append("(s.\"name\" = ").append(MonetWrapper.sq(schName));
								query.append(" AND t.\"name\" = ").append(MonetWrapper.sq(tblName));
								query.append(" AND c.\"name\" = ").append(MonetWrapper.sq(colName));
								query.append(")");
								_is_queried[col] = true;	// flag it
								queriedcolcount++;
							}
						}
					}
					if (_is_queried[col] != true) {
						// make sure we do not try to query it again next time as it is not queryable
						_is_fetched[col] = true;
					}
				}
			}
		}

		if (queriedcolcount == 0)
			return;

		// execute query to get information on queriedcolcount (or less) columns.
		final java.sql.Statement stmt = conn.createStatement();
		if (stmt != null) {
			// for debug: System.out.println("SQL (len " + query.length() + "): " + query.toString());
			final java.sql.ResultSet rs = stmt.executeQuery(query.toString());
			if (rs != null) {
				String rsSchema = null;
				String rsTable = null;
				String rsColumn = null;
				while (rs.next()) {
					rsSchema = rs.getString(1);	// col 1 is schnm
					rsTable = rs.getString(2);	// col 2 is tblnm
					rsColumn = rs.getString(3);	// col 3 is colnm
					// find the matching schema.table.column entry in the array
					for (int col = 1; col < array_size; col++) {
						if (_is_fetched[col] != true && _is_queried[col]) {
							colName = getColumnName(col);
							if (colName != null && colName.equals(rsColumn)) {
								tblName = getTableName(col);
								if (tblName != null && tblName.equals(rsTable)) {
									schName = getSchemaName(col);
									if (schName != null && schName.equals(rsSchema)) {
										// found matching entry
										// for debug: System.out.println("Found match at [" + col + "] for " + schName + "." + tblName + "." + colName);
										_isNullable[col] = rs.getInt(4);	// col 4 is nullable (or "NULLABLE")
										_isAutoincrement[col] = rs.getBoolean(5); // col 5 is isautoincrement (or "IS_AUTOINCREMENT")
										_is_fetched[col] = true;
										queriedcolcount--;
										// we found the match, exit the for-loop
										col = array_size;
									}
								}
							}
						}
					}
				}
				rs.close();
			}
			stmt.close();
		}

		if (queriedcolcount != 0) {
			// not all queried columns have resulted in a returned data row.
			// make sure we do not match those columns again next run
			for (int col = startcol; col < array_size; col++) {
				if (_is_fetched[col] != true && _is_queried[col]) {
					_is_fetched[col] = true;
					// for debug: System.out.println("Found NO match at [" + col + "] for " + getSchemaName(col) + "." + getTableName(col) + "." + getColumnName(col));
				}
			}
		}
	}
}

