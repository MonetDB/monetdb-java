/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

package org.monetdb.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Types;

/**
 *<pre>
 * A {@link ParameterMetaData} suitable for the MonetDB database.
 *
 * An object that can be used to get information about the types and properties
 * for each parameter marker in a PreparedStatement or CallableStatement object.
 *</pre>
 *
 * @author Martin van Dinther
 * @version 1.0
 */
final class MonetParameterMetaData
	extends MonetWrapper
	implements ParameterMetaData
{
	/** The parental Connection object */
	private final MonetConnection conn;

	/** The number of parameters, it can be zero !! */
	private final int paramCount;

	/** The MonetDB type names of the parameters in the PreparedStatement */
	private final String[] monetdbTypes;
	/** The JDBC SQL type codes of the parameters in the PreparedStatement */
	private final int[] JdbcSQLTypes;
	/** The precisions of the parameters in the PreparedStatement */
	private final int[] precisions;
	/** The scales of the parameters in the PreparedStatement */
	private final int[] scales;

	/**
	 * Constructor backed by the given connection and metadata arrays.
	 * It is used by MonetPreparedStatement.
	 *
	 * @param connection the parent connection
	 * @param paramcount the number of parameters, it can be zero !!
	 * @param types the MonetDB type names
	 * @param jdbcTypes the JDBC SQL type codes
	 * @param precisions the precision for each parameter
	 * @param scales the scale for each parameter
	 * @throws IllegalArgumentException if called with null for one of the arguments
	 */
	MonetParameterMetaData(
		final MonetConnection connection,
		final int paramcount,
		final String[] types,
		final int[] jdbcTypes,
		final int[] precisions,
		final int[] scales)
		throws IllegalArgumentException
	{
		if (connection == null) {
			throw new IllegalArgumentException("Connection may not be null!");
		}
		if (types == null) {
			throw new IllegalArgumentException("MonetDB Types may not be null!");
		}
		if (jdbcTypes == null) {
			throw new IllegalArgumentException("JDBC Types may not be null!");
		}
		if (precisions == null) {
			throw new IllegalArgumentException("Precisions may not be null!");
		}
		if (scales == null) {
			throw new IllegalArgumentException("Scales may not be null!");
		}
		if (types.length != precisions.length || types.length != (paramcount +1)) {
			throw new IllegalArgumentException("Inconsistent Parameters metadata");
		}
		this.conn = connection;
		this.paramCount = paramcount;
		this.monetdbTypes = types;
		this.JdbcSQLTypes = jdbcTypes;
		this.precisions = precisions;
		this.scales = scales;
	}

	/**
	 * Retrieves the number of parameters in the PreparedStatement object
	 * for which this ParameterMetaData object contains information.
	 *
	 * @return the number of parameters
	 */
	@Override
	public int getParameterCount() {
		return paramCount;
	}

	/**
	 * Retrieves whether null values are allowed in the
	 * designated parameter.
	 *
	 * This is currently always unknown for MonetDB/SQL.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return the nullability status of the given parameter;
	 *         one of ParameterMetaData.parameterNoNulls,
	 *         ParameterMetaData.parameterNullable, or
	 *         ParameterMetaData.parameterNullableUnknown
	 */
	@Override
	public int isNullable(final int param) throws SQLException {
		checkParameterIndexValidity(param);
		return ParameterMetaData.parameterNullableUnknown;
	}

	/**
	 * Retrieves whether values for the designated parameter can
	 * be signed numbers.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return true if so; false otherwise
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public boolean isSigned(final int param) throws SQLException {
		// we can hardcode this, based on the parameter type
		switch (getParameterType(param)) {
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.DECIMAL:
			case Types.NUMERIC:
			case Types.DATE:	// year can be negative
			case Types.TIMESTAMP:	// year can be negative
			case Types.TIMESTAMP_WITH_TIMEZONE:
				return true;
			case Types.BIGINT:
			{
				final String monettype = getParameterTypeName(param);
				// data of type oid or ptr is not signed
				if ("oid".equals(monettype)
				 || "ptr".equals(monettype))
					return false;
				return true;
			}
			default:
				return false;
		}
	}

	/**
	 * Retrieves the designated parameter's specified column size.
	 * The returned value represents the maximum column size for
	 * the given parameter.
	 * For numeric data, this is the maximum precision.
	 * For character data, this is the length in characters.
	 * For datetime datatypes, this is the length in characters
	 * of the String representation (assuming the maximum allowed
	 * precision of the fractional seconds component).
	 * For binary data, this is the length in bytes.
	 * For the ROWID datatype, this is the length in bytes.
	 * 0 is returned for data types where the column size is not applicable.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return precision
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public int getPrecision(final int param) throws SQLException {
		switch (getParameterType(param)) {
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
				try {
					return precisions[param];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			case Types.CHAR:
			case Types.VARCHAR:
		/*	case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR */
			case Types.CLOB:
				// these data types have a variable length
				try {
					return precisions[param];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
			case Types.BINARY:
			case Types.VARBINARY:
		/*	case Types.LONGVARBINARY: // MonetDB doesn't use type LONGVARBINARY */
			case Types.BLOB:
				// these data types have a variable length
				// It expects number of bytes, not number of hex chars
				try {
					return precisions[param];
				} catch (IndexOutOfBoundsException e) {
					throw newSQLInvalidParameterIndexException(param);
				}
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
	 * Retrieves the designated parameter's number of digits to
	 * right of the decimal point.
	 * 0 is returned for data types where the scale is not applicable.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return scale
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public int getScale(final int param) throws SQLException {
		checkParameterIndexValidity(param);
		try {
			return scales[param];
		} catch (IndexOutOfBoundsException e) {
			throw newSQLInvalidParameterIndexException(param);
		}
	}

	/**
	 * Retrieves the designated parameter's SQL type.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return SQL type from java.sql.Types
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public int getParameterType(final int param) throws SQLException {
		checkParameterIndexValidity(param);
		try {
			return JdbcSQLTypes[param];
		} catch (IndexOutOfBoundsException e) {
			throw newSQLInvalidParameterIndexException(param);
		}
	}

	/**
	 * Retrieves the designated parameter's database-specific type name.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return type the name used by the database.  If the
	 *         parameter type is a user-defined type, then a
	 *         fully-qualified type name is returned.
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public String getParameterTypeName(final int param) throws SQLException {
		checkParameterIndexValidity(param);
		try {
			final String monettype = monetdbTypes[param];
			if (monettype != null && monettype.endsWith("_interval")) {
				/* convert the interval type names to valid SQL data type names */
				switch (precisions[param]) {
					case 1: return "interval year";
					case 2: return "interval year to month";
					case 3: return "interval month";
					case 4: return "interval day";
					case 5: return "interval day to hour";
					case 6: return "interval day to minute";
					case 7: return "interval day to second";
					case 8: return "interval hour";
					case 9: return "interval hour to minute";
					case 10: return "interval hour to second";
					case 11: return "interval minute";
					case 12: return "interval minute to second";
					case 13: return "interval second";
					default:
					{	// fall back to the 3 available monettype names
						if ("day_interval".equals(monettype))
							return "interval day";
						if ("month_interval".equals(monettype))
							return "interval month";
						if ("sec_interval".equals(monettype))
							return "interval second";
					}
				}
			}
			return monettype;
		} catch (IndexOutOfBoundsException e) {
			throw newSQLInvalidParameterIndexException(param);
		}
	}

	/**
	 * Retrieves the fully-qualified name of the Java class whose instances
	 * should be passed to the method PreparedStatement.setObject.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return the fully-qualified name of the class in the Java
	 *         programming language that would be used by the
	 *         method PreparedStatement.setObject to set the
	 *         value in the specified parameter. This is the
	 *         class name used for custom mapping.
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public String getParameterClassName(final int param) throws SQLException {
		final String MonetDBType = getParameterTypeName(param);
		final java.util.Map<String,Class<?>> map = conn.getTypeMap();
		final Class<?> type;
		if (map != null && map.containsKey(MonetDBType)) {
			type = (Class)map.get(MonetDBType);
		} else {
			type = MonetDriver.getClassForType(getParameterType(param));
		}
		if (type != null) {
			return type.getCanonicalName();
		}
		throw new SQLException("parameter type mapping null: " + MonetDBType, "M0M03");
	}

	/**
	 * Retrieves the designated parameter's mode.
	 * For MonetDB/SQL we currently only support INput parameters.
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @return mode of the parameter; one of
	 *         ParameterMetaData.parameterModeIn,
	 *         ParameterMetaData.parameterModeOut,
	 *         ParameterMetaData.parameterModeInOut or
	 *         ParameterMetaData.parameterModeUnknown.
	 */
	@Override
	public int getParameterMode(final int param) throws SQLException {
		checkParameterIndexValidity(param);
		return ParameterMetaData.parameterModeIn;
	}


	/**
	 * A private utility method to check validity of parameter index number
	 *
	 * @param param - the first parameter is 1, the second is 2, ...
	 * @throws SQLDataException when invalid parameter index number
	 */
	private final void checkParameterIndexValidity(final int param) throws SQLDataException {
		if (param < 1 || param > paramCount)
			throw newSQLInvalidParameterIndexException(param);
	}

	/**
	 * Small helper method that formats the "Invalid Parameter Index number ..."
	 * message and creates a new SQLDataException object whose SQLState is set
	 * to "22010": invalid indicator parameter value.
	 *
	 * @param paramIdx the parameter index number
	 * @return a new created SQLDataException object with SQLState 22010
	 */
	private final SQLDataException newSQLInvalidParameterIndexException(final int paramIdx) {
		return new SQLDataException("Invalid Parameter Index number: " + paramIdx, "22010");
	}
}
