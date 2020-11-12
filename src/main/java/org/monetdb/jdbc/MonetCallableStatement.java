/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

package org.monetdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;	// new as of Java 1.8
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@link CallableStatement} suitable for the MonetDB database.
 *
 * The interface used to execute SQL stored procedures.
 * The JDBC API provides a stored procedure SQL escape syntax that allows stored procedures to be called in a standard way for all RDBMSs.
 * This escape syntax has one form that includes a result parameter (MonetDB does not support this) and one that does not.
 * If used, the result parameter must be registered as an OUT parameter (MonetDB does not support this).
 * The other parameters can be used for input, output or both. Parameters are referred to sequentially, by number, with the first parameter being 1.
 *
 * <code>
 *  { call procedure-name [ (arg1, arg2, ...) ] }
 *  { ?= call procedure-name [ (arg1, arg2, ...) ] }
 * </code>
 *
 * IN parameter values are set using the set methods inherited from PreparedStatement.
 * The type of all OUT parameters must be registered prior to executing the stored procedure;
 * their values are retrieved after execution via the get methods provided here.
 * Note: MonetDB does not support OUT or INOUT parameters. Only input parameters are supported.
 *
 * A CallableStatement can return one ResultSet object or multiple ResultSet objects.
 * Multiple ResultSet objects are handled using operations inherited from Statement.
 *
 * For maximum portability, a call's ResultSet objects and update counts should be processed prior to getting the values of output parameters.
 *
 * This implementation of the CallableStatement interface reuses the implementation of MonetPreparedStatement for
 * preparing the call statement, bind parameter values and execute the call, possibly multiple times with different parameter values.
 *
 * Note: currently we can not implement:
 * - all getXyz(parameterIndex/parameterName, ...) methods
 * - all registerOutParameter(parameterIndex/parameterName, int sqlType, ...) methods
 * - wasNull() method
 * because output parameters in stored procedures are not supported by MonetDB.
 *
 * @author Martin van Dinther
 * @version 1.1
 */

public class MonetCallableStatement
	extends MonetPreparedStatement
	implements CallableStatement
{
	/**
	 * MonetCallableStatement constructor which checks the arguments for validity.
	 * A MonetCallableStatement is backed by a {@link MonetPreparedStatement},
	 * which deals with most of the required stuff of this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of {@link ResultSet} to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @param callQuery - an SQL CALL statement that may contain one or more '?' parameter placeholders.
	 *	Typically this statement is specified using JDBC call escape syntax:
	 *	{ call procedure_name [(?,?, ...)] }
	 *	or
	 *	{ ?= call procedure_name [(?,?, ...)] }
	 * @throws SQLException if an error occurs during creation
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetCallableStatement(
			final MonetConnection connection,
			final int resultSetType,
			final int resultSetConcurrency,
			final int resultSetHoldability,
			final String callQuery)
		throws SQLException, IllegalArgumentException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability,
			removeEscapes(callQuery)
		);
	}

	/** parse call query string on
	 *  { [?=] call <procedure-name> [(<arg1>,<arg2>, ...)] }
	 * and remove the JDBC escapes pairs: { and }
	 */
	final private static String removeEscapes(final String query) {
		if (query == null)
			return null;

		final int firstAccOpen = query.indexOf("{");
		if (firstAccOpen == -1)
			// nothing to remove
			return query;

		final int len = query.length();
		final StringBuilder buf = new StringBuilder(len);
		int countAccolades = 0;
		// simple scanner which copies all characters except the first '{' and matching '}' character
		// we currently do not check if 'call' appears after the first '{' and before the '}' character
		// we currently also do not deal correctly with { or } appearing as comment or as part of a string value
		for (int i = 0; i < len; i++) {
			char c = query.charAt(i);
			switch (c) {
			case '{':
				countAccolades++;
				if (i == firstAccOpen)
					continue;
				else
					buf.append(c);
				break;
			case '}':
				countAccolades--;
				if (i > firstAccOpen && countAccolades == 0)
					continue;
				else
					buf.append(c);
				break;
			default:
				buf.append(c);
			}
		}
		return buf.toString();
	}

	/** utility method to convert a parameter name to an int (which represents the parameter index)
	 *  this will only succeed for strings like: "1", "2", "3", etc
	 *  throws SQLException if it cannot convert the string to an integer number
	 */
	final private int nameToIndex(final String parameterName) throws SQLException {
		if (parameterName == null)
			throw new SQLException("Missing parameterName value", "22002");
		try {
			return Integer.parseInt(parameterName);
		} catch (NumberFormatException nfe) {
			throw new SQLException("Cannot convert parameterName '" + parameterName + "' to integer value", "22010");
		}
	}


	// methods of interface CallableStatement

	// all getXyz(parameterIndex/parameterName, ...) methods are NOT supported
	// because output parameters in stored procedures are not supported by MonetDB
	@Override
	public Array getArray(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getArray");
	}
	@Override
	public Array getArray(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getArray");
	}
	@Override
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBigDecimal");
	}
	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBigDecimal");
	}
	@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBigDecimal");
	}
	@Override
	public Blob getBlob(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBlob");
	}
	@Override
	public Blob getBlob(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBlob");
	}
	@Override
	public boolean getBoolean(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBoolean");
	}
	@Override
	public boolean getBoolean(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBoolean");
	}
	@Override
	public byte getByte(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getByte");
	}
	@Override
	public byte getByte(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getByte");
	}
	@Override
	public byte[] getBytes(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBytes");
	}
	@Override
	public byte[] getBytes(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getBytes");
	}
	@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getCharacterStream");
	}
	@Override
	public Reader getCharacterStream(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getCharacterStream");
	}
	@Override
	public Clob getClob(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getClob");
	}
	@Override
	public Clob getClob(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getClob");
	}
	@Override
	public Date getDate(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDate");
	}
	@Override
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDate");
	}
	@Override
	public Date getDate(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDate");
	}
	@Override
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDate");
	}
	@Override
	public double getDouble(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDouble");
	}
	@Override
	public double getDouble(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getDouble");
	}
	@Override
	public float getFloat(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getFloat");
	}
	@Override
	public float getFloat(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getFloat");
	}
	@Override
	public int getInt(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getInt");
	}
	@Override
	public int getInt(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getInt");
	}
	@Override
	public long getLong(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getLong");
	}
	@Override
	public long getLong(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getLong");
	}
	@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNCharacterStream");
	}
	@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNCharacterStream");
	}
	@Override
	public NClob getNClob(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNClob");
	}
	@Override
	public NClob getNClob(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNClob");
	}
	@Override
	public String getNString(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNString");
	}
	@Override
	public String getNString(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getNString");
	}
	@Override
	public Object getObject(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public Object getObject(int parameterIndex, Map<String,Class<?>> map) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public Object getObject(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException {
		throw newSQLFeatureNotSupportedException("getObject");
	}
	@Override
	public Ref getRef(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getRef");
	}
	@Override
	public Ref getRef(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getRef");
	}
	@Override
	public RowId getRowId(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getRowId");
	}
	@Override
	public RowId getRowId(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getRowId");
	}
	@Override
	public short getShort(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getShort");
	}
	@Override
	public short getShort(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getShort");
	}
	@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getSQLXML");
	}
	@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getSQLXML");
	}
	@Override
	public String getString(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getString");
	}
	@Override
	public String getString(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getString");
	}
	@Override
	public Time getTime(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTime");
	}
	@Override
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTime");
	}
	@Override
	public Time getTime(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTime");
	}
	@Override
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTime");
	}
	@Override
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTimestamp");
	}
	@Override
	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTimestamp");
	}
	@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTimestamp");
	}
	@Override
	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		throw newSQLFeatureNotSupportedException("getTimestamp");
	}
	@Override
	public URL getURL(int parameterIndex) throws SQLException {
		throw newSQLFeatureNotSupportedException("getURL");
	}
	@Override
	public URL getURL(String parameterName) throws SQLException {
		throw newSQLFeatureNotSupportedException("getURL");
	}


	// all registerOutParameter(parameterIndex/parameterName, int sqlType, ...) methods are NOT supported
	// because output parameters in stored procedures are not supported by MonetDB
	@Override
	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}


	// all setXyz(parameterName, ...) methods are mapped to setXyz(nameToIndex(parameterName), ...) methods
	// this only works for parameter names "1", "2", "3", etc.
	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		setAsciiStream(nameToIndex(parameterName), x);
	}
	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		setAsciiStream(nameToIndex(parameterName), x, length);
	}
	@Override
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		setAsciiStream(nameToIndex(parameterName), x, length);
	}
	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		setBigDecimal(nameToIndex(parameterName), x);
	}
	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		setBinaryStream(nameToIndex(parameterName), x);
	}
	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		setBinaryStream(nameToIndex(parameterName), x, length);
	}
	@Override
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		setBinaryStream(nameToIndex(parameterName), x, length);
	}
	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		setBlob(nameToIndex(parameterName), x);
	}
	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		setBlob(nameToIndex(parameterName), inputStream);
	}
	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		setBlob(nameToIndex(parameterName), inputStream, length);
	}
	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		setBoolean(nameToIndex(parameterName), x);
	}
	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		setByte(nameToIndex(parameterName), x);
	}
	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		setBytes(nameToIndex(parameterName), x);
	}
	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		setCharacterStream(nameToIndex(parameterName), reader);
	}
	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		setCharacterStream(nameToIndex(parameterName), reader, length);
	}
	@Override
	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		setCharacterStream(nameToIndex(parameterName), reader, length);
	}
	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		setClob(nameToIndex(parameterName), x);
	}
	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		setClob(nameToIndex(parameterName), reader);
	}
	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		setClob(nameToIndex(parameterName), reader, length);
	}
	@Override
	public void setDate(String parameterName, Date x) throws SQLException {
		setDate(nameToIndex(parameterName), x);
	}
	@Override
	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		setDate(nameToIndex(parameterName), x, cal);
	}
	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		setDouble(nameToIndex(parameterName), x);
	}
	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		setFloat(nameToIndex(parameterName), x);
	}
	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		setInt(nameToIndex(parameterName), x);
	}
	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		setLong(nameToIndex(parameterName), x);
	}
	@Override
	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		setNCharacterStream(nameToIndex(parameterName), value);
	}
	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		setNCharacterStream(nameToIndex(parameterName), value, length);
	}
	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		setNClob(nameToIndex(parameterName), value);
	}
	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		setNClob(nameToIndex(parameterName), reader);
	}
	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		setNClob(nameToIndex(parameterName), reader, length);
	}
	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		setNString(nameToIndex(parameterName), value);
	}
	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		setNull(nameToIndex(parameterName), sqlType);
	}
	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		setNull(nameToIndex(parameterName), sqlType, typeName);
	}
	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		setObject(nameToIndex(parameterName), x);
	}
	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		setObject(nameToIndex(parameterName), x, targetSqlType);
	}
	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		setObject(nameToIndex(parameterName), x, targetSqlType, scale);
	}
	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		setRowId(nameToIndex(parameterName), x);
	}
	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		setShort(nameToIndex(parameterName), x);
	}
	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		setSQLXML(nameToIndex(parameterName), xmlObject);
	}
	@Override
	public void setString(String parameterName, String x) throws SQLException {
		setString(nameToIndex(parameterName), x);
	}
	@Override
	public void setTime(String parameterName, Time x) throws SQLException {
		setTime(nameToIndex(parameterName), x);
	}
	@Override
	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		setTime(nameToIndex(parameterName), x, cal);
	}
	@Override
	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		setTimestamp(nameToIndex(parameterName), x);
	}
	@Override
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		setTimestamp(nameToIndex(parameterName), x, cal);
	}
	@Override
	public void setURL(String parameterName, URL val) throws SQLException {
		setURL(nameToIndex(parameterName), val);
	}

	/* Retrieves whether the last OUT parameter read had the value of SQL NULL. */
	@Override
	public boolean wasNull() throws SQLException {
		// wasNull() method is NOT supported
		// because output parameters in stored procedures are not supported by MonetDB
		throw newSQLFeatureNotSupportedException("wasNull");
	}

	//== Java 1.8 methods (JDBC 4.2)

	@Override
	public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
		// setObject(nameToIndex(parameterName), x, convertSQLType(targetSqlType), scaleOrLength);	// TODO implement convertSQLType(targetSqlType)
		throw newSQLFeatureNotSupportedException("setObject");
	}

	@Override
	public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
		// setObject(nameToIndex(parameterName), x, convertSQLType(targetSqlType));	// TODO implement convertSQLType(targetSqlType)
		throw newSQLFeatureNotSupportedException("setObject");
	}

	@Override
	public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}
	@Override
	public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
		throw newSQLFeatureNotSupportedException("registerOutParameter");
	}

	// end methods interface CallableStatement
}
