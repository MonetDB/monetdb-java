/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package org.monetdb.jdbc;

import org.monetdb.mcl.net.MonetUrlParser;
import org.monetdb.mcl.net.Parameter;
import org.monetdb.mcl.net.Target;
import org.monetdb.mcl.net.ValidationError;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *<pre>
 * A JDBC {@link Driver} suitable for the MonetDB RDBMS.
 *
 * This driver will be used by the DriverManager to determine if an URL
 * is to be handled by this driver, and if it does, then this driver
 * will supply a Connection suitable for MonetDB.
 *
 * This class has no explicit constructor, the default constructor
 * generated by the Java compiler will be sufficient since nothing has
 * to be set in order to use this driver.
 *
 * This Driver supports MonetDB database URLs. MonetDB URLs are defined as:
 * <code>jdbc:monetdb://&lt;host&gt;[:&lt;port&gt;]/&lt;database&gt;</code>
 * where [:&lt;port&gt;] denotes that a port is optional. If not
 * given, port 50000 will be used.
 *</pre>
 *
 * @author Fabian Groffen
 */
public final class MonetDriver implements Driver {
	// the url kind will be jdbc:monetdb://<host>[:<port>]/<database>
	// Chapter 9.2.1 from Sun JDBC 3.0 specification

	// initialize this class: register it at the DriverManager
	// Chapter 9.2 from Sun JDBC 3.0 specification
	static {
		try {
			DriverManager.registerDriver(new MonetDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//== methods of interface Driver

	/**
	 * Retrieves whether the driver thinks that it can open a connection to the
	 * given URL. Typically drivers will return true if they understand the
	 * subprotocol specified in the URL and false if they do not.
	 *
	 * @param url the URL of the database
	 * @return true if this driver understands the given URL; false otherwise
	 */
	@Override
	public boolean acceptsURL(final String url) {
        if (url == null)
			return false;
        if (url.startsWith("jdbc:monetdb:") || url.startsWith("jdbc:monetdbs:"))
			return true;
        return false;
	}

	/**
	 * Attempts to make a database connection to the given URL. The driver
	 * should return "null" if it realizes it is the wrong kind of driver to
	 * connect to the given URL. This will be common, as when the JDBC driver
	 * manager is asked to connect to a given URL it passes the URL to each
	 * loaded driver in turn.
	 *
	 * The driver should throw an SQLException if it is the right driver to
	 * connect to the given URL but has trouble connecting to the database.
	 *
	 * The java.util.Properties argument can be used to pass arbitrary string
	 * tag/value pairs as connection arguments. Normally at least "user" and
	 * "password" properties should be included in the Properties object.
	 *
	 * @param url the URL of the database to which to connect
	 * @param info a list of arbitrary string tag/value pairs as connection
	 *        arguments. Normally at least a "user" and "password" property
	 *        should be included
	 * @return a Connection object that represents a connection to the URL
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public Connection connect(final String url, Properties info)
		throws SQLException
	{
		// url should be of style jdbc:monetdb://<host>/<database>
		if (!acceptsURL(url))
			return null;

		try {
			Target target = new Target(url, info);
			return new MonetConnection(target);
		} catch (ValidationError | URISyntaxException e) {
			throw new SQLException(e.getMessage());
		}
	}

	/**
	 * Retrieves the driver's major version number. Initially this should be 1.
	 *
	 * @return this driver's major version number
	 */
	@Override
	public int getMajorVersion() {
		// defer to the static version of this method
		return getDriverMajorVersion();
	}

	/**
	 * Gets the driver's minor version number. Initially this should be 0.
	 *
	 * @return this driver's minor version number
	 */
	@Override
	public int getMinorVersion() {
		// defer to the static version of this method
		return getDriverMinorVersion();
	}

	/**
	 * Gets information about the possible properties for this driver.
	 *
	 * The getPropertyInfo method is intended to allow a generic GUI tool to
	 * discover what properties it should prompt a human for in order to get
	 * enough information to connect to a database. Note that depending on the
	 * values the human has supplied so far, additional values may become
	 * necessary, so it may be necessary to iterate though several calls to the
	 * getPropertyInfo method.
	 *
	 * @param url the URL of the database to which to connect
	 * @param info a proposed list of tag/value pairs that will be sent on
	 *        connect open
	 * @return an array of DriverPropertyInfo objects describing possible
	 *         properties. This array may be an empty array if no properties
	 *         are required.
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
		if (!acceptsURL(url))
			return null;

		final String[] boolean_choices = new String[] { "true", "false" };
		final DriverPropertyInfo[] dpi = new DriverPropertyInfo[10];	// we currently support 10 connection properties

		DriverPropertyInfo prop = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
		prop.required = true;
		prop.description = "The user loginname to use when authenticating on the database server";
		dpi[0] = prop;

		prop = new DriverPropertyInfo("password", info != null ? info.getProperty("password") : null);
		prop.required = true;
		prop.description = "The password to use when authenticating on the database server";
		dpi[1] = prop;

		prop = new DriverPropertyInfo("debug", "false");
		prop.required = false;
		prop.description = "Whether or not to create a log file for debugging purposes";
		prop.choices = boolean_choices;
		dpi[2] = prop;

		prop = new DriverPropertyInfo("logfile", null);
		prop.required = false;
		prop.description = "The filename to write the debug log to. Only takes effect if debug is set to true. If the file exists, an incrementing number is added, till the filename is unique.";
		dpi[3] = prop;

		prop = new DriverPropertyInfo("language", "sql");
		prop.required = false;
		prop.description = "What language to use for MonetDB conversations (experts only)";
		prop.choices = new String[] { "sql", "mal" };
		dpi[4] = prop;

		prop = new DriverPropertyInfo("hash", null);
		prop.required = false;
		prop.description = "Force the use of the given hash algorithm (SHA512 or SHA384 or SHA256 or SHA1) during challenge response";
		prop.choices = new String[] { "SHA512", "SHA384", "SHA256", "SHA1" };
		dpi[5] = prop;

		prop = new DriverPropertyInfo("treat_blob_as_binary", "true");
		prop.required = false;
		prop.description = "Should blob columns be mapped to Types.VARBINARY instead of Types.BLOB in ResultSets and PreparedStatements"; // recommend for increased performance due to less overhead
		prop.choices = boolean_choices;
		dpi[6] = prop;

		prop = new DriverPropertyInfo("treat_clob_as_varchar", "true");
		prop.required = false;
		prop.description = "Should clob columns be mapped to Types.VARCHAR instead of Types.CLOB in ResultSets and PreparedStatements"; // recommend for increased performance due to less overhead
		prop.choices = boolean_choices;
		dpi[7] = prop;

		prop = new DriverPropertyInfo("so_timeout", "0");
		prop.required = false;
		prop.description = "Defines the maximum time to wait in milliseconds on a blocking read socket call"; // this corresponds to the Connection.setNetworkTimeout() method introduced in JDBC 4.1
		dpi[8] = prop;

		prop = new DriverPropertyInfo("autocommit", "true");
		prop.required = false;
		prop.description = "Whether the connection should start in auto-commit mode";
		prop.choices = boolean_choices;
		dpi[9] = prop;

		return dpi;
	}

	/**
	 * Reports whether this driver is a genuine JDBC Compliant&trade; driver. A
	 * driver may only report true here if it passes the JDBC compliance tests;
	 * otherwise it is required to return false.
	 *
	 * JDBC compliance requires full support for the JDBC API and full support
	 * for SQL 92 Entry Level. It is expected that JDBC compliant drivers will
	 * be available for all the major commercial databases.
	 *
	 * This method is not intended to encourage the development of non-JDBC
	 * compliant drivers, but is a recognition of the fact that some vendors are
	 * interested in using the JDBC API and framework for lightweight databases
	 * that do not support full database functionality, or for special databases
	 * such as document information retrieval where a SQL implementation may not
	 * be feasible.
	 *
	 * @return true if this driver is JDBC Compliant; false otherwise
	 */
	@Override
	public boolean jdbcCompliant() {
		// We're not fully JDBC compliant, but what we support is compliant
		return false;
	}

	/**
	 * Return the parent Logger of all the Loggers used by this data source.
	 * This should be the Logger farthest from the root Logger that is
	 * still an ancestor of all of the Loggers used by this data source.
	 * Configuring this Logger will affect all of the log messages
	 * generated by the data source.
	 * In the worst case, this may be the root Logger.
	 *
	 * @return the parent Logger for this data source
	 * @throws SQLFeatureNotSupportedException if the data source does
	 *         not use java.util.logging
	 * @since 1.7
	 */
	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw MonetWrapper.newSQLFeatureNotSupportedException("getParentLogger");
	}

	//== end methods of interface driver


	/**
	 * Get MonetDB JDBC Driver major version number
	 * method called by MonetDatabaseMetaData methods
	 * @return MonetDB JDBC Driver major version number
	 */
	static final int getDriverMajorVersion() {
		// defer to the generated MonetVersion class
		return MonetVersion.majorVersion;
	}

	/**
	 * Get MonetDB JDBC Driver minor version number
	 * method called by MonetDatabaseMetaData methods
	 * @return MonetDB JDBC Driver minor version number
	 */
	static final int getDriverMinorVersion() {
		// defer to the generated MonetVersion class
		return MonetVersion.minorVersion;
	}

	/**
	 * Returns a touched up identifying version string of this driver.
	 * It is made public as it is called from  org/monetdb/client/JdbcClient.java
	 * @return the version string
	 */
	public static final String getDriverVersion() {
		return MonetVersion.driverVersion;
	}

	/** A static Map containing the mapping between MonetDB types and Java SQL types */
	/* use SELECT sqlname, * FROM sys.types order by 1, id; to view all MonetDB types */
	/* see http://docs.oracle.com/javase/8/docs/api/java/sql/Types.html to view all supported java SQL types */
	private static final java.util.Map<String, Integer> typeMap = new java.util.HashMap<String, Integer>();
	static {
		// fill the typeMap once
		// typeMap.put("any", Integer.valueOf(Types.???));
		typeMap.put("bigint", Integer.valueOf(Types.BIGINT));
		typeMap.put("blob", Integer.valueOf(Types.BLOB));
		typeMap.put("boolean", Integer.valueOf(Types.BOOLEAN));
		typeMap.put("char", Integer.valueOf(Types.CHAR));
		typeMap.put("clob", Integer.valueOf(Types.CLOB));
		typeMap.put("date", Integer.valueOf(Types.DATE));
		typeMap.put("day_interval", Integer.valueOf(Types.NUMERIC));	// New as of Oct2020 release
		typeMap.put("decimal", Integer.valueOf(Types.DECIMAL));
		typeMap.put("double", Integer.valueOf(Types.DOUBLE));
		// typeMap.put("geometry", Integer.valueOf(Types.???));
		// typeMap.put("geometrya", Integer.valueOf(Types.???));
		typeMap.put("hugeint", Integer.valueOf(Types.NUMERIC));
		typeMap.put("inet", Integer.valueOf(Types.VARCHAR));
		typeMap.put("int", Integer.valueOf(Types.INTEGER));
		typeMap.put("json", Integer.valueOf(Types.VARCHAR));
		// typeMap.put("mbr", Integer.valueOf(Types.???));
		typeMap.put("month_interval", Integer.valueOf(Types.INTEGER));
		typeMap.put("oid", Integer.valueOf(Types.BIGINT));
		// typeMap.put("ptr", Integer.valueOf(Types.???));
		typeMap.put("real", Integer.valueOf(Types.REAL));
		typeMap.put("sec_interval", Integer.valueOf(Types.DECIMAL));
		typeMap.put("smallint", Integer.valueOf(Types.SMALLINT));
		typeMap.put("str", Integer.valueOf(Types.VARCHAR));	// MonetDB prepare <stmt> uses type 'str' (instead of varchar) for the schema, table and column metadata output. DO NOT REMOVE this entry!
		// typeMap.put("table", Integer.valueOf(Types.???));
		typeMap.put("time", Integer.valueOf(Types.TIME));
		typeMap.put("timestamp", Integer.valueOf(Types.TIMESTAMP));
		typeMap.put("timestamptz", Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE));	// new in Java 8: Types.TIMESTAMP_WITH_TIMEZONE (value 2014)
		typeMap.put("timetz", Integer.valueOf(Types.TIME_WITH_TIMEZONE));	// new in Java 8: Types.TIME_WITH_TIMEZONE (value 2013)
		typeMap.put("tinyint", Integer.valueOf(Types.TINYINT));
		typeMap.put("url", Integer.valueOf(Types.VARCHAR));
		typeMap.put("uuid", Integer.valueOf(Types.VARCHAR));
		typeMap.put("varchar", Integer.valueOf(Types.VARCHAR));
		typeMap.put("wrd", Integer.valueOf(Types.BIGINT));	// keep it in for old (pre Dec2016) MonetDB servers
		typeMap.put("xml", Integer.valueOf(Types.VARCHAR));	// used when "CREATE TYPE xml EXTERNAL NAME xml;" is executed
	}

	/**
	 * Returns the java.sql.Types equivalent of the given MonetDB type name.
	 *
	 * @param type the SQL data type name as used by MonetDB
	 * @return the matching java.sql.Types constant or
	 *         java.sql.Types.OTHER if nothing matched the given type name
	 */
	static final int getJdbcSQLType(final String type) {
		// find the column type name in the typeMap
		final Integer tp = typeMap.get(type);
		if (tp != null) {
			return tp.intValue();
		}
		// When type name is not found in the map, for instance
		// when it is a new type (not yet added in the above typeMap) or
		// when type name is: any or geometry or geometrya or mbr or ptr or table.
		return Types.OTHER;
	}

	/**
	 * Returns the Class object for a given java.sql.Types value.
	 *
	 * @param type a value from java.sql.Types
	 * @return a Class object from which an instance would be returned
	 */
	static final Class<?> getClassForType(final int type) {
		/**
		 * This switch returns the types as objects according to table B-3 from
		 * Oracle's JDBC specification 4.1
		 */
		switch(type) {
			case Types.CHAR:
			case Types.VARCHAR:
		/*	case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR */
				return String.class;
			case Types.NUMERIC:
			case Types.DECIMAL:
				return java.math.BigDecimal.class;
			case Types.BOOLEAN:
				return Boolean.class;
			case Types.TINYINT:
			case Types.SMALLINT:
				return Short.class;
			case Types.INTEGER:
				return Integer.class;
			case Types.BIGINT:
				return Long.class;
			case Types.REAL:
				return Float.class;
			case Types.FLOAT:
			case Types.DOUBLE:
				return Double.class;
			case Types.BINARY:      // MonetDB currently does not support these
			case Types.VARBINARY:   // see treat_blob_as_binary property
		/*	case Types.LONGVARBINARY: // MonetDB doesn't use type LONGVARBINARY */
				return byte[].class;
			case Types.DATE:
				return java.sql.Date.class;
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
				return java.sql.Time.class;
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				return java.sql.Timestamp.class;
			case Types.CLOB:
				return java.sql.Clob.class;
			case Types.BLOB:
				return java.sql.Blob.class;

			// all the rest are currently not implemented and used
			default:
				return String.class;
		}
	}

	private static String TypeMapppingSQL;	// cache to optimise getSQLTypeMap()
	/**
	 * Returns a String usable in an SQL statement to map the server types
	 * to values of java.sql.Types using the global static type map.
	 * The returned string will be a SQL CASE x statement where the x is
	 * replaced with the given column name (or expression) string.
	 *
	 * @param column a String representing the value that should be evaluated
	 *               in the SQL CASE statement
	 * @return a SQL CASE statement
	 */
	static final String getSQLTypeMap(final String column) {
		if (TypeMapppingSQL == null) {
			// first time, compose TypeMappping SQL string
			final StringBuilder val = new StringBuilder((typeMap.size() * (7 + 7 + 7 + 4)) + 14);
			for (Entry<String, Integer> entry : typeMap.entrySet()) {
				val.append(" WHEN '").append(entry.getKey()).append("' THEN ").append(entry.getValue().toString());
			}
			val.append(" ELSE " + Types.OTHER + " END");
			// as the typeMap is static, cache this SQL part for all next calls
			TypeMapppingSQL = val.toString();
		}
		return "CASE " + column + TypeMapppingSQL;
	}
}
