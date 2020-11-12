/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

package org.monetdb.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;
import java.util.Properties;

/**
 * A DataSource suitable for the MonetDB database.
 *
 * This DataSource allows retrieval of a Connection using the JNDI bean like
 * framework.  A DataSource has numerous advantages over using the DriverManager
 * to retrieve a Connection object.  Using the DataSource interface enables a
 * more transparent application where the location or database can be changed
 * without changing any application code.
 *
 * Additionally, pooled connections can be used when using a DataSource.
 *
 * @author Fabian Groffen
 * @version 0.2
 */
public final class MonetDataSource
	extends MonetWrapper
	implements DataSource
{
	private String description;
	private int loginTimeout = 0;
	private String user;
	// insecure, but how to do it better?
	private String password;
	private String url;

	// the following properties are also standard:
	// private String dataSourceName;
	// private String networkProtocol;
	// private String serverName;
	// private String role;


	private final MonetDriver driver;

	/**
	 * Constructor of a MonetDataSource which uses default settings for a
	 * connection.  You probably want to change this setting using the
	 * method setURL.
	 */
	public MonetDataSource() {
		description = "MonetDB database";
		url = "jdbc:monetdb://localhost/";

		driver = new MonetDriver();
	}

	/**
	 * Attempts to establish a connection with the data source that this
	 * DataSource object represents.
	 *
	 * @return a MonetConnection
	 * @throws SQLException if connecting to the database fails
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection(user, password);
	}

	/**
	 * Attempts to establish a connection with the data source that this
	 * DataSource object represents.
	 *
	 * @param username the username to use
	 * @param password the password to use
	 * @return a MonetConnection
	 * @throws SQLException if connecting to the database fails
	 */
	@Override
	public Connection getConnection(final String username, final String password)
		throws SQLException
	{
		if (loginTimeout > 0) {
			/// could enable Socket.setSoTimeout(int timeout) here...
		}
		final Properties props = new Properties();
		props.put("user", username);
		props.put("password", password);

		return driver.connect(url, props);
	}


	/**
	 * Gets the maximum time in seconds that this data source can wait while
	 * attempting to connect to a database.
	 *
	 * @return login timeout default is 0 (infinite)
	 */
	@Override
	public int getLoginTimeout() {
		return loginTimeout;
	}

	/**
	 * Sets the maximum time in seconds that this data source will wait while
	 * attempting to connect to a database.
	 *
	 * @param seconds the number of seconds to wait before aborting the connect
	 */
	@Override
	public void setLoginTimeout(final int seconds) {
		loginTimeout = seconds;
	}

	/**
	 * Retrieves the log writer for this DataSource object.
	 *
	 * @return null, since there is no log writer
	 */
	@Override
	public PrintWriter getLogWriter() {
		return null;
	}

	/**
	 * Sets the log writer for this DataSource object to the given
	 * java.io.PrintWriter object.
	 *
	 * @param out a PrintWriter - ignored
	 */
	@Override
	public void setLogWriter(final PrintWriter out) {
	}

	/**
	 * Sets the password to use when connecting.  There is no getter
	 * for obvious reasons.
	 *
	 * @param password the password
	 */
	public void setPassword(final String password) {
		this.password = password;
	}

	/**
	 * Gets the username
	 *
	 * @return the username
	 */
	public String getUser() {
		return user;
	}

	/**
	 * Sets the username
	 *
	 * @param user the username
	 */
	public void setUser(final String user) {
		this.user = user;
	}

	/**
	 * Gets the connection URL
	 *
	 * @return the connection URL
	 */
	public String getURL() {
		return url;
	}

	/**
	 * Sets the connection URL
	 *
	 * @param url the connection URL
	 */
	public void setDatabaseName(final String url) {
		this.url = url;
	}

	/**
	 * Gets the description
	 *
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Sets the description
	 *
	 * @param description the description
	 */
	public void setDescription(final String description) {
		this.description = description;
	}

	/**
	 * Return the parent Logger of all the Loggers used by this data
	 * source.  This should be the Logger farthest from the root Logger
	 * that is still an ancestor of all of the Loggers used by this data
	 * source.  Configuring this Logger will affect all of the log
	 * messages generated by the data source. In the worst case, this
	 * may be the root Logger.
	 *
	 * @return the parent Logger for this data source
	 * @throws SQLFeatureNotSupportedException if the data source does
	 *         not use java.util.logging
	 */
	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw newSQLFeatureNotSupportedException("getParentLogger");
	}
}
