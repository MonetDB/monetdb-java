/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;
import java.util.Properties;
import java.util.logging.Logger;

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
 * @author Fabian Groffen, Pedro Ferreira
 * @version 0.1
 */
public class MonetDataSource extends MonetWrapper implements DataSource {

	private String user;
	private String password; // insecure, but how to do it better?
	private String description = "MonetDB database";
	private String url = "jdbc:monetdb://localhost/";
	private int loginTimeout;
	private String directory;
	private final MonetDriver driver = new MonetDriver();

	// the following properties are also standard:
	// private String dataSourceName;
	// private String networkProtocol;
	// private String serverName;
	// private String role;

	public MonetDataSource() {}

	/**
	 * Attempts to establish a connection with the data source that this DataSource object represents.
	 *
	 * @return a MonetConnection
	 * @throws SQLException if connecting to the database fails
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection(user, password);
	}

	/**
	 * Attempts to establish a connection with the data source that this DataSource object represents.
	 *
	 * @param username the username to use
	 * @param password the password to use
	 * @return a MonetConnection
	 * @throws SQLException if connecting to the database fails
	 */
	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		Properties props = new Properties();
		props.put("user", username);
		props.put("password", password);
		if (loginTimeout > 0) {
			props.put("so_timeout", Integer.toString(loginTimeout));
		}
		if(directory != null) {
			props.put("embedded", "true");
			props.put("directory", directory);
		}
		return driver.connect(url, props);
	}

	/**
	 * Gets the maximum time in seconds that this data source can wait while attempting to connect to a database.
	 *
	 * @return login timeout default is 0 (infinite)
	 */
	@Override
	public int getLoginTimeout() {
		return loginTimeout;
	}

	/**
	 * Sets the maximum time in seconds that this data source will wait while attempting to connect to a database.
	 *
	 * @param seconds the number of seconds to wait before aborting the connect
	 */
	@Override
	public void setLoginTimeout(int seconds) {
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
	 * Sets the log writer for this DataSource object to the given java.io.PrintWriter object.
	 *
	 * @param out a PrintWriter - ignored
	 */
	@Override
	public void setLogWriter(PrintWriter out) {}

	/**
	 * Sets the password to use when connecting.  There is no getter
	 * for obvious reasons.
	 *
	 * @param password the password
	 */
	public void setPassword(String password) {
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
	public void setUser(String user) {
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
	public void setURL(String url) {
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
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Gets the directory value
	 *
	 * @return the directory value
	 */
	public String getDirectory() {
		return directory;
	}

	/**
	 * Sets the directory value, meaning it wil start an embedded connection
	 *
	 * @param directory The directory location
	 */
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	/**
	 * Gets the embedded connection directory. If not, then a MAPI connection will be created instead.
	 *
	 * @return If the connection will be embedded. If not, then a MAPI connection will be created instead.
	 */
	public boolean isEmbedded() {
		return directory != null;
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
	 * @throws SQLFeatureNotSupportedException if the data source does not use java.util.logging
	 */
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw newSQLFeatureNotSupportedException("getParentLogger");
	}
}
