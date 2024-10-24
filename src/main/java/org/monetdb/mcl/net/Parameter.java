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

package org.monetdb.mcl.net;

import java.sql.DriverPropertyInfo;
import java.util.Calendar;
import java.util.Properties;

/**
 * Enumerates things that can be configured on a connection to MonetDB.
 */
public enum Parameter {
	//  String name, ParameterType type, Object defaultValue, String description, boolean isCore
	TLS("tls", ParameterType.Bool, false, "secure the connection using TLS", true),
	HOST("host", ParameterType.Str, "", "IP number, domain name or one of the special values `localhost` and `localhost.`", true),
	PORT("port", ParameterType.Int, -1, "Port to connect to, 1..65535 or -1 for 'not set'", true),
	DATABASE("database", ParameterType.Str, "", "name of database to connect to", true),
	TABLESCHEMA("tableschema", ParameterType.Str, "", "only used for REMOTE TABLE, otherwise unused", true),
	TABLE("table", ParameterType.Str, "", "only used for REMOTE TABLE, otherwise unused", true),
	SOCK("sock", ParameterType.Path, "", "path to Unix domain socket to connect to", false),
	SOCKDIR("sockdir", ParameterType.Path, "/tmp", "Directory for implicit Unix domain sockets (.s.monetdb.PORT)", false),
	CERT("cert", ParameterType.Path, "", "path to TLS certificate to authenticate server with", false),
	CERTHASH("certhash", ParameterType.Str, "", "hash of server TLS certificate must start with these hex digits; overrides cert", false),
	CLIENTKEY("clientkey", ParameterType.Path, "", "path to TLS key (+certs) to authenticate with as client", false),
	CLIENTCERT("clientcert", ParameterType.Path, "", "path to TLS certs for 'clientkey', if not included there", false),
	USER("user", ParameterType.Str, "", "user name to authenticate as", false),
	PASSWORD("password", ParameterType.Str, "", "password to authenticate with", false),
	LANGUAGE("language", ParameterType.Str, "sql", "for example, \"sql\", \"mal\", \"msql\", \"profiler\"", false),
	AUTOCOMMIT("autocommit", ParameterType.Bool, true, "initial value of autocommit", false),
	SCHEMA("schema", ParameterType.Str, "", "initial schema", false),
	TIMEZONE("timezone", ParameterType.Int, null, "client time zone as minutes east of UTC", false),
	BINARY("binary", ParameterType.Str, "on", "whether to use binary result set format (number or bool)", false),
	REPLYSIZE("replysize", ParameterType.Int, 250, "rows beyond this limit are retrieved on demand, <1 means unlimited", false),
	FETCHSIZE("fetchsize", ParameterType.Int, null, "alias for replysize, specific to jdbc", false),
	HASH("hash", ParameterType.Str, "", "specific to jdbc", false),
	DEBUG("debug", ParameterType.Bool, false, "enable tracing of socket communication for debugging", false),
	LOGFILE("logfile", ParameterType.Str, "", "when debug is enabled its output will be written to this logfile", false),
	SO_TIMEOUT("so_timeout", ParameterType.Int, 0, "abort if network I/O does not complete in this many milliseconds, 0 means no timeout", false),
	CLOB_AS_VARCHAR("treat_clob_as_varchar", ParameterType.Bool, true, "map CLOB/TEXT data to type VARCHAR instead of type CLOB", false),
	BLOB_AS_BINARY("treat_blob_as_binary", ParameterType.Bool, true, "map BLOB data to type BINARY instead of type BLOB", false),

	CLIENT_INFO("client_info", ParameterType.Bool, true, "whether to send ClientInfo when connecting", false),
	CLIENT_APPLICATION("client_application", ParameterType.Str, "", "application name to send in ClientInfo", false),
	CLIENT_REMARK("client_remark", ParameterType.Str, "", "any client remark to send in ClientInfo", false),
	;

	public final String name;
	public final ParameterType type;
	private final Object defaultValue;
	public final String description;
	public final boolean isCore;

	Parameter(String name, ParameterType type, Object defaultValue, String description, boolean isCore) {
		this.name = name;
		this.type = type;
		this.defaultValue = defaultValue;
		this.description = description;
		this.isCore = isCore;
	}

	public static Parameter forName(String name) {
		switch (name) {
			case "tls":
				return TLS;
			case "host":
				return HOST;
			case "port":
				return PORT;
			case "database":
				return DATABASE;
			case "tableschema":
				return TABLESCHEMA;
			case "table":
				return TABLE;
			case "sock":
				return SOCK;
			case "sockdir":
				return SOCKDIR;
			case "cert":
				return CERT;
			case "certhash":
				return CERTHASH;
			case "clientkey":
				return CLIENTKEY;
			case "clientcert":
				return CLIENTCERT;
			case "user":
				return USER;
			case "password":
				return PASSWORD;
			case "language":
				return LANGUAGE;
			case "autocommit":
				return AUTOCOMMIT;
			case "schema":
				return SCHEMA;
			case "timezone":
				return TIMEZONE;
			case "binary":
				return BINARY;
			case "replysize":
				return REPLYSIZE;
			case "fetchsize":
				return FETCHSIZE;
			case "hash":
				return HASH;
			case "debug":
				return DEBUG;
			case "logfile":
				return LOGFILE;
			case "so_timeout":
				return SO_TIMEOUT;
			case "treat_clob_as_varchar":
				return CLOB_AS_VARCHAR;
			case "treat_blob_as_binary":
				return BLOB_AS_BINARY;
			case "client_info":
				return CLIENT_INFO;
			case "client_application":
				return CLIENT_APPLICATION;
			case "client_remark":
				return CLIENT_REMARK;
			default:
				return null;
		}
	}

	/**
	 * Determine if a given setting can safely be ignored.
	 * The ground rule is that if we encounter an unknown setting
	 * without an underscore in the name, it is an error. If it has
	 * an underscore in its name, it can be ignored.
	 *
	 * @param name the name of the setting to check
	 * @return true if it can safely be ignored
	 */
	public static boolean isIgnored(String name) {
		if (Parameter.forName(name) != null)
			return false;
		return name.contains("_");
	}

	/**
	 * Return a default value for the given setting, as an Object of the appropriate type.
	 * Note that the value returned for TIMEZONE may change if the system time zone
	 * is changed or if Daylight Saving Time starts or ends.
	 *
	 * @return default value for the given setting, as an Object of the appropriate type
	 */
	public Object getDefault() {
		switch (this) {
			case TIMEZONE:
				Calendar cal = Calendar.getInstance();
				int offsetMillis = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);
				int offsetMinutes = offsetMillis / 60000;
				return offsetMinutes;
			default:
				return defaultValue;
		}
	}

	/**
	 * Determine if this Parameter is only relevant when TlS is enabled.
	 *
	 * Such parameters need not be shown to the user unless the URL starts with <code>monetdbs://</code>.
	 *
	 * @return true if this Parameter is only relevant when TLS is enabled
	 */
	public boolean isTlsOnly() {
		switch (this) {
			case CERT:
			case CERTHASH:
			case CLIENTCERT:
			case CLIENTKEY:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Gets information about the possible properties for this driver.
	 *
	 * The getPropertyInfo method is intended to allow a generic GUI tool to
	 * discover what properties it should prompt a human for in order to get
	 * enough information to connect to a database. Note that depending on the
	 * values the human has supplied so far, additional values may become
	 * necessary, so it may be necessary to iterate through several calls to the
	 * getPropertyInfo method.
	 *
	 * Note: This method is called from  jdbc.MonetDriver.getPropertyInfo()
	 *
	 * @param info a proposed list of tag/value pairs that will be sent on
	 *        connect open
	 * @return an array of DriverPropertyInfo objects describing possible
	 *         properties. This array may be an empty array if no properties
	 *         are required.
	 */
	public static DriverPropertyInfo[] getPropertyInfo(final Properties info, boolean requires_tls) {
		final String tls = info != null ? info.getProperty("tls") : null;
		final boolean tls_enabled = requires_tls || (tls != null && tls.equals("true"));
		final int dpi_size = (tls_enabled ? 4 : 2);
		final DriverPropertyInfo[] dpi = new DriverPropertyInfo[dpi_size];
		DriverPropertyInfo prop = null;

		// minimal required connection settings are "user" and "password"
		prop = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
		prop.required = true;
		prop.description = "User loginname to use when authenticating on the database server";
		dpi[0] = prop;

		prop = new DriverPropertyInfo("password", info != null ? info.getProperty("password") : null);
		prop.required = true;
		prop.description = "Password to use when authenticating on the database server";
		dpi[1] = prop;

		if (tls_enabled && dpi_size > 2) {
			// when tls is enabled or required also "tls" and "cert" become required
			final String[] boolean_choices = new String[] { "true", "false" };

			prop = new DriverPropertyInfo("tls", tls);
			prop.required = true;
			prop.description = "secure the connection using TLS";
			prop.choices = boolean_choices;
			dpi[2] = prop;

			prop = new DriverPropertyInfo("cert", info != null ? info.getProperty("cert") : null);
			prop.required = true;
			prop.description = "path to TLS certificate to authenticate server with";
			dpi[3] = prop;
		}

		return dpi;
	}
}
