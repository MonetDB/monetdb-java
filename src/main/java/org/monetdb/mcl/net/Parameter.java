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

import java.util.Calendar;

/**
 * Enumerates things that can be configured on a connection to MonetDB.
 */
public enum Parameter {
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
	CLOB_AS_VARCHAR("treat_clob_as_varchar", ParameterType.Bool, true, "return CLOB/TEXT data as type VARCHAR instead of type CLOB", false),
	BLOB_AS_BINARY("treat_blob_as_binary", ParameterType.Bool, true, "return BLOB data as type BINARY instead of type BLOB", false),
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
				int offsetSeconds = offsetMillis / 1000;
				return offsetSeconds;
			default:
				return defaultValue;
		}
	}

	/**
	 * Determine if this Parameter is onlyu relevant when TlS is enabled.
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
}
