package org.monetdb.mcl.net;

import org.monetdb.jdbc.MonetDriver;

import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ClientInfo {
	private static final String defaultHostname = findHostname();

	private static final String defaultClientLibrary = findClientLibrary();

	private static final int defaultPid = findPid();

	private final Properties props;

	public ClientInfo() {
		props = new Properties();
		props.setProperty("ClientHostname", defaultHostname);
		props.setProperty("ClientLibrary", defaultClientLibrary);
		props.setProperty("ClientPid", "" + defaultPid);
		props.setProperty("ApplicationName", "");
		props.setProperty("ClientRemark", "");
	}

	private static String findHostname() {
		return "my host";
	}

	private static int findPid() {
		return 42;
	}

	private static String findClientLibrary() {
		return "monetdb-java " + MonetDriver.getDriverVersion();
	}

	public String format() {
		StringBuilder builder = new StringBuilder(200);
		for (String name : props.stringPropertyNames()) {
			String value = props.getProperty(name);
			builder.append(name);
			builder.append('=');
			builder.append(value);
			builder.append('\n');
		}
		return builder.toString();
	}

	public Properties get() {
		Properties ret = new Properties();
		ret.putAll(props);
		return ret;
	}

	public boolean set(String name, String value) throws SQLClientInfoException {
		if (value == null)
			value = "";
		if (value.contains("\n")) {
			Map<String, ClientInfoStatus> map = Collections.singletonMap(name, ClientInfoStatus.REASON_VALUE_INVALID);
			throw new SQLClientInfoException(map);
		}
		if (props.containsKey(name)) {
			props.setProperty(name, value);
			return true;
		} else {
			return false;
		}
	}

}
