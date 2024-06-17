package org.monetdb.mcl.net;

import org.monetdb.jdbc.MonetDriver;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Manage ClientInfo properties to track, and help generating a
 * @{link SQLClientInfoException} if there is a failure
 */
public class ClientInfo {
	private static final String defaultHostname = findHostname();

	private static final String defaultClientLibrary = findClientLibrary();

	private static final String defaultApplicationName = findApplicationName();

	private static final String defaultPid = findPid();

	private final Properties props;
	private HashMap<String, ClientInfoStatus> problems = null;

	public ClientInfo() {
		props = new Properties();
	}

	public void setDefaults() {
		props.setProperty("ClientHostname", defaultHostname);
		props.setProperty("ClientLibrary", defaultClientLibrary);
		props.setProperty("ClientPid", defaultPid);
		props.setProperty("ApplicationName", defaultApplicationName);
		props.setProperty("ClientRemark", "");
	}

	private static String findHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "";
		}
	}

	private static String findApplicationName() {
		String appName = "";
		try {
			String prop = System.getProperty("sun.java.command");
			if (prop != null) {
				// we want only the command, and not the arguments
				prop = prop.split("\\s", 2)[0];
				// keep only the basename5
				int idx = prop.lastIndexOf(File.separatorChar);
				if (idx >= 0)
					prop = prop.substring(idx + 1);
				appName = prop;
			}
		} catch (SecurityException e) {
			// ignore
		}

		return appName;
	}

	private static String findPid() {
		try {
			RuntimeMXBean mxbean = ManagementFactory.getRuntimeMXBean();
			String pidAtHostname = mxbean.getName();
			return pidAtHostname.split("@", 2)[0];
		} catch (RuntimeException e) {
			return "";
		}
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
		return props;
	}

	public HashMap<String,ClientInfoStatus> getProblems() {
		return problems;
	}

	public void set(String name, String value, Set<String> known) throws SQLClientInfoException {
		if (value == null)
			value = "";

		if (known != null && !known.contains(name)) {
			addProblem(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
		} else if (value.contains("\n")) {
			addProblem(name, ClientInfoStatus.REASON_VALUE_INVALID);
			throw new SQLClientInfoException("Invalid value for Client Info property '" + name + "'", "01M07", problems);
		} else {
			props.setProperty(name, value);
		}
	}

	public void set(String name, String value) throws SQLClientInfoException {
		set(name, value, null);
	}

	private void addProblem(String name, ClientInfoStatus status) {
		if (problems == null)
			problems = new HashMap<>();
		ClientInfoStatus old = problems.get(name);
		if (old == null || status.compareTo(old) > 0)
			problems.put(name, status);
	}

	public SQLClientInfoException wrapException(SQLException e) {
		return new SQLClientInfoException(problems, e);
	}

	public SQLWarning warnings() {
		SQLWarning ret = null;
		if (problems == null)
			return null;
		for (Map.Entry<String, ClientInfoStatus> entry: problems.entrySet()) {
			if (!entry.getValue().equals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY))
				continue;
			SQLWarning warning = new SQLWarning("unknown client info property: " + entry.getKey(), "01M07");
			if (ret == null)
				ret = warning;
			else
				ret.setNextWarning(warning);
		}
		return ret;
	}
}
