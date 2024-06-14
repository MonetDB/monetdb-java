package org.monetdb.mcl.net;

import org.monetdb.jdbc.MonetDriver;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ClientInfo {
	private static final String defaultHostname = findHostname();

	private static final String defaultClientLibrary = findClientLibrary();

	private static final String defaultApplicationName = findApplicationName();

	private static final String defaultPid = findPid();

	private final Properties props;

	public ClientInfo() {
		props = new Properties();
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
