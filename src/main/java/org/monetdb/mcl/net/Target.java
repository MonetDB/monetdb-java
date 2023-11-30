package org.monetdb.mcl.net;

import java.util.Calendar;
import java.util.Properties;
import java.util.regex.Pattern;

public class Target {
    private static Pattern namePattern = Pattern.compile("^[a-zzA-Z_][-a-zA-Z0-9_.]*$");
    private static Pattern hashPattern = Pattern.compile("^sha256:[0-9a-fA-F:]*$");
    private final boolean tls;
    private final String host;
    private final int port;
    private final String database;
    private final String tableschema;
    private final String table;
    private final String sock;
    private final String sockdir;
    private final String cert;
    private final String certhash;
    private final String clientkey;
    private final String clientcert;
    private final String user;
    private final String password;
    private final String language;
    private final boolean autocommit;
    private final String schema;
    private final int timezone;
    private final int binary;
    private final int replysize;
    private final String hash;
    private final boolean debug;
    private final String logfile;

    public Target(Properties properties) throws ValidationError {

        // 1. The parameters have the types listed in the table in [Section
        //    Parameters](#parameters).
        tls = validateBoolean(properties, Parameter.TLS);
        host = validateString(properties, Parameter.HOST);
        port = validateInt(properties, Parameter.PORT);
        database = validateString(properties, Parameter.DATABASE);
        tableschema = validateString(properties, Parameter.TABLESCHEMA);
        table = validateString(properties, Parameter.TABLE);
        sock = validateString(properties, Parameter.SOCK);
        sockdir = validateString(properties, Parameter.SOCKDIR);
        cert = validateString(properties, Parameter.CERT);
        certhash = validateString(properties, Parameter.CERTHASH);
        clientkey = validateString(properties, Parameter.CLIENTKEY);
        clientcert = validateString(properties, Parameter.CLIENTCERT);
        user = validateString(properties, Parameter.USER);
        password = validateString(properties, Parameter.PASSWORD);
        language = validateString(properties, Parameter.LANGUAGE);
        autocommit = validateBoolean(properties, Parameter.AUTOCOMMIT);
        schema = validateString(properties, Parameter.SCHEMA);
        timezone = validateInt(properties, Parameter.TIMEZONE);
        replysize = validateInt(properties, Parameter.REPLYSIZE);
        hash = validateString(properties, Parameter.HASH);
        debug = validateBoolean(properties, Parameter.DEBUG);
        logfile = validateString(properties, Parameter.LOGFILE);

        for (String name: properties.stringPropertyNames()) {
            if (Parameter.forName(name) != null)
                continue;
            if (name.contains("_"))
                continue;
            throw new ValidationError("unknown parameter: " + name);
        }

        String binaryString = validateString(properties, Parameter.BINARY);
        int binaryInt;
        try {
            binaryInt = (int) ParameterType.Int.parse(Parameter.BINARY.name, binaryString);
        } catch (ValidationError e) {
            try {
                boolean b = (boolean) ParameterType.Bool.parse(Parameter.BINARY.name, binaryString);
                binaryInt = b ? 65535 : 0;
            } catch (ValidationError ee) {
                throw new ValidationError("binary= must be either a number or true/yes/on/false/no/off");
            }
        }
        if (binaryInt < 0)
            throw new ValidationError("binary= cannot be negative");
        binary = binaryInt;


        // 2. At least one of **sock** and **host** must be empty.
        if (!sock.isEmpty() && !host.isEmpty())
            throw new ValidationError("sock=" + sock + " cannot be combined with host=" + host);

        // 3. The string parameter **binary** must either parse as a boolean or as a
        //    non-negative integer.
        //
        // (checked above)

        // 4. If **sock** is not empty, **tls** must be 'off'.
        if (!sock.isEmpty() && tls) throw new ValidationError("monetdbs:// cannot be combined with sock=");

        // 5. If **certhash** is not empty, it must be of the form `{sha256}hexdigits`
        //    where hexdigits is a non-empty sequence of 0-9, a-f, A-F and colons.
        // TODO
        if (!certhash.isEmpty()) {
            if (!certhash.toLowerCase().startsWith("sha256:"))
                throw new ValidationError("certificate hash must start with 'sha256:'");
            if (!hashPattern.matcher(certhash).matches())
                throw new ValidationError("invalid certificate hash");
        }

        // 6. If **tls** is 'off', **cert** and **certhash** must be 'off' as well.
        if (!tls) {
            if (!cert.isEmpty() || !certhash.isEmpty())
                throw new ValidationError("cert= and certhash= are only allowed in combination with monetdbs://");
        }

        // 7. Parameters **database**, **tableschema** and **table** must consist only of
        //    upper- and lowercase letters, digits, periods, dashes and underscores. They must not
        //    start with a dash.
        //    If **table** is not empty, **tableschema** must also not be empty.
        //    If **tableschema** is not empty, **database** must also not be empty.
        if (database.isEmpty() && !tableschema.isEmpty())
            throw new ValidationError("table schema cannot be set without database");
        if (tableschema.isEmpty() && !table.isEmpty())
            throw new ValidationError("table cannot be set without schema");
        if (!database.isEmpty() && !namePattern.matcher(database).matches())
            throw new ValidationError("invalid database name");
        if (!tableschema.isEmpty() && !namePattern.matcher(tableschema).matches())
            throw new ValidationError("invalid table schema name");
        if (!table.isEmpty() && !namePattern.matcher(table).matches())
            throw new ValidationError("invalid table name");


        // 8. Parameter **port** must be -1 or in the range 1-65535.
        if (port < -1 || port == 0 || port > 65535) throw new ValidationError("invalid port number " + port);

        // 9. If **clientcert** is set, **clientkey** must also be set.
        if (!clientcert.isEmpty() && clientkey.isEmpty())
            throw new ValidationError("clientcert= is only valid in combination with clientkey=");
    }

    public static boolean validateBoolean(Properties props, Parameter parm) throws ValidationError {
        Object value = props.get(parm.name);
        if (value != null) {
            return (Boolean) parm.type.parse(parm.name, (String) value);
        } else {
            return (Boolean) getDefault(parm);
        }
    }

    public static int validateInt(Properties props, Parameter parm) throws ValidationError {
        Object value = props.get(parm.name);
        if (value != null) {
            return (Integer) parm.type.parse(parm.name, (String) value);
        } else {
            return (Integer) getDefault(parm);
        }
    }

    public static String validateString(Properties props, Parameter parm) throws ValidationError {
        Object value = props.get(parm.name);
        if (value != null) {
            return (String) parm.type.parse(parm.name, (String) value);
        } else {
            return (String) getDefault(parm);
        }
    }

    private static int timezone() {
        Calendar cal = Calendar.getInstance();
        int offsetMillis = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);
        int offsetSeconds = offsetMillis / 1000;
        return offsetSeconds;
    }

    public static Object getDefault(Parameter parm) {
        if (parm == Parameter.TIMEZONE) return timezone();
        else return parm.defaultValue;
    }

    public static Properties defaultProperties() {
        Properties props = new Properties();
        return props;
    }

    public boolean getTls() {
        return tls;
    }

    // Getter is private because you probably want connectTcp() instead
    private String getHost() {
        return host;
    }

    // Getter is private because you probably want connectPort() instead
    private int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableschema() {
        return tableschema;
    }

    public String getTable() {
        return table;
    }

    // Getter is private because you probably want connectUnix() instead
    private String getSock() {
        return sock;
    }

    public String getSockdir() {
        return sockdir;
    }

    public String getCert() {
        return cert;
    }

    public String getCerthash() {
        return certhash;
    }

    public String getClientkey() {
        return clientkey;
    }

    public String getClientcert() {
        return clientcert;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getLanguage() {
        return language;
    }

    public boolean getAutocommit() {
        return autocommit;
    }

    public String getSchema() {
        return schema;
    }

    public int getTimezone() {
        return timezone;
    }

    // Getter is private because you probably want connectBinary() instead
    public int getBinary() {
        return binary;
    }

    public int getReplysize() {
        return replysize;
    }

    public String getHash() {
        return hash;
    }

    public boolean getDebug() {
        return debug;
    }

    public String getLogfile() {
        return logfile;
    }

    public boolean connectScan() {
        if (database.isEmpty()) return false;
        if (!sock.isEmpty() || !host.isEmpty() || port != -1) return false;
        return !tls;
    }

    public int connectPort() {
        return port == -1 ? 50000 : port;
    }

    public String connectUnix() {
        if (!sock.isEmpty()) return sock;
        if (tls) return "";
        if (host.isEmpty()) return sockdir + "/.s.monetdb." + connectPort();
        return "";
    }

    public String connectTcp() {
        if (!sock.isEmpty()) return "";
        if (host.isEmpty()) return "localhost";
        return host;
    }

    public Verify connectVerify() {
        if (!tls) return Verify.None;
        if (!certhash.isEmpty()) return Verify.Hash;
        if (!cert.isEmpty()) return Verify.Cert;
        return Verify.System;
    }

    public String connectCertHashDigits() {
        if (!tls) return null;
        StringBuilder builder = new StringBuilder(certhash.length());
        for (int i = "sha256:".length(); i < certhash.length(); i++) {
            char c = certhash.charAt(i);
            if (Character.digit(c, 16) >= 0) builder.append(Character.toLowerCase(c));
        }
        return builder.toString();
    }

    public int connectBinary() {
        return binary;
    }

    public String connectClientKey() {
        return clientkey;
    }

    public String connectClientCert() {
        return clientcert.isEmpty() ? clientkey : clientcert;
    }
}
