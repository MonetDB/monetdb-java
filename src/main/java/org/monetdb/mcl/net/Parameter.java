package org.monetdb.mcl.net;


import java.util.Calendar;

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
    REPLYSIZE("replysize", ParameterType.Int, 200, "rows beyond this limit are retrieved on demand, <1 means unlimited", false),
    FETCHSIZE("fetchsize", ParameterType.Int, null, "alias for replysize, specific to jdbc", false),
    HASH("hash", ParameterType.Str, "", "specific to jdbc", false),
    DEBUG("debug", ParameterType.Bool, false, "specific to jdbc", false),
    LOGFILE("logfile", ParameterType.Str, "", "specific to jdbc", false),

    ;

    public final String name;
    public final ParameterType type;
    private final Object defaultValue;
    public final String description;
    public final boolean isCore;

    Parameter(String name, ParameterType type, Object defaultValue, String description, boolean isCore) {
        this.name = name;
        this.type = type;
        this.isCore = isCore;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public static Parameter forName(String name) {
        switch (name) {
            case "tls": return TLS;
            case "host": return HOST;
            case "port": return PORT;
            case "database": return DATABASE;
            case "tableschema": return TABLESCHEMA;
            case "table": return TABLE;
            case "sock": return SOCK;
            case "sockdir": return SOCKDIR;
            case "cert": return CERT;
            case "certhash": return CERTHASH;
            case "clientkey": return CLIENTKEY;
            case "clientcert": return CLIENTCERT;
            case "user": return USER;
            case "password": return PASSWORD;
            case "language": return LANGUAGE;
            case "autocommit": return AUTOCOMMIT;
            case "schema": return SCHEMA;
            case "timezone": return TIMEZONE;
            case "binary": return BINARY;
            case "replysize": return REPLYSIZE;
            case "fetchsize": return FETCHSIZE;
            case "hash": return HASH;
            case "debug": return DEBUG;
            case "logfile": return LOGFILE;
            default: return null;
        }
    }

    public static boolean isIgnored(String name) {
        if (Parameter.forName(name) != null)
            return false;
        return name.contains("_");
    }

    public Object getDefault() {
        switch (this) {
            case TIMEZONE:
                Calendar cal = Calendar.getInstance();
                int offsetMillis = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);
                int offsetSeconds = offsetMillis / 1000;
                return (Integer)offsetSeconds;
            default:
                return defaultValue;
        }
    }
}
