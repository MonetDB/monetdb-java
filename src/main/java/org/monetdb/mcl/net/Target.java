package org.monetdb.mcl.net;

import java.util.regex.Pattern;

public class Target {
    private boolean tls = false;
    private String host = "";
    private int port = -1;
    private String database = "";
    private String tableschema = "";
    private String table = "";
    private String sock = "";
    private String sockdir = "/tmp";
    private String cert = "";
    private String certhash = "";
    private String clientkey = "";
    private String clientcert = "";
    private String user = "";
    private String password = "";
    private String language = "sql";
    private boolean autocommit = true;
    private String schema = "";
    private int timezone;
    private String binary = "on";
    private int replySize = 200;
    private String hash = "";
    private boolean debug = false;
    private String logfile = "";
    private int soTimeout = 0;
    private boolean treatClobAsVarchar = true;
    private boolean treatBlobAsBinary = true;

    private boolean userWasSet = false;
    private boolean passwordWasSet = false;
    protected static final Target defaults = new Target();
    private Validated validated = null;

    private static Pattern namePattern = Pattern.compile("^[a-zzA-Z_][-a-zA-Z0-9_.]*$");
    private static Pattern hashPattern = Pattern.compile("^sha256:[0-9a-fA-F:]*$");

    public Target() {
        this.timezone = (int)Parameter.TIMEZONE.getDefault();
    }

    public void barrier() {
        if (userWasSet && !passwordWasSet)
            password = "";
        userWasSet = false;
        passwordWasSet = false;
    }

    public static String packHost(String host) {
        switch (host) {
            case "localhost":
                return "localhost.";
            case "":
                return "localhost";
            default:
                return host;
        }
    }

    public void setString(String key, String value) throws ValidationError {
        Parameter parm = Parameter.forName(key);
        if (parm != null)
            setString(parm, value);
        else if (!Parameter.isIgnored(key))
            throw new ValidationError(key, "unknown parameter");
    }

    public void setString(Parameter parm, String value) throws ValidationError {
        if (value == null)
            throw new NullPointerException("'value' must not be null");
        assign(parm, parm.type.parse(parm.name, value));
    }

    public void clear(Parameter parm) {
        assign(parm, parm.getDefault());
    }

    private void assign(Parameter parm, Object value) {
        switch (parm) {
            case TLS: setTls((boolean)value); break;
            case HOST: setHost((String)value); break;
            case PORT: setPort((int)value); break;
            case DATABASE: setDatabase((String)value); break;
            case TABLESCHEMA: setTableschema((String)value); break;
            case TABLE: setTable((String)value); break;
            case SOCK: setSock((String)value); break;
            case SOCKDIR: setSockdir((String)value); break;
            case CERT: setCert((String)value); break;
            case CERTHASH: setCerthash((String)value); break;
            case CLIENTKEY: setClientkey((String)value); break;
            case CLIENTCERT: setClientcert((String)value); break;
            case USER: setUser((String)value); break;
            case PASSWORD: setPassword((String)value); break;
            case LANGUAGE: setLanguage((String)value); break;
            case AUTOCOMMIT: setAutocommit((boolean)value); break;
            case SCHEMA: setSchema((String)value); break;
            case TIMEZONE: setTimezone((int)value); break;
            case BINARY: setBinary((String)value); break;
            case REPLYSIZE: setReplySize((int)value); break;
            case FETCHSIZE: setReplySize((int)value); break;
            case HASH: setHash((String)value); break;
            case DEBUG: setDebug((boolean)value); break;
            case LOGFILE: setLogfile((String)value); break;

            case SO_TIMEOUT: setSoTimeout((int)value); break;
            case CLOB_AS_VARCHAR: setTreatClobAsVarchar((boolean)value); break;
            case BLOB_AS_BINARY: setTreatBlobAsBinary((boolean)value); break;

            default:
                throw new IllegalStateException("unreachable -- missing case: " + parm.name);
        }
    }

    public String getString(Parameter parm) {
        Object value = getObject(parm);
        return parm.type.format(value);
    }

    public Object getObject(Parameter parm) {
        switch (parm) {
            case TLS: return tls;
            case HOST: return host;
            case PORT: return port;
            case DATABASE: return database;
            case TABLESCHEMA: return tableschema;
            case TABLE: return table;
            case SOCK: return sock;
            case SOCKDIR: return sockdir;
            case CERT: return cert;
            case CERTHASH: return certhash;
            case CLIENTKEY: return clientkey;
            case CLIENTCERT: return clientcert;
            case USER: return user;
            case PASSWORD: return password;
            case LANGUAGE: return language;
            case AUTOCOMMIT: return autocommit;
            case SCHEMA: return schema;
            case TIMEZONE: return timezone;
            case BINARY: return binary;
            case REPLYSIZE: return replySize;
            case FETCHSIZE: return replySize;
            case HASH: return hash;
            case DEBUG: return debug;
            case LOGFILE: return logfile;
            case SO_TIMEOUT: return soTimeout;
            case CLOB_AS_VARCHAR: return treatClobAsVarchar;
            case BLOB_AS_BINARY: return treatBlobAsBinary;
            default:
                throw new IllegalStateException("unreachable -- missing case");
        }
    }

    public static String unpackHost(String host) {
        switch (host) {
            case "localhost.":
                return "localhost";
            case "localhost":
                return "";
            default:
                return host;
        }
    }

    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
        validated = null;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
        validated = null;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
        validated = null;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
        validated = null;
    }

    public String getTableschema() {
        return tableschema;
    }

    public void setTableschema(String tableschema) {
        this.tableschema = tableschema;
        validated = null;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
        validated = null;
    }

    public String getSock() {
        return sock;
    }

    public void setSock(String sock) {
        this.sock = sock;
        validated = null;
    }

    public String getSockdir() {
        return sockdir;
    }

    public void setSockdir(String sockdir) {
        this.sockdir = sockdir;
        validated = null;
    }

    public String getCert() {
        return cert;
    }

    public void setCert(String cert) {
        this.cert = cert;
        validated = null;
    }

    public String getCerthash() {
        return certhash;
    }

    public void setCerthash(String certhash) {
        this.certhash = certhash;
        validated = null;
    }

    public String getClientkey() {
        return clientkey;
    }

    public void setClientkey(String clientkey) {
        this.clientkey = clientkey;
        validated = null;
    }

    public String getClientcert() {
        return clientcert;
    }

    public void setClientcert(String clientcert) {
        this.clientcert = clientcert;
        validated = null;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
        this.userWasSet = true;
        validated = null;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
        this.passwordWasSet = true;
        validated = null;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
        validated = null;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
        validated = null;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
        validated = null;
    }

    public int getTimezone() {
        return timezone;
    }

    public void setTimezone(int timezone) {
        this.timezone = timezone;
        validated = null;
    }

    public String getBinary() {
        return binary;
    }

    public void setBinary(String binary) {
        this.binary = binary;
        validated = null;
    }

    public int getReplySize() {
        return replySize;
    }

    public void setReplySize(int replySize) {
        this.replySize = replySize;
        validated = null;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
        validated = null;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
        validated = null;
    }

    public String getLogfile() {
        return logfile;
    }

    public void setLogfile(String logfile) {
        this.logfile = logfile;
        validated = null;
    }

    public int getSoTimeout() {
        return soTimeout;
    }


    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        validated = null;
    }

    public void setTreatClobAsVarchar(boolean treatClobAsVarchar) {
        this.treatClobAsVarchar = treatClobAsVarchar;
        validated = null;
    }

    public boolean isTreatClobAsVarchar() {
        return treatClobAsVarchar;
    }

    public boolean isTreatBlobAsBinary() {
        return treatBlobAsBinary;
    }

    public void setTreatBlobAsBinary(boolean treatBlobAsBinary) {
        this.treatBlobAsBinary = treatBlobAsBinary;
        validated = null;
    }

    public Validated validate() throws ValidationError {
        if (validated == null)
            validated = new Validated();
        return validated;
    }

    public String buildUrl() {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("jdbc:monetdb://").append(host)
                .append(':').append(port)
                .append('/').append(database);
        if (!language.equals("sql"))
            sb.append("?language=").append(language);
        return sb.toString();
    }

    public class Validated {

        private final int nbinary;

        Validated() throws ValidationError {

            // 1. The parameters have the types listed in the table in [Section
            //    Parameters](#parameters).

            String binaryString = binary;
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
            nbinary = binaryInt;


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

            // JDBC specific
            if (soTimeout < 0)
                throw new ValidationError("so_timeout= must not be negative");
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
            return nbinary;
        }

        public int getReplySize() {
            return replySize;
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

        public int getSoTimeout() {
            return soTimeout;
        }

        public boolean isTreatClobAsVarchar() {
            return treatClobAsVarchar;
        }

        public boolean isTreatBlobAsBinary() {
            return treatBlobAsBinary;
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
            return nbinary;
        }

        public String connectClientKey() {
            return clientkey;
        }

        public String connectClientCert() {
            return clientcert.isEmpty() ? clientkey : clientcert;
        }
    }
}
