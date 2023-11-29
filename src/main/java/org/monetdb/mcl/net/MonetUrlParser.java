package org.monetdb.mcl.net;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Properties;

public class MonetUrlParser {
    private final Properties props;
    private final String urlText;
    private final URI url;
    boolean userWasSet = false;
    boolean passwordWasSet = false;

    public MonetUrlParser(Properties props, String url) throws URISyntaxException {
        this.props = props;
        this.urlText = url;
        this.url = new URI(url);
    }

    public static void parse(Properties props, String url) throws URISyntaxException {
        boolean modern = true;
        if (url.startsWith("mapi:")) {
            modern = false;
            url = url.substring(5);
            if (url.equals("monetdb://")) {
                // deal with peculiarity of Java's URI parser
                url = "monetdb:///";
            }

        }
        try {
            MonetUrlParser parser = new MonetUrlParser(props, url);
            if (modern) {
                parser.parseModern();
            } else {
                parser.parseClassic();
            }
            if (parser.userWasSet && !parser.passwordWasSet) parser.clear(Parameter.PASSWORD);
        } catch (URISyntaxException e) {
            int idx = e.getIndex();
            if (idx >= 0 && !modern) {
                // "mapi:"
                idx += 5;
            }
            throw new URISyntaxException(e.getInput(), e.getReason(), idx);
        }
    }

    private static String percentDecode(String context, String text) throws URISyntaxException {
        try {
            return URLDecoder.decode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("should be unreachable: UTF-8 unknown??", e);
        } catch (IllegalArgumentException e) {
            throw new URISyntaxException(text, context + ": invalid percent escape");
        }
    }

    private void set(Parameter parm, String value) {
        parm = keyMagic(parm);
        props.setProperty(parm.name, value != null ? value : "");
    }

    private void set(String key, String value) {
        Parameter parm = Parameter.forName(key);
        if (parm != null)
            set(parm, value);
        else
            props.setProperty(key, value);
    }

    private void clear(Parameter parm) {
        parm = keyMagic(parm);
        String value = parm.type.format(Target.getDefault(parm));
        props.setProperty(parm.name, value);
    }

    private Parameter keyMagic(Parameter key) {
        switch (key) {
            case USER:
                userWasSet = true;
                break;
            case PASSWORD:
                passwordWasSet = true;
                break;
            case FETCHSIZE:
                key = Parameter.REPLYSIZE;
                break;
            default:
                break;
        }
        return key;
    }

    private void parseModern() throws URISyntaxException {
        clearBasic();

        String scheme = url.getScheme();
        if (scheme == null) throw new URISyntaxException(urlText, "URL scheme must be monetdb:// or monetdbs://");
        switch (scheme) {
            case "monetdb":
                set(Parameter.TLS, "false");
                break;
            case "monetdbs":
                set(Parameter.TLS, "true");
                break;
            default:
                throw new URISyntaxException(urlText, "URL scheme must be monetdb:// or monetdbs://");
        }

        // The built-in getHost and getPort methods do strange things
        // in edge cases such as percent-encoded host names and
        // invalid port numbers
        String authority = url.getAuthority();
        String host;
        String remainder;
        int pos;
        String raw = url.getRawSchemeSpecificPart();
        if (authority == null) {
            if (!url.getRawSchemeSpecificPart().startsWith("//")) {
                throw new URISyntaxException(urlText, "expected //");
            }
            host = "";
            remainder = "";
        } else if (authority.startsWith("[")) {
            // IPv6
            pos = authority.indexOf(']');
            if (pos < 0)
                throw new URISyntaxException(urlText, "unmatched '['");
            host = authority.substring(1, pos);
            remainder = authority.substring(pos + 1);
        } else if ((pos = authority.indexOf(':')) >= 0){
            host = authority.substring(0, pos);
            remainder = authority.substring(pos);
        } else {
            host = authority;
            remainder = "";
        }
        switch (host) {
            case "localhost":
                set(Parameter.HOST, "");
                break;
            case "localhost.":
                set(Parameter.HOST, "localhost");
                break;
            default:
                set(Parameter.HOST, host);
                break;
        }

        if (remainder.isEmpty()) {
            // do nothing
        } else if (remainder.startsWith(":")) {
            String portStr = remainder.substring(1);
            try {
                int port = Integer.parseInt(portStr);
                if (port <= 0 || port > 65535)
                    portStr = null;
            } catch (NumberFormatException e) {
                portStr = null;
            }
            if (portStr == null)
                throw new URISyntaxException(urlText, "invalid port number");
            set(Parameter.PORT, portStr);
        }

        String path = url.getRawPath();
        String[] parts = path.split("/", 5);
        // <0: empty before leading slash> / <1: database> / <2: tableschema> / <3: table> / <4: should not exist>
        switch (parts.length) {
            case 5:
                throw new URISyntaxException(urlText, "table name should not contain slashes");
            case 4:
                set(Parameter.TABLE, percentDecode(Parameter.TABLE.name, parts[3]));
                // fallthrough
            case 3:
                set(Parameter.TABLESCHEMA, percentDecode(Parameter.TABLESCHEMA.name, parts[2]));
                // fallthrough
            case 2:
                set(Parameter.DATABASE, percentDecode(Parameter.DATABASE.name, parts[1]));
            case 1:
            case 0:
                // fallthrough
                break;
        }

        final String query = url.getRawQuery();
        if (query != null) {
            final String args[] = query.split("&");
            for (int i = 0; i < args.length; i++) {
                pos = args[i].indexOf('=');
                if (pos <= 0) {
                    throw new URISyntaxException(args[i], "invalid key=value pair");
                }
                String key = args[i].substring(0, pos);
                key = percentDecode(key, key);
                Parameter parm = Parameter.forName(key);
                if (parm != null && parm.isCore)
                    throw new URISyntaxException(key, key + "= is not allowed as a query parameter");

                String value = args[i].substring(pos + 1);
                set(key, percentDecode(key, value));
            }
        }
    }


    private void parseClassic() throws URISyntaxException {
        String scheme = url.getScheme();
        if (scheme == null) throw new URISyntaxException(urlText, "URL scheme must be mapi:monetdb:// or mapi:merovingian://");
        switch (scheme) {
            case "monetdb":
                clearBasic();
                break;
            case "merovingian":
                throw new IllegalStateException("mapi:merovingian: not supported yet");
            default:
                throw new URISyntaxException(urlText, "URL scheme must be mapi:monetdb:// or mapi:merovingian://");
        }

        if (!url.getRawSchemeSpecificPart().startsWith("//")) {
            throw new URISyntaxException(urlText, "expected //");
        }

        String authority = url.getRawAuthority();
        String host;
        String portStr;
        int pos;
        if (authority == null) {
            host = "";
            portStr = "";
        } else if (authority.indexOf('@') >= 0) {
            throw new URISyntaxException(urlText, "user@host syntax is not allowed");
        } else if ((pos = authority.indexOf(':')) >= 0) {
            host = authority.substring(0, pos);
            portStr = authority.substring(pos + 1);
        } else {
            host = authority;
            portStr = "";
        }

        if (!portStr.isEmpty()) {
            int port;
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                port = -1;
            }
            if (port <= 0) {
                throw new URISyntaxException(urlText, "invalid port number");
            }
            set(Parameter.PORT, portStr);
        }

        String path = url.getRawPath();
        boolean isUnix;
        if (host.isEmpty() && portStr.isEmpty()) {
            // socket
            isUnix = true;
            clear(Parameter.HOST);
            set(Parameter.SOCK, path != null ? path : "");
        } else {
            // tcp
            isUnix = false;
            clear(Parameter.SOCK);
            set(Parameter.HOST, host);
            if (path == null || path.isEmpty()) {
                // do nothing
            } else if (!path.startsWith("/")) {
                throw new URISyntaxException(urlText, "expect path to start with /");
            } else {
                String database = path.substring(1);
                if (database.contains("/"))
                    throw new URISyntaxException(urlText, "no slashes allowed in database name");
                set(Parameter.DATABASE, database);
            }
        }

        final String query = url.getRawQuery();
        if (query != null) {
            final String args[] = query.split("&");
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("language=")) {
                    String language = arg.substring(9);
                    set(Parameter.LANGUAGE, language);
                } else if (arg.startsWith("database=")) {
                    String database = arg.substring(9);
                    set(Parameter.DATABASE, database);
                } else {
                    // ignore
                }
            }
        }
    }

    private void clearBasic() {
        clear(Parameter.HOST);
        clear(Parameter.PORT);
        clear(Parameter.SOCK);
        clear(Parameter.DATABASE);
    }
}
