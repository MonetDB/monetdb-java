package org.monetdb.mcl.net;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class MonetUrlParser {
    private final Target target;
    private final String urlText;
    private final URI url;

    public MonetUrlParser(Target target, String url) throws URISyntaxException {
        this.target = target;
        this.urlText = url;
        // we want to accept monetdb:// but the Java URI parser rejects that.
        switch (url) {
            case "monetdb:-":
            case "monetdbs:-":
                throw new URISyntaxException(url, "invalid MonetDB URL");
            case "monetdb://":
            case "monetdbs://":
                url += "-";
                break;
        }
        this.url = new URI(url);
    }

    public static void parse(Target target, String url) throws URISyntaxException, ValidationError {
        if (url.equals("monetdb://")) {
            // deal with peculiarity of Java's URI parser
            url = "monetdb:///";
        }

        target.barrier();
        if (url.startsWith("mapi:")) {
            try {
                MonetUrlParser parser = new MonetUrlParser(target, url.substring(5));
                parser.parseClassic();
            } catch (URISyntaxException e) {
                URISyntaxException exc = new URISyntaxException(e.getInput(), e.getReason(), -1);
                exc.setStackTrace(e.getStackTrace());
                throw exc;
            }
        } else {
            MonetUrlParser parser = new MonetUrlParser(target, url);
            parser.parseModern();
        }
        target.barrier();
    }

    public static String percentDecode(String context, String text) throws URISyntaxException {
        try {
            return URLDecoder.decode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("should be unreachable: UTF-8 unknown??", e);
        } catch (IllegalArgumentException e) {
            throw new URISyntaxException(text, context + ": invalid percent escape");
        }
    }

    public static String percentEncode(String text) {
        try {
            return URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private void parseModern() throws URISyntaxException, ValidationError {
        clearBasic();

        String scheme = url.getScheme();
        if (scheme == null) throw new URISyntaxException(urlText, "URL scheme must be monetdb:// or monetdbs://");
        switch (scheme) {
            case "monetdb":
                target.setTls(false);
                break;
            case "monetdbs":
                target.setTls(true);
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
        if (authority == null) {
            if (!url.getRawSchemeSpecificPart().startsWith("//")) {
                throw new URISyntaxException(urlText, "expected //");
            }
            host = "";
            remainder = "";
        } else if (authority.equals("-")) {
            host = "";
            remainder = "";
        } else {
            if (authority.startsWith("[")) {
                // IPv6
                pos = authority.indexOf(']');
                if (pos < 0)
                    throw new URISyntaxException(urlText, "unmatched '['");
                host = authority.substring(1, pos);
                remainder = authority.substring(pos + 1);
            } else if ((pos = authority.indexOf(':')) >= 0) {
                host = authority.substring(0, pos);
                remainder = authority.substring(pos);
            } else {
                host = authority;
                remainder = "";
            }
        }
        host = Target.unpackHost(host);
        target.setHost(host);

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
                throw new ValidationError(urlText, "invalid port number");
            target.setString(Parameter.PORT, portStr);
        }

        String path = url.getRawPath();
        String[] parts = path.split("/", 4);
        // <0: empty before leading slash> / <1: database> / <2: tableschema> / <3: table> / <4: should not exist>
        switch (parts.length) {
            case 4:
                target.setString(Parameter.TABLE, percentDecode(Parameter.TABLE.name, parts[3]));
                // fallthrough
            case 3:
                target.setString(Parameter.TABLESCHEMA, percentDecode(Parameter.TABLESCHEMA.name, parts[2]));
                // fallthrough
            case 2:
                target.setString(Parameter.DATABASE, percentDecode(Parameter.DATABASE.name, parts[1]));
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
                target.setString(key, percentDecode(key, value));
            }
        }
    }

    private void parseClassic() throws URISyntaxException, ValidationError {
        if (!url.getRawSchemeSpecificPart().startsWith("//")) {
            throw new URISyntaxException(urlText, "expected //");
        }

        String scheme = url.getScheme();
        if (scheme == null)
            scheme = "";
        switch (scheme) {
            case "monetdb":
                parseClassicAuthorityAndPath();
                break;
            case "merovingian":
                String authority = url.getRawAuthority();
                // authority must be "proxy" ignore authority and path
                boolean valid = urlText.startsWith("merovingian://proxy?") || urlText.equals("merovingian://proxy");
                if (!valid)
                    throw new URISyntaxException(urlText, "with mapi:merovingian:, only //proxy is supported");
                break;
            default:
                throw new URISyntaxException(urlText, "URL scheme must be mapi:monetdb:// or mapi:merovingian://");
        }

        final String query = url.getRawQuery();
        if (query != null) {
            final String args[] = query.split("&");
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("language=")) {
                    String language = arg.substring(9);
                    target.setString(Parameter.LANGUAGE, language);
                } else if (arg.startsWith("database=")) {
                    String database = arg.substring(9);
                    target.setString(Parameter.DATABASE, database);
                } else {
                    // ignore
                }
            }
        }
    }

    private void parseClassicAuthorityAndPath() throws URISyntaxException, ValidationError {
        clearBasic();
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
                throw new ValidationError(urlText, "invalid port number");
            }
            target.setString(Parameter.PORT, portStr);
        }

        String path = url.getRawPath();
        if (host.isEmpty() && portStr.isEmpty()) {
            // socket
            target.clear(Parameter.HOST);
            target.setString(Parameter.SOCK, path != null ? path : "");
        } else {
            // tcp
            target.clear(Parameter.SOCK);
            target.setString(Parameter.HOST, host);
            if (path == null || path.isEmpty()) {
                // do nothing
            } else if (!path.startsWith("/")) {
                throw new URISyntaxException(urlText, "expect path to start with /");
            } else {
                String database = path.substring(1);
                target.setString(Parameter.DATABASE, database);
            }
        }
    }

    private void clearBasic() {
        target.clear(Parameter.TLS);
        target.clear(Parameter.HOST);
        target.clear(Parameter.PORT);
        target.clear(Parameter.DATABASE);
    }
}
