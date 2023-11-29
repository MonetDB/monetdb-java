import org.monetdb.mcl.net.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Properties;

public class UrlTester {
    String filename = null;
    int verbose = 0;
    BufferedReader reader = null;
    int lineno = 0;
    int testCount = 0;
    Properties props = null;
    Target validated = null;

    public UrlTester() {
    }

    public UrlTester(String filename) {
        this.filename = filename;
    }

    public static void main(String[] args) throws Exception {
        int exitcode;
        UrlTester tester = new UrlTester();
        exitcode = tester.parseArgs(args);
        if (exitcode == 0)
            exitcode = tester.run();
        System.exit(exitcode);
    }

    private int parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                int result = handleFlags(arg);
                if (result != 0)
                    return result;
            } else if (filename == null) {
                filename = arg;
            } else {
                System.err.println("Unexpected argument: " + arg);
                return 1;
            }
        }
        return 0;
    }

    private int run() throws IOException {
        if (filename != null) {
            reader = new BufferedReader(new FileReader(filename));
        } else {
            String resourceName = "/tests.md";
            InputStream stream = this.getClass().getResourceAsStream(resourceName);
            if (stream == null) {
                System.err.println("Resource " + resourceName + " not found");
                return 1;
            }
            reader = new BufferedReader(new InputStreamReader(stream));
            filename = "tests/tests.md";
        }

        try {
            processFile();
        } catch (Failure e) {
            System.err.println("" + filename + ":" + lineno + ": " + e.getMessage());
            return 1;
        }
        return 0;
    }

    private int handleFlags(String arg) {
        if (!arg.startsWith("-") || arg.equals("-")) {
            System.err.println("Invalid flag: " + arg);
        }
        String a = arg.substring(1);

        while (!a.isEmpty()) {
            char letter = a.charAt(0);
            a = a.substring(1);
            switch (letter) {
                case 'v':
                    verbose++;
                    break;
                default:
                    System.err.println("Unexpected flag " + letter + " in " + arg);
                    return -1;
            }
        }

        return 0;
    }

    private void processFile() throws IOException, Failure {
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;
            lineno++;
            processLine(line);
        }
        if (verbose >= 1) {
            System.out.println();
            System.out.println("Ran " + testCount + " tests in " + lineno + " lines");
        }
    }

    private void processLine(String line) throws Failure {
        line = line.replaceFirst("\\s+$", ""); // remove trailing
        if (props == null && line.equals("```test")) {
            if (verbose >= 2) {
                if (testCount > 0) {
                    System.out.println();
                }
                System.out.println("\u25B6 " + filename + ":" + lineno);
            }
            props = Target.defaultProperties();
            testCount++;
            return;
        }
        if (props != null) {
            if (line.equals("```")) {
                stopProcessing();
                return;
            }
            handleCommand(line);
        }
    }

    private void stopProcessing() {
        props = null;
        validated = null;
    }

    private void handleCommand(String line) throws Failure {
        if (verbose >= 3) {
            System.out.println(line);
        }
        if (line.isEmpty())
            return;

        String[] parts = line.split("\\s+", 2);
        String command = parts[0];
        switch (command.toUpperCase()) {
            case "ONLY":
                handleOnly(true, parts[1]);
                return;
            case "NOT":
                handleOnly(false, parts[1]);
                return;
            case "PARSE":
                handleParse(parts[1], null);
                return;
            case "ACCEPT":
                handleParse(parts[1], true);
                return;
            case "REJECT":
                handleParse(parts[1], false);
                return;
            case "SET":
                handleSet(parts[1]);
                return;
            case "EXPECT":
                handleExpect(parts[1]);
                return;
            default:
                throw new Failure("Unexpected command: " + command);
        }

    }

    private void handleOnly(boolean mustBePresent, String rest) throws Failure {
        boolean found = false;
        for (String part: rest.split("\\s+")) {
            if (part.equals("jdbc")) {
                found = true;
                break;
            }
        }
        if (found != mustBePresent) {
            // do not further process this block
            stopProcessing();
        }
    }

    private int findEqualSign(String rest) throws Failure {
        int index = rest.indexOf('=');
        if (index < -1)
            throw new Failure("Expected to find a '='");
        return index;
    }

    private String splitKey(String rest) throws Failure {
        int index = findEqualSign(rest);
        return rest.substring(0, index);
    }

    private String splitValue(String rest) throws Failure {
        int index = findEqualSign(rest);
        return rest.substring(index + 1);
    }

    private void handleSet(String rest) throws Failure {
        validated = null;
        String key = splitKey(rest);
        String value = splitValue(rest);

        props.put(key, value);
    }

    private void handleParse(String rest, Boolean shouldSucceed) throws Failure {
        URISyntaxException parseError = null;
        ValidationError validationError = null;

        validated = null;
        try {
            MonetUrlParser.parse(props, rest);
        } catch (URISyntaxException e) {
            parseError = e;
        }

        if (parseError == null) {
            try {
                validated = new Target(props);
            } catch (ValidationError e) {
                validationError = e;
            }
        }

        if (shouldSucceed == Boolean.FALSE) {
            if (parseError != null || validationError != null)
                return; // happy
            else
                throw new Failure("URL unexpectedly parsed and validated");
        }

        if (parseError != null)
            throw new Failure("Parse error: " + parseError);
        if (validationError != null && shouldSucceed == Boolean.TRUE)
            throw new Failure("Validation error: " + validationError);
    }

    private void handleExpect(String rest) throws Failure {
        String key = splitKey(rest);
        String expectedString = splitValue(rest);

        Object actual = null;
        try {
            actual = extract(key);
        } catch (ValidationError e) {
            throw new Failure(e.getMessage());
        }

        Object expected;
        try {
            if (actual instanceof Boolean)
                expected = ParameterType.Bool.parse(key, expectedString);
            else if (actual instanceof Integer)
                expected = ParameterType.Int.parse(key, expectedString);
            else
                expected = expectedString;
        } catch (ValidationError e) {
            String typ = actual.getClass().getName();
            throw new Failure("Cannot convert expected value <" + expectedString + "> to " + typ + ": " + e.getMessage());
        }

        if (actual.equals(expected))
            return;
        throw new Failure("Expected " + key + "=<" + expectedString + ">, found <" + actual + ">");
    }

    private Target tryValidate() throws ValidationError {
        if (validated == null)
            validated = new Target(props);
        return validated;
    }

    private Object extract(String key) throws ValidationError, Failure {
        switch (key) {
            case "tls":
                return Target.validateBoolean(props, Parameter.TLS);
            case "host":
                return Target.validateString(props, Parameter.HOST);
            case "port":
                return Target.validateInt(props, Parameter.PORT);
            case "database":
                return Target.validateString(props, Parameter.DATABASE);
            case "tableschema":
                return Target.validateString(props, Parameter.TABLESCHEMA);
            case "table":
                return Target.validateString(props, Parameter.TABLE);
            case "sock":
                return Target.validateString(props, Parameter.SOCK);
            case "sockdir":
                return Target.validateString(props, Parameter.SOCKDIR);
            case "cert":
                return Target.validateString(props, Parameter.CERT);
            case "certhash":
                return Target.validateString(props, Parameter.CERTHASH);
            case "clientkey":
                return Target.validateString(props, Parameter.CLIENTKEY);
            case "clientcert":
                return Target.validateString(props, Parameter.CLIENTCERT);
            case "user":
                return Target.validateString(props, Parameter.USER);
            case "password":
                return Target.validateString(props, Parameter.PASSWORD);
            case "language":
                return Target.validateString(props, Parameter.LANGUAGE);
            case "autocommit":
                return Target.validateBoolean(props, Parameter.AUTOCOMMIT);
            case "schema":
                return Target.validateString(props, Parameter.SCHEMA);
            case "timezone":
                return Target.validateInt(props, Parameter.TIMEZONE);
            case "binary":
                return Target.validateString(props, Parameter.BINARY);
            case "replysize":
                return Target.validateInt(props, Parameter.REPLYSIZE);
            case "fetchsize":
                return Target.validateInt(props, Parameter.FETCHSIZE);
            case "hash":
                return Target.validateString(props, Parameter.HASH);
            case "debug":
                return Target.validateBoolean(props, Parameter.DEBUG);
            case "logfile":
                return Target.validateString(props, Parameter.LOGFILE);

            case "valid":
                try {
                    tryValidate();
                } catch (ValidationError e) {
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;

            case "connect_scan":
                return tryValidate().connectScan();
            case "connect_port":
                return tryValidate().connectPort();
            case "connect_unix":
                return tryValidate().connectUnix();
            case "connect_tcp":
                return tryValidate().connectTcp();
            case "connect_tls_verify":
                switch (tryValidate().connectVerify()) {
                    case None: return "";
                    case Cert: return "cert";
                    case Hash: return "hash";
                    case System: return "system";
                    default:
                        throw new IllegalStateException("unreachable");
                }
            case "connect_certhash_digits":
                return tryValidate().connectCertHashDigits();
            case "connect_binary":
                return tryValidate().connectBinary();
            case "connect_clientkey":
                return tryValidate().connectClientKey();
            case "connect_clientcert":
                return tryValidate().connectClientCert();

            default:
                throw new Failure("Unknown attribute: " + key);
        }
    }

    private class Failure extends Exception {
        public Failure(String message) {
            super(message);
        }
    }
}