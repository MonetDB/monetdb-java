import org.monetdb.testinfra.MtestLauncher;

import static java.lang.System.exit;

/**
 * Common code for the main methods of the legacy Mtest entry points
 * {@link JDBC_API_Tester#main(String[])}, {@link OnClientTester#main(String[])}
 * and {@link TLSTester#main(String[])}.
 */
public abstract class JUnitTester {

	/**
	 * Method to be invoked from the main() class of subclasses.
	 * The main methods are called with a parameter, usually a jdbc: URL
	 * but sometimes something else. This method stores the parameter
	 * in the given system property for the tests to find.
	 * @param tags JUnit tag expression to select tests to run, for example
	 *             "api&!slow" or "tls".
	 * @param property System property in which to store the value passed
	 *                 on the command line
	 * @param args Arguments passed to main()
	 */
	public static void runTests(String tags, String property, String[] args) {
		String value = null;
		boolean verbose = false;

		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-v"))
				verbose = true;
			else if (arg.equals("-a"))
				i++ /* sometimes passed to TLSTester, ignore */;
			else if (arg.equals("-supportsNestedTypes"))
				i++ /* sometimes passed to JDBC_API_Tester, ignore */;
			else if (arg.startsWith("-")) {
				System.err.println("Invalid flag " + arg);
				exit(1);
			} else if (value != null) {
				System.err.println("Duplicate value: " + arg + ". Already have " + value);
			} else {
				value = arg;
			}
		}

		MtestLauncher launcher = new MtestLauncher(tags);
		launcher.setVerbose(verbose);

		if (value != null) {
			launcher.logAlways().println("Command line: setting " + property + " to " + value);
			System.setProperty(property, value);
		}

		int status = launcher.run();
		System.exit(status);
	}
}


