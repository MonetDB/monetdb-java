import org.monetdb.testinfra.Config;

/**
 * Invoked by Mtest to run the TLS tests.
 */
public class TLSTester extends JUnitTester {
	public static void main(String[] args) {
		runTests("tls", Config.TLSTESTER_PROPERTY, args);
	}
}


