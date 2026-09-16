import org.monetdb.testinfra.Config;

/**
 * Invoked by Mtest to run the TLS tests.
 */
public class SlowTester extends JUnitTester {
	public static void main(String[] args) {
		runTests("slow", Config.TLSTESTER_PROPERTY, args);
	}
}


