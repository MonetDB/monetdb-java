import org.monetdb.merovingian.Control;
import org.monetdb.merovingian.MerovingianException;

import java.io.IOException;

public class ControlTester {
	public static final String USAGE = "Usage: ControlTester HOST[:PORT] PASSWORD";

	private final String host;
	private final int port;
	private final String password;

	public ControlTester(String host, int port, String password) {
		this.host = host;
		this.port = port;
		this.password = password;
	}

	public static void main(String[] args) throws MerovingianException, IOException {
		final String host;
		final int port;
		final String password;
		if (args.length != 2) {
			System.err.println(USAGE);
			System.exit(1);
		}
		password = args[1];
		int colonPos = args[0].indexOf(':');
		if (colonPos < 0) {
			host = args[0];
			port = 50000;
		} else {
			host = args[0].substring(0, colonPos);
			port = Integer.parseInt(args[0].substring(colonPos + 1));
		}

		ControlTester tester = new ControlTester(host, port, password);
		tester.run();
	}

	private void run() throws MerovingianException, IOException {
		testGetStatus();
	}

	private void testGetStatus() throws MerovingianException, IOException {
		Control c = new Control(host, port, password);
		c.getAllStatuses();
	}
}
