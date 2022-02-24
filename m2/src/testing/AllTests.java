package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	public static final int PORT = 50000;

	static {
		try {
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);

			new KVServer(PORT, "127.0.0.1", 0, null).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(CLITest.class);
		clientSuite.addTestSuite(LoadTest.class);
		clientSuite.addTestSuite(ECSTest.class);
		return clientSuite;
	}
	
}
