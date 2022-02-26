package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	public static final int PORT = 50000;
	public static final String CACHE_STRATEGY = "FIFO";
	public static final int CACHE_SIZE = 5;

	static {
		try {
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);


			new KVServer(PORT, "127.0.0.1", "1", 2, CACHE_STRATEGY,
					CACHE_SIZE).start();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
//		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(CLITest.class);
//		clientSuite.addTestSuite(LoadTest.class);
		//Commenting out until we figure out how to test with zookeeper
		//clientSuite.addTestSuite(ECSTest.class);
		return clientSuite;
	}
	
}
