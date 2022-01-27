package testing;

import java.io.IOException;

import app_kvServer.FileOp;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			/* Refresh data directory when running tests */
			FileOp.deleteDirectory(System.getProperty("user.dir") + "/data");
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new KVServer(50000, 0, null).start();
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
		return clientSuite;
	}
	
}
