package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import persistence.DataBase;


public class AllTests {
	static {
		try {
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(DataBasePutGetTest.class);
		clientSuite.addTestSuite(CacheTest.class);
		clientSuite.addTestSuite(DataBaseReBootTest.class);
		//clientSuite.addTestSuite(SimpleECSTest.class);

		return clientSuite;
	}
}
