package testing;

import app_kvECS.ECSClient;
import ecs.ECSNode;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import persistence.FileOp;

import java.io.File;
import java.io.IOException;


public class AllTests {
    public static final String CACHE_STRATEGY = "FIFO";
    public static final int CACHE_SIZE = 5;
    public static ECSClient ecs;
    public static int port;
    private static Logger logger = Logger.getRootLogger();

	static {
		try {
            deleteDataDir();
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);
            File ecsConfigFile = new File("src/testing/ecs.config");
            ecs = new ECSClient(ecsConfigFile);
            ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);
            port = node.getNodePort();
            ecs.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    // Delete the data directory if it exists
    private static void deleteDataDir() throws IOException {
        String rootPath = System.getProperty("user.home");

        logger.info("Deleting data dir");
        FileOp.deleteDirectory(rootPath + "/data");
    }


    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(DataBasePutGetTest.class);
        clientSuite.addTestSuite(CacheTest.class);
        clientSuite.addTestSuite(DataBaseReBootTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(CLITest.class);
//		clientSuite.addTestSuite(LoadTest.class);
		clientSuite.addTestSuite(ECSTest.class);
        // We *NEED* this to be the last test in the suite!!!!!
        clientSuite.addTestSuite(ShutDownTest.class);
        return clientSuite;
    }

}