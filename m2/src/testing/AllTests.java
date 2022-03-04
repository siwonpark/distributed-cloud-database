package testing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import app_kvECS.ECSClient;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.ZKWatcher;
import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Logger;


public class AllTests {
    public static final int PORT = 50235;
    public static final String CACHE_STRATEGY = "FIFO";
    public static final int CACHE_SIZE = 5;
    public static ECSClient ecs;
    private static Logger logger = Logger.getRootLogger();

	static {
		try {
            deleteDataDir();
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);
            File ecsConfigFile = new File("src/testing/ecs.config");
            ecs = new ECSClient(ecsConfigFile);
            ecs.addNodes(2, CACHE_STRATEGY, CACHE_SIZE);
            ecs.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    // Delete the data directory if it exists
    private static void deleteDataDir() throws IOException {
        Runtime run = Runtime.getRuntime();
        String rootPath = System.getProperty("user.dir");

        String script = String.format("rm -rf %s/data", rootPath);
        logger.info("Deleting data dir");
        run.exec(script);
    }


    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        //clientSuite.addTestSuite(DataBasePutGetTest.class);
        //clientSuite.addTestSuite(CacheTest.class);
        //clientSuite.addTestSuite(DataBaseReBootTest.class);
		//clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		//clientSuite.addTestSuite(AdditionalTest.class);
		//clientSuite.addTestSuite(CLITest.class);
		//clientSuite.addTestSuite(LoadTest.class);
//        //Commenting out until we figure out how to test with zookeeper
		clientSuite.addTestSuite(ECSTest.class);
        // We *NEED* this to be the last test in the suite!!!!!
        clientSuite.addTestSuite(ShutDownTest.class);
        return clientSuite;
    }

}