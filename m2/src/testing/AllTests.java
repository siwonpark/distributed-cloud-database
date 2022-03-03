package testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Logger;
import persistence.DataBase;

public class AllTests {
    public static final int PORT = 50000;
    public static final String CACHE_STRATEGY = "FIFO";
    public static final int CACHE_SIZE = 5;
    public static ECS ecsServer;
    private static Logger logger = Logger.getRootLogger();

	static {
		try {
			/* Refresh data directory when running tests */
			new LogSetup("logs/testing/test.log", Level.ERROR);
            ecsServer = new ECS("src/testing/ecs.config");
            ArrayList<IECSNode> nodes =  ecsServer.addNodes(2, CACHE_STRATEGY, CACHE_SIZE);
            for(IECSNode node : nodes){
               ecsServer.start((ECSNode) node);
            }

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(DataBasePutGetTest.class);
        clientSuite.addTestSuite(CacheTest.class);
        clientSuite.addTestSuite(DataBaseReBootTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(CLITest.class);
		clientSuite.addTestSuite(LoadTest.class);
//        //Commenting out until we figure out how to test with zookeeper
//        clientSuite.addTestSuite(ECSTest.class);
        // We *NEED* this to be the last test in the suite!!!!!
        clientSuite.addTestSuite(ShutDownTest.class);

        return clientSuite;
    }

}