package testing;

import junit.framework.TestCase;
import persistence.FileOp;

import static testing.AllTests.ecs;

public class ShutDownTest extends TestCase {
    /**
     * A bit of a hacky way to shutDown all the servers in the ECS.
     * Add this as the last test suite in AllTests.
     */

    /**
     * Test that the ECSServer can shut down
     */
    public void testShutDown() {
        Exception ex = null;
        boolean success = true;
        try {
            ecs.shutdown();
            deleteDataDir();
        } catch (Exception e){
            ex = e;
        }

        assertNull(ex);
        assertTrue(success);
    }

    // Delete the data directory if it exists
    private static void deleteDataDir() {
        String rootPath = System.getProperty("user.dir");
        FileOp.deleteDirectory(rootPath + "/data");
    }
}
