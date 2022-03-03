package testing;

import ecs.ECSNode;
import junit.framework.TestCase;

import static testing.AllTests.ecsServer;

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
            ECSNode[] nodes = ecsServer.hashRing.values().toArray(new ECSNode[0]);

            for (ECSNode node: nodes) {
                if(!ecsServer.shutDown(node)) {
                    success = false;
                }
            }
        } catch (Exception e){
            ex = e;
        }
        assertNull(ex);
        assertTrue(success);
    }
}
