package testing;

import ecs.ECSNode;
import junit.framework.TestCase;

import java.io.IOException;

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
        } catch (Exception e){
            ex = e;
        }

        assertNull(ex);
        assertTrue(success);
    }

}
