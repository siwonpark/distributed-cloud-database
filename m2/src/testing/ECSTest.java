package testing;

import ecs.ECS;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashSet;

import static testing.AllTests.ecsServer;

public class ECSTest extends TestCase {
//    @Test
//    /**
//     * Test that the nodes in the hash ring have the correct hash ranges set
//     */
//    public void testHashRing() {
//        ECS ecs = new ECS("ecs.config");
//        HashSet<String> visited = new HashSet<>();
//
//        String currHash = ecs.hashRing.firstKey();
//
//        while (!visited.contains(currHash)) {
//            visited.add(currHash);
//
//            ECSNode node = ecs.hashRing.get(currHash);
//            assertEquals(currHash, node.getNodeHashRange()[1]);
//
//            if (currHash != ecs.hashRing.firstKey()) {
//                assertTrue(node.getNodeHashRange()[0].compareTo(node.getNodeHashRange()[1]) < 0);
//            }
//
//            currHash = node.getNodeHashRange()[0];
//        }
//
//        assertTrue(visited.containsAll(ecs.hashRing.keySet()));
//    }


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
