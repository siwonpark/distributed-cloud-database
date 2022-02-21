package testing;

import ecs.ECS;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashSet;

public class ECSTest extends TestCase {
    @Test
    /**
     * Test that the nodes in the hash ring have the correct hash ranges set
     */
    public void testHashRing() {
        ECS ecs = new ECS("ecs.config");
        HashSet<String> visited = new HashSet<>();

        String currHash = ecs.hashRing.firstKey();

        while (!visited.contains(currHash)) {
            visited.add(currHash);

            ECSNode node = ecs.hashRing.get(currHash);
            assertEquals(currHash, node.getNodeHashRange()[1]);

            currHash = node.getNodeHashRange()[0];
        }

        assertTrue(visited.containsAll(ecs.hashRing.keySet()));
    }
}
