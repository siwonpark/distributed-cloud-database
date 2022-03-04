package testing;

import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import java.util.HashSet;
import java.util.TreeMap;


public class ECSTest extends TestCase {
    @Test
    /**
     * Test that the nodes in the hash ring have the correct hash ranges set
     */
    public void testHashRing() {
        TreeMap<String, ECSNode> hashRing = (TreeMap<String, ECSNode>) AllTests.ecs.getNodes();
        HashSet<String> visited = new HashSet<>();

        String currHash = hashRing.firstKey();

        while (!visited.contains(currHash)) {
            visited.add(currHash);

            ECSNode node = hashRing.get(currHash);
            assertEquals(currHash, node.getNodeHashRange()[1]);

            if (currHash != hashRing.firstKey()) {
                assertTrue(node.getNodeHashRange()[0].compareTo(node.getNodeHashRange()[1]) < 0);
            }

            currHash = node.getNodeHashRange()[0];
        }

        assertTrue(visited.containsAll(hashRing.keySet()));
    }
}
