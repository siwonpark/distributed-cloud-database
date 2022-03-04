package testing;

import junit.framework.TestCase;

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
}