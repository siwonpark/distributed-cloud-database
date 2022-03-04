package testing;

import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import java.util.*;
import shared.HashUtils;
import static testing.AllTests.*;


public class ECSTest extends TestCase {

    @Override
    protected void setUp(){

    }

    @Override
    protected void tearDown(){
        ecs.shutdown();
//        ecs.addNodes(2, CACHE_STRATEGY, CACHE_SIZE);
//        ecs.start();
    }

    /**
     * Test that the nodes in the hash ring have the correct hash ranges set
     */
    public void testHashRing() {
        TreeMap<String, ECSNode> hashRing = (TreeMap<String, ECSNode>) ecs.getNodes();
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

    public void testMigrateData() {
        Exception ex = null;
        ArrayList<String> addedKeys = new ArrayList<>();

        // start with no nodes
        ecs.shutdown();

        // add initial node
        ECSNode initialNode = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start service
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("127.0.0.1", initialNode.getNodePort());
            kvClient.connect();

            // populate datastore
            for (int i = 0; i < 5; i++) {
                kvClient.put(String.valueOf(i), String.valueOf(i));
                addedKeys.add(String.valueOf(i));
            }

            // add new node
            ECSNode newNode = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);
//
//            // add a key that newNode is responsible for
//            int num = 5;
//
//            while (true) {
//                if (newNode.isResponsibleForKey(HashUtils.computeHash(String.valueOf(num)))) {
//                    kvClient.put(String.valueOf(num), String.valueOf(num));
//                    addedKeys.add(String.valueOf(num));
//                    break;
//                }
//                num++;
//            }
//
//            // disconnect kvClient
//            kvClient.disconnect();
//
//            // remove old node
//            ecs.removeNodes(Arrays.asList(initialNode.getNodeName()));
//
//            // reconnect kvClient
//            kvClient = new KVStore("localhost", newNode.getNodePort());
//            kvClient.connect();
//
//            // check that we can still get all keys we added
//            for (String key: addedKeys) {
//                assertEquals(key, kvClient.get(key).getValue());
//            }

        } catch (Exception e)  {
            ex = e;
        }


//        assertNull(ex);
    }
}
