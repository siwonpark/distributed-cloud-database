package testing;

import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import java.util.*;
import shared.HashUtils;
import shared.MetadataUtils;
import shared.messages.KVMessage;
import shared.messages.Message;

import static testing.AllTests.*;


public class ECSTest extends TestCase {

    @Override
    protected void setUp(){

    }

    @Override
    protected void tearDown(){
        ecs.shutdown();
        ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);
        ecs.start();
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

    /**
     * Test that data between nodes are migrated when new nodes are added/old nodes are removed
     */
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
            KVStore kvClient = new KVStore("localhost", initialNode.getNodePort());
            kvClient.connect();

            // populate datastore
            for (int i = 0; i < 5; i++) {
                kvClient.put(String.valueOf(i), String.valueOf(i));
                addedKeys.add(String.valueOf(i));
            }

            // add new node
            ECSNode newNode = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

            // add a key that newNode is responsible for
            int num = 5;

            while (true) {
                if (newNode.isResponsibleForKey(HashUtils.computeHash(String.valueOf(num)))) {
                    kvClient.put(String.valueOf(num), String.valueOf(num));
                    addedKeys.add(String.valueOf(num));
                    break;
                }
                num++;
            }

            // disconnect kvClient
            kvClient.disconnect();

            // remove old node
            ecs.removeNodes(Arrays.asList(initialNode.getNodeName()));

            // reconnect kvClient
            kvClient = new KVStore("localhost", newNode.getNodePort());
            kvClient.connect();

            // check that we can still get all keys we added
            for (String key: addedKeys) {
                assertEquals(key, kvClient.get(key).getValue());
            }

        } catch (Exception e)  {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * Test that the service boots up with all nodes in STOPPED mode and calling the start command puts them in
     * START mode and ready to receive requests
     */
    public void testStart() {
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", node.getNodePort());
            kvClient.connect();

            // try making request
            KVMessage response = kvClient.put("testStart", "hello");

            assertEquals(KVMessage.StatusType.SERVER_STOPPED, response.getStatus());

            // start server
            ecs.start();

            // try making request
            response = kvClient.put("testStart", "hello");
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * Test that all nodes are in STOPPED mode when we call command stop
     */
    public void testStop() {
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start server
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", node.getNodePort());
            kvClient.connect();

            // try making request
            KVMessage response = kvClient.put("testStop", "hello");

            assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());

            // stop server
            ecs.stop();

            // try making request
            response = kvClient.get("testStop");
            assertEquals(KVMessage.StatusType.SERVER_STOPPED, response.getStatus());
        } catch (Exception e) {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * Test that even if there are multiple responsible servers, each key can be retrieved by one client connected to
     * a single node using the client-server retry mechanism
     */
    public void testClientServerRetry() {
        Exception ex = null;
        ArrayList<String> addedKeys = new ArrayList<>();

        // start with no nodes
        ecs.shutdown();

        // add 3 nodes
        IECSNode[] addedNodes = ecs.addNodes(3, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

        // start service
        ecs.start();

        try {
            // start kv client and connect to one node
            KVStore kvClient = new KVStore("localhost", addedNodes[0].getNodePort());
            kvClient.connect();

            HashSet<String> needed =
                    new HashSet<>(
                            Arrays.asList(
                                    addedNodes[0].getNodeName(),
                                    addedNodes[1].getNodeName(),
                                    addedNodes[2].getNodeName()));
            int num = 100;

            // populate datastore until all nodes responsible for at least one key
            while (!needed.isEmpty()) {
                ECSNode responsible = MetadataUtils.getResponsibleServerForKey(String.valueOf(num), (TreeMap<String, ECSNode>) ecs.getNodes());

                kvClient.put(String.valueOf(num), String.valueOf(num));
                addedKeys.add(String.valueOf(num));

                needed.remove(responsible.getNodeName());

                num++;
            }

            // check that we can still get all keys we added even when not connected to the other servers
            for (String key: addedKeys) {
                assertEquals(key, kvClient.get(key).getValue());
            }

        } catch (Exception e)  {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * Test that if server is writeLocked client can't put keys into it
     */
    public void testWriteLocked() {
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start server
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", node.getNodePort());
            kvClient.connect();

            KVMessage response = kvClient.put("prelock", "prelock");
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());

            // lock writes to server
            ecs.lockWrite(node.getNodeName());

            // try adding key
            response = kvClient.put("lock", "lock");
            assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK, response.getStatus());

            // get should not be locked
            response = kvClient.get("prelock");
            assertEquals(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
            assertEquals("prelock", response.getValue());

            // unlock writes to server
            ecs.unlockWrite(node.getNodeName());
            response = kvClient.put("lock", "lock");
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * To keep transactions atomic we test that we lock the participating servers appropriately
     */
    public void testTransactionAtomicLock() {
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start server
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", node.getNodePort());
            kvClient.connect();

            // start another kv client
            KVStore kvClient2 = new KVStore("localhost", node.getNodePort());
            kvClient2.connect();

            // define transaction
            ArrayList<Message> operations = new ArrayList<>();

            for (int i = 0; i < 25; i++) {
                operations.add(new Message(Integer.toString(i), Integer.toString(i), KVMessage.StatusType.PUT));
            }

            // commit large transaction
            kvClient.commit(operations);

            // try adding key with other client should get write lock
            KVMessage response = kvClient2.put("checkLock", "checkLock");
            assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK, response.getStatus());
        } catch (Exception e) {
            ex = e;
        }


        assertNull(ex);
    }

    /**
     * Test that transactions
     */
    public void testTransactionUnlockedAfter() {
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start server
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", node.getNodePort());
            kvClient.connect();

            // define transaction
            ArrayList<Message> operations = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                operations.add(new Message(Integer.toString(i), Integer.toString(i), KVMessage.StatusType.PUT));
            }

            // commit large transaction
            kvClient.commit(operations);

            // try adding key with other client should get write lock
            KVMessage response = kvClient.put("checkLock2", "checkLock2");
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
            ex = e;
        }


        assertNull(ex);
    }
}
