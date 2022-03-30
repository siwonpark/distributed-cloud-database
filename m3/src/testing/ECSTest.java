package testing;

import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import java.util.*;
import shared.HashUtils;
import shared.MetadataUtils;
import shared.messages.KVMessage;

import static java.lang.Thread.sleep;
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
     * Test that the ECS detects failure correctly and spawns up a new node to replace it
     */
    public void testFailureDetection() {
        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // kill the node
        ecs.kill(node.getNodeName());

        // sleep as there is delay until emphemeral node has been deleted and new node has spawned
        try {
            sleep(10000);
        } catch (InterruptedException ignored) {
        }

        // check node is no longer in hash ring
        assertFalse(ecs.getNodes().containsKey(node.getHash()));

        // check new node has been spawned to replace
        assertEquals(1, ecs.getNodes().size());
    }

    /**
     * Test that the ECS detects failure correctly and is able to recover lost data properly from a replica
     */
    public void testFailureDetectionDataTransfer() {
        ArrayList<String> addedKeys = new ArrayList<>();
        Exception ex = null;

        // start with no nodes
        ecs.shutdown();

        // add node
        ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // add node to kill
        ECSNode nodeToKill = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);

        // start service
        ecs.start();

        try {
            // start kv client
            KVStore kvClient = new KVStore("localhost", nodeToKill.getNodePort());
            kvClient.connect();

            HashSet<String> seenNodes = new HashSet<>();

            // populate datastore until each node responsible for at least 1 key
            int num = 1234;
            while (true) {
                kvClient.put(String.valueOf(num), String.valueOf(num));
                addedKeys.add(String.valueOf(num));
                if (nodeToKill.isResponsibleForKey(HashUtils.computeHash(String.valueOf(num)))) {
                    seenNodes.add(nodeToKill.getNodeName());
                } else {
                    seenNodes.add(node.getNodeName());
                }

                if (seenNodes.size() == 2) {
                    break;
                }

                num++;
            }

            // disconnect kvClient
            kvClient.disconnect();

            // kill the node
            ecs.kill(nodeToKill.getNodeName());

            // sleep as there is delay until emphemeral node has been deleted and new node has spawned
            try {
                sleep(10000);
            } catch (InterruptedException ignored) {
            }

            // reconnect to node
            kvClient = new KVStore("localhost", node.getNodePort());
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
     * Test that when a server fails in the service, a client gets automatically reconnected to another node
     */
    public void testFailureDetectionClientReconnection() {
        Exception ex = null;
        // start with no nodes
        ecs.shutdown();

        // add 3 nodes
        IECSNode[] addedNodes = ecs.addNodes(2, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

        // start service
        ecs.start();
        try {
            // start kv client and connect to one node
            KVStore kvClient = new KVStore("localhost", addedNodes[0].getNodePort());
            kvClient.connect();
            // Put some keys, until we get metadata in the client
            HashSet<String> needed =
                    new HashSet<>(
                            Arrays.asList(
                                    addedNodes[0].getNodeName(),
                                    addedNodes[1].getNodeName()));
            int num = 100;

            // populate datastore until all nodes responsible for at least one key
            while (!needed.isEmpty()) {
                ECSNode responsible = MetadataUtils.getResponsibleServerForKey(String.valueOf(num), (TreeMap<String, ECSNode>) ecs.getNodes());
                kvClient.put(String.valueOf(num), String.valueOf(num));
                needed.remove(responsible.getNodeName());
                num++;
            }

            // At this point, the client should have metadata of the hash ring
            // When we disconnect from one server, it should connect to the other

            ecs.kill(addedNodes[0].getNodeName());

            try {
                sleep(10000);
            } catch (InterruptedException ignored) {
            }

            assert(kvClient.isRunning());
            assert(kvClient.getPort() == addedNodes[1].getNodePort());
            assert(Objects.equals(kvClient.getHost(), addedNodes[1].getNodeHost()));
        } catch (Exception e)  {
            ex = e;
        }
        assertNull(ex);
    }
}
