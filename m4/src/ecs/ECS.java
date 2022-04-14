package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import shared.MetadataUtils;
import shared.KVAdminMessage;
import shared.KVAdminMessage.OperationType;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Array;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECS {
    // TODO: Set hashRing to private after testing
    private static Logger logger = Logger.getRootLogger();
    public TreeMap<String, ECSNode> hashRing = new TreeMap<>();
    private ArrayList<ECSNode> availableNodes;
    private ZKWatcher zkWatcher;
    public boolean serviceRunning = false;

    public ECS(String configFilePath) {
        // Start ZK server
        startZKServer();
        startZKWatcher();

        // Initialize nodes/metadata from config
        availableNodes = getNodesFromConfig(configFilePath);
    }

    public ArrayList<IECSNode> addNodes(int numberOfNodes, String cacheStrategy, int cacheSize) {
        ArrayList<IECSNode> addedNodes = new ArrayList<>();

        if (numberOfNodes > availableNodes.size()) {
            logger.error("Not enough servers available");
            return addedNodes;
        }
        // shuffle availableNodes to make random
        Collections.shuffle(availableNodes);

        for (int i = 0; i < numberOfNodes; i++) {
            addedNodes.add(addNode(cacheStrategy, cacheSize, false));
        }

        broadcastMetadataAndWait();

        return addedNodes;
    }

    public boolean start(ECSNode node) {
        logger.info("SENDING START to " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.START);
        zkWatcher.setData(node.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node " + node.getNodeName() + " failed to respond to START");
            return false;
        }

        return true;
    }

    public boolean stop(ECSNode node) {
        logger.info("SENDING STOP to " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.STOP);
        zkWatcher.setData(node.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node " + node.getNodeName() + " failed to respond to STOP");
            return false;
        }

        return true;
    }

    public boolean shutDown(ECSNode node) {
        logger.info("SENDING SHUTDOWN to " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.SHUT_DOWN);
        zkWatcher.setData(node.getNodeName(), data);

        // KVServer should delete the path instead of sending ACK here
        if (!awaitNodes(1, 10000)) {
            logger.error("Did not receive acknowledgement from all nodes");
            return false;
        }

        removeNodeFromHashRing(node);
        availableNodes.add(node);
        zkWatcher.deleteZnode(node.getNodeName());

        return true;
    }

    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean broadcastMetadata) {
        if (availableNodes.size() == 0) {
            logger.error("No available nodes to provision!");
            return null;
        }
        ECSNode node = availableNodes.remove(availableNodes.size() - 1);

        zkWatcher.watchNode(node.getNodeName());
        zkWatcher.create(ZKWatcher.COMMAND_PATH + "/" + node.getNodeName());

        zkWatcher.create(ZKWatcher.OPERATIONS_PATH + "/" + node.getNodeName());
        zkWatcher.watchOperations(node.getNodeName());

        spawnKVServer(node, cacheStrategy, cacheSize);

        // Wait for KVServer to create znode, if awaitNode is false, adding failed
        if (!awaitNodes(1, 10000)) {
            logger.error(
                    "Node " + node.getNodeName() + " was not able to be added please try again.");
            availableNodes.add(node);
            return null;
        }

        // Update metadata in ECS
        addNodeToHashRing(node);

        boolean success = initServer(node);
        if (!success) {
            logger.error("Failed to init, rolling back changes");
            availableNodes.add(node);
            removeNodeFromHashRing(node);
            return null;
        }

        // Only auto start the nodes if the service is already running
        if (serviceRunning) {
            success = start(node);
            if (!success) {
                logger.error("Failed to start, rolling back changes");
                availableNodes.add(node);
                removeNodeFromHashRing(node);
                return null;
            }
        }

        // Move data if successor exists
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, node);
        if (successor != node) {
            boolean result =
                    moveData(
                            successor,
                            node,
                            node.getNodeHashRange()[0],
                            node.getNodeHashRange()[1]);
            if (!result) {
                logger.error("Move data failed, rolling back changes");
                availableNodes.add(node);
                removeNodeFromHashRing(node);
                return null;
            }
        }

        // Update metadata
        if (broadcastMetadata) {
            broadcastMetadataAndWait();
        }

        return node;
    }

    public boolean initServer(ECSNode node) {
        logger.info("SENDING INIT to " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(hashRing, KVAdminMessage.OperationType.INIT);
        zkWatcher.setData(node.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node " + node.getNodeName() + " failed to respond to INIT");
            return false;
        }

        return true;
    }

    public void broadcastMetadataAndWait() {
        logger.info("BROADCASTING METADATA");
        KVAdminMessage data = new KVAdminMessage(hashRing, KVAdminMessage.OperationType.METADATA);
        // Watch all child nodes
        for (ECSNode node : hashRing.values()) {
            zkWatcher.watchNode(node.getNodeName());
        }
        zkWatcher.setData("metadata", data);

        if (!awaitNodes(hashRing.size(), 10000)) {
            logger.error("Did not receive acknowledgement from all nodes");
        }
    }

    public boolean handleOperations(String initialNodeName, ArrayList<Message> operations) {
        logger.info("Start processing transaction");
        for (Message operation : operations) {
            if (operation.getStatus() == StatusType.GET) {
                logger.info("a get operation, the key is " + operation.getKey());
            } else if (operation.getStatus() == StatusType.PUT) {
                logger.info("a put operation, the key is " + operation.getKey() + " the value is " + operation.getValue());
            } else {
                logger.error("the operation type is wrong!!!");
            }
        }
        ArrayList<Message> replies = new ArrayList<>();
        ArrayList<Message> rollbackMessages = new ArrayList<>();
        OperationType commitStatus = OperationType.COMMIT_SUCCESS;

        logger.info("Locking writes for all participating servers");
        for (Message operation : operations) {
            ECSNode responsibleServer =
                    MetadataUtils.getResponsibleServerForKey(operation.getKey(), hashRing);

            if (responsibleServer == null || !lockWrite(responsibleServer)) {
                logger.error("Failed to lock participating server");
                return false;
            }
        }

        for (Message operation : operations) {
            logger.info("Processing operation");
            Message reply;

            try {
                if (operation.getStatus() == StatusType.GET) {
                    String value = get(operation.getKey());
                    reply = new Message(operation.getKey(), value, StatusType.GET_SUCCESS);
                } else {
                    // get previous value
                    String value = get(operation.getKey());
                    rollbackMessages.add(new Message(operation.getKey(), value, StatusType.PUT));

                    OperationType operationType = put(operation.getKey(), operation.getValue());
                    StatusType statusType = operationType == OperationType.PUT_SUCCESS ? StatusType.PUT_SUCCESS : StatusType.PUT_UPDATE;
                    reply = new Message(
                            operation.getKey(),
                            operation.getValue(),
                            statusType);
                }
                replies.add(reply);
            } catch (Exception e) {
                logger.error("Error processing operation");
                commitStatus = OperationType.COMMIT_FAILED;

                StatusType statusType = operation.getStatus() == StatusType.GET ? StatusType.GET_ERROR : StatusType.PUT_ERROR;
                reply = new Message(operation.getKey(), operation.getValue(), statusType);
                replies.add(reply);

                // process rollback
                for (Message rollbackMessage: rollbackMessages) {
                    logger.info("Rolling back key " + rollbackMessage.getKey() + "to value " + rollbackMessage.getValue());
                    try {
                        put(rollbackMessage.getKey(), rollbackMessage.getValue());
                    } catch (Exception e2) {
                        logger.error("rollback failed for key " + rollbackMessage.getKey());
                    }
                }

                break;
            }

        }

        // once all operations are done unlock nodes
        logger.info("Unlocking writes for all participating servers");
        for (Message operation : operations) {
            ECSNode responsibleServer =
                    MetadataUtils.getResponsibleServerForKey(operation.getKey(), hashRing);

            if (responsibleServer == null || !unlockWrite(responsibleServer)) {
                logger.error("Failed to unlock participating server");
                return false;
            }
        }

        KVAdminMessage allReplies = new KVAdminMessage(null, null, commitStatus);
        allReplies.setOperations(replies);

        zkWatcher.setReplies(initialNodeName, allReplies);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node was not responsive to lock write");
            return false;
        }

        return true;
    }

    public boolean moveData(ECSNode fromNode, ECSNode toNode, String keyStart, String keyEnd) {
        if (!lockWrite(fromNode)) {
            return false;
        }

        // Apply move command
        logger.info("MOVING DATA from " + fromNode.getNodeName() + " to " + toNode.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.MOVE_DATA);
        data.setKeyStart(keyStart);
        data.setKeyEnd(keyEnd);
        data.setTargetNode(toNode);

        zkWatcher.setData(fromNode.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node was not responsive, moveData stopped");
            return false;
        }

        return unlockWrite(fromNode);
    }

    public boolean lockWrite(ECSNode node) {
        logger.info("LOCKING WRITE for " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.LOCK_WRITE);
        zkWatcher.setData(node.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node was not responsive to lock write");
            return false;
        }

        return true;
    }

    public boolean unlockWrite(ECSNode node) {
        logger.info("UNLOCKING WRITE for " + node.getNodeName());
        KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.UNLOCK_WRITE);
        zkWatcher.setData(node.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node was not responsive to unlock write");
            return false;
        }

        return true;
    }

    public OperationType put(String key, String value) throws Exception {
        ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(key, hashRing);
        if (responsibleServer == null) {
            throw new Exception("no server");
        }
        logger.info("PUT " + key + " : " + value + " in " + responsibleServer.getNodeName());

        KVAdminMessage data = new KVAdminMessage(key, value, OperationType.PUT);
        zkWatcher.setData(responsibleServer.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("put failed");
            throw new Exception("put failed");
        }
        return zkWatcher.operationType;
    }

    public String get(String key) throws Exception {
        ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(key, hashRing);
        if (responsibleServer == null) {
            throw new Exception("no server");
        }
        logger.info("GET " + key + " from " + responsibleServer.getNodeName());
        KVAdminMessage data = new KVAdminMessage(key, null, OperationType.GET);
        zkWatcher.setData(responsibleServer.getNodeName(), data);

        if (!awaitNodes(1, 10000)) {
            logger.error("Node was not responsive to unlock write");
            throw new Exception("get failed");
        }
        return zkWatcher.value;
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     *
     * @return array of strings, containing unique names of servers
     */
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    /**
     * Wait for all nodes to report status or until timeout expires
     *
     * @param count   number of nodes to wait for
     * @param timeout the timeout in milliseconds
     * @return true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) {
        try {
            zkWatcher.awaitSignal = new CountDownLatch(count);

            boolean success = zkWatcher.awaitSignal.await(timeout, TimeUnit.MILLISECONDS);
            if (!success) {
                logger.error("awaitNodes timed out");
            }

            return success;
        } catch (Exception e) {
            logger.error("An exception occurred while awaitNodes");
            return false;
        }
    }

    public boolean removeNode(String nodeName) {
        // find node with name nodeName
        ECSNode nodeToRemove = null;
        for (ECSNode node : hashRing.values()) {
            if (node.getNodeName().equals(nodeName)) {
                nodeToRemove = node;
                break;
            }
        }

        if (nodeToRemove == null) {
            logger.error("Node does not exist");
            return false;
        }

        // Move data if successor exists
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, nodeToRemove);
        if (successor != nodeToRemove) {
            boolean result =
                    moveData(
                            nodeToRemove,
                            successor,
                            nodeToRemove.getNodeHashRange()[0],
                            nodeToRemove.getNodeHashRange()[1]);
            if (!result) {
                logger.error("Move data failed, rolling back changes");
                addNodeToHashRing(nodeToRemove);
                return false;
            }
        }

        shutDown(nodeToRemove);
        broadcastMetadataAndWait();

        return true;
    }

    private ArrayList<ECSNode> getNodesFromConfig(String configFilePath) {
        ArrayList<ECSNode> nodes = new ArrayList<>();

        try {
            File config = new File(configFilePath);
            Scanner s = new Scanner(config);

            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] serverInfo = line.split(" ");
                String nodeName = serverInfo[0];
                String nodeHost = serverInfo[1];
                String nodePort = serverInfo[2];

                // Initialize ECS Node with config
                // String hash = HashUtils.computeHash(nodeHost + ":" + nodePort);
                ECSNode node = new ECSNode(nodeName, nodeHost, Integer.parseInt(nodePort));
                nodes.add(node);
            }

            s.close();
        } catch (FileNotFoundException e) {
            System.out.println("Not found");
            // logger.error("ecs config missing, running with no default servers");
        }

        return nodes;
    }

    private void addNodeToHashRing(ECSNode node) {
        logger.info("Adding " + node.getNodeName() + " to internal hash ring");
        // find successor and predecessor
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, node);
        ECSNode predecessor = MetadataUtils.getPredecessor(hashRing, node);

        // Update range of node
        node.setEndHash(node.getHash());
        node.setStartHash(predecessor.getHash());

        // Add node to hashRing
        hashRing.put(node.getHash(), node);

        // Update range of successor (might be itself)
        successor.setStartHash(node.getHash());
    }

    private void removeNodeFromHashRing(ECSNode node) {
        // find successor and predecessor
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, node);
        ECSNode predecessor = MetadataUtils.getPredecessor(hashRing, node);

        // Update range of successor
        successor.setStartHash(predecessor.getHash());

        // Remove node from hashRing
        hashRing.remove(node.getHash());
    }

    /**
     * Get all the managed nodes of the ECS Server right now
     *
     * @return
     */
    public TreeMap<String, ECSNode> getNodes() {
        return this.hashRing;
    }

    private void spawnKVServer(ECSNode node, String cacheStrategy, int cacheSize) {
        Runtime run = Runtime.getRuntime();
        String rootPath = System.getProperty("user.dir");

        String script =
                String.format(
                        "ssh -n %s nohup java -jar %s/m2-server.jar %d %s %s %d %s %d INFO &",
                        node.getNodeHost(),
                        rootPath,
                        node.getNodePort(),
                        node.getNodeName(),
                        ZKWatcher.ZK_HOST,
                        ZKWatcher.ZK_PORT,
                        cacheStrategy,
                        cacheSize);
        try {
            logger.info("SPAWNING KVSERVER: " + node.getNodeName());
            run.exec(script);
        } catch (IOException e) {
            logger.error("Could not start up KVServer through ssh");
        }
    }

    private void startZKServer() {
        Runtime run = Runtime.getRuntime();
        String rootPath = System.getProperty("user.dir");
        String script = String.format("%s/zookeeper-3.4.11/bin/zkServer.sh start", rootPath);
        try {
            run.exec(script);
        } catch (IOException e) {
            logger.error("Could not start up KVServer through ssh");
        }
    }

    private void startZKWatcher() {
        // Connect with Zookeeper watcher client
        zkWatcher = new ZKWatcher(this);
        zkWatcher.connect();

        // create root node
        zkWatcher.create(ZKWatcher.ROOT_PATH);

        // Create command node
        zkWatcher.create(ZKWatcher.COMMAND_PATH);

        // Create operations node
        zkWatcher.create(ZKWatcher.OPERATIONS_PATH);

        // Create metadata node
        zkWatcher.create(ZKWatcher.COMMAND_PATH + "/metadata");

        // Create ack node
        zkWatcher.create(ZKWatcher.ACK_PATH);
    }
}
