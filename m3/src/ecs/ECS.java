package ecs;

import org.apache.log4j.Logger;
import shared.MetadataUtils;
import shared.KVAdminMessage;

import java.io.File;
import java.io.FileNotFoundException;
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

    public boolean forceConsistency(ECSNode node) {
        int try_times = 3;
        while(try_times >= 0){
            try_times -= 1;
            logger.info("SENDING FORCE CONSISTENCY to " + node.getNodeName());
            KVAdminMessage data = new KVAdminMessage(null, KVAdminMessage.OperationType.FORCE_CONSISTENCY);
            zkWatcher.setData(node.getNodeName(), data);
            if (!awaitNodes(1, 1000)) {
                continue;
            }else{
                return true;
            }
        }
        return false;
    }



    public boolean InitServerWithData(ECSNode node){
        if(MetadataUtils.getServersNum(hashRing) <= 1){// no need to move anything
            return true;
        }
        if(MetadataUtils.getServersNum(hashRing) <= 3){// move all the data from another Node (replica)
            ECSNode oneotherNode = MetadataUtils.getSuccessor(hashRing, node);
            return  moveData(
                        oneotherNode,
                        node,
                        node.getNodeHashRange()[0],
                        node.getNodeHashRange()[0]);
        }
        // total servers on the ring(including new one) > 3
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, node);
        if(!moveData(successor, node, node.getNodeHashRange()[0], node.getNodeHashRange()[1])){// init coordinator database
            return false;
        }
        ECSNode predecessor = MetadataUtils.getPredecessor(hashRing, node);
        if(!moveData(predecessor, node, predecessor.getNodeHashRange()[0], predecessor.getNodeHashRange()[1])){// init middle replica
            return false;
        }
        ECSNode predecessor2 = MetadataUtils.getPredecessor(hashRing, predecessor);
        if(!moveData(predecessor2, node, predecessor2.getNodeHashRange()[0], predecessor2.getNodeHashRange()[1])){// init tail replica
            return false;
        }
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
        
        if(!InitServerWithData(node)){
            logger.error("Init Server With Moving Data failed, rolling back changes");
            availableNodes.add(node);
            removeNodeFromHashRing(node);
            return null;
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

    public ECSNode getECSNode(String nodeName) {
        for (ECSNode node : hashRing.values()) {
            if (node.getNodeName().equals(nodeName)) {
                return node;
            }
        }

        return null;
    }

    public void handleServerFailure(String nodeName) {
        // reconstruct service
        // find node with name nodeName
        ECSNode nodeToRemove = getECSNode(nodeName);

        if (nodeToRemove == null) {
            logger.error("Node does not exist");
            return;
        }

        //removeNodeFromHashRing(nodeToRemove);
        removeNode(nodeName, false);
        //redistributeReplicas(nodeToRemove);

        // replace failed node with new node
        // TODO: decide what to do with cache strategy/size
        if (addNode("FIFO", 100, true) == null) {
            broadcastMetadataAndWait();
        }
    }

    private boolean redistributeReplicas(ECSNode failedNode) {
        // Move data from failed node's replica to that node's last replica
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, failedNode);
        ECSNode successorReplica = MetadataUtils.getSuccessor(hashRing, successor);
        ECSNode furthestSuccessorReplica = MetadataUtils.getSuccessor(hashRing, successorReplica);
        if (successor != furthestSuccessorReplica && successor != failedNode && furthestSuccessorReplica != failedNode) {
            boolean result =
                    moveData(
                            successor,
                            furthestSuccessorReplica,
                            failedNode.getNodeHashRange()[0],
                            failedNode.getNodeHashRange()[1]);
            if (!result) {
                logger.error("Move data failed between node " + successor.getNodeName() + " and " + furthestSuccessorReplica.getNodeName());
                return false;
            }
        }

        // Move data from failed node's predecessor to new replica
        ECSNode predecessor = MetadataUtils.getPredecessor(hashRing, failedNode);
        if (predecessor != failedNode && predecessor != successorReplica && failedNode != successorReplica) {
            boolean result =
                    moveData(
                            predecessor,
                            successorReplica,
                            predecessor.getNodeHashRange()[0],
                            predecessor.getNodeHashRange()[1]);
            if (!result) {
                logger.error("Move data failed between node " + predecessor.getNodeName() + " and " + successorReplica.getNodeName());
                return false;
            }
        }

        // Move data from furthest predecessor replica to successor
        ECSNode furthestPredecessorReplica = MetadataUtils.getPredecessor(hashRing, predecessor);
        if (furthestPredecessorReplica != failedNode && furthestPredecessorReplica != successor && successor != failedNode) {
            boolean result =
                    moveData(
                            furthestPredecessorReplica,
                            successor,
                            furthestPredecessorReplica.getNodeHashRange()[0],
                            furthestPredecessorReplica.getNodeHashRange()[1]);
            if (!result) {
                logger.error("Move data failed between node " + furthestPredecessorReplica.getNodeName() + " and " + successor.getNodeName());
                return false;
            }
        }

        return true;
    }

    public void broadcastMetadataAndWait() {
        logger.info("BROADCASTING METADATA");
        KVAdminMessage data = new KVAdminMessage(hashRing, KVAdminMessage.OperationType.METADATA);
        // Watch all child nodes
        for (ECSNode node : hashRing.values()) {
            logger.info("Watching node " + node.getNodeName());
            zkWatcher.watchNode(node.getNodeName());
        }
        zkWatcher.setData("metadata", data);

        if (!awaitNodes(hashRing.size(), 10000)) {
            logger.error("Did not receive acknowledgement from all nodes");
        }
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


    /**
     * need to use a class to store the locked servers in case the ring is changed and can't find the locked servers.
    */
    class ChainLocker{
        private ECSNode coordinator, middle, tail;
        ChainLocker(ECSNode coordinator, TreeMap<String, ECSNode> hashring){
            this.coordinator = coordinator;
            try{
                this.middle = MetadataUtils.getSuccessor(hashRing, coordinator);
                this.tail = MetadataUtils.getSuccessor(hashRing, middle);
            } catch (Exception e) {
                logger.error("An exception occurred while init Chain Locker");
            }
        }
        @Override
        protected void finalize() throws Throwable {
            this.unlock();
        }
        public boolean lock(){
            return (lockWrite(coordinator) && lockWrite(middle) && lockWrite(tail));
        }
        public boolean unlock(){
            return (unlockWrite(coordinator) && unlockWrite(middle) && unlockWrite(tail));
        }
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
     * @param count number of nodes to wait for
     * @param timeout the timeout in milliseconds
     * @return true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) {
        try {
            logger.info("Waiting for " + count + " nodes to acknowledge");
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

    /**
     * when we call MoveDataAfterNodeRemoved, the nodeToRemove is still on the hashring
    */
    public boolean MoveDataAfterNodeRemoved(ECSNode nodeToRemove, boolean isServerAlive){
        if(MetadataUtils.getServersNum(hashRing) <= 3){// no need to move anything, because every node has all the data
            return true;
        }
        // total servers on the ring(including the node to remove) > 3
        ECSNode successor = MetadataUtils.getSuccessor(hashRing, nodeToRemove);
        ECSNode successor2 = MetadataUtils.getSuccessor(hashRing, successor);
        ECSNode predecessor = MetadataUtils.getPredecessor(hashRing, nodeToRemove);
        if(!moveData(predecessor, successor2, predecessor.getNodeHashRange()[0], predecessor.getNodeHashRange()[1])){// init successor2's tail replica
            return false;
        }
        ECSNode predecessor2 = MetadataUtils.getPredecessor(hashRing, predecessor);
        if(!moveData(predecessor2, successor, predecessor2.getNodeHashRange()[0], predecessor2.getNodeHashRange()[1])){// init successor's tail replica
            return false;
        }
        ECSNode successor3 = MetadataUtils.getSuccessor(hashRing, successor2);
        if(!moveData(successor, successor3, nodeToRemove.getNodeHashRange()[0], nodeToRemove.getNodeHashRange()[1])){// init successor3's tail replica
            return false;
        }
        return true;
    }


    public boolean removeNode(String nodeName) {
        return removeNode(nodeName, true);
    }

    public boolean removeNode(String nodeName, boolean isServerAlive) {
        // find node with name nodeName
        ECSNode nodeToRemove = getECSNode(nodeName);

        if (nodeToRemove == null) {
            logger.error("Node does not exist");
            return false;
        }

        
        if(!MoveDataAfterNodeRemoved(nodeToRemove, isServerAlive)){
            logger.error("Move data failed.");
            return false;
        }

        removeNodeFromHashRing(nodeToRemove);

        if(isServerAlive){
            shutDown(nodeToRemove);
        } else {
            zkWatcher.deleteZnode(nodeName);
        }
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
        String script = String.format("%s/apache-zookeeper-3.7.0-bin/bin/zkServer.sh start", rootPath);
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

        // Create metadata node
        zkWatcher.create(ZKWatcher.COMMAND_PATH + "/metadata");

        // Create ack node
        zkWatcher.create(ZKWatcher.ACK_PATH);
    }
}
