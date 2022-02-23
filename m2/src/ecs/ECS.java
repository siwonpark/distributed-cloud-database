package ecs;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import shared.HashUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ECS {
    // TODO: Set hashRing to private after testing
    private static Logger logger = Logger.getRootLogger();
    public TreeMap<String, ECSNode> hashRing = new TreeMap<>();
    private ArrayList<ECSNode> availableNodes;
    private ZKWatcher zkWatcher;

    public ECS(String configFileName) {
        // Start ZK server
        startZKServer();
        startZKWatcher();

        // Initialize nodes/metadata from config
        availableNodes = getNodesFromConfig(configFileName);
    }

    public void addNodes(int numberOfNodes) {
        ArrayList<ECSNode> newNodes = new ArrayList<>();

        if (numberOfNodes > availableNodes.size()) {
            logger.error("Not enough servers available");
            return;
        }
        // shuffle availableNodes to make random
        Collections.shuffle(availableNodes);

        for (int i = 0; i < numberOfNodes; i++) {
            ECSNode node = availableNodes.remove(availableNodes.size() - 1);

            // execute ssh call to bring up server
            spawnKVServer(node);
            newNodes.add(node);
        }

        addNodesToHashRing(newNodes);

        //        // create z-node for server with metadata
        //        ZKData data = new ZKData(null, ZKData.OperationType.CREATE);
        //        zkWatcher.create(ZKWatcher.ROOT_PATH + "/" + node.getNodeName(), data);
    }

    public void start() {}

    public void stop() {}

    public void shutDown() {}

    public void addNode() {
        if (availableNodes.size() == 0) {
            logger.error("No available nodes to provision!");
            return;
        }
        ECSNode node = availableNodes.remove(availableNodes.size() - 1);

        zkWatcher.createdSignal = new CountDownLatch(1);
        spawnKVServer(node);

        try {
            zkWatcher.createdSignal.await();
        } catch (InterruptedException e) {
            logger.error("z-node creation failed");
        }



    }

    public void removeNode(int indexOfServer) {}

    private ArrayList<ECSNode> getNodesFromConfig(String configFileName) {
        ArrayList<ECSNode> nodes = new ArrayList<>();

        try {
            String rootPath = System.getProperty("user.dir");
            File config = new File(rootPath + "/src/ecs/" + configFileName);
            Scanner s = new Scanner(config);

            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] serverInfo = line.split(" ");
                String nodeName = serverInfo[0];
                String nodeHost = serverInfo[1];
                String nodePort = serverInfo[2];

                // Initialize ECS Node with config
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

    }

    private void addNodesToHashRing(ArrayList<ECSNode> nodes) {
        // compute position in ring
        for (ECSNode node : nodes) {
            String hash = HashUtils.computeHash(node.getNodeHost() + ":" + node.getNodePort());
            hashRing.put(hash, node);
        }

        // populate hash ranges
        String prevHash = null;

        for (Map.Entry<String, ECSNode> entry : hashRing.entrySet()) {
            String hash = entry.getKey();
            ECSNode node = entry.getValue();
            if (prevHash != null) {
                node.setStartHash(prevHash);
            }
            node.setEndHash(hash);
            prevHash = hash;
        }

        // connect first node to last
        hashRing.firstEntry().getValue().setStartHash(prevHash);
    }

    private void spawnKVServer(ECSNode node) {
        Runtime run = Runtime.getRuntime();
        String rootPath = System.getProperty("user.dir");

        String script =
                String.format(
                        "ssh -n %s nohup java -jar %s/m2-server.jar %d %s %s %d &",
                        node.getNodeHost(), rootPath, node.getNodePort(), node.getNodeName(), ZKWatcher.ZK_HOST, ZKWatcher.ZK_PORT);
        try {
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
        zkWatcher = new ZKWatcher();
        zkWatcher.connect();

        // Wait until client has connected
        try {
            zkWatcher.connectedSignal.await();
        } catch (InterruptedException e) {
            logger.error("Admin node connection failed");
        }

        // Create root z-node
        zkWatcher.create(ZKWatcher.ROOT_PATH, null);
    }

    /**
     * TODO: Remove in future PR, the ECS server should be just initialized in the ECS client.
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
        new LogSetup("logs/ecs.log", Level.INFO);
        ECS ecs = new ECS("ecs.config");
        ecs.addNodes(2);
    }
}
