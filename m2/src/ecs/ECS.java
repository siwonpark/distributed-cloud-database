package ecs;

import shared.HashUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class ECS {
    // TODO: Set up logger on client with name ecs.log
    //private static Logger logger = Logger.getRootLogger();
    // TODO: Set hashRing to private after testing
    public TreeMap<String, ECSNode> hashRing = new TreeMap<>();

    public ECS(String configFileName) {
        ArrayList<ECSNode> nodes = getNodesFromConfig(configFileName);
        addNodesToHashRing(nodes);
    }

    public void addNodes(int numberOfNodes) {

    }

    public void start() {

    }

    public void stop() {

    }

    public void shutDown() {

    }

    public void addNode() {

    }

    public void removeNode(int indexOfServer) {

    }

    private ArrayList<ECSNode> getNodesFromConfig(String configFileName) {
        ArrayList<ECSNode> nodes = new ArrayList<>();

        try {
            String rootPath = System.getProperty("user.dir");
            System.out.println(rootPath);
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
            //logger.error("ecs config missing, running with no default servers");
        }

        return nodes;
    }

    private void addNodesToHashRing(ArrayList<ECSNode> nodes) {
        // compute position in ring
        for (ECSNode node: nodes) {
            String hash = HashUtils.computeHash(node.getNodeHost() + ":" + node.getNodePort());
            hashRing.put(hash, node);
        }

        // populate hash ranges
        String prevHash = null;

        for (Map.Entry<String, ECSNode> entry: hashRing.entrySet()) {
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

    public static void main(String[] args) {
        ECS ecs = new ECS("ecs.config");
    }
}