package ecs;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.security.MessageDigest;

public class ECS {
    // TODO: Set up logger on client with name ecs.log
    //private static Logger logger = Logger.getRootLogger();
    // TODO: Set hashRing to private after testing
    public TreeMap<String, ECSNode> hashRing = new TreeMap<>();
    private MessageDigest md5;

    public ECS(String configFileName) {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // Shouldn't get here as MD5 exists
        }

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
            String hash = computeHash(node.getNodeHost() + ":" + node.getNodePort());
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

    private String computeHash(String input) {
        // reset to make sure md5 instance is cleared
        md5.reset();
        byte[] hash = md5.digest(input.getBytes());
        return Base64.getEncoder().encodeToString(hash);
    }

    public static void main(String[] args) {
        ECS ecs = new ECS("ecs.config");
    }
}