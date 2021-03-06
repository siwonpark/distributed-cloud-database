package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.PrintUtils;

import static shared.LogUtils.setLevel;
import static shared.PrintUtils.*;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    
    
    private String PROMPT = PrintUtils.ECS_PROMPT;
    private boolean stop = false;
    private BufferedReader stdin;
    private File configFile;
    private ECS ecs;

    public ECSClient(File configFile){
        // Establish a connection to ECS Backend
        this.ecs = new ECS(configFile.getPath());
    }

    @Override
    public boolean start() {
        boolean success = true;
        for (ECSNode node: ecs.hashRing.values()) {
            success = ecs.start(node);
        }

        if (success) {
            ecs.serviceRunning = true;
        }
        return success;
    }

    @Override
    public boolean stop() {
        boolean success = true;
        for (ECSNode node: ecs.hashRing.values()) {
            success = ecs.stop(node);
        }

        if (success) {
            ecs.serviceRunning = false;
        }
        return success;
    }

    @Override
    public boolean shutdown() {
        ECSNode[] nodes = ecs.hashRing.values().toArray(new ECSNode[0]);
        boolean success = true;
        for (ECSNode node: nodes) {
            if(!ecs.shutDown(node)){
                success = false;
            };
        }

        if (success) {
            ecs.serviceRunning = false;
        }
        return success;
    }


    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return ecs.addNode(cacheStrategy, cacheSize, true);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return ecs.addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return ecs.setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public boolean awaitNodes(int count, int timeout) {
        return ecs.awaitNodes(count, timeout);
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        int nodesRemoved = 0;
        for(String nodeName: nodeNames){
            if(ecs.removeNode(nodeName)){
                nodesRemoved += 1;
            }
        }
        return nodesRemoved == nodeNames.size();
    }


    @Override
    public Map<String, ECSNode> getNodes() {
        return ecs.getNodes();
    }

    @Override
    public boolean lockWrite(String nodeName) {
        ECSNode nodeToLock = ecs.getECSNode(nodeName);

        if (nodeToLock == null) {
            logger.error("Node does not exist");
            return false;
        }

        return ecs.lockWrite(nodeToLock);
    }

    @Override
    public boolean unlockWrite(String nodeName) {
        ECSNode nodeToUnlock = ecs.getECSNode(nodeName);

        if (nodeToUnlock == null) {
            logger.error("Node does not exist");
            return false;
        }

        return ecs.unlockWrite(nodeToUnlock);
    }

    public boolean kill(String nodeName) {
        ECSNode killedNode = ecs.getECSNode(nodeName);

        if (killedNode == null) {
            logger.error("Node does not exist");
            return false;
        }

        return ecs.kill(killedNode);
    }

    public void addToAvailableNodes(ECSNode node) {
        ecs.addToAvailableNodes(node);
    }


    /**
     * Return the currently managed nodes of the ECS server,
     * as an array list
     * @return The nodes managed by ECS
     */
    private ArrayList<ECSNode> getNodeList(){
        Map<String, ECSNode> nodes = getNodes();
        ArrayList<ECSNode> nodeList = new ArrayList<>();
        for(Map.Entry<String, ECSNode> entry : nodes.entrySet()){
            nodeList.add(entry.getValue());
        }
        return nodeList;
    }

    /**
     * Convenience utility to list the currently active nodes in the hash ring
     */
    private void listNodes(){
        Map<String, ECSNode> nodes = getNodes();
        String running = ecs.serviceRunning? "STARTED" : "STOPPED";
        System.out.printf("The service is currently %s. The currently active servers are: \n", running);
        int index = 0;
        for(Map.Entry<String, ECSNode> entry : nodes.entrySet()){
            ECSNode node = entry.getValue();
            System.out.printf("\t\t Index: %d, Name: %s, Host Address: %s, Port: %s%n",
                    index, node.getNodeName(), node.getNodeHost(), node.getNodePort());
            index += 1;
        }
    }

    private boolean sync(){
        boolean consistent = true;
        Map<String, ECSNode> nodes = getNodes();
        for (Map.Entry<String, ECSNode> entry : nodes.entrySet()) {
            ECSNode node = entry.getValue();
            try{
                ecs.forceConsistency(node);
            } catch (Exception e){
                consistent = false;
            }
        }
        return consistent;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    /**
     * Handle command line input and perform appropriate actions
     * @param cmdLine Command line input
     */
    public void handleCommand(String cmdLine) {
        if(cmdLine == null || cmdLine.isEmpty() ){
            return;
        }
        String[] tokens = cmdLine.split("\\s+");
        String command = tokens[0];
        switch (command) {
            case "quit": {
                stop = true;
                try {
                    this.shutdown();
                } catch (Exception e){
                    logger.error("Unable to shut down servers succesfully while quitting ECSClient");
                }
                System.out.println("Application exit!");
                break;
            }
            case "start": {
                try {
                    if (this.start()) {
                        printSuccess("Start succesfully");
                    } else {
                        printError("Unable to start successfully");
                    }
                } catch (Exception e) {
                    printError("Unable to start successfully");
                }
                break;
            }
            case "stop": {
                try {
                    if (this.stop()) {
                        printSuccess("Servers stopped succesfully");
                    } else {
                        printError("Unable to stop servers");
                    }
                } catch (Exception e) {
                    printError("Unable to stop servers");
                }
                break;
            }
            case "shutDown": {
                try {
                    if (this.shutdown()) {
                        printSuccess("Shut down succesfully");
                    } else {
                        printError("Unable to shut down successfully");
                    }
                } catch (Exception e) {
                    printError("Unable to shut down successfully");
                }
                break;
            }
            case "addNode": {
                if(tokens.length == 3){
                    try {
                        String cacheStrategy = tokens[1];
                        int cacheSize = Integer.parseInt(tokens[2]);
                        addNode(cacheStrategy, cacheSize);
                        listNodes();
                    } catch (NumberFormatException e){
                        printError("Could not parse parameters properly");
                    }
                } else{
                    printError("Invalid Number of Parameters! Use help to see usage");
                }
                break;
            }
            case "addNodes": {
                if (tokens.length == 4) {
                    try {
                        int count = Integer.parseInt(tokens[1]);
                        String cacheStrategy = tokens[2];
                        int cacheSize = Integer.parseInt(tokens[3]);
                        addNodes(count, cacheStrategy, cacheSize);
                        listNodes();
                    } catch (NumberFormatException e){
                        printError("Could not parse parameters properly");
                    }
                } else{
                    printError("Invalid Number of Parameters! Use help to see usage");
                }
                break;
            }
            case "removeNodeName": {
                if(tokens.length == 2){
                    String nodeName = tokens[1];
                    ArrayList<String> nodeNames = new ArrayList<>();
                    nodeNames.add(nodeName);
                    if(removeNodes(nodeNames)){
                        printSuccess(String.format("Node %s removed successfully", nodeName));
                        listNodes();
                    } else{
                        printError("Unable to remove nodes");
                    }
                } else{
                    printError("Invalid Number of Parameters! Use help to see usage");
                }
                break;
            }
            case "removeNode": {
                if(tokens.length == 2){
                    try {
                        int nodeIndex = Integer.parseInt(tokens[1]);
                        ArrayList<String> nodeNames = new ArrayList<>();
                        ArrayList<ECSNode> nodes = getNodeList();
                        if(nodeIndex < 0 || nodeIndex >= nodes.size()){
                            printError("An invalid node index was given. Use 'list' to see active nodes");
                        } else{
                            String nodeName = nodes.get(nodeIndex).getNodeName();
                            nodeNames.add(nodeName);
                            if (removeNodes(nodeNames)) {
                                printSuccess(String.format("Node %s (Index %d) removed successfully",
                                        nodeName, nodeIndex));
                                listNodes();
                            } else {
                                printError("Unable to remove nodes");
                            }
                        }
                    } catch (NumberFormatException e){
                        printError("Could not parse parameters properly");
                    }
                } else{
                    printError("Invalid Number of Parameters! Use help to see usage");
                }
                break;
            }
            case "list": {
                listNodes();
                break;
            }
            case "logLevel": {
                handleLogLevel(tokens);
                break;
            }
            case "help": {
                printECSClientHelp();
                break;
            }
            case "sync":
                if(tokens.length == 1){
                    try {
                        if(sync()){
                            printSuccess("Consistency is achieved among all the servers!");
                        }else{
                            printError("Consistency haven't been achieved! There might be partition.");
                        }
                    } catch (NumberFormatException e){
                        printError("Could not parse parameters properly");
                    }
                } else{
                    printError("Invalid Number of Parameters! Use help to see usage");
                }
                break;
            default: {
                printError("Unknown command");
                printECSClientHelp();
                break;
            }
        }
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO: Implement if needed
        return null;
    }

    /**
     * Handle adjustment of logging level
     * @param tokens The command line arguments
     */
    public static void handleLogLevel(String[] tokens) {
        if(tokens.length == 2) {
            String level = setLevel(tokens[1]);
            if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                printError("Not a valid log level!");
                printPossibleLogLevels();
            } else {
                System.out.println("Log level changed to level " + level);
            }
        } else {
            printError("Invalid number of parameters! Use the help command to see usage instructions");
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.INFO);
            if(args.length != 1){
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: java -jar ECS.jar <ecs-config-file>!");
            } else{
                File ecsConfigFile = new File(args[0]);
                if (!ecsConfigFile.exists() || !ecsConfigFile.isFile()){
                    System.out.println("ECS Configuration file path is invalid!");
                    System.exit(1);
                }
                ECSClient app = new ECSClient(ecsConfigFile);
                app.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
