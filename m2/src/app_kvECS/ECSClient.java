package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
        this.ecs = new ECS(configFile.getName());
    }

    @Override
    public boolean start() {
        return ecs.start();
    }

    @Override
    public boolean stop() {
        return ecs.stop();
    }

    @Override
    public boolean shutdown() {
        return ecs.shutDown();
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return ecs.addNode(cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        ArrayList<IECSNode> addedNodes = new ArrayList<IECSNode>();
        for (int i = 0; i < count; i++){
            IECSNode addedNode = ecs.addNode(cacheStrategy, cacheSize);
            addedNodes.add(addedNode);
        }
        return addedNodes;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return ecs.setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
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
        if(tokens[0].equals("quit")) {
            stop = true;
            try {
                this.shutdown();
            } catch (Exception e){
                logger.error("Unable to shut down servers succesfully while quitting ECSClient");
            }
            System.out.println("Application exit!");
        } else if (tokens[0].equals("start")){
            try {
                if (this.start()) {
                    printSuccess("Start succesfully");
                } else {
                    printError("Unable to start successfully");
                }
            } catch (Exception e) {
                printError("Unable to start successfully");
            };
        } else  if (tokens[0].equals("stop")) {
            try {
                if (this.stop()) {
                    printSuccess("Servers stopped succesfully");
                } else {
                    printError("Unable to stop servers");
                }
            } catch (Exception e) {
                printError("Unable to stop servers");
            };
        } else if (tokens[0].equals("shutDown")) {
            try {
                if (this.shutdown()) {
                    printSuccess("Shut down succesfully");
                } else {
                    printError("Unable to shut down successfully");
                }
            } catch (Exception e) {
                printError("Unable to shut down successfully");
            };
        } else if(tokens[0].equals("addNode")) {
            if(tokens.length == 3){
                try {
                    String cacheStrategy = tokens[1];
                    int cacheSize = Integer.parseInt(tokens[2]);
                    addNode(cacheStrategy, cacheSize);
                } catch (NumberFormatException e){
                    printError("Could not parse parameters properly");
                }
            } else{
                printError("Invalid Number of Parameters! Use help to see usage");
            }
        }
        else if(tokens[0].equals("addNodes")) {
            if(tokens.length == 4){
                try {
                    int count = Integer.parseInt(tokens[1]);
                    String cacheStrategy = tokens[2];
                    int cacheSize = Integer.parseInt(tokens[3]);
                    addNodes(count, cacheStrategy, cacheSize);
                } catch (NumberFormatException e){
                    printError("Could not parse parameters properly");
                }
            } else{
                printError("Invalid Number of Parameters! Use help to see usage");
            }
        }
        else if(tokens[0].equals("removeNode")) {
            if(tokens.length == 2){
                String nodeName = tokens[1];
                ArrayList<String> nodeNames = new ArrayList<>();
                nodeNames.add(nodeName);
                if(removeNodes(nodeNames)){
                    printSuccess("Nodes removed successfully");
                } else{
                    printError("Unable to remove nodes");
                };
            } else{
                printError("Invalid Number of Parameters! Use help to see usage");
            }
        } else if(tokens[0].equals("logLevel")) {
            handleLogLevel(tokens);
        } else if(tokens[0].equals("help")) {
            printECSClientHelp();
        } else {
            printError("Unknown command");
            printECSClientHelp();
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
