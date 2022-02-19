package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Map;
import java.util.Collection;

import app_kvClient.KVClient;
import client.KVCommInterface;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.PrintUtils;

import static app_kvECS.ECSClientCommandHandler.*;
import static shared.PrintUtils.printError;
import static shared.PrintUtils.printHelp;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private String PROMPT = PrintUtils.ECS_PROMPT;
    private boolean stop = false;
    private BufferedReader stdin;
    private File configFile;

    public ECSClient(File configFile){
        // TODO (not implemented): establish connection to ECS backend
        // Establish a connection to ECS Backend
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
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
            this.shutdown();
            System.out.println("Application exit!");
        } else if (tokens[0].equals("start")){
            this.start();
        } else  if (tokens[0].equals("stop")) {
            this.stop();
        } else if (tokens[0].equals("shutDown")) {
            this.shutdown();
        } else if(tokens[0].equals("addNode")) {
            handleAddNode();
        }
        else if(tokens[0].equals("addNodes")) {
            handleAddMultipleNodes(tokens);
        }
        else if(tokens[0].equals("removeNode")) {
            handleRemoveNode(tokens);
        } else if(tokens[0].equals("logLevel")) {
            handleLogLevel(tokens);
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.INFO);
            if(args.length != 1){
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: java -jar ECS.jar <ecs-config-file>!");
            } else{
                String ecsConfig = args[0];
                File ecsConfigFile = new File(ecsConfig);
                if (!ecsConfigFile.exists() || !ecsConfigFile.isFile()){
                    System.out.println("ECS Configuration file path is invalid!");
                    System.exit(1);
                }
                ECSClient app = new ECSClient(ecsConfigFile);
                app.run();
            }

            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
