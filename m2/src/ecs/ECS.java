package ecs;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class ECS {
    // TODO: Set up logger on client with name ecs.log
    //private static Logger logger = Logger.getRootLogger();

    public ECS(String configFileName) {
        loadConfig(configFileName);
    }

    private void loadConfig(String configFileName) {
        try {
            String rootPath = System.getProperty("user.dir");
            File config = new File(rootPath + "/m2/src/ecs/" + configFileName);
            Scanner s = new Scanner(config);

            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] serverInfo = line.split(" ");
                String serverName = serverInfo[0];
                String serverHost = serverInfo[1];
                String serverPort = serverInfo[2];

                System.out.println("Server " + serverName + " connected with address " + serverHost + " at port " + serverPort);
            }
            s.close();
        } catch (FileNotFoundException e) {
            System.out.println("Not found");
            //logger.error("ecs config missing, running with no default servers");
        }
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

    public static void main(String[] args) {
        ECS ecs = new ECS("ecs.config");
    }
}