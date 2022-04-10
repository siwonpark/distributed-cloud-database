package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.PrintUtils;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;

import static shared.LogUtils.setLevel;
import static shared.PrintUtils.*;



public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getRootLogger();
    private String PROMPT = PrintUtils.PROMPT;
    private boolean stop = false;
    private BufferedReader stdin;
    private String serverAddress;
    private int serverPort;
    private KVStore kvStore;
    private Heartbeat heartbeat;
    private boolean isInTransaction = false;
    private ArrayList<Message> currTransaction = new ArrayList<>();


    @Override
    public void newConnection(String hostname, int port) throws Exception{
        kvStore = new KVStore(hostname, port);
        kvStore.addListener(this);
        kvStore.connect();
        heartbeat = new Heartbeat(kvStore);
        heartbeat.addListener(this);
        new Thread(heartbeat).start();
    }

    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(getPrompt());

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
            disconnect();
            System.out.println("Application exit!");
        } else if (tokens[0].equals("connect")){
            handleConnect(tokens);
        } else  if (tokens[0].equals("get")) {
            if(this.isInTransaction) {
                addToTransaction(tokens, KVMessage.StatusType.GET);
            } else{
                handleGet(tokens);
            }
        } else if (tokens[0].equals("put")) {
            if(this.isInTransaction){
                addToTransaction(tokens, KVMessage.StatusType.PUT);
            } else{
                handlePut(tokens);
            }
        } else if(tokens[0].equals("disconnect")) {
            disconnect();
        } else if(tokens[0].equals("logLevel")) {
            handleLogLevel(tokens);
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else if(tokens[0].equals("initTransaction")) {
            initTransaction();
        } else if(tokens[0].equals("commit")) {
            commitTransaction();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private String getPrompt(){
        return this.isInTransaction? TRANSACTION_PROMPT : PROMPT;
    }

    /**
     * Inititate a transaction
     */
    public void initTransaction(){
        if (kvStore != null && kvStore.isRunning()) {
            if (this.isInTransaction) {
                System.out.println("Already currently in a transaction!");
            } else {
                System.out.println("Transaction initiated.");
                this.isInTransaction = true;
            }
        } else {
            printError("Not Connected!");
        }
    }

    /**
     * commit the current transaction
     */
    public void commitTransaction(){
        if (!this.isInTransaction){
            printError("Not currently in a transaction! Please type help for usage");
        } else if(this.currTransaction.isEmpty()){
            printError("No operations to commit!");
            this.isInTransaction = false;
        } else {
            if (kvStore != null && kvStore.isRunning()) {
                try {
                    KVMessage response = kvStore.commit(this.currTransaction);
                    printResponseToUser(response);
                } catch (Exception e) {
                    String errMsg = String.format("Unable to commit transaction of size %s! ",
                            this.currTransaction.size()) + e;
                    printError(errMsg);
                } finally {
                    this.isInTransaction = false;
                }
            } else {
                printError("Not Connected!");
                this.isInTransaction = false;
            }
        }
        this.currTransaction.clear();
    }

    public void addToTransaction(String[] tokens, KVMessage.StatusType status){
        if(kvStore != null && kvStore.isRunning()){
            Message newOperation;
            if (status == KVMessage.StatusType.PUT){
                if(tokens.length == 3) {
                    String key = tokens[1];
                    String value = tokens[2];
                    newOperation = new Message(key, value, status);
                    this.currTransaction.add(newOperation);
                    System.out.println("Operation added to transaction.");
                } else {
                    printError("Invalid number of parameters. Use the help command to see usage instructions");
                }
            } else if (status == KVMessage.StatusType.GET){
                if (tokens.length == 2){
                    String key = tokens[1];
                    newOperation = new Message(key, null, status);
                    this.currTransaction.add(newOperation);
                    System.out.println("Operation added to transaction.");
                } else{
                    printError("Invalid number of parameters. Use the help command to see usage instructions");
                }
            } else {
                logger.error(String.format("An invalid status code of %s has been attempted " +
                        "to be added to the current transaction", status));
                printError("An internal error has occurred. Operation was not added to transaction");
            }
        } else {
            printError("Not connected!");
        }
    }

    /**
     * Handle adjustment of logging level
     * @param tokens The command line arguments
     */
    private void handleLogLevel(String[] tokens) {
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

    /**
     * Handle the 'get' operation from the command line interface
     * @param tokens The command line arguments
     */
    private void handlePut(String[] tokens) {
        if(tokens.length == 3) {
            if(kvStore != null && kvStore.isRunning()){
                String key = tokens[1];
                String value = tokens[2];
                try {
                    KVMessage response = kvStore.put(key, value);
                    printResponseToUser(response);
                } catch (Exception e){
                    String errMsg = !value.equals(DELETE_STRING) ?
                            String.format("Unable to put value %s into key %s! ", value, key) + e :
                            String.format("Unable to delete entry corresponding to key %s! ", key) + e;
                    printError(errMsg);
                }

            } else {
                printError("Not connected!");
            }
        } else {
            printError("Invalid number of parameters. Use the help command to see usage instructions");
        }
    }

    /**
     * Handle the 'get' operation from the command line interface
     * @param tokens The command line arguments
     */
    private void handleGet(String[] tokens) {
        if(tokens.length == 2) {
            if(kvStore != null && kvStore.isRunning()){
                String key = tokens[1];
                try {
                    KVMessage response = kvStore.get(key);
                    printResponseToUser(response);
                } catch (Exception e){
                    String errMsg = String.format("Unable to get key %s! ", key) + e;
                    printError(errMsg);
                }

            } else {
                printError("Not connected!");
            }
        } else {
            printError("Invalid number of parameters. Use the help command to see usage instructions");
        }
    }

    /**
     * Handle the 'connect' command
     * @param tokens The command line arguments
     */
    private void handleConnect(String[] tokens) {
        if(tokens.length == 3) {
            try{
                serverAddress = tokens[1];
                serverPort = Integer.parseInt(tokens[2]);
                newConnection(serverAddress, serverPort);
                System.out.printf("Connected to %s port %s!%n", serverAddress, serverPort);
            } catch(NumberFormatException nfe) {
                printError("No valid address. Port must be a number!");
                logger.info("Unable to parse argument <port>", nfe);
            } catch (UnknownHostException e) {
                printError("Unknown Host!");
                logger.info("Unknown Host!", e);
            } catch (IOException e) {
                printError("Could not establish connection!");
                logger.warn("Could not establish connection!", e);
            } catch (Exception e) {
                printError("An unexpected error occurred while trying to connect! Please try again.");
                logger.warn("An unexpected error occurred!", e);
            }
        } else {
            printError("Invalid number of parameters! Use the help command for usage instructions.");
        }
    }

    /**
     * Handle the disconnect of the client
     */
    private void disconnect() {
        if(kvStore != null) {
            if (heartbeat != null){
                heartbeat.stopProbing();
            }
            kvStore.disconnect();
            kvStore = null;
        } else {
            printError("Not connected!");
        }
    }

    /**
     * Perform actions based on socket status
     * @param status The status of the socket
     */
    @Override
    public void handleStatus(SocketStatus status) {
        if (kvStore != null){
            serverPort = kvStore.getPort();
            serverAddress = kvStore.getHost();
        }

        if(status == SocketStatus.CONNECTED) {
        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(getPrompt());
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);
        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(getPrompt());
            heartbeat.stopProbing();
            kvStore = null;
        }
    }

    public static void main(String[] args){
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }


}
