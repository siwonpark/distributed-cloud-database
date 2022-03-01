package shared;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class PrintUtils {


    public static final String PROMPT = "KVClient> ";
    public static final String ECS_PROMPT = "ECSClient> ";
    // Handout and piazza specifies the String "null" to delete
    public static final String DELETE_STRING = "null";

    /**
     * Prints the help string of the KVClient
     */
    public static void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("KVClient HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("connect <host> <port>");
        sb.append("\t Establishes a connection to a server\n");
        sb.append("put <key> <value>");
        sb.append("\t Put a key-value pair in the database. " +
                "If value is not supplied, then deletes entry key from the database \n");
        sb.append("get <key>");
        sb.append("\t\t Get the value corresponding to key\n");
        sb.append("disconnect");
        sb.append("\t\t Disconnects from the server \n");

        sb.append("logLevel");
        sb.append("\t\t Changes the logLevel \n");
        sb.append("\t\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append("help");
        sb.append("\t\t\t Prints this help message");

        sb.append("quit");
        sb.append("\t\t\t Exits the program");
        System.out.println(sb.toString());
    }

    /**
     * Print the help string for the ECSClient
     */
    public static void printECSClientHelp(){
        StringBuilder sb = new StringBuilder();
        sb.append("ECSClient HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("start");
        sb.append("\t\t\t Start the storage service\n");
        sb.append("stop");
        sb.append("\t\t\t Stop the storage service; all participating KVServers " +
                "are stopped for processing clients, but the processes remain running.\n");

        sb.append("shutDown");
        sb.append("\t\t\t" +
                "Stops all server instances and exits the remote processes.\n");

        sb.append("addNode <cacheStrategy> <cacheSize>");
        sb.append("\t\t\t" +
                "Create a new KVServer and add it to the storage service at an arbitrary position. \n");

        sb.append("addNodes <numberOfNodes> <cacheStrategy> <cacheSize>");
        sb.append("\t\t\t Randomly choose <numberOfNodes> servers from the available machines " +
                "and start the KVServer by issuing an SSH call to the respective machine. " +
                "This call launches the storage server but does not start them.\n");

        sb.append("removeNode <nodeName>");
        sb.append("\t\t\tRemove a server with nodeName from the storage service.\n");

        sb.append("removeNodeIndex <index>");
        sb.append("\t\t\tRemove a server with arbitrary index from the storage service.\n");

        sb.append("logLevel");
        sb.append("\t\t\t Changes the logLevel (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)\n");

        sb.append("list");
        sb.append("\t\t\t Lists the current nodes in the storage service \n");

        sb.append("help");
        sb.append("\t\t\t Prints this help message\n");

        sb.append("quit");
        sb.append("\t\t\t Exits the program (and shutdown)");
        System.out.println(sb.toString());
    }

    /**
     * Wrapper function to print errors
     * @param error
     */
    public static void printError(String error){
        System.out.println("Error! " +  error);
    }

    /**
     * Wrapper function to print errors
     * @param msg
     */
    public static void printSuccess(String msg){
        System.out.println("Success! " +  msg);
    }

    /**
     * Prints the possible log levels
     */
    public static void printPossibleLogLevels() {
        System.out.println("Possible log levels are:");
        System.out.println("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * Prints the response that the user receives in shell based on the message
     * @param message
     */
    public static void printResponseToUser(KVMessage message){
        String key = message.getKey();
        String value = message.getValue();
        StatusType status = message.getStatus();

        switch (status) {
            case FAILED:
                printFailedResponseToUser(key);
                break;
            case PUT_SUCCESS:
                printSuccess(String.format("Inserted {\"%s\": \"%s\"} to the database.", key, value));
                break;
            case DELETE_SUCCESS:
                printSuccess("Deleted key \"" + key + "\" from the database.");
                break;
            case DELETE_ERROR:
                printError("Failed to delete key \"" + key + "\" from the database.");
                break;
            case PUT_ERROR:
                printError(String.format("Failed to insert {\"%s\": \"%s\"} to the database.", key, value));
                break;
            case GET_SUCCESS:
                printSuccess("Retrieved value \"" + value + "\" from the database.");
                break;
            case GET_ERROR:
                printError("There is no entry with key: \"" + key + "\" in the database.");
                break;
            case PUT_UPDATE:
                printSuccess(String.format("Updated key \"%s\" with value \"%s\".", key, value));
                break;
            case SERVER_WRITE_LOCK:
                printError("The server is currently not accepting write requests");
                break;
            case SERVER_STOPPED:
                printError("Client requests are currently not being processed by the server");
                break;
            default:
                break;
        }
    }

    /**
     *
     * @param text The Failure text associated with the failed message
     */
    public static void printFailedResponseToUser(String text){
        String outputString = "The request failed with error response: " + text;
        System.out.println(outputString);
    }
}
