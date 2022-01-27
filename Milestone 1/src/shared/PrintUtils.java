package shared;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class PrintUtils {


    public static final String PROMPT = "KVClient> ";
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

        sb.append("quit");
        sb.append("\t\t\t Exits the program");
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
