package app_kvClient;

public class PrintUtils {


    public static final String PROMPT = "KVClient> ";

    /**
     * Prints the help string of the KVClient
     */
    public static void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KVClient HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t Establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t Put a key-value pair in the database. " +
                "If value is not supplied, then deletes entry key from the database \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t\t Get the value corresponding to key\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    /**
     * Wrapper function to print errors
     * @param error
     */
    public static void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Prints the possible log levels
     */
    public static void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }
}
