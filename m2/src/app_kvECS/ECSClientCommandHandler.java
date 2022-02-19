package app_kvECS;

import logger.LogSetup;
import org.apache.log4j.Logger;

import static shared.LogUtils.setLevel;
import static shared.PrintUtils.printError;
import static shared.PrintUtils.printPossibleLogLevels;

public class ECSClientCommandHandler {
    private static Logger logger = Logger.getRootLogger();


    /**
     * Utility functions to handle user-input to ECS front-end
     */

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

    public static void handleRemoveNode(String[] tokens){
        //TODO: Implement
    }

    public static void handleAddMultipleNodes(String[] tokens){
        //TODO: Implement
    }
    public static void handleAddNode(){
        //TODO: Implement
    }
}
