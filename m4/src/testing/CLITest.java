package testing;

import client.KVStore;
import junit.framework.TestCase;
import app_kvClient.KVClient;

import java.io.*;

import static testing.AllTests.port;


public class CLITest extends TestCase{
    private KVClient app;
    private final PrintStream SysOut = System.out;
    private final ByteArrayOutputStream testOut = new ByteArrayOutputStream();

    @Override
    protected void setUp(){
        app = new KVClient();
        System.setOut(new PrintStream(testOut));

    }

    @Override
    protected void tearDown(){
        app = null;
        System.setOut(SysOut);
    }

    /**
     * Test the CLI when inputting an invalid command
     */
    public void testInvalidCommand(){
        String INVALID_STRING = "Hello world";
        app.handleCommand(INVALID_STRING);
        String output = testOut.toString();
        assertTrue(output.startsWith("Error! Unknown command"));
    }

    /**
     * Test the CLI when inputting an invalid parameters for get
     */
    public void testInvalidGetParameters(){
        String INVALID_STRING = "get";
        String EXPECTED_OUTPUT = "Error! Invalid number of parameters.";
        app.handleCommand(INVALID_STRING);
        String output = testOut.toString();
        assertTrue(output.startsWith(EXPECTED_OUTPUT));

        String INVALID_STRING2 = "get KEY1 KEY2";
        app.handleCommand(INVALID_STRING2);
        String output2 = testOut.toString();
        assertTrue(output2.startsWith(EXPECTED_OUTPUT));
    }

    /**
     * Test that a transaction cannot be started without being connected to a server
     */
    public void testInitTransactionCli(){
        String cmd = "initTransaction";
        String EXPECTED_OUTPUT = "Error! Not Connected!";
        app.handleCommand(cmd);
        String output = testOut.toString();
        assertTrue(output.startsWith(EXPECTED_OUTPUT));
    }

    /**
     * Test the error cases of the commit CLI operation
     * I.e. If there are no operations to commit
     * Or if we are not currently in a transaction
     */
    public void testCommitTransactionCli(){
        String cmd = "commit";
        String EXPECTED_OUTPUT = "Error! Not currently in a transaction!";
        app.handleCommand(cmd);
        String output = testOut.toString();
        assertTrue(output.startsWith(EXPECTED_OUTPUT));

        Exception ex = null;
        try {
            app.newConnection("localhost", port);
            app.handleCommand("initTransaction");
            app.handleCommand(cmd);
            String EXPECTED_OUTPUT2 = "Error! No operations to commit!";
            String output2 = testOut.toString();
            assertTrue(output2.contains(EXPECTED_OUTPUT2));
        } catch(Exception e) {
            ex = e;
        }
        assertNull(ex);
    }


    /**
     * Test that once we are in a transaction,
     * Subsequent commands get added to the transaction
     */
    public void testTransaction(){
        Exception ex = null;
        String EXPECTED_OUTPUT = "The size of the current transaction is: 3";
        String EXPECTED_OUTPUT2 = "The size of the current transaction is: 5";
        try {
            app.newConnection("localhost", port);
            app.handleCommand("initTransaction");
            app.handleCommand("put 10 10");
            app.handleCommand("get 10");
            app.handleCommand("put 20 5");
            app.handleCommand("transactionStatus");
            String output = testOut.toString();
            assertTrue(output.contains(EXPECTED_OUTPUT));

            app.handleCommand("get 50");
            app.handleCommand("put 50 52");
            app.handleCommand("transactionStatus");
            String output2 = testOut.toString();
            assertTrue(output2.contains(EXPECTED_OUTPUT2));
        } catch(Exception e) {
            ex = e;
        }
        assertNull(ex);
    }



    /**
     * Test the CLI when inputting an invalid parameters for put
     */
    public void testInvalidPutParameters(){
        String INVALID_STRING = "put";
        String EXPECTED_OUTPUT = "Error! Invalid number of parameters.";
        app.handleCommand(INVALID_STRING);
        String output = testOut.toString();
        assertTrue(output.startsWith(EXPECTED_OUTPUT));

        String INVALID_STRING2 = "get KEY1 VALUE1 VALUE2";
        app.handleCommand(INVALID_STRING2);
        String output2 = testOut.toString();
        assertTrue(output2.startsWith(EXPECTED_OUTPUT));
    }
}


