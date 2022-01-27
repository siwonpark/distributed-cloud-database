package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import static shared.PrintUtils.DELETE_STRING;


public class LoadTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    /**
     * Runs ITERATIONS of put/get
     */
    public void testPutGetDeleteIntertwined() throws Exception {
        KVMessage response = null;
        Exception ex = null;
        final int ITERATIONS = 100;

        int i = 0;
        while (i < ITERATIONS) {
            kvClient.put(String.valueOf(i), String.valueOf(i));
            response = kvClient.get(String.valueOf(i));
            assertTrue(
                    response.getStatus() == StatusType.GET_SUCCESS
                    && response.getValue().equals(String.valueOf(i)));
            assertSame(kvClient.put(String.valueOf(i), DELETE_STRING).getStatus(),
                    StatusType.DELETE_SUCCESS);
            i += 1;
        }
    }


    @Test
    /**
     * Runs ITERATIONS of puts and then ITERATIONS of gets
     */
    public void testPutThenGetThenDelete() throws Exception {
        KVMessage response = null;
        Exception ex = null;
        final int ITERATIONS = 100;

        int i = 0;
        while (i < ITERATIONS) {
            // Put in some values
            response = kvClient.put(String.valueOf(i), String.valueOf(i));
            assertSame(response.getStatus(), StatusType.PUT_SUCCESS);
            i += 1;
        }

        i = 0;
        while (i < ITERATIONS){
            // Get the keys just inserted
            response = kvClient.get(String.valueOf(i));
            assertTrue(response.getStatus() == StatusType.GET_SUCCESS &&
                    response.getValue().equals(String.valueOf(i)));
            i += 1;
        }

        i = 0;
        while (i < ITERATIONS){
            // Delete the keys
            assertSame(kvClient.put(String.valueOf(i), DELETE_STRING).getStatus(),
                    StatusType.DELETE_SUCCESS);
            i += 1;
        }
    }
}
