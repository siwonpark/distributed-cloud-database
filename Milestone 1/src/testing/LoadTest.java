package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;


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
     * Runs 100 puts and then gets
     */
    public void test100PutGet() throws Exception {
        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;
        Exception ex = null;

        int i = 0;
        while (i < 100) {
            kvClient.put(String.valueOf(i), String.valueOf(i));
            response = kvClient.get(String.valueOf(i));
            assertTrue(
                    response.getStatus() == StatusType.GET_SUCCESS
                    && response.getValue().equals(String.valueOf(i)));
            i += 1;
        }

    }

}
