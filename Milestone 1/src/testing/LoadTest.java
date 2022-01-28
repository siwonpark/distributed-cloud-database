package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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

    public void testPerformance() throws Exception {

        final int NUM_ITERATIONS = 100;
        final double PUT_RATIO = 0.8;
        final int numPuts = (int) (NUM_ITERATIONS * PUT_RATIO);
        final int numGets = NUM_ITERATIONS - numPuts;
        long durationNanos = 0;

        ArrayList<StatusType> ratio = createRatioOfPutsAndGets(numPuts, numGets);
        ArrayList<String> createdKeys = new ArrayList<>();
        createdKeys.add("asdiao");
        Random rand = new Random();

        for (StatusType status: ratio) {
            if (status == StatusType.PUT) {
                String randomKey = BTreeTest.getRandomString(10);
                String randomValue = BTreeTest.getRandomString(20);
                long startTime = System.nanoTime();
                kvClient.put(randomKey, randomValue);
                long duration = System.nanoTime() - startTime;
                durationNanos += duration;
                createdKeys.add(randomKey);
            } else {
                int randomIndex = rand.nextInt(createdKeys.size());
                long startTime = System.nanoTime();
                kvClient.get(createdKeys.get(randomIndex));
                long duration = System.nanoTime() - startTime;
                durationNanos += duration;
            }
        }
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
        System.out.printf("The server did %d iterations in %d milliseconds at a put ratio of %.2f. " +
                        "This is a latency of %d ms per operation and a throughput of %f operations/s",
                NUM_ITERATIONS, durationMillis, PUT_RATIO, durationMillis / NUM_ITERATIONS, NUM_ITERATIONS / (float)durationMillis * 1000);
    }

    private ArrayList<StatusType> createRatioOfPutsAndGets(int puts, int gets){
        ArrayList<StatusType> res = new ArrayList<>();
        int totalIterations = puts + gets;

        if(puts > gets){
            int putsPerGet = Math.floorDiv(puts,gets);
            for(int i = 0; i <= totalIterations; i++){
                StatusType status = i % putsPerGet == 0 ? StatusType.GET : StatusType.PUT;
                res.add(status);
            }
        } else {
            int getsPerPut = Math.floorDiv(gets, puts);
            for (int i = 0; i <= totalIterations; i++) {
                StatusType status = i % getsPerPut == 0 ? StatusType.PUT : StatusType.GET;
                res.add(status);
            }
        }
        return res;
    }
}
