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


public class PerformanceTest extends TestCase {

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

    public void testPerformance() throws Exception {

        final int NUM_ITERATIONS = 1000;
        final double PUT_RATIO = 0.5;
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
