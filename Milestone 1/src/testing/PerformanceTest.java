package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testPerformance() throws Exception {

        final int NUM_ITERATIONS = 100;
        final double PUT_RATIO = 0.8;
        final int numPuts = (int) (NUM_ITERATIONS * PUT_RATIO);
        final int numGets = NUM_ITERATIONS - numPuts;
        long durationNanos = 0;

        ArrayList<KVMessage.StatusType> ratio = createRatioOfPutsAndGets(numPuts, numGets);
        ArrayList<String> createdKeys = new ArrayList<>();
        createdKeys.add("asdiao");
        Random rand = new Random();

        for (KVMessage.StatusType status: ratio) {
            if (status == KVMessage.StatusType.PUT) {
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

    private ArrayList<KVMessage.StatusType> createRatioOfPutsAndGets(int puts, int gets){
        ArrayList<KVMessage.StatusType> res = new ArrayList<>();
        int totalIterations = puts + gets;

        if(puts > gets){
            int putsPerGet = Math.floorDiv(puts,gets);
            for(int i = 0; i <= totalIterations; i++){
                KVMessage.StatusType status = i % putsPerGet == 0 ? KVMessage.StatusType.GET : KVMessage.StatusType.PUT;
                res.add(status);
            }
        } else {
            int getsPerPut = Math.floorDiv(gets, puts);
            for (int i = 0; i <= totalIterations; i++) {
                KVMessage.StatusType status = i % getsPerPut == 0 ? KVMessage.StatusType.PUT : KVMessage.StatusType.GET;
                res.add(status);
            }
        }
        return res;
    }
}
