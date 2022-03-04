package testing;

import app_kvECS.ECSClient;
import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test performance of storage service
 * There are a number of constants that can be adjusted to try different storage
 * service configurations, such as number of clients, number of servers, etc.
 */
public class EnronPerformanceTest extends TestCase {

    /**
     * Location of the enron data set.
     */
    public final String ENRON_DATA_PATH =
           System.getProperty("user.dir") + "/maildir/blair-l/inbox";

    public final int[] numClients = {1, 5, 20, 50, 100};
    public final int[] numServers = {1, 5, 20, 50, 100};
    public final int NUM_OPS = 1000;
    public final int NUM_CLIENTS = 1;
    public final int NUM_SERVERS = 1;
    public final int CACHE_SIZE = 20;
    public final String CACHE_STRATEGY = "FIFO";
    public final int GETS_PER_PUT = 5;

    private Logger logger = Logger.getRootLogger();
    private long durationNanos = 0;


    public void incrementDuration(long duration){
        durationNanos += duration;
    }

    private HashMap<String, String> loadEnronData(int numEntries){
        HashMap<String, String> data = new HashMap<>();
        File enronData = new File(ENRON_DATA_PATH);
        int counter = 0;
        logger.info(ENRON_DATA_PATH);
        while(counter < numEntries) {
            for (File email : enronData.listFiles()) {
                try {
                    Scanner reader = new Scanner(email);
                    while (reader.hasNextLine()) {
                        String value = reader.nextLine();
                        if (value.length() > 500) {
                            value = value.substring(0, 500);
                        }
                        data.put(Integer.toString(counter), value);
                        counter += 1;
                        if (counter >= numEntries) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    System.out.printf("Exception at %s", email.getAbsolutePath());
                    e.printStackTrace();
                }
            }
        }
        return data;
    }

    @Test
    public void testPerformance() throws Exception {
        File configFile = new File("src/testing/performancetest.config");
        ECSClient ecs = new ECSClient(configFile);
        ArrayList<KVStore> clients = new ArrayList<>();
        HashMap<String, String> enronData = loadEnronData(NUM_OPS + 500);
        ArrayList<String> keys = new ArrayList<>(enronData.keySet());
        ArrayList<String> putKeys = new ArrayList<>();
        ArrayList<Thread> threads = new ArrayList<>();
        putKeys.add("AnInitialKey");

        long shutdownDurationNanos = 0;
        long startupDurationNanos = 0;
        int currOp = 0;
        int numPuts = 0;
        int numGets = 0;
        int opsPerClient = NUM_OPS / NUM_CLIENTS;

        long startTime = System.nanoTime();
        ArrayList<IECSNode> nodesAdded = (ArrayList<IECSNode>)
                ecs.addNodes(NUM_SERVERS, CACHE_STRATEGY, CACHE_SIZE);
        ecs.start();
        startupDurationNanos = System.nanoTime() - startTime;

        ArrayList<ParalleledClient> paralleledClients = new ArrayList<>();
        for(int i = 0 ; i < NUM_CLIENTS; i++){
            int serverToConnect = nodesAdded.get(i % nodesAdded.size()).getNodePort();
            KVStore kvClient = new KVStore("localhost", serverToConnect);
            int keyRangeStart = i * opsPerClient;
            int keyRangeEnd = keyRangeStart + opsPerClient;
            List<String> clientKeys = keys.subList(keyRangeStart, keyRangeEnd);
            ParalleledClient paralleledClient =
                    new ParalleledClient(kvClient, this, enronData, clientKeys);
            paralleledClients.add(paralleledClient);
        }

        for (ParalleledClient client: paralleledClients){
            Thread clientThread = new Thread(client);
            threads.add(clientThread);
            clientThread.start();
        }

        for(Thread clientThread: threads){
            clientThread.join();
        }

        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
        String result = String.format("The server did %d iterations (%d puts, %d gets) in %d milliseconds" +
                        "This is a latency of %d ms per operation",
                NUM_OPS + GETS_PER_PUT * NUM_OPS , NUM_OPS, GETS_PER_PUT * NUM_OPS, durationMillis, durationMillis / NUM_OPS);
        logger.info(result);

        startTime = System.nanoTime();
        ecs.shutdown();
        shutdownDurationNanos = System.nanoTime() - startTime;

        long shutdownDurationMillis = TimeUnit.NANOSECONDS.toMillis(shutdownDurationNanos);
        long startupDurationMillis = TimeUnit.NANOSECONDS.toMillis(startupDurationNanos);

        logger.info(String.format("For %d nodes, ECS Took %d ms to add," +
                " and %d ms to remove.", NUM_SERVERS, startupDurationMillis, shutdownDurationMillis));
    }
}
