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

public class EnronPerformanceTest extends TestCase {

    /**
     * Location of the enron data set.
     */
    public final String ENRON_DATA_PATH =
           System.getProperty("user.dir") + "/maildir/blair-l/inbox";

    public final int[] numClients = {1, 5, 20, 50, 100};
    public final int[] numServers = {1, 5, 20, 50, 100};
    private Logger logger = Logger.getRootLogger();


    private HashMap<String, String> loadEnronData(int numEntries){
        HashMap<String, String> data = new HashMap<>();
        File enronData = new File(ENRON_DATA_PATH);
        int counter = 0;
        logger.info(ENRON_DATA_PATH);
        for (File email: enronData.listFiles()){
            try{
                Scanner reader = new Scanner(email);
                while(reader.hasNextLine()){
                    String value = reader.nextLine();
                    if (value.length() > 500){
                        value = value.substring(0, 500);
                    }
                    data.put(Integer.toString(counter), value);
                    counter += 1;
                    if (counter >= numEntries){
                        break;
                    }
                }
            } catch (Exception e){
                System.out.printf("Exception at %s", email.getAbsolutePath());
                e.printStackTrace();
            }
        }
        return data;
    }

    @Test
    public void testPerformance() throws Exception {
        File configFile = new File("src/testing/performancetest.config");
        ECSClient ecs = new ECSClient(configFile);
        ArrayList<KVStore> clients = new ArrayList<>();
        HashMap<String, String> enronData = loadEnronData(10500);
        ArrayList<String> keys = new ArrayList<>(enronData.keySet());
        ArrayList<String> putKeys = new ArrayList<>();
        putKeys.add("AnInitialKey");

        long durationNanos = 0;
        long shutdownDurationNanos = 0;
        long startupDurationNanos = 0;
        final int NUM_OPS = 1000;
        final int NUM_CLIENTS = 1;
        final int NUM_SERVERS = 1;
        final int CACHE_SIZE = 20;
        final String CACHE_STRATEGY = "FIFO";
        int currOp = 0;
        int numPuts = 0;
        int numGets = 0;
        final int GETS_PER_PUT = 5;
        int opsPerClient = NUM_OPS / NUM_CLIENTS;

        long startTime = System.nanoTime();
        ArrayList<IECSNode> nodesAdded = (ArrayList<IECSNode>)
                ecs.addNodes(NUM_SERVERS, CACHE_STRATEGY, CACHE_SIZE);
        ecs.start();
        startupDurationNanos = System.nanoTime() - startTime;


        for(int i = 0 ; i < NUM_CLIENTS; i++){
            int serverToConnect = nodesAdded.get(i % nodesAdded.size()).getNodePort();
            KVStore kvClient = new KVStore("localhost", serverToConnect);
            kvClient.connect();
            clients.add(kvClient);
        }

        for(KVStore client: clients) {
            for (int i = 0; i < opsPerClient; i++){
                String key = keys.get(currOp);
                long duration;
                if (currOp % GETS_PER_PUT == 0) {
                    String value = enronData.get(key);
                    startTime = System.nanoTime();
                    client.put(key, value);
                    duration = System.nanoTime() - startTime;
                    numPuts += 1;
                    putKeys.add(key);
                } else {
                    String keyToGet = putKeys.get(numGets % putKeys.size());
                    startTime = System.nanoTime();
                    client.get(keyToGet);
                    duration = System.nanoTime() - startTime;
                    numGets += 1;
                }
                durationNanos += duration;
                currOp += 1;
            }
        }

        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
        String result = String.format("The server did %d iterations (%d puts, %d gets) in %d milliseconds" +
                        "This is a latency of %d ms per operation and a throughput of %f operations/s",
                currOp, numPuts, numGets, durationMillis, durationMillis / currOp, currOp / (float)durationMillis * 1000);
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
