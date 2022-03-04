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

    @Before
    public void setUp(){

    }

    @After
    public void tearDown(){

    }

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
        File configFile = new File("src/testing/ecs.config");
        ECSClient ecs = new ECSClient(configFile);
        ArrayList<KVStore> clients = new ArrayList<>();
        HashMap<String, String> enronData = loadEnronData(10000);
        ArrayList<String> keys = new ArrayList<>(enronData.keySet());
        ArrayList<String> putKeys = new ArrayList<>();
        putKeys.add("AnInitialKey");

        long durationNanos = 0;
        int numOps = 0;
        int numPuts = 0;
        int numGets = 0;
        int opsPerClient = 500;

        ArrayList<IECSNode> nodesAdded = (ArrayList<IECSNode>) ecs.addNodes(1, "FIFO", 20);
        ecs.start();


        for(int i = 0 ; i < 1; i++){
            int serverToConnect = nodesAdded.get(i % nodesAdded.size()).getNodePort();
            KVStore kvClient = new KVStore("localhost", serverToConnect);
            kvClient.connect();
            clients.add(kvClient);
        }

        for(KVStore client: clients) {
            for (int i = 0; i < opsPerClient; i++){
                String key = keys.get(numOps);
                long duration;
                if (numOps % 2 == 0) {
                    String value = enronData.get(key);
                    long startTime = System.nanoTime();
                    client.put(key, value);
                    duration = System.nanoTime() - startTime;
                    numPuts += 1;
                    putKeys.add(key);
                } else {
                    String keyToGet = putKeys.get(numGets % putKeys.size());
                    long startTime = System.nanoTime();
                    client.get(keyToGet);
                    duration = System.nanoTime() - startTime;
                    numGets += 1;
                }
                durationNanos += duration;
                numOps += 1;
            }
        }

        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
        String result = String.format("The server did %d iterations (%d puts, %d gets) in %d milliseconds" +
                        "This is a latency of %d ms per operation and a throughput of %f operations/s",
                numOps, numPuts, numGets, durationMillis, durationMillis / numOps, numOps / (float)durationMillis * 1000);
        logger.info(result);

        ecs.shutdown();
    }






}
