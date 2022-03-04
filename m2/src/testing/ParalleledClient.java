package testing;

import client.KVStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/*
    This class is for use in performance tests, to
    emulate multiple concurrent client operations.
    Meant to be run in a separate thread, and each thread
    will put/get some number of keys.

 */
public class ParalleledClient implements Runnable{
    KVStore client;
    EnronPerformanceTest caller;
    HashMap<String, String> data;
    List<String> keys;
    Random rand = new Random(); //instance of random class

    public ParalleledClient(KVStore client,
                            EnronPerformanceTest EnronPerformanceTest,
                            HashMap<String, String> data, List<String> keys){
        this.client = client;
        this.caller = EnronPerformanceTest;
        this.data = data;
        this.keys = keys;
    }


    @Override
    public void run() {
        try {
            client.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int currOp = 0;
        long startTime;
        long duration;
        long durationNanos = 0;
        ArrayList<String> putKeys = new ArrayList<>();

        putKeys.add("AnInitialKey");


        for(String key: keys){
            String value = data.get(key);
            startTime = System.nanoTime();
            try {
                client.put(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
            putKeys.add(key);
            duration = System.nanoTime() - startTime;
            durationNanos += duration;
            for(int j = 0; j < caller.GETS_PER_PUT; j++ ) {
                int random = rand.nextInt(caller.NUM_OPS);
                String keyToGet = putKeys.get(random % putKeys.size());
                startTime = System.nanoTime();
                try {
                    client.get(keyToGet);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                duration = System.nanoTime() - startTime;
                durationNanos += duration;
            }
        }
        caller.incrementDuration(durationNanos);
    }
}
