package testing;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.Message;

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
        long startTime = 0;
        long duration;
        long durationNanos = 0;
        ArrayList<String> putKeys = new ArrayList<>();

        putKeys.add("AnInitialKey");

        // Create the array list of operations to batch
        ArrayList<Message> operations = new ArrayList<>();
        for(String key: keys){
            String value = data.get(key);
            operations.add(new Message(key, value, KVMessage.StatusType.PUT));
            putKeys.add(key);
            for(int j = 0; j < caller.GETS_PER_PUT; j++ ) {
                int random = rand.nextInt(caller.NUM_PUTS);
                String keyToGet = putKeys.get(random % putKeys.size());
                operations.add(new Message(keyToGet, null, KVMessage.StatusType.GET));
            }
        }
        try {
            startTime = System.nanoTime();
            client.commit(operations);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            duration = System.nanoTime() - startTime;
            durationNanos += duration;
        }

        caller.incrementDuration(durationNanos);
    }
}
