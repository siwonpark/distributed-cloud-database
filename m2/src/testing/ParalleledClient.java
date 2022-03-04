package testing;

import client.KVStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ParalleledClient implements Runnable{
    KVStore client;
    EnronPerformanceTest caller;
    HashMap<String, String> data;
    List<String> keys;

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
            if(currOp % caller.GETS_PER_PUT == 0){
                String value = data.get(key);
                startTime = System.nanoTime();
                try {
                    client.put(key, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                putKeys.add(key);
                duration = System.nanoTime() - startTime;
            } else{
                String keyToGet = putKeys.get(currOp % putKeys.size());
                startTime = System.nanoTime();
                try {
                    client.get(keyToGet);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                duration = System.nanoTime() - startTime;
            }
            durationNanos += duration;
            currOp += 1;
        }
        caller.incrementDuration(durationNanos);
    }
}
