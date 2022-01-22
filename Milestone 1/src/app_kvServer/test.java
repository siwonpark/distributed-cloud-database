package app_kvServer;

import java.util.Random;


class Data {
    public final String key;
    public String value;

    public Data(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

public class test {

    static String getRandomString(int length) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < length; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }

    public static void main(String[] args) throws Exception {
        int test_length = 5;
        Data[] lis = new Data[test_length];
        for (int i = 0; i < test_length; i++) {
            lis[i] = new Data(test.getRandomString(10), test.getRandomString(20));
        }
        long time1 = System.nanoTime();
        Btree b = new Btree(3);
        for (int i = 0; i < test_length; i++) {
            System.out.println("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        b.printTree();
        for (int i = 0; i < test_length; i++) {
            lis[i].value = test.getRandomString(20);
            System.out.println("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        b.printTree();
        long time2 = System.nanoTime();
        for (int i = 0; i < test_length; i++) {
            System.out.println("test: get " + lis[i].key);
            String s = b.get(lis[i].key);
            if (!s.equals(lis[i].value)) {
                System.out.println("shit" + lis[i].value);
                break;
            }
        }
        b.printTree();
        long time3 = System.nanoTime();

        System.out.println("time for put: " + (time2 - time1));
        System.out.println("time for get: " + (time3 - time2));
    }
}