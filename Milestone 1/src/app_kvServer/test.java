package app_kvServer;

class Data{
    public final String key;
    public final String value;
    public Data(String key, String value){
        this.key = key;
        this.value = value;
    }
}

public class Test {
    public static void main(String[] args){
        Data[] lis = new Data[10000];
        for (int i = 0;  i < 10000; i++) {
            lis[i] = new Data("something random","something random");
        }

        long time1 = System.nanoTime();
        Btree b = new Btree();
        long time2 = System.nanoTime();

        Product p1 = b.find(345);

        long time3 = System.nanoTime();

        System.out.println("插入耗时: " + (time2 - time1));
        System.out.println("查询耗时: " + (time3 - time2));
    }
}