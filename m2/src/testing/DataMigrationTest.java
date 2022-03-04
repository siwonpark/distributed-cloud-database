 TODO: Commenting out until we figure out how to test this

package testing;

import app_kvClient.KVClient;
import app_kvServer.KVServer;
import ecs.ECS;
import ecs.ECSNode;
import junit.framework.TestCase;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static testing.AllTests.*;

public class DataMigrationTest extends TestCase {
    private KVServer originServer;
    private ArrayList<ECSNode> nodes;



    @Override
    protected void setUp(){
        originServer = new KVServer(PORT+1, "127.0.0.1", CACHE_SIZE, CACHE_STRATEGY);

        // Generate test metadata
        nodes = new ArrayList<>();
        ECSNode node1 = new ECSNode("node1", "127.0.0.1", PORT+1);
        nodes.add(node1);
        TreeMap<String, ECSNode> metadata = new ECS(nodes).getNodes();

        originServer.update(metadata);
    }

    @Override
    protected void tearDown(){
        originServer.shutDown();
    }

    /**
     * Test that data can be migrated from one server to another
     *
     * TODO: This test doesn't actually test anything yet since the
     * Database separation between servers is broken.
     */
    public void testDataMigration(){
        Exception ex = null;
        KVServer destServer = null;

        try {
            originServer.putKV("1", "2");
            originServer.putKV("3", "4");

            // Create a new server to move data to
            destServer = new KVServer(PORT+2, "127.0.0.1", CACHE_SIZE, CACHE_STRATEGY);
            ECSNode destServerNode = new ECSNode("node2", "127.0.0.1", PORT+2);
            nodes.add(destServerNode);
            TreeMap<String, ECSNode> metadata = new ECS(nodes).getNodes();

            originServer.update(metadata);
            destServer.update(metadata);

            originServer.start();
            destServer.start();

            ArrayList<String> range = new ArrayList<>();

            for (Map.Entry<String, ECSNode> entry : metadata.entrySet()) {
                range.add(entry.getKey());
            }

            String[] stringRange = range.toArray(new String[range.size()]);
            originServer.moveData(stringRange,
                    destServerNode);

            assertEquals(destServer.getKV("1"), "2");
            assertEquals(destServer.getKV("3"), "4");
        } catch (Exception e) {
            ex = e;
        } finally {
            if(destServer != null){
                destServer.shutDown();
            }
        }

        assertNull(ex);

    }

}
