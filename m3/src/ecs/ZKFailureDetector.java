package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ZKFailureDetector implements Watcher {
    private ECS ecs;
    private Logger logger = Logger.getRootLogger();

    public ZKFailureDetector(ECS ecs) {
        this.ecs = ecs;
    }

    @Override
    public synchronized void process(WatchedEvent event) {
        logger.info("Watcher triggered");
        if (event == null) {
            return;
        }

        // Get connection status
        Event.KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Connection status:" + keeperState.toString());
        logger.info("Event type:" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            if (EventType.NodeDeleted == eventType) {
                String[] pathParts = path.split("/");
                String nodeName = pathParts[pathParts.length - 1];
                if (ecs.getECSNode(nodeName) != null) {
                    logger.info("Node deleted at znode " + path);
                    ecs.handleServerFailure(nodeName);
                }
            }
        }
    }
}
