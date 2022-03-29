package shared;

import ecs.ECSNode;
import ecs.ECS;
import java.util.Queue;
import java.util.LinkedList;

public class LockManager {

    private Queue<ECSNode> lockedNodes;
    private ECS ecs;

    public LockManager(ECS ecs) {
        this.ecs = ecs;
        this.lockedNodes = new LinkedList<>();
    }

    public boolean add(ECSNode node) throws Exception{
        if (ecs.lockWrite(node)) {
            return this.lockedNodes.offer(node);
        } else {
            throw new Exception(String.format("Can't do lock write on %s", node.getNodeName()));
        }
    }

    public boolean unlockAllNodes() {
        boolean success = true;
        while (!lockedNodes.isEmpty()) {
            if (!ecs.unlockWrite(lockedNodes.poll())) {
                success = false;
            }
        }
        return success;
    }
}
