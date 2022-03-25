package persistence;
import static shared.PrintUtils.DELETE_STRING;
/**
 * the index node, For indexing, without any values
 */
public class IndexNode extends Node {
    String[] children;

    /**
     * @param name      the name of this node, corresponds to the name of the file on the disk
     * @param type      INDEX
     * @param maxNumber The maximum number of children of a node in the B+ tree
     *                  For the file B+ tree, later this should be replaced with the maximum size of the index file
     */
    public IndexNode(String name, FileType type, int maxNumber) {
        super(name, type, maxNumber);
        this.children = new String[maxNumber];
    }

    /**
     * get the value with key
     *
     * @return if the tree didn't find the value, would return DELETE_STRING
     */
    @Override
    String get(String key) {
        int pos = this.findInsertPos(key);
        if (pos == this.number) {
            return DELETE_STRING;
        } else {
            return FileOp.loadFile(this.children[pos]).get(key);
        }
    }

    /**
     * Insert or update the value, recursive entry the correct child node, also deal with splitting due to insertion and save changes to disk
     *
     * @return if the splitting happens, would return a node to be added to the parent node. Otherwise, would return null
     */
    @Override
    String put(String key, String value) {
        int putPos = this.findInsertPos(key);
        if (putPos == this.number) {//exceed the maximum
            //renew the rightmost key
            this.keys[this.number - 1] = key;
            putPos--;
            FileOp.dumpFile(this);
        }

        String newNode = FileOp.loadFile(this.children[putPos]).put(key, value);
        if (newNode == null) {
            return null;
        } else {//we got a new node for inserting.
            //renew the lef key
            Node tmp_child = FileOp.loadFile(this.children[putPos]);
            this.keys[putPos] = tmp_child.keys[tmp_child.number - 1];
            //insert the newNode(rigNode)
            Node tmp_newNode = FileOp.loadFile(newNode);
            String newKey = tmp_newNode.keys[tmp_newNode.number - 1];
            //the insert position must be putPos + 1
            int insertPos = putPos + 1;
            if (this.number + 1 <= this.maxNumber) {//2. don't need to split
                this.insertDirectly(this.children, insertPos, newKey, newNode);
                BTree.logger.debug("inserted k-n without split: " + newKey);
                FileOp.dumpFile(this);
                return null;
            } else {//3. need to split
                String rigNode = FileOp.newFile(FileType.INDEX);
                IndexNode tmp_rigNode = (IndexNode) FileOp.loadFile(rigNode);
                this.splitInsert(tmp_rigNode, this.children, tmp_rigNode.children, insertPos, newKey, newNode);
                BTree.logger.debug("inserted k-n, split: " + newKey);
                FileOp.dumpFile(tmp_rigNode);
                FileOp.dumpFile(this);
                //return the new node
                return rigNode;
            }
        }
    }

    /**
     * find the leftmost node
     *
     * @return leftmost node
     */
    @Override
    String refreshLeft() {
        return FileOp.loadFile(this.children[0]).refreshLeft();
    }
}
