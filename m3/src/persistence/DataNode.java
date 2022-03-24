package persistence;
import static shared.PrintUtils.DELETE_STRING;
/**
 * the data node, For data storage, could be large, have all the values
 * all the data nodes form a link list, can improve the efficiency of continuous reading
 */
public class DataNode extends Node {
    //these leaf nodes form a chain
    String left;
    String right;
    String[] values;

    /**
     * @param name      the name of this node, corresponds to the name of the file on the disk
     * @param type      DATA
     * @param maxNumber The maximum number of value of a node in the B+ tree
     *                  For the file B+ tree, later this should be replaced with the maximum size of the data file
     */
    public DataNode(String name, FileType type, int maxNumber) {
        super(name, type, maxNumber);
        this.values = new String[this.maxNumber];
        this.left = null;
        this.right = null;
    }

    /**
     * get the value with key in the data node
     *
     * @return if tree didn't find the value, would return DELETE_STRING
     */
    @Override
    String get(String key) {
        int pos = this.findKey(key);
        if (pos == -1) {
            return DELETE_STRING;
        } else {
            return this.values[pos];
        }
    }

    /**
     * Insert into this data node or update the value, deal with splitting due to insertion and save changes to disk
     *
     * @return if the splitting happens, would return a node to be added to the parent node. Otherwise, would return null
     */
    @Override
    String put(String key, String value) {

        //find the insert position
        int insertPos = this.findInsertPos(key);
        if (insertPos < this.number && key.equals(this.keys[insertPos])) {//1. the key is existed
            this.values[insertPos] = value;
            FileOp.dumpFile(this);
            return null;
        } else if (this.number + 1 <= this.maxNumber) {//2. don't need to split
            this.insertDirectly(this.values, insertPos, key, value);
            BTree.logger.debug("leaf node inserted k-v without split: " + key + "-" + value);
            FileOp.dumpFile(this);
            return null;
        } else {//3. need to split
            //new the right leaf
            String rigNode = FileOp.newFile(FileType.DATA);
            DataNode tmp_rigNode = (DataNode) FileOp.loadFile(rigNode);
            this.splitInsert(tmp_rigNode, this.values, tmp_rigNode.values, insertPos, key, value);
            BTree.logger.debug("leaf node inserted k-v, split: " + key + "-" + value);
            //re-chain the leaves
            if (this.right != null) {
                tmp_rigNode.right = this.right;
                DataNode tmp_rig = (DataNode) FileOp.loadFile(this.right);
                tmp_rig.left = rigNode;
                FileOp.dumpFile(tmp_rig);
            }
            this.right = rigNode;
            tmp_rigNode.left = this.name;
            //return the new node
            FileOp.dumpFile(this);
            FileOp.dumpFile(tmp_rigNode);
            return rigNode;
        }
    }

    /**
     * find the leftmost node
     *
     * @return This node is the leftmost node, return this
     */
    @Override
    String refreshLeft() {
        if (this.number <= 0)
            return null;
        return this.name;
    }
}
