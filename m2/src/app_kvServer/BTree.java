package app_kvServer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//TODO make BTree split according to size rather than maxNumber

public class BTree {
    public String treeName;
    public int maxNumber;
    public String root;
    static Logger logger = Logger.getRootLogger();
    FileOp f;

    public BTree(int maxNumber, FileOp f, String treeName, String root) {
        this.treeName = treeName;
        this.maxNumber = maxNumber;
        this.f = f;
        this.root = root;
    }

    /**
     * @return the leftmost node's name
     */
    public String getLeft() {
        return f.loadFile(this.root).refreshLeft();
    }

    /**
     * print the node and all the children for debugging
     */
    private void printNode(String node, int depth) {
        Node tmp_node = f.loadFile(node);
        if (BTree.logger.getLevel() == Level.DEBUG) {
            for (int i = 0; i < tmp_node.number; i++) {
                StringBuilder space = new StringBuilder();
                for (int j = 0; j < depth; j++)
                    space.append("    ");
                BTree.logger.debug(space + "key: " + tmp_node.keys[i]);
                if (tmp_node.type == FileType.DATA) {
                    assert tmp_node instanceof DataNode;
                    BTree.logger.debug(space + "values: " + ((DataNode) tmp_node).values[i]);
                } else {
                    assert tmp_node instanceof IndexNode;
                    this.printNode(((IndexNode) tmp_node).children[i], depth + 1);
                }
            }
        }
    }

    /**
     * print the tree for debugging
     */
    public void printTree() {
        BTree.logger.debug("\nPrint BTree");
        this.printNode(this.root, 0);
        BTree.logger.debug("END of BTree\n");
    }

    /**
     * get a value from the B+ tree.
     *
     * @return if not find, would be null.
     */
    public String get(String key) {
        BTree.logger.debug("BTree: get successfully. ");
        return f.loadFile(this.root).get(key);
    }

    /**
     * Insert or update the value, recursive entry the correct child node, also deal with splitting due to insertion and save changes to disk
     */
    public void put(String key, String value) {
        if (key == null)
            return;
        String rigNode = f.loadFile(this.root).put(key, value);
        if (rigNode != null) {//we need a new root
            String newRoot = f.newFile(FileType.INDEX);
            IndexNode tmp_newRoot = (IndexNode) f.loadFile(newRoot);
            Node tmp_rigNode = f.loadFile(rigNode);
            Node tmp_root = f.loadFile(this.root);
            tmp_newRoot.keys[0] = tmp_root.keys[tmp_root.number - 1];
            tmp_newRoot.keys[1] = tmp_rigNode.keys[tmp_root.number - 1];
            tmp_newRoot.children[0] = this.root;
            tmp_newRoot.children[1] = rigNode;
            tmp_newRoot.number = 2;
            f.dumpFile(tmp_newRoot);
            this.root = newRoot;
            f.dumpTree(this);
        }
        BTree.logger.debug("BTree: put successfully. ");
        if (logger.getLevel() == Level.DEBUG)
            this.printTree();
    }
}

abstract class Node {
    int size = 0;
    int number;
    int maxNumber;
    String[] keys;
    FileOp f;
    String name;
    FileType type;

    public Node(FileOp file, String name, FileType type, int maxNumber) {
        this.maxNumber = maxNumber;
        this.keys = new String[maxNumber];
        this.number = 0;
        this.f = file;
        this.name = name;
        this.type = type;
    }

    /**
     * get the value with key
     *
     * @return if the tree didn't find the value, would return null
     */
    abstract String get(String key);

    /**
     * Insert or update the value, recursive entry the correct child node, also deal with splitting due to insertion and save changes to disk
     *
     * @return if the splitting happens, would return a node to be added to the parent node. Otherwise, would return null
     */
    abstract String put(String key, String value);

    abstract String refreshLeft();

    /**
     * binary search for the key in a list
     *
     * @param key the key need to be found
     * @return the position, if can't be find, would be null
     */
    int findKey(String key) {
        if (this.number == 0)
            return -1;
        int left = 0;
        int right = this.number;
        int middle = (left + right) / 2;
        while (left < right) {
            String middleKey = this.keys[middle];
            if (key.compareTo(middleKey) == 0) {
                //find the position exactly equal
                return middle;
            } else if (key.compareTo(middleKey) < 0) {
                right = middle;
            } else {
                left = middle + 1;
            }
            middle = (left + right) / 2;
        }
        return -1;
    }

    /**
     * find the correct inserting place for a key,
     * for the tree, the children of the node's key should not be bigger than the node's key.
     *
     * @param key the key need to be inserted
     * @return the correct position
     */
    int findInsertPos(String key) {
        int left = 0;
        int right = this.number;
        int middle = (left + right) / 2;
        while (left < right) {
            String middleKey = this.keys[middle];
            if (key.compareTo(middleKey) == 0) {
                //find the position exactly equal
                return middle;
            } else if (key.compareTo(middleKey) < 0) {
                if (middle == 0 || key.compareTo(this.keys[middle - 1]) > 0) {//smallest or common insert place
                    return middle;
                } else {
                    right = middle;
                }
            } else {
                if (middle == this.number - 1) {
                    //biggest place
                    return this.number;
                } else {
                    left = middle + 1;
                }
            }
            middle = (left + right) / 2;
        }
        if (this.number != 0) {
            System.out.println("shouldn't get here");
        }
        return 0;
    }

    /**
     * this function is just for code reuse
     *
     * @param lis       if data node calls the function, it would be values list.
     *                  if index node calls the function, it would be children list.
     * @param insertPos The position where the child node needs to be inserted
     * @param key       the child node's key
     * @param value     the child node's value
     */
    void insertDirectly(String[] lis, int insertPos, String key, String value) {
        //insert the key and value/node, I think for loop is better than arraycopy which need more buffer and time
        BTree.logger.debug("insertPos: " + insertPos);
        for (int i = this.number; i > insertPos; i--) {
            this.keys[i] = this.keys[i - 1];
            lis[i] = lis[i - 1];
        }
        this.keys[insertPos] = key;
        lis[insertPos] = value;
        this.number++;
    }

    /**
     * this function is just for code reuse
     *
     * @param rigNode   The new node on the right side when node split
     * @param lefLis    if data node calls the function, it would be values of new node on the left side.
     *                  if index node calls the function, it would be children of new node on the left side.
     * @param rigLis    if data node calls the function, it would be values of new node on the right side.
     *                  if index node calls the function, it would be children of new node on the right side.
     * @param insertPos The position where the child node needs to be inserted and it is this node that causes the split
     * @param key       the child node's key
     * @param value     the child node's value
     */
    void splitInsert(Node rigNode, String[] lefLis, String[] rigLis, int insertPos, String key, String value) {
        BTree.logger.debug("insertPos: " + insertPos);
        int middle = (this.number + 1) / 2;
        rigNode.number = this.number + 1 - middle;
        if (middle > insertPos) {//need to insert into lefNode(this)
            //copy data to right
            for (int i = middle - 1, j = 0; i < this.number; i++, j++) {
                rigNode.keys[j] = this.keys[i];
                rigLis[j] = lefLis[i];
            }
            //adjust the lef
            for (int i = middle - 1; i > insertPos; i--) {
                this.keys[i] = this.keys[i - 1];
                lefLis[i] = lefLis[i - 1];
            }
            //insert into lefNode
            this.keys[insertPos] = key;
            lefLis[insertPos] = value;
        } else {//insert into rigNode
            //copy to right
            int i = middle;
            int j = 0;
            for (; i < insertPos; i++, j++) {
                rigNode.keys[j] = this.keys[i];
                rigLis[j] = lefLis[i];
            }
            //insert into rigNode
            rigNode.keys[j] = key;
            rigLis[j] = value;
            j++;
            //copy to right
            for (; i < this.number; i++, j++) {
                rigNode.keys[j] = this.keys[i];
                rigLis[j] = lefLis[i];
            }
        }
        this.number = middle;
    }
}

/**
 * the index node, For indexing, without any values
 */
class IndexNode extends Node {
    String[] children;

    /**
     * @param file      the FileOp handle
     * @param name      the name of this node, corresponds to the name of the file on the disk
     * @param type      INDEX
     * @param maxNumber The maximum number of children of a node in the B+ tree
     *                  For the file B+ tree, later this should be replaced with the maximum size of the index file
     */
    public IndexNode(FileOp file, String name, FileType type, int maxNumber) {
        super(file, name, type, maxNumber);
        this.children = new String[maxNumber];
    }

    /**
     * get the value with key
     *
     * @return if the tree didn't find the value, would return null
     */
    @Override
    String get(String key) {
        int pos = this.findInsertPos(key);
        if (pos == this.number) {
            return null;
        } else {
            return f.loadFile(this.children[pos]).get(key);
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
            f.dumpFile(this);
        }

        String newNode = f.loadFile(this.children[putPos]).put(key, value);
        if (newNode == null) {
            return null;
        } else {//we got a new node for inserting.
            //renew the lef key
            Node tmp_child = f.loadFile(this.children[putPos]);
            this.keys[putPos] = tmp_child.keys[tmp_child.number - 1];
            //insert the newNode(rigNode)
            Node tmp_newNode = f.loadFile(newNode);
            String newKey = tmp_newNode.keys[tmp_newNode.number - 1];
            //the insert position must be putPos + 1
            int insertPos = putPos + 1;
            if (this.number + 1 <= this.maxNumber) {//2. don't need to split
                this.insertDirectly(this.children, insertPos, newKey, newNode);
                BTree.logger.debug("inserted k-n without split: " + newKey);
                f.dumpFile(this);
                return null;
            } else {//3. need to split
                String rigNode = f.newFile(FileType.INDEX);
                IndexNode tmp_rigNode = (IndexNode) f.loadFile(rigNode);
                this.splitInsert(tmp_rigNode, this.children, tmp_rigNode.children, insertPos, newKey, newNode);
                BTree.logger.debug("inserted k-n, split: " + newKey);
                f.dumpFile(tmp_rigNode);
                f.dumpFile(this);
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
        return f.loadFile(this.children[0]).refreshLeft();
    }
}

/**
 * the data node, For data storage, could be large, have all the values
 * all the data nodes form a link list, can improve the efficiency of continuous reading
 */
class DataNode extends Node {
    //these leaf nodes form a chain
    String left;
    String right;
    String[] values;

    /**
     * @param file      the FileOp handle
     * @param name      the name of this node, corresponds to the name of the file on the disk
     * @param type      DATA
     * @param maxNumber The maximum number of value of a node in the B+ tree
     *                  For the file B+ tree, later this should be replaced with the maximum size of the data file
     */
    public DataNode(FileOp file, String name, FileType type, int maxNumber) {
        super(file, name, type, maxNumber);
        this.values = new String[this.maxNumber];
        this.left = null;
        this.right = null;
    }

    /**
     * get the value with key in the data node
     *
     * @return if tree didn't find the value, would return null
     */
    @Override
    String get(String key) {
        int pos = this.findKey(key);
        if (pos == -1) {
            return null;
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
            f.dumpFile(this);
            return null;
        } else if (this.number + 1 <= this.maxNumber) {//2. don't need to split
            this.insertDirectly(this.values, insertPos, key, value);
            BTree.logger.debug("leaf node inserted k-v without split: " + key + "-" + value);
            f.dumpFile(this);
            return null;
        } else {//3. need to split
            //new the right leaf
            String rigNode = f.newFile(FileType.DATA);
            DataNode tmp_rigNode = (DataNode) f.loadFile(rigNode);
            this.splitInsert(tmp_rigNode, this.values, tmp_rigNode.values, insertPos, key, value);
            BTree.logger.debug("leaf node inserted k-v, split: " + key + "-" + value);
            //re-chain the leaves
            if (this.right != null) {
                tmp_rigNode.right = this.right;
                DataNode tmp_rig = (DataNode) f.loadFile(this.right);
                tmp_rig.left = rigNode;
                f.dumpFile(tmp_rig);
            }
            this.right = rigNode;
            tmp_rigNode.left = this.name;
            //return the new node
            f.dumpFile(this);
            f.dumpFile(tmp_rigNode);
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
