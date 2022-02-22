package persistence;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;

//TODO make BTree split according to size rather than maxNumber

public class BTree {
    public String root;
    static Logger logger = Logger.getRootLogger();

    public BTree(String root) {
        this.root = root;
    }

    /**
     * @return the leftmost node's name
     */
    public String getLeft() {
        return FileOp.loadFile(this.root).refreshLeft();
    }

    /**
     * print the node and all the children for debugging
     */
    private void printNode(String node, int depth) {
        Node tmp_node = FileOp.loadFile(node);
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
        return FileOp.loadFile(this.root).get(key);
    }

    /**
     * Insert or update the value, recursive entry the correct child node, also deal with splitting due to insertion and save changes to disk
     */
    public void put(String key, String value) {
        if (key == null)
            return;
        String rigNode = FileOp.loadFile(this.root).put(key, value);
        if (rigNode != null) {//we need a new root
            String newRoot = FileOp.newFile(FileType.INDEX);
            IndexNode tmp_newRoot = (IndexNode) FileOp.loadFile(newRoot);
            Node tmp_rigNode = FileOp.loadFile(rigNode);
            Node tmp_root = FileOp.loadFile(this.root);
            tmp_newRoot.keys[0] = tmp_root.keys[tmp_root.number - 1];
            tmp_newRoot.keys[1] = tmp_rigNode.keys[tmp_root.number - 1];
            tmp_newRoot.children[0] = this.root;
            tmp_newRoot.children[1] = rigNode;
            tmp_newRoot.number = 2;
            FileOp.dumpFile(tmp_newRoot);
            this.root = newRoot;
            FileOp.dumpTree(this);
        }
        BTree.logger.debug("BTree: put successfully. ");
        if (logger.getLevel() == Level.TRACE)
            this.printTree();
    }
}






