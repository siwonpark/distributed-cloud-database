package app_kvServer;


public class Btree {
    static int maxNumber;
    private Node root;
    private LeafNode left = null;

    public Btree(int maxNumber) {
        Btree.maxNumber = maxNumber;
        root = new LeafNode();
    }

    public LeafNode getLeft() {
        return this.left;
    }

    private void printNode(Node node, int depth) {
        for (int i = 0; i < node.number; i++) {
            for (int j = 0; j < depth; j++)
                System.out.print("  ");
            System.out.println("key: " + node.keys[i]);
            if (node.values != null) {
                for (int j = 0; j < depth; j++)
                    System.out.print("  ");
                System.out.println("values: " + node.values[i]);
            } else if (node.children != null) {
                this.printNode(node.children[i], depth + 1);
            }
        }
    }

    public void printTree() {
        System.out.println("\nPrint Btree");
        this.printNode(this.root, 0);
        System.out.println("END of Btree\n");
    }

    public String get(String key) throws Exception {
        System.out.println("Btree: get successfully. ");
        return this.root.get(key);
    }

    public void put(String key, String value) throws Exception {
        if (key == null)
            return;
        Node rigNode = this.root.put(key, value);
        if (rigNode != null) {//we need a new root
            BtreeNode newRoot = new BtreeNode();
            newRoot.keys[0] = this.root.keys[this.root.number - 1];
            newRoot.keys[1] = rigNode.keys[this.root.number - 1];
            newRoot.children[0] = this.root;
            newRoot.children[1] = rigNode;
            newRoot.number = 2;
            rigNode.parent = newRoot;
            this.root.parent = newRoot;

            this.root = newRoot;
        }
        this.left = this.root.refreshLeft();

        System.out.println("Btree: put successfully. ");
        this.printTree();
    }

}

abstract class Node {
    protected Node parent;
    protected int number;
    protected String[] keys;
    protected Node[] children;
    protected String[] values;

    public Node() {
        this.keys = new String[Btree.maxNumber];
        this.number = 0;
        this.parent = null;
    }

    abstract String get(String key) throws Exception;

    abstract Node put(String key, String value) throws Exception;

    abstract LeafNode refreshLeft();

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
                left = middle;
            }
            middle = (left + right) / 2;
        }
        return -1;
    }

    int findInsertPos(String key) throws Exception {
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
                    return middle;
                } else {
                    left = middle;
                }
            }
            middle = (left + right) / 2;
        }
        if (this.number == 0)
            return 0;
        else {
            throw new Exception("shouldn't get here");
        }
    }

    <T> void insertDirectly(T[] lis, int insertPos, String key, T value) {
        //insert the key and value/node, I think for loop is better than arraycopy which need more buffer and time
        for (int i = this.number; i > insertPos; i--) {
            this.keys[i] = this.keys[i - 1];
            lis[i] = lis[i - 1];
        }
        this.keys[insertPos] = key;
        lis[insertPos] = value;
        this.number++;
    }

    void updateParentKey(String oldKey) {
        int parentKeyPos = this.parent.findKey(oldKey);
        this.parent.keys[parentKeyPos] = this.keys[this.number - 1];
    }

    <T> void splitInsert(Node rigNode, T[] lefLis, T[] rigLis, int insertPos, String key, T value) {
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
            for (; i < this.number - 1; i++, j++) {
                rigNode.keys[j] = this.keys[i];
                rigLis[j] = lefLis[i];
            }
        }
        this.number = middle;
    }
}

class BtreeNode extends Node {
    public BtreeNode() {
        super();
        this.children = new Node[Btree.maxNumber];
    }

    @Override
    String get(String key) throws Exception {
        int pos = this.findInsertPos(key);
        if (pos == this.number - 1 && !key.equals(this.keys[this.number - 1])) {
            return null;
        } else {
            return this.children[pos].get(key);
        }
    }

    @Override
    Node put(String key, String value) throws Exception {
        int putPos = this.findInsertPos(key);
        if (putPos == this.number - 1) {//may exceed the maximum
            //renew the rightmost key
            this.keys[this.number - 1] = key;
        }

        Node newNode = this.children[putPos].put(key, value);

        if (newNode == null) {
            return null;
        } else {//we got a new node for inserting.
            String oldKey = this.keys[this.number - 1];
            String newKey = newNode.keys[newNode.number - 1];
            //find the insert position
            int insertPos = this.findInsertPos(newKey);
            if (this.number + 1 <= Btree.maxNumber) {//2. don't need to split
                this.insertDirectly(this.children, insertPos, newKey, newNode);
                System.out.println("inserted k-n without split: " + newKey);

                newNode.parent = this;
                return null;
            } else {//3. need to split
                BtreeNode rigNode = new BtreeNode();
                this.splitInsert(rigNode, this.children, rigNode.children, insertPos, newKey, newNode);
                System.out.println("inserted k-n, split: " + newKey);

                //renew the nodes' parent link
                newNode.parent = this;
                for (int i = 0; i < rigNode.number; i++) {
                    rigNode.children[i].parent = rigNode;
                }

                if (this.parent != null) {
                    this.updateParentKey(oldKey);
                }
                //return the new node
                return rigNode;
            }
        }
    }

    @Override
    LeafNode refreshLeft() {
        return this.children[0].refreshLeft();
    }
}

class LeafNode extends Node {
    //these leaf nodes form a chain
    protected LeafNode left;
    protected LeafNode right;

    public LeafNode() {
        super();
        this.values = new String[Btree.maxNumber];
        this.left = null;
        this.right = null;
    }

    @Override
    String get(String key) {
        int pos = this.findKey(key);
        if (this.number == 0 || !this.keys[pos].equals(key)) {
            return null;
        } else {
            return this.values[pos];
        }
    }

    @Override
    Node put(String key, String value) throws Exception {
        String oldKey = null;
        if (this.number != 0) {
            oldKey = this.keys[this.number - 1];
        } else {
            if (this.parent != null) {
                throw new Exception("should have parent");
            }
        }

        //find the insert position
        int insertPos = this.findInsertPos(key);
        if (key.equals(this.keys[insertPos])) {//1. the key is existed
            this.values[insertPos] = value;
            return null;
        } else if (this.number + 1 <= Btree.maxNumber) {//2. don't need to split
            this.insertDirectly(this.values, insertPos, key, value);
            System.out.println("leaf node inserted k-v without split: " + key + "-" + value);
            return null;
        } else {//3. need to split
            //new the right leaf
            LeafNode rigNode = new LeafNode();
            this.splitInsert(rigNode, this.values, rigNode.values, insertPos, key, value);
            System.out.println("leaf node inserted k-v, split: " + key + "-" + value);
            if (this.parent != null) {
                this.updateParentKey(oldKey);
            }
            //re-chain the leaves
            rigNode.right = this.right;
            this.right = rigNode;
            rigNode.left = this;
            //return the new node
            return rigNode;
        }
    }

    @Override
    LeafNode refreshLeft() {
        if (this.number <= 0)
            return null;
        return this;
    }
}

