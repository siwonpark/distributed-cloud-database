package app_kvServer;

public class Btree {
    private final Integer bTreeOrder = 3;
    private final Integer maxNumber = bTreeOrder;
    private Node root = new LeafNode();
    private LeafNode left = null;

    public Btree() {
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
        System.out.println("Print Btree");
        this.printNode(this.root, 0);
    }

    public String get(String key) {
        System.out.println("Btree: get successfully. ");
        return this.root.get(key);
    }

    public void put(String key, String value) {
        if (key == null)
            return;
        Node s = this.root.put(key, value);
        if (s != null)
            this.root = s;
        this.left = (LeafNode) this.root.refreshLeft();

        System.out.println("Btree: put successfully. ");
        for (int j = 0; j < this.root.number; j++) {
            System.out.print(this.root.keys[j] + " ");
        }
        System.out.println();
    }

    abstract class Node {
        protected Node parent;
        protected int number;
        protected String[] keys;
        protected Node[] children;
        protected String[] values;

        public Node() {
            this.keys = new String[maxNumber];
            this.number = 0;
            this.parent = null;
        }

        abstract String get(String key);

        abstract Node put(String key, String value);

        abstract LeafNode refreshLeft();
    }


    class BtreeNode extends Node {
        public BtreeNode() {
            super();
            this.children = new Node[maxNumber];
        }

        @Override
        String get(String key) {
            int i = 0;
            while (i < this.number) {
                if (key.compareTo(this.keys[i]) <= 0)
                    break;
                i++;
            }
            if (this.number == i)
                return null;
            return this.children[i].get(key);
        }

        @Override
        Node put(String key, String value) {
            int putPosi = 0;
            //TODO make it binary search
            while (putPosi < this.number) {
                if (key.compareTo(this.keys[putPosi]) <= 0) {
                    break;
                }
                putPosi++;
            }
            //exceed the maximum
            if (key.compareTo(this.keys[this.number - 1]) >= 0) {
                putPosi--;
                //renew the rightmost key
                this.keys[this.number - 1] = key;
            }
            Node newNode = this.children[putPosi].put(key, value);

            if (newNode == null) {
                return null;
            } else {//we got a new node for inserting.
                //find the insert position
                int insertPosi = 0;
                String newKey = newNode.keys[newNode.number - 1];
                //TODO make it binary search
                while (insertPosi < this.number) {
                    if (newKey.compareTo(this.keys[insertPosi]) < 0)
                        break;
                    insertPosi++;
                }
                if (this.number + 1 <= bTreeOrder) {//2. don't need to split
                    //insert this key and value
                    for (int i = this.number; i > insertPosi; i--) {
                        this.keys[i] = this.keys[i - 1];
                        this.children[i] = this.children[i - 1];
                    }
                    this.keys[insertPosi] = newKey;
                    this.children[insertPosi] = newNode;
                    newNode.parent = this;
                    this.number++;
                    System.out.println("leaf node inserted k-v without split: " + newKey + "-" + value);
                    return null;
                } else {//3. need to split
                    int middle = (this.number + 1) / 2;
                    //new the right leaf
                    BtreeNode tempNode = new BtreeNode();
                    tempNode.number = this.number + 1 - middle;
                    if (middle > insertPosi) {//insert into lefNode
                        //copy to right
                        for (int i = middle - 1, j = 0; i < this.number; i++, j++) {
                            tempNode.keys[j] = this.keys[i];
                            tempNode.children[j] = this.children[i];
                        }
                        //adjust the lef
                        for (int i = middle - 1; i > insertPosi; i--) {
                            this.keys[i] = this.keys[i - 1];
                            this.children[i] = this.children[i - 1];
                        }
                        //insert into lefNode
                        this.keys[insertPosi] = newKey;
                        this.children[insertPosi] = newNode;
                        newNode.parent = this;
                        this.number = middle;
                    } else {//insert into rigNode
                        //copy to right
                        int i = middle;
                        int j = 0;
                        for (; i < insertPosi; i++, j++) {
                            tempNode.keys[j] = this.keys[i];
                            tempNode.children[j] = this.children[i];
                        }
                        //insert into rigNode
                        j++;
                        tempNode.keys[j] = newKey;
                        tempNode.children[j] = newNode;
                        newNode.parent = this;
                        //copy to right
                        for (; i < this.number - 1; i++) {
                            tempNode.keys[j] = this.keys[i];
                            tempNode.children[j] = this.children[i];
                        }
                        //adjust lef
                        this.number = middle;
                    }
                    //return the new node
                    return tempNode;
                }
            }
        }

        @Override
        LeafNode refreshLeft() {
            return this.children[0].refreshLeft();
        }
//        Node insertNode(Node Node) {
//            String oldKey = null;
//            if (this.number > 0)
//                oldKey = this.keys[this.number - 1];
//
//            if (key == null) {//if the key is null, this is the new root, just put nodes in
//                this.keys[0] = lefNode.keys[lefNode.number - 1];
//                this.keys[1] = RigNode.keys[RigNode.number - 1];
//                this.children[0] = lefNode;
//                this.children[1] = lefNode;
//                this.number += 2;
//                return this;
//            }
//            if (this.number <= 0) {
//                System.out.println("shit");
//            }
//            //TODO make it binary search
//            int insertPosi = 0;
//            while (key.compareTo(this.keys[insertPosi]) != 0) {
//                insertPosi++;
//            }
////            TODO check this is neccessary or not
////            this.keys[insertPosi] = lefNode.keys[lefNode.number - 1];
////            this.children[insertPosi] = lefNode;
//            //insert the right node
//            insertPosi++;
//            if (this.number + 1 <= bTreeOrder) {//don't need to split
//                //TODO make it take less space
//                String[] tempKeys = new String[this.number - insertPosi];
//                Node[] tempChildren = new Node[this.number - insertPosi];
//                System.arraycopy(this.keys, insertPosi, tempKeys, 0, this.number - insertPosi);
//                System.arraycopy(this.children, insertPosi, tempChildren, 0, this.number - insertPosi);
//                System.arraycopy(tempKeys, 0, this.keys, insertPosi + 1, this.number - insertPosi);
//                System.arraycopy(tempChildren, 0, this.children, insertPosi + 1, this.number - insertPosi);
//                tempKeys[insertPosi] = RigNode.keys[RigNode.number - 1];
//                tempChildren[insertPosi] = RigNode;
//                this.number++;
//                return null;
//            } else {//3. need to split
//                int middle = this.number / 2;
//                //新建非叶子节点,作为拆分的右半部分
//                BtreeNode tempNode = new BtreeNode();
//                //非叶节点拆分后应该将其子节点的父节点指针更新为正确的指针
//                tempNode.number = this.number - middle;
//                tempNode.parent = this.parent;
//                //如果父节点为空,则新建一个非叶子节点作为父节点,并且让拆分成功的两个非叶子节点的指针指向父节点
//                if (this.parent == null) {
//                    System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ",新建父节点");
//                    BtreeNode tempBtreeNode = new BtreeNode();
//                    tempNode.parent = tempBtreeNode;
//                    this.parent = tempBtreeNode;
//                    oldKey = null;
//                }
//                System.arraycopy(tempKeys, middle, tempNode.keys, 0, tempNode.number);
//                System.arraycopy(tempChildren, middle, tempNode.children, 0, tempNode.number);
//                for (int j = 0; j < tempNode.number; j++) {
//                    tempNode.children[j].parent = tempNode;
//                }
//                //让原有非叶子节点作为左边节点
//                this.number = middle;
//                this.keys = new String[maxNumber];
//                this.children = new Node[maxNumber];
//                System.arraycopy(tempKeys, 0, this.keys, 0, middle);
//                System.arraycopy(tempChildren, 0, this.children, 0, middle);
//
//                //叶子节点拆分成功后,需要把新生成的节点插入父节点
//                BtreeNode parentNode = (BtreeNode) this.parent;
//                return parentNode.insertNode(this, tempNode, oldKey);
//            }
//        }
    }

    class LeafNode extends Node {
        //these leaf nodes form a chain
        protected LeafNode left;
        protected LeafNode right;

        public LeafNode() {
            super();
            this.values = new String[maxNumber];
            this.left = null;
            this.right = null;
        }

        @Override
        String get(String key) {
            if (this.number <= 0)
                return null;
            int left = 0;
            int right = this.number;
            int middle = (left + right) / 2;
            while (left < right) {
                String middleKey = this.keys[middle];
                if (key.compareTo(middleKey) == 0)
                    return this.values[middle];
                else if (key.compareTo(middleKey) < 0)
                    right = middle;
                else
                    left = middle;
                middle = (left + right) / 2;
            }
            return null;
        }

        @Override
        Node put(String key, String value) {
            //find the insert position
            int insertPosi = 0;
            //TODO make it binary search
            while (insertPosi < this.number) {
                if (key.compareTo(this.keys[insertPosi]) <= 0)
                    break;
                insertPosi++;
            }
            if (key.equals(this.keys[insertPosi])) {//1. the key is existed
                this.values[insertPosi] = value;
                return null;
            } else if (this.number + 1 <= bTreeOrder) {//2. don't need to split
                //insert this key and value
                for (int i = this.number; i > insertPosi; i--) {
                    this.keys[i] = this.keys[i - 1];
                    this.values[i] = this.keys[i - 1];
                }
                this.keys[insertPosi] = key;
                this.values[insertPosi] = value;
                this.number++;
                System.out.println("leaf node inserted k-v without split: " + key + "-" + value);
                return null;
            } else {//3. need to split
                int middle = (this.number + 1) / 2;
                //new the right leaf
                LeafNode tempNode = new LeafNode();
                tempNode.number = this.number + 1 - middle;
                if (middle > insertPosi) {//insert into lefNode
                    //copy to right
                    for (int i = middle - 1, j = 0; i < this.number; i++, j++) {
                        tempNode.keys[j] = this.keys[i];
                        tempNode.values[j] = this.values[i];
                    }
                    //adjust the lef
                    for (int i = middle - 1; i > insertPosi; i--) {
                        this.keys[i] = this.keys[i - 1];
                        this.values[i] = this.values[i - 1];
                    }
                    //insert into lefNode
                    this.keys[insertPosi] = key;
                    this.values[insertPosi] = value;
                    this.number = middle;
                } else {//insert into rigNode
                    //copy to right
                    int i = middle;
                    int j = 0;
                    for (; i < insertPosi; i++, j++) {
                        tempNode.keys[j] = this.keys[i];
                        tempNode.values[j] = this.values[i];
                    }
                    //insert into rigNode
                    j++;
                    tempNode.keys[j] = key;
                    tempNode.values[j] = value;
                    //copy to right
                    for (; i < this.number - 1; i++) {
                        tempNode.keys[j] = this.keys[i];
                        tempNode.values[j] = this.values[i];
                    }
                    //adjust lef
                    this.number = middle;
                }
                //re-chain the leaves
                tempNode.right = this.right;
                this.right = tempNode;
                tempNode.left = this;
                //return the new node
                return tempNode;
            }
        }

        @Override
        LeafNode refreshLeft() {
            if (this.number <= 0)
                return null;
            return this;
        }
    }
}
