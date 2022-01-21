package app_kvServer;

public class Btree {
    private final Integer bTreeOrder = 3;
    //B+树的非叶子节点最小拥有的子节点数量（同时也是键的最小数量）
    //private Integer minNUmber;
    //B+树的非叶子节点最大拥有的节点数量（同时也是键的最大数量）
    private final Integer maxNumber = bTreeOrder + 1;
    private Node root = new LeafNode();
    private LeafNode left = null;

    public Btree() {
    }

    //查询
    public String get(String key) {
        return this.root.get(key);
    }

    //插入
    public void put(String key, String value) {
        if (key == null)
            return;
        Node s = this.root.put(key, value);
        if (s != null)
            this.root = s;
        this.left = (LeafNode) this.root.refreshLeft();

        System.out.println("插入完成,当前根节点为:");
        for (int j = 0; j < this.root.number; j++) {
            System.out.print(this.root.keys[j] + " ");
        }
        System.out.println();
    }


    /**
     * 节点父类，因为在B+树中，非叶子节点不用存储具体的数据，只需要把索引作为键就可以了
     * 所以叶子节点和非叶子节点的类不太一样，但是又会公用一些方法，所以用Node类作为父类,
     * 而且因为要互相调用一些公有方法，所以使用抽象类
     */
    abstract class Node {
        protected Node parent;
        protected int number;
        protected String[] keys;

        public Node() {
            this.keys = new String[maxNumber];
            this.number = 0;
            this.parent = null;
        }

        abstract String get(String key);

        abstract Node put(String key, String value);

        abstract LeafNode refreshLeft();
    }


    /**
     * 非叶节点类
     */

    class BPlusNode extends Node {
        protected Node[] children;

        public BPlusNode() {
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
            int i = 0;
            while (i < this.number) {
                if (key.compareTo(this.keys[i]) < 0)
                    break;
                i++;
            }
            //TODO
            if (key.compareTo(this.keys[this.number - 1]) >= 0) {
                i--;
            }
            System.out.println("非叶子节点查找key: " + this.keys[i]);
            return this.children[i].put(key, value);
        }

        @Override
        LeafNode refreshLeft() {
            return this.children[0].refreshLeft();
        }

        /**
         * 当叶子节点插入成功完成分解时,递归地向父节点插入新的节点以保持平衡
         */
        Node insertNode(Node node1, Node node2, String key) {
            System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1]);
            String oldKey = null;
            if (this.number > 0)
                oldKey = this.keys[this.number - 1];
            //如果原有key为null,说明这个非节点是空的,直接放入两个节点即可
            if (key == null || this.number <= 0) {
                //System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + "直接插入");
                this.keys[0] = node1.keys[node1.number - 1];
                this.keys[1] = node2.keys[node2.number - 1];
                this.children[0] = node1;
                this.children[1] = node2;
                this.number += 2;
                return this;
            }
            //原有节点不为空,则应该先寻找原有节点的位置,然后将新的节点插入到原有节点中
            int i = 0;
            while (key.compareTo(this.keys[i]) != 0) {
                i++;
            }
            //左边节点的最大值可以直接插入,右边的要挪一挪再进行插入
            this.keys[i] = node1.keys[node1.number - 1];
            this.children[i] = node1;

            String[] tempKeys = new String[maxNumber];
            Node[] tempChildren = new Node[maxNumber];

            System.arraycopy(this.keys, 0, tempKeys, 0, i + 1);
            System.arraycopy(this.children, 0, tempChildren, 0, i + 1);
            System.arraycopy(this.keys, i + 1, tempKeys, i + 2, this.number - i - 1);
            System.arraycopy(this.children, i + 1, tempChildren, i + 2, this.number - i - 1);
            tempKeys[i + 1] = node2.keys[node2.number - 1];
            tempChildren[i + 1] = node2;

            this.number++;

            //判断是否需要拆分
            //如果不需要拆分,把数组复制回去,直接返回
            if (this.number <= bTreeOrder) {
                System.arraycopy(tempKeys, 0, this.keys, 0, this.number);
                System.arraycopy(tempChildren, 0, this.children, 0, this.number);
                System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ", 不需要拆分");
                return null;
            }

            System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ",需要拆分");

            //如果需要拆分,和拆叶子节点时类似,从中间拆开
            int middle = this.number / 2;

            //新建非叶子节点,作为拆分的右半部分
            BPlusNode tempNode = new BPlusNode();
            //非叶节点拆分后应该将其子节点的父节点指针更新为正确的指针
            tempNode.number = this.number - middle;
            tempNode.parent = this.parent;
            //如果父节点为空,则新建一个非叶子节点作为父节点,并且让拆分成功的两个非叶子节点的指针指向父节点
            if (this.parent == null) {
                System.out.println("非叶子节点,插入key: " + node1.keys[node1.number - 1] + " " + node2.keys[node2.number - 1] + ",新建父节点");
                BPlusNode tempBPlusNode = new BPlusNode();
                tempNode.parent = tempBPlusNode;
                this.parent = tempBPlusNode;
                oldKey = null;
            }
            System.arraycopy(tempKeys, middle, tempNode.keys, 0, tempNode.number);
            System.arraycopy(tempChildren, middle, tempNode.children, 0, tempNode.number);
            for (int j = 0; j < tempNode.number; j++) {
                tempNode.children[j].parent = tempNode;
            }
            //让原有非叶子节点作为左边节点
            this.number = middle;
            this.keys = new String[maxNumber];
            this.children = new Node[maxNumber];
            System.arraycopy(tempKeys, 0, this.keys, 0, middle);
            System.arraycopy(tempChildren, 0, this.children, 0, middle);

            //叶子节点拆分成功后,需要把新生成的节点插入父节点
            BPlusNode parentNode = (BPlusNode) this.parent;
            return parentNode.insertNode(this, tempNode, oldKey);
        }

    }

    /**
     * 叶节点类
     */
    class LeafNode extends Node {

        protected String[] values;
        protected LeafNode left;
        protected LeafNode right;

        public LeafNode() {
            super();
            this.values = new String[maxNumber];
            this.left = null;
            this.right = null;
        }

        /**
         * 进行查找,经典二分查找,不多加注释
         */
        @Override
        String get(String key) {
            if (this.number <= 0)
                return null;
            System.out.println("叶子节点查找");
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
            System.out.println("叶子节点,插入key: " + key);
            //保存原始存在父节点的key值
            String oldKey = null;
            if (this.number > 0)
                oldKey = this.keys[this.number - 1];
            //先插入数据
            int i = 0;
            while (i < this.number) {
                if (key.compareTo(this.keys[i]) < 0)
                    break;
                i++;
            }

            //复制数组,完成添加
            Object[] tempKeys = new Object[maxNumber];
            Object[] tempValues = new Object[maxNumber];
            System.arraycopy(this.keys, 0, tempKeys, 0, i);
            System.arraycopy(this.values, 0, tempValues, 0, i);
            System.arraycopy(this.keys, i, tempKeys, i + 1, this.number - i);
            System.arraycopy(this.values, i, tempValues, i + 1, this.number - i);
            tempKeys[i] = key;
            tempValues[i] = value;

            this.number++;

            System.out.println("插入完成,当前节点key为:");
            for (int j = 0; j < this.number; j++)
                System.out.print(tempKeys[j] + " ");
            System.out.println();

            //判断是否需要拆分
            //如果不需要拆分完成复制后直接返回
            if (this.number <= bTreeOrder) {
                System.arraycopy(tempKeys, 0, this.keys, 0, this.number);
                System.arraycopy(tempValues, 0, this.values, 0, this.number);

                //有可能虽然没有节点分裂，但是实际上插入的值大于了原来的最大值，所以所有父节点的边界值都要进行更新
                Node node = this;
                while (node.parent != null) {
                    String tmp = node.keys[node.number - 1];
                    if (tmp.compareTo(node.parent.keys[node.parent.number - 1]) > 0) {
                        node.parent.keys[node.parent.number - 1] = tmp;
                        node = node.parent;
                    } else {
                        break;
                    }
                }
                System.out.println("叶子节点,插入key: " + key + ",不需要拆分");
                return null;
            }
            System.out.println("叶子节点,插入key: " + key + ",需要拆分");
            //如果需要拆分,则从中间把节点拆分差不多的两部分
            int middle = this.number / 2;
            //新建叶子节点,作为拆分的右半部分
            LeafNode tempNode = new LeafNode();
            tempNode.number = this.number - middle;
            tempNode.parent = this.parent;
            //如果父节点为空,则新建一个非叶子节点作为父节点,并且让拆分成功的两个叶子节点的指针指向父节点
            if (this.parent == null) {
                System.out.println("叶子节点,插入key: " + key + ",父节点为空 新建父节点");
                BPlusNode tempBPlusNode = new BPlusNode();
                tempNode.parent = tempBPlusNode;
                this.parent = tempBPlusNode;
                oldKey = null;
            }
            System.arraycopy(tempKeys, middle, tempNode.keys, 0, tempNode.number);
            System.arraycopy(tempValues, middle, tempNode.values, 0, tempNode.number);

            //让原有叶子节点作为拆分的左半部分
            this.number = middle;
            this.keys = new String[maxNumber];
            this.values = new String[maxNumber];
            System.arraycopy(tempKeys, 0, this.keys, 0, middle);
            System.arraycopy(tempValues, 0, this.values, 0, middle);

            this.right = tempNode;
            tempNode.left = this;

            //叶子节点拆分成功后,需要把新生成的节点插入父节点
            BPlusNode parentNode = (BPlusNode) this.parent;
            return parentNode.insertNode(this, tempNode, oldKey);
        }

        @Override
        LeafNode refreshLeft() {
            if (this.number <= 0)
                return null;
            return this;
        }
    }
}
