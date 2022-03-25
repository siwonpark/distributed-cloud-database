package persistence;

public abstract class Node {
    int size = 0;
    int number;
    int maxNumber;
    String[] keys;
    String name;
    FileType type;

    public Node(String name, FileType type, int maxNumber) {
        this.maxNumber = maxNumber;
        this.keys = new String[maxNumber];
        this.number = 0;
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
     * @return if the splitting happens, would return a node to be added to the parent node. Otherwise, would return DELETE_STRING
     */
    abstract String put(String key, String value);

    abstract String refreshLeft();

    /**
     * binary search for the key in a list
     *
     * @param key the key need to be found
     * @return the position, if can't be find, would be -1
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
