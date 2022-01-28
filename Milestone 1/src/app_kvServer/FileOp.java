package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

enum FileType {
    INDEX, DATA
}

//TODO make all the FileOp functions static

public class FileOp {
    static Logger logger = Logger.getRootLogger();
    final static int fileNameLength = 20;
    final Random random = new Random();
    final static int charNum = 26 + 26 + 10;
    ArrayList<TrieNode> FileNameRootList = new ArrayList<>();
    String filePath;
    BTree b;

    /**
     * Check if the data in the data node is ordered
     */
    public static boolean SeqenceOrderAndChainTest(BTree b) {
        String lef = b.getLeft();
        DataNode tmp_lef = (DataNode) b.f.loadFile(lef);
        while (tmp_lef.right != null) {
            for (int i = 0; i < tmp_lef.number - 1; i++) {
                if (tmp_lef.keys[i].compareTo(tmp_lef.keys[i + 1]) >= 0) {
                    return false;
                }
            }
            DataNode tmp = tmp_lef;
            tmp_lef = (DataNode) b.f.loadFile(tmp_lef.right);

            if (!tmp_lef.left.equals(tmp.name)) {
                return false;
            }
            if (tmp.keys[tmp.number - 1].compareTo(tmp_lef.keys[0]) >= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Conversion of filename character to trie tree node position
     */
    public static int char2int(char c) {
        if ((int) c >= 97) {
            return (int) c - 61;
        } else if ((int) c >= 65) {
            return (int) c - 55;
        } else {
            return (int) c - 48;
        }
    }

    /**
     * Conversion of filename trie tree node position to character
     */
    public static char int2char(int n) {
        if (n < 10) {
            return (char) (n + 48);
        } else if (n < 36) {
            return (char) (n + 55);
        } else {
            return (char) (n + 61);
        }
    }

    /**
     * Determine if a file exists by trie tree
     */
    public boolean isFileExists(String name) {
        ArrayList<TrieNode> lis = this.FileNameRootList;
        for (int i = 0; ; i++) {
            int tmp = char2int(name.charAt(i));
            if (lis.get(tmp) == null) {
                return false;
            } else {
                if (i == name.length() - 1) {
                    return lis.get(tmp).occupied;
                } else {
                    lis = lis.get(tmp).children;
                }
            }
        }
    }

    /**
     * Add a filename into trie tree
     */
    public boolean addFileName(String name, FileType type) {
        ArrayList<TrieNode> lis = this.FileNameRootList;
        for (int i = 0; ; i++) {
            int tmp = char2int(name.charAt(i));
            if (lis.get(tmp) == null) {
                lis.set(tmp, new TrieNode());
            }
            TrieNode son = lis.get(tmp);
            if (i == name.length() - 1) {
                if (!son.occupied && son.type == null) {
                    son.occupied = true;
                    son.type = type;
                    return true;
                } else {
                    return false;
                }
            } else {
                lis = son.children;
            }
        }
    }

    //    public boolean delFileName(String name, FileType type) {
//        ArrayList<TrieNode> lis = this.rootList;
//        for (int i = 0; ; i++) {
//            int tmp = char2int(name.charAt(i));
//            if (lis.get(tmp) == null) {
//                return false;
//            }
//            TrieNode son = lis.get(tmp);
//            if (i == name.length() - 1) {
//                son.occupied = false;
//                son.type = null;
//                for (int j = 0; j < charNum; j++) {
//                    if (son.children.get(j) != null)
//                        return true;
//                }
//                return true;
//            } else {
//                lis = son.children;
//            }
//        }
//    }

    /**
     * Generate a random file name
     */
    public String genFileName() {
        StringBuilder buffer = new StringBuilder(fileNameLength);
        for (int i = 0; i < fileNameLength; i++) {
            buffer.append(int2char(random.nextInt(charNum)));
        }
        return buffer.toString();
    }

    /**
     * Create a new index/data file on disk
     *
     * @param type INDEX or DATA
     * @return the file name
     */
    public String newFile(FileType type) {
        String name;
        do {
            name = genFileName();
        } while (isFileExists(name));
        this.addFileName(name, type);
        try {
            OutputStream output = new FileOutputStream(filePath + name);
            if (type == FileType.INDEX) {
                output.write(0);
            } else if (type == FileType.DATA) {
                output.write(1);
                //left, right
                writeLine(output, "");
                writeLine(output, "");
            }
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.debug("Can't new file!");
        }
        logger.debug("new file " + name);
        return name;
    }

    /**
     * Read a line from a file
     */
    private static String readLine(InputStream input) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while (true) {
            b = input.read();
            if (b == 0) {
                if (sb.length() == 0) {
                    return null;
                } else {
                    return sb.toString();
                }
            }
            if (b == -1)
                return null;
            sb.append((char) b);
        }

    }

    /**
     * Write a line into a file
     */
    private static void writeLine(OutputStream output, String s) throws IOException {
        if (s != null)
            output.write(s.getBytes(StandardCharsets.US_ASCII));
        output.write(0);
    }

    /**
     * create a node in memory from a disk file, TODO: find the node in cache first, or add the node to the cache
     */
    public Node loadFile(String name) {
        logger.debug("load file " + name);
        try {
            InputStream input = new FileInputStream(filePath + name);
            int t = input.read();
            if (t == 0) {
                IndexNode node = new IndexNode(this, name, FileType.INDEX, b.maxNumber);
                for (node.number = 0; ; node.number++) {
                    String s = readLine(input);
                    if (s == null)
                        break;
                    node.keys[node.number] = s;
                    node.children[node.number] = readLine(input);
                }
                node.size = input.available();
                input.close();
                return node;
            } else if (t == 1) {
                DataNode node = new DataNode(this, name, FileType.DATA, b.maxNumber);
                String s_lef = readLine(input);
                String s_rig = readLine(input);
                node.left = "".equals(s_lef) ? null : s_lef;
                node.right = "".equals(s_rig) ? null : s_rig;
                for (node.number = 0; ; node.number++) {
                    String s = readLine(input);
                    if (s == null)
                        break;
                    node.keys[node.number] = s;
                    node.values[node.number] = readLine(input);
                }
                node.size = input.available();
                input.close();
                return node;
            } else {
                return null;
            }


        } catch (IOException e) {
            return null;
        }
    }

    /**
     * update the node's disk file. TODO: update the cache
     */
    public boolean dumpFile(Node node) {
        logger.debug("dump file " + node.name);
//        if(logger.getLevel()== Level.DEBUG){
//            for (int i = 0; i < node.number; i++) {
//                BTree.logger.debug("dump file key: " + node.keys[i]);
//                if (node.type == FileType.DATA) {
//                    assert node instanceof DataNode;
//                    BTree.logger.debug("dump file values: " + ((DataNode) node).values[i]);
//                }
//            }
//        }
        try {
            OutputStream output = new FileOutputStream(filePath + node.name);
            if (node.type == FileType.INDEX) {
                IndexNode bn = (IndexNode) node;
                output.write(0);
                for (int i = 0; i < node.number; i++) {
                    writeLine(output, bn.keys[i]);
                    writeLine(output, bn.children[i]);
                }
            } else if (node.type == FileType.DATA) {
                DataNode ln = (DataNode) node;
                output.write(1);
                writeLine(output, ln.left);
                writeLine(output, ln.right);
                for (int i = 0; i < node.number; i++) {
                    writeLine(output, ln.keys[i]);
                    writeLine(output, ln.values[i]);
                }
            }
            output.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public FileOp() {
        for (int i = 0; i < FileOp.charNum; i++) {
            this.FileNameRootList.add(null);
        }
    }

    /**
     * Load the tree information from the file and rebuild an instance of BTree
     */
    public BTree loadTree(String treeName) {
        String rootPath = System.getProperty("user.dir");
        logger.debug(rootPath);
        File treeInfoPath = new File(rootPath + "/data/" + treeName + "/treeinfo");
        if (!treeInfoPath.exists()) {
            logger.error("this tree doesn't exist!");
            return null;
        }
        this.filePath = rootPath + "/data/" + treeName + "/";
        logger.debug(this.filePath);
        try {
            InputStream input = new FileInputStream(filePath + "treeinfo");
            String root = readLine(input);
            String maxNumerString = readLine(input);
            if (maxNumerString == null) {
                logger.error("the tree information is broken!");
                return null;
            }
            int maxNumber = Integer.parseInt(maxNumerString);
            this.b = new BTree(maxNumber, this, treeName, root);
            return this.b;
        } catch (IOException e) {
            logger.error("Can't load the tree information!");
            return null;
        }
    }

    /**
     * write the information of tree to disk. Automatically executed when the tree information is changed
     */
    public boolean dumpTree(BTree b) {
        String rootPath = System.getProperty("user.dir");
        File treeInfoPath = new File(rootPath + "/data/" + b.treeName);
        if (!treeInfoPath.exists()) {
            logger.error("this tree doesn't exist!");
            return false;
        }
        try {
            OutputStream ouput = new FileOutputStream(rootPath + "/data/" + b.treeName + "/treeinfo");
            writeLine(ouput, b.root);
            writeLine(ouput, Integer.toString(b.maxNumber));
            return true;
        } catch (IOException e) {
            logger.error("Can't load the tree information!");
            return false;
        }
    }

    /**
     * delete a directory
     */
    public static boolean deleteDirectory(String dir) {

        File dirFile = new File(dir);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            logger.error("this tree doesn't exist!");
            return false;
        }
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                boolean flag = files[i].delete();
                if (!flag) {
                    logger.error("Can't delete the tree due to IOException!");
                    return false;
                }
            } else {
                boolean flag = deleteDirectory(files[i].toString());
                if (!flag) {
                    logger.error("Can't delete the tree due to IOException!");
                    return false;
                }
            }
        }
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Delete all information of a BTree from disk
     */
    public static boolean deleteTree(String treeName) {
        String rootPath = System.getProperty("user.dir");
        logger.debug(rootPath);
        return deleteDirectory(rootPath + "/data/" + treeName);
    }

    public BTree newTree(int maxNumber, String treeName) {//make a new tree
        if (treeName == null)
            return null;
        String rootPath = System.getProperty("user.dir");
        logger.debug(rootPath);
        File treePath = new File(rootPath + "/data/" + treeName);
        logger.debug(treePath);
        if (treePath.exists() || !treePath.mkdirs()) {
            logger.error("tree folder exist or can't make the tree folder!");
            return null;
        }
        this.filePath = treePath + "/";
        logger.debug(this.filePath);
        this.b = new BTree(maxNumber, this, treeName, newFile(FileType.DATA));
        this.dumpTree(b);
        return this.b;
    }
}

/**
 * a node in the trie tree
 */
class TrieNode {
    ArrayList<TrieNode> children = new ArrayList<>();
    boolean occupied = false;
    FileType type = null;

    TrieNode() {
        for (int i = 0; i < FileOp.charNum; i++) {
            this.children.add(null);
        }
    }
}