package persistence;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Random;

enum FileType {
    INDEX, DATA
}

public class FileOp {
    static Logger logger = Logger.getRootLogger();
    final static int fileNameLength = 20;
    final static Random random = new Random();
    final static int charNum = 26 + 26 + 10;

    private FileOp() {
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
     * Generate a random file name
     */
    public static String genFileName() {
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
    public static String newFile(FileType type) {
        String name;
        name = genFileName();
        try {
            OutputStream output = new FileOutputStream(DBConfig.getInstance().filePath + name);
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
            logger.error("Can't new file!");
        }
        logger.trace("new file " + name);

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
     * create a node in memory from a disk file, find the node in cache first
     */
    public static Node loadFile(String name) {
        logger.trace("load file " + name);
        if (DataBase.getInstance().cache != null && DataBase.getInstance().cache.containsKey(name)) {
            return DataBase.getInstance().cache.get(name);
        } else {
            try {
                InputStream input = new FileInputStream(DBConfig.getInstance().filePath + name);
                int t = input.read();
                if (t == 0) {
                    IndexNode node = new IndexNode(name, FileType.INDEX, DBConfig.getInstance().maxNumber);
                    for (node.number = 0; ; node.number++) {
                        String s = readLine(input);
                        if (s == null)
                            break;
                        node.keys[node.number] = s;
                        node.children[node.number] = readLine(input);
                    }
                    node.size = input.available();
                    input.close();

                    DataBase.getInstance().cache.put(node.name, node);

                    return node;
                } else {
                    DataNode node = new DataNode(name, FileType.DATA, DBConfig.getInstance().maxNumber);
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

                    DataBase.getInstance().cache.put(node.name, node);

                    return node;
                }


            } catch (IOException e) {
                return null;
            }
        }

    }

    public static boolean dumpFile(Node node) {
        return dumpFile(node, true);
    }

    /**
     * update the node's disk file.
     */
    public static boolean dumpFile(Node node, boolean writeDisk) {
        logger.trace("dump file " + node.name);
        DataBase.getInstance().cache.put(node.name, node);
        if (writeDisk) {
            try {
                OutputStream output = new FileOutputStream(DBConfig.getInstance().filePath + node.name);
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
        }else{
            return true;
        }
    }


    /**
     * Load the tree information from the file and rebuild an instance of BTree
     */
    public static BTree loadTree() {
        logger.debug(DBConfig.getInstance().filePath);
        try {
            InputStream input = new FileInputStream(DBConfig.getInstance().filePath + "treeinfo");
            String root = readLine(input);
            String maxNumerString = readLine(input);
            if (maxNumerString == null) {
                logger.error("the tree information is broken!");
                return null;
            }
            int maxNumber = Integer.parseInt(maxNumerString);
            return new BTree(root);
        } catch (IOException e) {
            logger.error("Can't load the tree information!");
            return null;
        }
    }

    /**
     * write the information of tree to disk. Automatically executed when the tree information is changed
     */
    public static boolean dumpTree(BTree b) {
        try {
            OutputStream ouput = new FileOutputStream(DBConfig.getInstance().filePath + "treeinfo");
            writeLine(ouput, b.root);
            writeLine(ouput, Integer.toString(DBConfig.getInstance().maxNumber));
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
            logger.debug("this tree doesn't exist!");
            return false;
        }
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                boolean flag = files[i].delete();
                if (!flag) {
                    logger.debug("Can't delete the tree due to IOException!");
                    return false;
                }
            } else {
                boolean flag = deleteDirectory(files[i].toString());
                if (!flag) {
                    logger.debug("Can't delete the tree due to IOException!");
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
    public static boolean deleteTree() {
        return deleteDirectory(DBConfig.getInstance().filePath);
    }

    public static BTree newTree() {//make a new tree
        File filePath = new File(DBConfig.getInstance().filePath);
        if (!filePath.exists()) {
            filePath.mkdirs();
        }
        BTree newTree = new BTree(FileOp.newFile(FileType.DATA));
        FileOp.dumpTree(newTree);
        return newTree;
    }
}