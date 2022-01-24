package app_kvServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Random;

enum FileType {
    INDEX, DATA
}


public class FileOp {
    final static int fileNameLength = 20;
    static int keyLength;
    static int valueLength;
    final Random random = new Random();
    final static int charNum = 26 + 26 + 10;
    ArrayList<TrieNode> FileNameRootList = new ArrayList<TrieNode>();

    public static int char2int(char c) {
        if ((int) c >= 97) {
            return (int) c - 61;
        } else if ((int) c >= 65) {
            return (int) c - 55;
        } else {
            return (int) c - 48;
        }
    }

    public static char int2char(int n) {
        if (n < 10) {
            return (char) (n + 48);
        } else if (n < 36) {
            return (char) (n + 55);
        } else {
            return (char) (n + 61);
        }
    }

    public boolean isFileExists(String name, FileType type) {
        ArrayList<TrieNode> lis = this.FileNameRootList;
        for (int i = 0; ; i++) {
            int tmp = char2int(name.charAt(i));
            if (lis.get(tmp) == null) {
                return false;
            } else {
                TrieNode son = lis.get(tmp);
                if (i == name.length() - 1) {
                    return son.type == type && lis.get(tmp).occupied;
                } else {
                    lis = lis.get(tmp).children;
                }
            }
        }
    }

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

    public String genFileName() {
        //TODO make it collision free(Trie tree or hash)
        StringBuilder buffer = new StringBuilder(fileNameLength);
        for (int i = 0; i < fileNameLength; i++) {
            buffer.append(int2char(random.nextInt(charNum)));
        }
        return buffer.toString();
    }

    public String newFile(FileType type) {
        if (type == FileType.INDEX) {
            String name = genFileName();
            this.addFileName(name, type);

            return name;
        } else if (type == FileType.DATA) {
            String name = genFileName();
            this.addFileName(name, type);
            return name;
        } else {
            return null;
        }
    }

    public Node loadFile(String name, FileType type) {
        InputStream input;
        try {
            input = new FileInputStream("./data/" + name);
            int t = input.read();
            Node node;
            if (t == 0 && type == FileType.INDEX) {
                node = new BtreeNode();
            } else if (t == 1 && type == FileType.DATA) {
                node = new LeafNode();
            } else {
                return null;
            }
            node.size = input.available();
            int b;
            for (node.number = 0; ; node.number++) {
                StringBuilder sb = new StringBuilder();
                while (true) {
                    b = input.read();
                    if (b == -1)
                        break;
                    if (b == 0)
                        break;
                    sb.append(b);
                }
                node.keys[node.number] = sb.toString();
                while (true) {
                    b = input.read();
                    if (b == -1)
                        break;
                    if (b == 0)
                        break;
                    sb.append(b);
                }
                node.keys[node.number] = sb.toString();
                if (b == -1) {
                    break;
                }
            }
            node.number += 1;
            return node;
        } catch (IOException e) {
            return null;
        }
    }

    public static boolean dumpFile(String name, FileType type, Node node) {
        if (type == FileType.INDEX) {

            return true;
        } else if (type == FileType.DATA) {

            return true;
        } else {

            return false;
        }
    }

    public FileOp(int keyLength, int valueLength) {
        FileOp.keyLength = keyLength;
        FileOp.valueLength = valueLength;
        for (int i = 0; i < FileOp.charNum; i++) {
            this.FileNameRootList.add(null);
        }
    }
}


class TrieNode {
    ArrayList<TrieNode> children = new ArrayList<TrieNode>();
    boolean occupied = false;
    FileType type = null;

    TrieNode() {
        for (int i = 0; i < FileOp.charNum; i++) {
            this.children.add(null);
        }
    }
}