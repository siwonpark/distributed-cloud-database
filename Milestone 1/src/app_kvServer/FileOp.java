package app_kvServer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

enum FileType {
    INDEX, DATA
}


public class FileOp {
    final static int fileNameLength = 20;
    final Random random = new Random();
    final static int charNum = 26 + 26 + 10;
    ArrayList<TrieNode> FileNameRootList = new ArrayList<>();
    final String filePath = "./data/";


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
        } catch (IOException e) {
            System.out.println("Can't new file!");
        }
        return name;
    }

    private static String readLine(InputStream input) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while (true) {
            b = input.read();
            if (b == 0)
                return sb.toString();
            if (b == -1)
                return null;
            sb.append(b);
        }
    }

    private static void writeLine(OutputStream output, String s) throws IOException {
        output.write(s.getBytes(StandardCharsets.US_ASCII));
        output.write(0);
    }

    public Node loadFile(String name) {
        try {
            InputStream input = new FileInputStream(filePath + name);
            int t = input.read();
            if (t == 0) {
                IndexNode node = new IndexNode(this, name, FileType.INDEX);
                for (node.number = 0; ; node.number++) {
                    String s = readLine(input);
                    if (s == null)
                        break;
                    node.keys[node.number] = readLine(input);
                    node.children[node.number] = readLine(input);
                }
                node.size = input.available();
                return node;
            } else if (t == 1) {
                DataNode node = new DataNode(this, name, FileType.DATA);
                node.left = readLine(input);
                node.right = readLine(input);
                for (node.number = 0; ; node.number++) {
                    String s = readLine(input);
                    if (s == null)
                        break;
                    node.keys[node.number] = readLine(input);
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

    public boolean dumpFile(Node node) {
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
}


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