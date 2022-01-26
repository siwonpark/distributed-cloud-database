package client;

import app_kvClient.ClientSocketListener;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashSet;

public class CommModule implements ICommModule {
    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private String address;
    private int port;
    private InputStream input;
    private OutputStream output;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int SOCKET_READ_TIMEOUT = 5000;

    public CommModule(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws IOException {
        clientSocket = new Socket(this.address, this.port);
        clientSocket.setSoTimeout(SOCKET_READ_TIMEOUT);
        input = clientSocket.getInputStream();
        output = clientSocket.getOutputStream();
    }

    @Override
    public void disconnect() throws IOException {
        logger.info("Trying to disconnect from the socket");
        if (clientSocket != null){
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
        }
        logger.info("Client socket disconnected");
    };


    @Override
    public Message receiveMessage() throws IOException {
        byte[] statusByte = new byte[1];

        /* read the status byte, always the first byte in the message */
        byte read = (byte) input.read(statusByte);
        if (read != 1) {
            logger.error("Did not receive correct status byte from server");
        }

        byte[] keyBytes = readText();
        byte[] valueBytes = null;

        int numRemainingBytes = input.available();

        if (numRemainingBytes > 0) {
            valueBytes = readText();
        }
        
        /* build final String */
        Message msg = new Message(keyBytes, valueBytes, statusByte);
        logger.debug("Received message: " + msg.getMessageString());
        return msg;
    };

    public byte[] readText() throws IOException {
        int index = 0;
        byte[] textBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        boolean reading = true;

        /* load start of message */
        byte read = (byte) input.read();
        if (read != 2) {
            return null;
        }

        while (reading && read != 3 /* 3 is the end of text control character */) {
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (textBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[textBytes.length + BUFFER_SIZE];
                    System.arraycopy(textBytes, 0, tmp, 0, textBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, textBytes.length, BUFFER_SIZE);
                }

                textBytes = tmp;

                /* reset buffer after msgBytes is populated */
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if (textBytes != null && textBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        /* commit what's left in the buffer */
        if (textBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[textBytes.length + index];
            System.arraycopy(textBytes, 0, tmp, 0, textBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, textBytes.length, index);
        }

        textBytes = tmp;

        return textBytes;
    };

    @Override
    public void sendMessage(Message message) throws IOException{
        byte[] msgBytes = message.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.debug("Send message: " + message.getMessageString());
    };
}
