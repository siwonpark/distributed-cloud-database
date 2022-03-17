package shared.communication;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class CommModule implements ICommModule {
    private Logger logger = Logger.getRootLogger();
    private Socket clientSocket;
    private String address;
    private int port;
    private ObjectInputStream input;
    private ObjectOutputStream output;

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
        input = new ObjectInputStream(clientSocket.getInputStream());
        output = new ObjectOutputStream(clientSocket.getOutputStream());
    }

    @Override
    public void disconnect() throws IOException {
        logger.info(String.format("Trying to disconnect from the socket at port %s", port));
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
        Message msg;
        try {
            msg = (Message) input.readObject();
        }
        catch (ClassNotFoundException e) {
            msg = new Message("Message was not able to be read properly", null,
                    KVMessage.StatusType.FAILED);
        }
        logger.debug("Receive message: " + msg.getMessageString());
        return msg;
    };

    @Override
    public void sendMessage(Message message) throws IOException{
        output.writeObject(message);
        output.flush();
        logger.debug("Send message: " + message.getMessageString());
    };

    public int getPort(){
        return this.port;
    }

    public String getHost(){
        return this.address;
    }
}
