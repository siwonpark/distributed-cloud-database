package client;

import app_kvClient.ClientSocketListener;
import org.apache.log4j.Logger;
import shared.messages.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;

public class CommModule implements ICommModule {
    private Logger logger =Logger.getRootLogger();
    private Socket clientSocket;
    private String address;
    private int port;
    private InputStream input;
    private OutputStream output;

    public CommModule(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws IOException {
        logger.info("HELLO WORLD");
        clientSocket = new Socket(this.address, this.port);
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
        return null;
    };
    @Override
    public void sendMessage(Message message) throws IOException{

    };

}
