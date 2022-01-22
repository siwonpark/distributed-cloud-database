package client;

import app_kvClient.ClientSocketListener;
import shared.messages.Message;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;

public class CommModule implements ICommModule {
    private Socket clientSocket;
    private HashSet listeners;
    private String address;
    private int port;

    public CommModule(String address, int port) throws UnknownHostException, IOException {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws IOException {
        // Check if there already is a connection if not do below
        clientSocket = new Socket(this.address, this.port);

    }

    @Override
    public void disconnect(){

    };

    @Override
    public Message receiveMessage(){

    };
    @Override
    public void sendMessage(Message message){

    };

}
