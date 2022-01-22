package client;

import shared.messages.Message;

import java.io.IOException;

public interface ICommModule {

    public void connect() throws IOException;

    public void disconnect();

    public Message receiveMessage();

    public void sendMessage(Message msg);
}
