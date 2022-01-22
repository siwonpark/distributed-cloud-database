package app_kvClient;

import shared.messages.Message;

public interface ClientSocketListener {

    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

    public void handleNewMessage(Message msg);

    public void handleStatus(SocketStatus status);
}
