package shared.messages;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class Message implements Serializable, KVMessage {

	private static Logger logger = Logger.getRootLogger();
	private static final long serialVersionUID = 5549512212003782618L;
	private String key;
	private String value;
	private StatusType status;
	private HashMap<String, String[]> serverMetadata;

	/**
     * Constructs a Message object with a given String that
     * forms the message.
     *
     * @param key the String that forms the key
	 * @param value the String that forms the value
	 * @param status the StatusType of the message
     */
	public Message(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}

	/**
	 * Helper method to make it easier to print out messages for server logs
	 * @return String
	 */
	public String getMessageString() {
		StringBuilder outputString = new StringBuilder("");
		outputString.append("Status: " + status);
		outputString.append(" Key: " + key);
		if (value != null){
			outputString.append(" Value: " + value);
		}

		return outputString.toString();
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public StatusType getStatus() {
		return this.status;
	}

	@Override
	public HashMap<String, String[]> getMetadata() {
		return this.serverMetadata;
	}
}
