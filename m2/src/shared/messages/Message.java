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

	private byte[] msgBytes;
	private static final char START_OF_TEXT = 0x02;
	private static final char END_OF_TEXT = 0x03;

	/**
	 * Constructs a Message object with a given array of bytes that
	 * forms the message.
	 * @param keyBytes
	 * @param valueBytes
	 * @param statusByte
	 */
	public Message(byte[] keyBytes, byte[] valueBytes, byte statusByte) {
		this.key = new String(keyBytes).trim();
		try {
			int index = statusByte;
			this.status = 0 <= index && index < StatusType.values().length ?
					StatusType.values()[index] : StatusType.FAILED;
		} catch(NumberFormatException e) {
			logger.error("The byte format supplied as statusBytes could not be parsed as an integer");
			this.status = StatusType.FAILED;
		}
		if (valueBytes == null) {
			this.msgBytes = this.toByteArray(key, status);
		} else {
			this.value = new String(valueBytes).trim();
			this.msgBytes = this.toByteArray(key, status, value);
		}
	}

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
		if (value == null) {
			this.msgBytes = this.toByteArray(key, status);
		} else {
			this.msgBytes = this.toByteArray(key, status, value);
		}
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
	}

	/**
	 * Helper function for toByteArray() to wrap the key/value text with control characters
	 * based on the ASCII protocol.
	 * @param text
	 * @return byte[]
	 */
	private byte[] wrapTextWithCtrChars(String text) {
		byte[] textBytes = text.getBytes();
		byte[] ctrBytes = new byte[]{START_OF_TEXT, END_OF_TEXT};

		byte[] tmp = new byte[textBytes.length + ctrBytes.length];
		System.arraycopy(ctrBytes, 0, tmp, 0, 1);
		System.arraycopy(textBytes, 0, tmp, 1, textBytes.length);
		System.arraycopy(ctrBytes, 1, tmp, tmp.length - 1, 1);
		return tmp;
	}

	/**
	 * Convert properties to a byte array used for socket communication.
	 * @param key
	 * @param status
	 * @param value
	 * @return byte[]
	 */
	private byte[] toByteArray(String key, StatusType status, String value) {
		byte statusByte = (byte) status.ordinal();
		byte[] wrappedKey = wrapTextWithCtrChars(key);
		byte[] wrappedValue = wrapTextWithCtrChars(value);
		byte[] tmp = new byte[wrappedKey.length + wrappedValue.length + 1];

		tmp[0] = statusByte;
		System.arraycopy(wrappedKey, 0, tmp, 1, wrappedKey.length);
		System.arraycopy(wrappedValue, 0, tmp, 1 + wrappedKey.length, wrappedValue.length);

		return tmp;
	}

	/**
	 * Convert properties to a byte array used for socket communication.
	 * @param key
	 * @param status
	 * @return byte[]
	 */
	private byte[] toByteArray(String key, StatusType status) {
		byte statusByte = (byte) status.ordinal();
		byte[] wrappedKey = wrapTextWithCtrChars(key);
		byte[] tmp = new byte[wrappedKey.length + 1];

		tmp[0] = statusByte;
		System.arraycopy(wrappedKey, 0, tmp, 1, wrappedKey.length);
		return tmp;
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
		outputString.append(" Bytes: " + new String(msgBytes));

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
