import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Node;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public abstract class Client extends RIONode {

	public Client() {
		super();
	}

	public void onCommand(String command) {
		// parse command, server, filename
		StringTokenizer tokens = new StringTokenizer(command, " ");
		try {
			String command = tokens.nextToken();
			String server = tokens.nextToken();
			String filename = tokens.nextToken();
		} catch (NoSuchElementException e) {
			// bad command
		}

		// parse contents for put and append
		String contents;
		if (command.equals("put") || command.equals("append")) {
			int parsedLength = command.length()	+ server.length() + filename.length() + 3;
			if (parsedLength >= command.length) {
				//no contents - error?
			}
			else  {
				contents = command.substring(parsedLength);
			}
		}
		
		// send message
		String payload = filename;
		if (contents != null) {
			payload += "\t" + contents;
		}
		int protocol = Protocol.stringToProtocol(command);
		if (protocol == -1) {
			//invalid command
		} else {
			RIOSend(server, protocol, payload);
		}
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if (protocol == Protocol.DATA) {
			RIOLayer.RIODataReceive(from, msg);
		} else if (protocol == Protocol.ACK) {
			RIOLayer.RIOAckReceive(from, msg);
		}
	}

	/**
	 * Send a message using the reliable, in-order delivery layer
	 * 
	 * @param destAddr
	 *            The address to send to
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param payload
	 *            The payload of the message
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		RIOLayer.RIOSend(destAddr, protocol, payload);
	}

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param msg
	 *            The message that was received
	 */
	public abstract void onRIOReceive(Integer from, int protocol, byte[] msg);

	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
