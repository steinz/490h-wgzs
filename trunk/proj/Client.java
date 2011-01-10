import java.util.StringTokenizer;
import java.util.NoSuchElementException;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public class Client extends RIONode {

	public Client() {
		super();
	}

	public void start() {
	}
	
	public void onCommand(String command) {
		// parse command, server, filename
		StringTokenizer tokens = new StringTokenizer(command, " ");
		String cmd = "", server = "", filename = "";
		try {
			cmd = tokens.nextToken();
			server = tokens.nextToken();
			filename = tokens.nextToken();
		} catch (NumberFormatException e) {
			// bad server address
		} catch (NoSuchElementException e) {
			// bad command
		}

		// parse contents for put and append
		String contents = "";
		if (command.equals("put") || command.equals("append")) {
			int parsedLength = command.length()	+ server.length() + filename.length() + 3;
			if (parsedLength >= command.length()) {
				//no contents - error?
			}
			else  {
				contents = command.substring(parsedLength);
			}
		}
		
		// send message
		String payload = filename;
		if (command.equals("put") || command.equals("append")) {
			payload += "\t" + contents;
		}
		int protocol = Protocol.stringToProtocol(cmd);
		if (protocol == -1) {
			//invalid command
		} else {
			RIOSend(Integer.parseInt(server), protocol, Utility.stringToByteArray(payload));
		}
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		
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
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		
	}

	@Override
	public String toString() {
		return "";
	}
}
