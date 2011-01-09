package proj;

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
public class Server extends RIONode {
	private ReliableInOrderMsgLayer RIOLayer;
	
	public static int NUM_NODES = 10;
	
	public Server() {
		super();
	}
	
	
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if(protocol == Protocol.DATA  || protocol == Protocol.CREATE ||
			protocol == Protocol.DELETE || protocol == Protocol.GET ||
			protocol = Protocol.PUT || protocol == Protocol.APPEND) {
			RIOLayer.RIODataReceive(from, msg);
		}else if(protocol == Protocol.ACK) {
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
	public void onRIOReceive(Integer from, int protocol, byte[] msg)
	{
		if (protocol == Protocol.CREATE) {
			// Create file
		}else if (protocol == Protocol.DELETE){
			// Delete file
		}else if (protocol == Protocol.GET){
			// Get file
		}else if (protocol == Protocol.PUT){
			// Put file
		}else if (protocol == Protocol.APPEND){
			// Append file
		}
		
		
	}
	
	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
