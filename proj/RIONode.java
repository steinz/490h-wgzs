/**
 * CSE 490h
 * @author wayger, steinz
 */

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.Utility;

import java.util.HashMap;
import java.util.UUID;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public abstract class RIONode extends Node {
	protected ReliableInOrderMsgLayer RIOLayer;

	/**
	 * This node's session UUID
	 */
	protected UUID ID;

	/**
	 * Mapping from other node addresses to session UUIDs
	 */
	protected HashMap<Integer, UUID> addrToSessionIDMap;

	/**
	 * Set session UUID
	 */
	public void setID(UUID iD) {
		ID = iD;
	}

	/**
	 * Get session UUID
	 */
	public UUID getID() {
		return ID;
	}

	public RIONode() {
		setID(UUID.randomUUID());
		addrToSessionIDMap = new HashMap<Integer, UUID>();
		RIOLayer = new ReliableInOrderMsgLayer(this);
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if (protocol == Protocol.ACK) {
			RIOLayer.RIOAckReceive(from, msg);
		} else {
			RIOLayer.RIODataReceive(from, msg);
		}
	}

	@Override
	public void send(int destAddr, int protocol, byte[] payload) {
		printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ destAddr + " with payload: "
				+ packetBytesToString(payload));
		super.send(destAddr, protocol, payload);
	}

	/**
	 * Send a message using the reliable, in-order delivery layer
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		printVerbose("rio-sending " + Protocol.protocolToString(protocol)
				+ " to " + destAddr + " with payload: "
				+ packetBytesToString(payload));
		RIOLayer.RIOSend(destAddr, protocol, payload);
	}

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 */
	public abstract void onRIOReceive(Integer from, int protocol, byte[] msg);

	/**
	 * Prepend the node address and then call Logger.verbose.
	 */
	public void printVerbose(String msg, boolean frame) {
		StringBuilder sb = appendNodeAddress();
		sb.append(msg);
		Logger.verbose(this, sb.toString(), frame);
	}

	/**
	 * Stub for printVerbose that doesn't print a frame.
	 */
	public void printVerbose(String msg) {
		printVerbose(msg, false);
	}

	/**
	 * Prepend the node address and then call Logger.info
	 */
	public void printInfo(String msg) {
		StringBuilder sb = appendNodeAddress();
		sb.append(msg);
		Logger.info(this, sb.toString());
	}

	/**
	 * Prints node name and then exception via logger
	 */
	public void printError(Throwable e) {
		printError("caught exception (see below)");
		Logger.error(this, e);
	}

	/**
	 * Print node name, error, then msg
	 */
	public void printError(String msg) {
		StringBuilder sb = appendNodeAddress();
		sb.append("Error: ");
		sb.append(msg);
		Logger.error(this, sb.toString());
	}

	/**
	 * Returns a new StringBuilder with this node's address prepended for
	 * logging.
	 */
	public StringBuilder appendNodeAddress() {
		StringBuilder sb = new StringBuilder();
		sb.append("Node ");
		sb.append(addr);
		sb.append(": ");
		return sb;
	}

	@Override
	public String packetBytesToString(byte[] bytes) {
		try {
			return RIOPacket.unpack(bytes).toString();
		} catch (Exception e) {
			// Just turn the bytes into a String for headerless packets
			return Utility.byteArrayToString(bytes);
		}
	}

	@Override
	public String toString() {
		return "RIONode|SessionID:" + ID + "|RIOLayer:" + RIOLayer.toString();
	}
	
	public abstract void killNode(int destAddr);
}
