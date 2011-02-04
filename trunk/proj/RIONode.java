/**
 * CSE 490h
 * @author wayger, steinz
 */

import edu.washington.cs.cse490h.lib.Node;
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

	/**
	 * Send a message using the reliable, in-order delivery layer
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		RIOLayer.RIOSend(destAddr, protocol, payload);
	}

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 */
	public abstract void onRIOReceive(Integer from, int protocol, byte[] msg);

	@Override
	public String toString() {
		return "RIONode|SessionID:" + ID + "|RIOLayer:" + RIOLayer.toString();
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
		// TODO: Unpack ACKs so we can log them
		try {
			return RIOPacket.unpack(bytes).toString();
		} catch (Exception e) {
			// ACKs don't unpack correctly, ignore them
			return "ACK";
		}
	}
}
