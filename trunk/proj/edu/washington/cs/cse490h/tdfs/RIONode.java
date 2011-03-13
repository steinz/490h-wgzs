package edu.washington.cs.cse490h.tdfs;

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
	/**
	 * Static empty payload for use by messages that don't have payloads
	 */
	protected static final byte[] emptyPayload = new byte[0];

	/**
	 * Packets sent by all RIONodes in simulator.
	 * 
	 * Identical to packetsSent in emulator.
	 */
	protected static int totalPacketsSent = 0;
	
	/**
	 * Packets sent by this instance.
	 */
	protected int packetsSent;
	
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
		// Turn the protocol into a message type
		MessageType mt = MessageType.ordinalToMessageType(protocol);
		
		if (mt == MessageType.Ack) {
			RIOLayer.RIOAckReceive(from, msg);
		} else {
			RIOLayer.RIODataReceive(from, msg);
		}
	}

	/**
	 * Use send(int, MessageType, byte[]) externally
	 */
	@Override
	@Deprecated
	public void send(int destAddr, int protocol, byte[] payload) {
		this.packetsSent++;
		RIONode.totalPacketsSent++;
		super.send(destAddr, protocol, payload);
	}

	/**
	 * Send a message
	 */
	public void send(int destAddr, MessageType type, byte[] payload) {
		printVerbose("sending " + type.name() + " to " + destAddr
				+ " with payload: " + packetBytesToString(payload));
		send(destAddr, type.ordinal(), payload);
	}

	/**
	 * Send a message with no payload
	 */
	public void send(int destAddr, MessageType type) {
		send(destAddr, type, emptyPayload);
	}

	/**
	 * Send a message using the rio layer with no payload
	 */
	public void RIOSend(int destAddr, MessageType type) {
		RIOSend(destAddr, type, emptyPayload);
	}

	/**
	 * Send a message using the rio layer
	 */
	public void RIOSend(int destAddr, MessageType type, byte[] payload) {
		printVerbose("rio-sending " + type.name() + " to " + destAddr
				+ " with payload: " + packetBytesToString(payload));
		RIOLayer.RIOSend(destAddr, type, payload);
	}

	/**
	 * Use broadcast(MessageType, byte[]) externally
	 */
	@Deprecated
	public void broadcast(int protocol, byte[] payload) {
		super.broadcast(protocol, payload);
	}

	/**
	 * Broadcast a message
	 */
	public void broadcast(MessageType type, byte[] payload) {
		printVerbose("broadcasting " + type.name() + " with payload: "
				+ packetBytesToString(payload));
		broadcast(type.ordinal(), payload);
	}

	/**
	 * Broadcast a message with no payload
	 */
	public void broadcast(MessageType type) {
		broadcast(type, emptyPayload);
	}

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 */
	public abstract void onRIOReceive(Integer from, MessageType protocol, byte[] msg);

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
}
