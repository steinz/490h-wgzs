package edu.washington.cs.cse490h.dfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery in the absence of faults. This layer does not provide much more than
 * the above.
 * 
 * At a minimum, the student should extend/modify this layer to provide
 * reliable, in-order message delivery, even in the presence of node failures.
 */
class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 5;

	private HashMap<Integer, InChannel> inConnections;
	private HashMap<Integer, OutChannel> outConnections;
	protected RIONode n;

	/**
	 * Constructor.
	 * 
	 * @param destAddr
	 *            The address of the destination host
	 * @param msg
	 *            The message that was sent
	 * @param timeSent
	 *            The time that the ping was sent
	 */
	public ReliableInOrderMsgLayer(RIONode n) {
		inConnections = new HashMap<Integer, InChannel>();
		outConnections = new HashMap<Integer, OutChannel>();
		this.n = n;
	}

	/**
	 * Receive a data packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIODataReceive(int from, byte[] msg) {
		try {
			RIOPacket riopkt = RIOPacket.unpack(msg);

			if (riopkt.getType() == MessageType.Handshake) {
				riopkt = mapUUID(from, riopkt);
			}
			// ack the packet immediately, then deal with handshakes, if not a
			// handshake or a packet from an old session, pass it along

			// at-most-once semantics

			byte[] seqNumByteArray = Utility.stringToByteArray(""
					+ riopkt.getSeqNum());
			n.send(from, MessageType.Ack, seqNumByteArray);

			// check if UUID is what we think it is.
			if (!(riopkt.getUUID().equals(n.getID()))) {
				// if it's not, we should initiate a handshake immediately and
				// makea new channel to clear our cache of bad packets from an
				// old session
				RIOSend(from, MessageType.Handshake,
						Utility.stringToByteArray(n.getID().toString()));

				// Don't send failure to client - they've crashed since they
				// sent
				// this packet

				inConnections.put(from, new InChannel());
				if (outConnections.containsKey(from))
					outConnections.get(from).reset();
			}

			InChannel in = inConnections.get(from);
			if (in == null) {
				in = new InChannel();
				inConnections.put(from, in);
			}

			if (riopkt.getUUID().equals(n.getID())
					&& riopkt.getType() != MessageType.Handshake) {

				LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);

				for (RIOPacket p : toBeDelivered) {
					// deliver in-order the next sequence of packets
					n.onRIOReceive(from, p.getType(), p.getPayload());
				}
			}
		} catch (PacketPackException e) {
			Logger.error(n, e);
		} catch (IOException e) {
			Logger.error(n, e);
		}
	}

	private RIOPacket mapUUID(int from, RIOPacket riopkt) {
		// handshake, so store this UUID
		UUID receivedID = UUID.fromString(Utility.byteArrayToString(riopkt
				.getPayload()));
		n.addrToSessionIDMap.put(from, receivedID);

		StringBuilder sb = n.appendNodeAddress();
		sb.append("received HANDSHAKE, mapping ");
		sb.append(from);
		sb.append(" to ");
		sb.append(receivedID);
		Logger.info(n, sb.toString());

		/*
		 * a handshake also means that whoever sent us this handshake probably
		 * dropped all of our packets. so, whatever we had in queue to be resent
		 * should be dropped.
		 */
		if (outConnections.containsKey(from))
			outConnections.get(from).reset();
		return riopkt;
	}

	/**
	 * Receive an acknowledgment packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIOAckReceive(int from, byte[] msg) {
		int seqNum = Integer.parseInt(Utility.byteArrayToString(msg));
		if (outConnections.containsKey(from))
			outConnections.get(from).gotACK(seqNum);
	}

	/**
	 * Send a packet using this reliable, in-order messaging layer. Note that
	 * this method does not include a reliable, in-order broadcast mechanism.
	 * 
	 * @param destAddr
	 *            The address of the destination for this packet
	 * @param protocol
	 *            The protocol identifier for the packet
	 * @param payload
	 *            The payload to be sent
	 */
	public void RIOSend(int destAddr, MessageType type, byte[] payload) {
		OutChannel out = outConnections.get(destAddr);
		if (out == null) {
			out = new OutChannel(this, destAddr);
			outConnections.put(destAddr, out);
		}

		// Decide what the UUID of the destination address is
		UUID ID = n.addrToSessionIDMap.get(destAddr);
		if (ID == null)
			ID = n.getID();

		out.sendRIOPacket(n, type, payload, ID);
	}

	/**
	 * Callback for timeouts while waiting for an ACK.
	 * 
	 * This method is here and not in OutChannel because OutChannel is not a
	 * public class.
	 * 
	 * @param destAddr
	 *            The receiving node of the unACKed packet
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(Integer destAddr, Integer seqNum) {
		outConnections.get(destAddr).onTimeout(n, seqNum);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Integer i : inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}

		return sb.toString();
	}

	public void printSeqStateDebug() {
		StringBuilder sb = n.appendNodeAddress();
		sb.append("sequence state");

		Iterator<Entry<Integer, InChannel>> inIter = inConnections.entrySet()
				.iterator();
		while (inIter.hasNext()) {
			Entry<Integer, InChannel> entry = inIter.next();
			sb.append("\nIn connection to ");
			sb.append(entry.getKey());
			sb.append(" last seqNumDelivered = ");
			sb.append(entry.getValue().seqNumDebug());
		}
		Iterator<Entry<Integer, OutChannel>> outIter = outConnections
				.entrySet().iterator();
		while (outIter.hasNext()) {
			Entry<Integer, OutChannel> entry = outIter.next();
			sb.append("\nOut connection to ");
			sb.append(entry.getKey());
			sb.append(" lastSeqNumSent = ");
			sb.append(entry.getValue().seqNumDebug());
		}

		Logger.info(n, sb.toString());
	}
}
