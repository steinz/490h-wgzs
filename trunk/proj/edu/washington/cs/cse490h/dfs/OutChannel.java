package edu.washington.cs.cse490h.dfs;
/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private HashMap<RIOPacket, Integer> resendCounts;
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private int destAddr;

	private int MAX_RESENDS = 5;

	OutChannel(ReliableInOrderMsgLayer parent, int destAddr) {
		lastSeqNumSent = -1;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		resendCounts = new HashMap<RIOPacket, Integer>();
		this.parent = parent;
		this.destAddr = destAddr;
	}

	/**
	 * Send a new RIOPacket out on this channel.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param protocol
	 *            The protocol identifier of this packet
	 * @param payload
	 *            The payload to be sent
	 * @param ID
	 *            What the node thinks the ID of the recipient node is currently
	 */
	protected void sendRIOPacket(RIONode n, int protocol, byte[] payload,
			UUID ID) {
		try {
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent,
					new String[] { "java.lang.Integer", "java.lang.Integer" });
			onTimeoutMethod.setAccessible(true); // HACK
			RIOPacket newPkt = new RIOPacket(protocol, ++lastSeqNumSent,
					payload, ID);
			unACKedPackets.put(lastSeqNumSent, newPkt);

			resendCounts.put(newPkt, 0);

			n.send(destAddr, protocol, newPkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[] {
					destAddr, lastSeqNumSent }),
					ReliableInOrderMsgLayer.TIMEOUT);
		} catch (PacketPackException e) {
			Logger.error(parent.n, e);
		} catch (Exception e) {
			Logger.error(parent.n, e);
		}
	}

	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(RIONode n, Integer seqNum) {
		
		if (!unACKedPackets.containsKey(seqNum)) {
			return;
		}

		RIOPacket packet = unACKedPackets.get(seqNum);

		if (resendCounts.get(packet) >= MAX_RESENDS) {
			resendCounts.remove(packet);
			unACKedPackets.remove(seqNum);

			StringBuilder sb = new StringBuilder();
			sb.append("Node: ");
			sb.append(n.addr);
			sb.append(" Error: TIMEOUT of " + packet.toString());
			parent.n.printError("TIMEOUT on packet: " + packet.toString());
			
			n.killNode(destAddr);

		} else if (unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
			resendCounts.put(packet, resendCounts.get(packet) + 1);
		}
	}

	/**
	 * Resets both the sequence number and the timeout data structures
	 */
	public void reset() {
		unACKedPackets.clear();
		resendCounts.clear();
		lastSeqNumSent = -1;
	}

	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is
	 * still in unACKedPackets.
	 * 
	 * @param seqNum
	 *            The sequence number that was just ACKed
	 */
	protected void gotACK(int seqNum) {
		RIOPacket packet = unACKedPackets.get(seqNum);
		resendCounts.remove(packet);
		unACKedPackets.remove(seqNum);
	}

	/**
	 * Resend an unACKed packet.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	private void resendRIOPacket(RIONode n, int seqNum) {
		try {
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent,
					new String[] { "java.lang.Integer", "java.lang.Integer" });
			onTimeoutMethod.setAccessible(true); // HACK
			RIOPacket riopkt = unACKedPackets.get(seqNum);

			StringBuilder sb = n.appendNodeAddress();
			sb.append("resending packet ");
			sb.append(riopkt.getSeqNum());
			sb.append(" protocol: ");
			sb.append(Protocol.protocolToString(riopkt.getProtocol()));
			Logger.verbose(parent.n, sb.toString());

			n.send(destAddr, riopkt.getProtocol(), riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[] {
					destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int seqNumDebug() {
		return lastSeqNumSent;
	}
}