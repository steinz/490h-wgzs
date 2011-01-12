import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private HashMap<RIOPacket, Integer> resendCounts;
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private int destAddr;
	
	OutChannel(ReliableInOrderMsgLayer parent, int destAddr){
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
	 */
	protected void sendRIOPacket(RIONode n, int protocol, byte[] payload) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket newPkt = new RIOPacket(protocol, ++lastSeqNumSent, payload);
			unACKedPackets.put(lastSeqNumSent, newPkt);
			
			resendCounts.put(newPkt, 0);
			
			n.send(destAddr, protocol, newPkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, lastSeqNumSent }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
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
		RIOPacket packet = unACKedPackets.get(seqNum);
		assert(packet != null);
		assert(resendCounts.containsKey(packet));
		if (resendCounts.get(packet) >= ReliableInOrderMsgLayer.TIMEOUT) {
			resendCounts.remove(packet);
			unACKedPackets.remove(seqNum);
		} else if(unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
			resendCounts.put(packet, resendCounts.get(packet) + 1);
		}
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
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket riopkt = unACKedPackets.get(seqNum);
			
			n.send(destAddr, Protocol.DATA, riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
