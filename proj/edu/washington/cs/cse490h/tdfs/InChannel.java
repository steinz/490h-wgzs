package edu.washington.cs.cse490h.tdfs;
/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	private int lastSeqNumDelivered;
	private HashMap<Integer, RIOPacket> outOfOrderMsgs;
	
	InChannel(){
		lastSeqNumDelivered = -1;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt
	 *            The packet
	 * @return A list of the packets that we can now deliver due to the receipt
	 *         of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {
		LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
		int seqNum = pkt.getSeqNum();
		
		if(seqNum == lastSeqNumDelivered + 1) {
			// We were waiting for this packet
			pktsToBeDelivered.add(pkt);
			++lastSeqNumDelivered;
			deliverSequence(pktsToBeDelivered);
		}else if(seqNum > lastSeqNumDelivered + 1){
			// We received a subsequent packet and should store it
			outOfOrderMsgs.put(seqNum, pkt);
		}
		// Duplicate packets are ignored
		
		return pktsToBeDelivered;
	}

	/**
	 * Helper method to grab all the packets we can now deliver.
	 * 
	 * @param pktsToBeDelivered
	 *            List to append to
	 */
	private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
		while(outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
			++lastSeqNumDelivered;
			pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
		}
	}
	
	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
	
	public int seqNumDebug() {
        return lastSeqNumDelivered;
	}
}