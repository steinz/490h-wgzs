import java.util.HashMap;
import java.util.LinkedList;
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
public class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 3;
	
	private HashMap<Integer, InChannel> inConnections;
	private HashMap<Integer, OutChannel> outConnections;
	private RIONode n;

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
		RIOPacket riopkt = RIOPacket.unpack(msg);
		
		// ack the packet immediately, then deal with handshakes, if not a handshake or a packet from an old session, pass it along
		
		// at-most-once semantics
		byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
		n.send(from, Protocol.ACK, seqNumByteArray);
		
		InChannel in = inConnections.get(from);
		if(in == null) {
			in = new InChannel();
			inConnections.put(from, in);
		}
		
		if (riopkt.getProtocol() == Protocol.HANDSHAKE){
			// handshake, so store this UUID
			n.addrToSessionIDMap.put(from, riopkt.getUUID());
			return;
		}
		// check if UUID is what we think it is. 
		if (!(riopkt.getUUID().equals(n.getID())))
		{
			// if it's not, we should initiate a handshake immediately and make a new channel to clear our cache of bad packets from an old session
			RIOSend(from, Protocol.HANDSHAKE, Utility.stringToByteArray(" "));
			inConnections.put(from, new InChannel());
			
		}
		else // only process packets that have valid UUIDs
		{

			
			LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
			for(RIOPacket p: toBeDelivered) {
				// deliver in-order the next sequence of packets
				n.onRIOReceive(from, p.getProtocol(), p.getPayload());
			}
		}
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
		int seqNum = Integer.parseInt( Utility.byteArrayToString(msg) );
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
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		OutChannel out = outConnections.get(destAddr);
		if(out == null) {
			out = new OutChannel(this, destAddr);
			outConnections.put(destAddr, out);
		}
		
		// Decide what the UUID of the destination address is
		UUID ID = n.addrToSessionIDMap.get(destAddr);
		if (ID == null)
			ID = n.getID();
		
		out.sendRIOPacket(n, protocol, payload, ID);
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
		for(Integer i: inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}
		
		return sb.toString();
	}
}
