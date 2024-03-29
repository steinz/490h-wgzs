package edu.washington.cs.cse490h.tdfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Packet;
import edu.washington.cs.cse490h.lib.Utility;

// TODO: HIGH: More Packet objects

/**
 * This conveys the header for reliable, in-order message transfer. This is
 * carried in the payload of a Packet, and in turn the data being transferred is
 * carried in the payload of the RIOPacket packet.
 */
class RIOPacket {

	public static final int MAX_PACKET_SIZE = Packet.MAX_PAYLOAD_SIZE;
	public static final int HEADER_SIZE = 21;
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

	private MessageType type;
	private int seqNum;
	private byte[] payload;
	private UUID ID;

	/**
	 * Constructing a new RIO packet.
	 * 
	 * @param type
	 *            The type of packet. Either SYN, ACK, FIN, or DATA
	 * @param seqNum
	 *            The sequence number of the packet
	 * @param payload
	 *            The payload of the packet.
	 */
	public RIOPacket(MessageType type, int seqNum, byte[] payload, UUID ID)
			throws IllegalArgumentException {
		if (!type.isPktProtocolValid() || payload.length > MAX_PAYLOAD_SIZE) {
			throw new IllegalArgumentException(
					"Illegal arguments given to RIOPacket");
		}

		this.type = type;
		this.seqNum = seqNum;
		this.payload = payload;
		this.ID = ID;
	}

	/**
	 * @return The protocol number
	 */
	public MessageType getType() {
		return this.type;
	}

	/**
	 * @return The sequence number
	 */
	public int getSeqNum() {
		return this.seqNum;
	}

	/**
	 * @return The payload
	 */
	public byte[] getPayload() {
		return this.payload;
	}

	/**
	 * @param newID
	 *            The new ID to be set
	 */
	public void setUUID(UUID newID) {
		ID = newID;
	}

	/**
	 * 
	 * @param newProtocol
	 *            The new protocol
	 */
	public void setType(MessageType newType) {
		this.type = newType;
	}

	/**
	 * 
	 * @return newID The ID
	 */
	public UUID getUUID() {
		return this.ID;
	}

	/**
	 * Convert the RIOPacket packet object into a byte array for sending over
	 * the wire. Format: protocol = 1 byte sequence number = 4 bytes payload <=
	 * MAX_PAYLOAD_SIZE bytes
	 * 
	 * @return A byte[] for transporting over the wire. Null if failed to pack
	 *         for some reason
	 * @throws PacketPackException
	 */
	public byte[] pack() throws PacketPackException {
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);

			if (payload.length + HEADER_SIZE > MAX_PACKET_SIZE) {
				throw new PacketPackException(
						"payload too large for one packet");
			}
			// write the UUID to the packet
			long IDMostSignificantBits = ID.getMostSignificantBits();
			long IDLeastSignificantBits = ID.getLeastSignificantBits();
			out.writeLong(IDMostSignificantBits);
			out.writeLong(IDLeastSignificantBits);

			out.writeByte(type.ordinal());
			out.writeInt(seqNum);

			out.write(payload, 0, payload.length);

			out.flush();
			out.close();
			return byteStream.toByteArray();
		} catch (IOException e) {
			// TODO: This should throw an exception I think...
			return null;
		}
	}

	/**
	 * Unpacks a byte array to create a RIOPacket object Assumes the array has
	 * been formatted using pack method in RIOPacket
	 * 
	 * @param packet
	 *            String representation of the transport packet
	 * @return RIOPacket object created or null if the byte[] representation was
	 *         corrupted
	 * @throws IOException
	 * @throws Exception
	 */
	public static RIOPacket unpack(byte[] packet) throws PacketPackException,
			IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(
				packet));

		// unpack the UUID
		long mostSigBits = in.readLong();
		long leastSigBits = in.readLong();
		UUID name = new UUID(mostSigBits, leastSigBits);

		int protocol = in.readByte();
		MessageType mt = MessageType.ordinalToMessageType(protocol);
		int seqNum = in.readInt();

		byte[] payload = new byte[packet.length - HEADER_SIZE];

		int bytesRead = in.read(payload, 0, payload.length);

		// If in is at EOF bytesRead will be -1 instead of 0, but that's
		// expected.
		if (bytesRead != payload.length
				&& !(payload.length == 0 && bytesRead == -1)) {
			throw new PacketPackException("failed to read entire payload");
		}

		return new RIOPacket(mt, seqNum, payload, name);
	}

	public String toString() {
		return "proto:" + type.name() + "|rio-seqNum:" + this.seqNum
				+ "|payload:" + Utility.byteArrayToString(payload);
	}
}
