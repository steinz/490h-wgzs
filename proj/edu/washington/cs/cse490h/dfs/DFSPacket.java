package edu.washington.cs.cse490h.dfs;

/**
 * Base class encapsulating all DFS packets
 */
public abstract class DFSPacket {

	protected int from;
	protected MessageType type;
	protected byte[] payload;

	public DFSPacket(int from, MessageType type, byte[] payload) {
		this.from = from;
		this.type = type;
		this.payload = payload;
	}

	protected abstract byte[] pack();

	protected static abstract DFSPacket unpack(int from, MessageType type, byte[] payload);

}