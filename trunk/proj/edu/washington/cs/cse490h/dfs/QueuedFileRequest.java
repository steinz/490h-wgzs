package edu.washington.cs.cse490h.dfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * TODO: HIGH: move to private inner class and describe
 */
class QueuedFileRequest {

	int from;
	MessageType type;
	byte[] msg;

	public QueuedFileRequest(int addr, MessageType type, byte[] buf) {
		this.from = addr;
		this.type = type;
		this.msg = buf;
	}
}
