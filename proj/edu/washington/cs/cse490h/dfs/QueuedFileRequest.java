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
	int protocol;
	byte[] msg;
	
	public QueuedFileRequest(int addr, int prot, byte[] buf) {
			from = addr;
			protocol = prot;
			msg = buf;
	}
}
