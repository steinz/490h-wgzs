/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * TODO: HIGH: move to private inner class and describe
 */
public class QueuedFileRequest {
	
	int from;
	int protocol;
	byte[] msg;
	
	public QueuedFileRequest(int addr, int prot, byte[] buf) {
			from = addr;
			protocol = prot;
			msg = buf;
	}
}
