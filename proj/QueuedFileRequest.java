/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * TODO: describe
 */
public class QueuedFileRequest {
	
	int from;
	int protocol;
	byte[] msg;
	
	String command;
	
	public QueuedFileRequest(int addr, int prot, byte[] buf) {
			from = addr;
			protocol = prot;
			msg = buf;
	}
		
	// TODO: Separate into two classes?
	
	public QueuedFileRequest(String command) {
		this.command = command;
	}
}
