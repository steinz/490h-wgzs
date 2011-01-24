
/**
 * stub
 * @author wayger, zstein
 *
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
