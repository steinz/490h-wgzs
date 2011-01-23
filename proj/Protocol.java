/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * <pre>
 * Contains details about the recognized protocols
 * </pre>
 */
public class Protocol {

	// TODO: @Discuss Should these be an enum?

	// Base RIO Types
	public static final int DATA = 0;
	public static final int ACK = 1;

	// FS RPC Commands
	public static final int CREATE = 2;
	public static final int DELETE = 3;
	public static final int GET = 4;
	public static final int PUT = 5;
	public static final int APPEND = 6;
	public static final int HANDSHAKE = 7;
	public static final int NOOP = 8;

	// Cache Coherence Commands
	public static final int WQ = 9;
	public static final int WD = 10;
	public static final int WF = 11;
	public static final int WC = 12;
	public static final int RQ = 13;
	public static final int RD = 14;
	public static final int RF = 15;
	public static final int RC = 16;
	public static final int IV = 17;
	public static final int IC = 18;

	// Error Type
	// TODO: Actually send ERROR packets on errors instead of DATA packets with
	// error message strings.
	public static final int ERROR = 127;

	public static final int MAX_PROTOCOL = 127;

	// Protocols for Testing Reliable in-order message delivery
	// These should be RIOPacket protocols
	@Deprecated
	public static final int RIOTEST_PKT = 126;

	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int protocol) {
		return ((19 > protocol && protocol > -1) || protocol == 127);
	}

	/**
	 * Returns a string representation of the given protocol for debugging.
	 * 
	 * @param protocol
	 *            The protocol whose string representation is desired
	 * @return The string representation of the given protocol.
	 *         "Unknown Protocol" if the protocol is not recognized
	 */
	public static String protocolToString(int protocol) {
		switch (protocol) {
		case DATA:
			return "RIO Data Packet";
		case ACK:
			return "RIO Acknowledgement Packet";
		case CREATE:
			return "RIO Create Packet";
		case PUT:
			return "RIO Put Packet";
		case APPEND:
			return "RIO Append Packet";
		case DELETE:
			return "RIO Delete Packet";
		case GET:
			return "RIO Get Packet";
		case HANDSHAKE:
			return "RIO Handshake";
		case NOOP:
			return "RIO Noop Packet";
		case RIOTEST_PKT:
			return "RIO Testing Packet";
		case ERROR:
			return "RIO Error Packet";
		default:
			return "Unknown Protocol";
		}
	}
}
