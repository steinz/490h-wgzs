/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * <pre>
 * Contains details about the recognized protocols
 * </pre>
 */
public class Protocol {

	// TODO: LOW: Replace this class w/ an enum
	
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
	public static final int SUCCESS = 125;
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
		// TODO: Update :(
		switch (protocol) {
		case DATA:
			return "RIO_DATA";
		case ACK:
			return "RIO_ACK";
		case CREATE:
			return "RIO_CREATE";
		case PUT:
			return "RIO_PUT";
		case APPEND:
			return "RIO_APPEND";
		case DELETE:
			return "RIO_DELETE";
		case GET:
			return "RIO_GET";
		case HANDSHAKE:
			return "RIO_HANDSHAKE";
		case NOOP:
			return "RIO_NOOP";
		case RIOTEST_PKT:
			return "RIO_TEST";
		case ERROR:
			return "RIO_ERROR";
		case WQ:
			return "RIO_WQ";
		case WD:
			return "RIO_WD";
		case WF:
			return "RIO_WF";
		case WC:
			return "RIO_WC";
		case RQ:
			return "RIO_RQ";
		case RD:
			return "RIO_RD";
		case RF:
			return "RIO_RF";
		case RC:
			return "RIO_RC";
		case IV:
			return "RIO_IV";
		case IC:
			return "RIO_IC";
		case SUCCESS:
			return "RIO_SUCCESS";
		default:
			return "UNKNOWN";
		}
	}
}
