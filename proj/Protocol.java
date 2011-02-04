import java.lang.reflect.Field;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Contains details about the recognized protocols
 */
public class Protocol {

	/*
	 * NOTE: No message type should have identifier 127 - methods below using
	 * reflection will behave unexpectedly
	 */
	public static final int MAX_PROTOCOL = 127;

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
	public static final int WD_DELETE = 19;

	// Transcactions - Sent to manager
	public static final int TX_START = 20;
	public static final int TX_COMMIT = 21;
	public static final int TX_ABORT = 22;
	// Transactions - Sent to client
	public static final int TX_SUCCESS = 23;
	public static final int TX_FAILURE = 24;

	/*
	 * TODO: These will probably need to be replaced w/ specific ERROR / SUCCESS
	 * types so that they can be correctly processed if received out of order.
	 */
	public static final int SUCCESS = 125;
	public static final int ERROR = 126;

	/**
	 * Tests if this is a valid protocol for a Packet
	 * 
	 * @param protocol
	 *            The protocol in question
	 * @return true if the protocol is valid, false otherwise
	 */
	public static boolean isPktProtocolValid(int protocol) {
		// TODO: Replace w/ reflection
		return ((25 > protocol && protocol > -1) || protocol == 125 || protocol == 127);
	}

	/**
	 * Returns a string representation of the given protocol for debugging.
	 * 
	 * @param protocol
	 *            The protocol whose string representation is desired
	 * @return The string representation of the given protocol.
	 *         "Unknown (int identifier)" if the protocol is not recognized
	 */
	public static String protocolToString(int protocol) {
		try {
			Class<?> proto = Class.forName("Protocol");

			Field[] fields = proto.getDeclaredFields();
			for (Field field : fields) {
				int value = field.getInt(null);
				if (value == protocol) {
					return field.getName();
				}
			}
		} catch (Exception e) {
			return "UNKNOWN (" + protocol + ")";
		}

		return "UNKNOWN (" + protocol + ")";
	}
}
