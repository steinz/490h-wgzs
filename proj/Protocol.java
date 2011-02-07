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

	// Base RIO Types
	@Deprecated
	public static final int DATA = 0;
	public static final int ACK = 1;

	// FS RPC Commands
	public static final int CREATE = 2;
	public static final int DELETE = 3;
	@Deprecated
	public static final int GET = 4;
	@Deprecated
	public static final int PUT = 5;
	@Deprecated
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
	public static final int HEARTBEAT = 25;

	// Command Status Result Types
	public static final int SUCCESS = 125;
	public static final int ERROR = 126;

	/*
	 * END MESSAGE TYPES
	 */
	
	/**
	 * Exclusive max valid message type
	 */
	public static final int MAX_PROTOCOL = 127;

	/**
	 * Cache of validity for different message type numbers
	 */
	private static Boolean[] validProtoCache = new Boolean[MAX_PROTOCOL];

	/**
	 * Uses reflection to check if this class has an int field with value
	 * protocol
	 */
	private static boolean checkValidProtocol(int protocol) {
		try {
			Class<?> proto = Class.forName("Protocol");
			Field[] fields = proto.getDeclaredFields();
			for (Field field : fields) {
				if (field.getType().equals(int.class)) {
					int value = field.getInt(null);
					if (value == protocol) {
						return true;
					}
				}
			}
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * Memoized check if the message type is valid.
	 */
	public static boolean isPktProtocolValid(int protocol) {
		if (protocol < 0 || protocol > MAX_PROTOCOL) {
			return false;
		} else if (validProtoCache[protocol] == null) {
			validProtoCache[protocol] = checkValidProtocol(protocol);
		}
		return validProtoCache[protocol];
	}

	/**
	 * Cache of message types to names for memoization
	 */
	private static String[] protoToStringCache = new String[MAX_PROTOCOL];

	/**
	 * Uses reflection to generate the string associated with this message type
	 */
	private static String generateProtocolToString(int protocol) {
		try {
			Class<?> proto = Class.forName("Protocol");
			Field[] fields = proto.getDeclaredFields();
			for (Field field : fields) {
				if (field.getType().equals(int.class)) {
					int value = field.getInt(null);
					if (value == protocol) {
						return field.getName().toUpperCase();
					}
				}
			}
		} catch (Exception e) {
		}
		return "UNKNOWN(" + protocol + ")";
	}

	/**
	 * Returns a memoized string representation of the given message type
	 */
	public static String protocolToString(int protocol) {
		if (protocol < 0 || protocol > MAX_PROTOCOL) {
			return "INVALID(" + protocol + ")";
		} else if (protoToStringCache[protocol] == null) {
			protoToStringCache[protocol] = generateProtocolToString(protocol);
		}
		return protoToStringCache[protocol];
	}
}
