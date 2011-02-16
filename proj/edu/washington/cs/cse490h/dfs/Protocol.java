package edu.washington.cs.cse490h.dfs;
import java.lang.reflect.Field;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Contains details about the recognized protocols
 * 
 * TODO: HIGH: I hate this class - switch everything to use MessageType instead
 */
class Protocol {
	// NOTE: Make sure this agrees w/ MessageType's ordering
	public static final int ACK = 0;
	public static final int HANDSHAKE = 1;
	public static final int NOOP = 2;
	public static final int HEARTBEAT = 3;
	public static final int MANAGERIS = 4;
	public static final int WD = 5;
	public static final int RD = 6;
	public static final int CREATE = 7;
	public static final int DELETE = 8;
	public static final int WQ = 9;
	public static final int RQ = 10;
	public static final int WC = 11;
	public static final int RC = 12;
	public static final int IC = 13;
	public static final int WD_DELETE = 14;
	public static final int TX_ABORT = 15;
	public static final int TX_COMMIT = 16;
	public static final int TX_START = 17;
	public static final int WF = 18;
	public static final int RF = 19;
	public static final int IV = 20;
	public static final int SUCCESS = 21;
	public static final int ERROR = 22;
	public static final int TX_SUCCESS = 23;
	public static final int TX_FAILURE = 24;
	@Deprecated
	public static final int DATA = 25;
	@Deprecated
	public static final int GET = 26;
	@Deprecated
	public static final int PUT = 27;
	@Deprecated
	public static final int APPEND = 28;

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
			Class<?> proto = Class.forName("edu.washington.cs.cse490h.dfs.Protocol");
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
			Class<?> proto = Class.forName("edu.washington.cs.cse490h.dfs.Protocol");
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
