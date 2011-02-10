import javax.net.ssl.HandshakeCompletedEvent;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Contains details about the recognized message types and what classes can
 * handle them
 */
public enum MessageType {

	ACK(HandlingClass.RIOLayer), HandshakeCompletedEvent(HandlingClass.RIOLayer),

	NOOP(HandlingClass.Client), HEARTBEAT(HandlingClass.Client),

	CREATE(HandlingClass.ManagerNode), DELETE(HandlingClass.ManagerNode), WQ(
			HandlingClass.ManagerNode), RQ(HandlingClass.ManagerNode), WC(
			HandlingClass.ManagerNode), RC(HandlingClass.ManagerNode), IC(
			HandlingClass.ManagerNode), WD_DELETE(HandlingClass.ManagerNode), TX_ABORT(
			HandlingClass.ManagerNode), TX_COMMIT(HandlingClass.ManagerNode), TX_START(
			HandlingClass.ManagerNode),

	WF(HandlingClass.ClientNode), RF(HandlingClass.ClientNode), IV(
			HandlingClass.ClientNode), SUCCESS(HandlingClass.ClientNode), ERROR(
			HandlingClass.ClientNode), TX_SUCCESS(HandlingClass.ClientNode), TX_FAILURE(
			HandlingClass.ClientNode),

	WD(HandlingClass.ClientAndManagerNode), RD(
			HandlingClass.ClientAndManagerNode),

	@Deprecated
	DATA(HandlingClass.None), @Deprecated
	GET(HandlingClass.None), @Deprecated
	PUT(HandlingClass.None), @Deprecated
	APPEND(HandlingClass.None);

	public enum HandlingClass {
		RIOLayer, Client, ClientNode, ManagerNode, ClientAndManagerNode, None
	};

	/**
	 * The maximum ordinal an enum can have
	 */
	private static int MAX_ORDINAL = 127;

	/**
	 * Node class that can handle this message type
	 */
	public final HandlingClass handlingClass;

	private MessageType(HandlingClass h) {
		this.handlingClass = h;
	}

	/**
	 * Checks that ordinal < MAX_ORDINAL for packet packing
	 */
	public boolean isPktProtocolValid() {
		return this.ordinal() < MAX_ORDINAL;
	}

	/**
	 * Turn an int ordinal into a MessageType
	 * 
	 * TODO: Memoize
	 */
	public static MessageType ordinalToMessageType(int i) {
		for (MessageType mt : values()) {
			if (mt.ordinal() == i) {
				return mt;
			}
		}
		return null;
	}
}
