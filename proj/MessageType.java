import java.util.HashMap;
import java.util.Map;

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

	Ack(HandlingClass.RIOLayer), Handshake(HandlingClass.RIOLayer),

	Noop(HandlingClass.DFSNode), Heartbeat(HandlingClass.DFSNode), ManagerIs(
			HandlingClass.DFSNode),

	WD(HandlingClass.DFSNode), RD(HandlingClass.DFSNode),

	Create(HandlingClass.ManagerNode), Delete(HandlingClass.ManagerNode), WQ(
			HandlingClass.ManagerNode), RQ(HandlingClass.ManagerNode), WC(
			HandlingClass.ManagerNode), RC(HandlingClass.ManagerNode), IC(
			HandlingClass.ManagerNode), WDDelete(HandlingClass.ManagerNode), TXAbort(
			HandlingClass.ManagerNode), TXCommit(HandlingClass.ManagerNode), TXStart(
			HandlingClass.ManagerNode),

	WF(HandlingClass.ClientNode), RF(HandlingClass.ClientNode), IV(
			HandlingClass.ClientNode), Success(HandlingClass.ClientNode), Error(
			HandlingClass.ClientNode), TXSuccess(HandlingClass.ClientNode), TXFailure(
			HandlingClass.ClientNode),

	@Deprecated
	Data(HandlingClass.None), @Deprecated
	Get(HandlingClass.None), @Deprecated
	Put(HandlingClass.None), @Deprecated
	Append(HandlingClass.None);

	public enum HandlingClass {
		RIOLayer, DFSNode, ManagerNode, ClientNode, None
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

	private static Map<Integer, MessageType> ordinalToMessageTypeCache = new HashMap<Integer, MessageType>(
			MAX_ORDINAL);

	/**
	 * Turn an int ordinal into a MessageType
	 */
	public static MessageType ordinalToMessageType(int i) {
		MessageType cached = ordinalToMessageTypeCache.get(i);
		if (cached == null) {
			for (MessageType mt : values()) {
				if (mt.ordinal() == i) {
					cached = mt;
					ordinalToMessageTypeCache.put(i, cached);
					break;
				}
			}
		}
		return cached;
	}
}
