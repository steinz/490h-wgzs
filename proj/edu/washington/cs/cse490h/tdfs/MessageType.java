package edu.washington.cs.cse490h.tdfs;

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

	RequestMembership(HandlingClass.TDFSNode),

	CoordinatorRebooted(HandlingClass.TDFSNode),

	CreateGroup(HandlingClass.TDFSNode),

	ObseleteOperationEntryId(HandlingClass.TDFSNode),

	Prepare(HandlingClass.TDFSNode), PromiseDenial(HandlingClass.TDFSNode), Promise(
			HandlingClass.TDFSNode), Accept(HandlingClass.TDFSNode), Accepted(
			HandlingClass.TDFSNode), Finished(HandlingClass.TDFSNode);

	public enum HandlingClass {
		RIOLayer, TDFSNode;
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
