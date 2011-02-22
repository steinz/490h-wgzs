package edu.washington.cs.cse490h.dfs;

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
enum MessageType {

	Ack(HandlingClass.RIOLayer), Handshake(HandlingClass.RIOLayer),

	Noop(HandlingClass.DFSNode), Heartbeat(HandlingClass.DFSNode), ManagerIs(
			HandlingClass.DFSNode),

	Create(HandlingClass.ManagerNode), Delete(HandlingClass.ManagerNode), WQ(
			HandlingClass.ManagerNode), RQ(HandlingClass.ManagerNode), WC(
			HandlingClass.ManagerNode), RC(HandlingClass.ManagerNode), IC(
			HandlingClass.ManagerNode), WDDelete(HandlingClass.ManagerNode), TXAbort(
			HandlingClass.ManagerNode), TXCommit(HandlingClass.ManagerNode), TXStart(
			HandlingClass.ManagerNode), giveClientRW(HandlingClass.ManagerNode), giveClientRO(
			HandlingClass.ManagerNode), giveClientOwnership(HandlingClass.ManagerNode), revokeClient(
			HandlingClass.ManagerNode),

	// TODO: HIGH: Success -> OperationSuccessful or something more descriptive

	WD(HandlingClass.ClientNode), RD(HandlingClass.ClientNode), WF(
			HandlingClass.ClientNode), RF(HandlingClass.ClientNode), IV(
			HandlingClass.ClientNode), Success(HandlingClass.ClientNode), Error(
			HandlingClass.ClientNode), TXSuccess(HandlingClass.ClientNode), TXFailure(
			HandlingClass.ClientNode),

	ReplicaAppend(HandlingClass.ReplicaNode), ReplicaCreate(
			HandlingClass.ReplicaNode), ReplicaDelete(HandlingClass.ReplicaNode), ReplicaPut(
			HandlingClass.ReplicaNode), ReplicaTXAbort(
			HandlingClass.ReplicaNode), ReplicaTXCommit(
			HandlingClass.ReplicaNode), ReplicaTXStart(
			HandlingClass.ReplicaNode),

	ReplicaOutOfDate(HandlingClass.ClientNode), // TODO: HIGH: use

	ReplicaStartReplicating(HandlingClass.ReplicaNode),

	@Deprecated
	Data(HandlingClass.None), @Deprecated
	Get(HandlingClass.None), @Deprecated
	Put(HandlingClass.None), @Deprecated
	Append(HandlingClass.None),

	Prepare(HandlingClass.PaxosNode), PromiseDenial(HandlingClass.PaxosNode), Promise(
			HandlingClass.PaxosNode), Accept(HandlingClass.PaxosNode), Accepted(
			HandlingClass.PaxosNode), Leader(HandlingClass.PaxosNode), Finished(
			HandlingClass.PaxosNode);

	public enum HandlingClass {
		RIOLayer, DFSNode, ClientNode, ManagerNode, PaxosNode, ReplicaNode, None
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
