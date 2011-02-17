package edu.washington.cs.cse490h.dfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * Extension to the RIONode class that adds support basic file system operations
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 * 
 * Event-Drive Framework + Reliable in Order Messaging + Reliable FS + RPC + IVY
 * CacheCoherencey (Extended w/ Create and Delete) + 2PC Transactions + Paxos
 * Primary Manager Election + Failover
 */
public class DFSNode extends RIONode {

	/**
	 * TODO: HIGH: Fix P1 feedback bugs
	 */

	/*
	 * TODO: ASK: I don't understand how all of this is going to get called from
	 * external code. I would imagine someone would do something like:
	 * 
	 * backend.txstart(); // backend.onCommand("txstart"); //
	 * backend.txstartHandler(null, "");
	 * 
	 * backend.append(steinz.friends, "wayne,"); //
	 * backend.appendHandler("append steinz.friends wayne,");
	 * 
	 * backend.append(wayne.friends, "steinz,"); //
	 * backend.appendHandler("append wayne.friends steinz,");
	 * 
	 * backend.txcommit(); // backend.onCommand("txcommit"); //
	 * backend.txcommitHandler(null, "");
	 * 
	 * But then how does the caller actually check that the commit succeeded?
	 * All commandHandlers return void right now. I guess we could have a
	 * wrapper to this whole mess that implements the first command listed per
	 * line, but does the second one internally, but I'm not sure how it would
	 * get the contents of a get really without the Client cooperating with it
	 * by, for instance, putting whatever it got last in a
	 * "public String lastGot;"
	 * 
	 * We should have AT LEAST one QZ on interfacing/architecture - how to
	 * actually build FB on top of this (tying in an interface / webserver /
	 * whatever) doesn't seem trivial.
	 */

	/*
	 * TODO: EC: Let clients exchange files w/o sending them to the manager
	 */

	/*
	 * TODO: EC: Batch requests
	 */

	/*
	 * TODO: EC: Only send file diffs for big files, keep multiple versions of
	 * files
	 */

	/*
	 * TODO: EC: Don't send ACKs for messages that always get responses - for
	 * ex, let WD be WQ's ACK
	 */

	/*
	 * TODO: EC: Multiple TXs for clients at the same time
	 */

	/*
	 * TODO: EC: Let managers function as clients too
	 */

	/**
	 * Delimiter used in protocol payloads. Should be a single character.
	 */
	protected static final String packetDelimiter = " ";

	/**
	 * Whether or not this node is a manager node
	 */
	protected boolean isManager;

	/**
	 * The library visible thread implementing multiple message operations with
	 * blocking calls
	 */
	protected DFSThread dfsThread;

	/*
	 * NOTE: when iterating through elements
	 */

	/**
	 * Queue of commands waiting to be handled by the dfsThread
	 */
	protected BlockingQueue<String> commandQueue;

	protected BlockingQueue<DFSPacket> packetQueue;

	protected Map<String, DFSPacket> expectedPackets;

	/**
	 * Starts the node as a client
	 */
	@Override
	public void start() {
		commandQueue = new LinkedBlockingQueue<String>();
		packetQueue = new LinkedBlockingQueue<DFSPacket>();
		expectedPackets = Collections
				.synchronizedMap(new HashMap<String, DFSPacket>());

		restartAsClient();
	}

	/**
	 * Resets everything to the initial client state
	 * 
	 * Does not reset RIOLayer components
	 * 
	 * called by start and when things get really messed up; currently when:
	 * 
	 * client fails aborting/committing a transaction (can't write changes to
	 * disk)
	 * 
	 * client doesn't hear a response back from the server after sending a
	 * commit request
	 * 
	 * TODO: HIGH: think about restart, runtime exceptions, etc
	 */
	public void restartAsClient() {
		printInfo("(re)starting as client");

		// tells the existing thread to stop handling commands
		dfsThread.interrupt();

		this.isManager = false;
		try {
			dfsThread = new ClientThread(this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		dfsThread.run();
	}

	/**
	 * Resets everything to the initial manager state
	 */
	public void restartAsManager() {
		printInfo("(re)starting as manager");

		// tells the existing thread to stop executing
		dfsThread.interrupt();

		this.isManager = true;
		try {
			dfsThread = new ManagerThread(this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		dfsThread.run();

		broadcast(Protocol.MANAGERIS, Utility.stringToByteArray(this.addr + ""));
	}

	/**
	 * TODO: Associate a unique command_id with every operation to make
	 * Synoptic's trace mapping easier
	 */

	/**
	 * Process a command from user or file. Lowercases the command for further
	 * internal use.
	 */
	public void onCommand(String line) {
		printVerbose("received command: " + line);
		try {
			commandQueue.put(line);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		printVerbose("received " + Protocol.protocolToString(protocol)
				+ " from Universe, giving to RIOLayer");

		/*
		 * Manager restarted or is talking to you for the first time - whatever
		 * a client was doing has been abandoned by the manager, so unlock
		 * everything locally
		 * 
		 * TODO: Pull some of the RIO HANDSHAKE handling up here?
		 */
		if (!isManager && protocol == Protocol.HANDSHAKE) {
			try {
				commandQueue.put("handshake");
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		super.onReceive(from, protocol, msg);
	}

	/**
	 * Method called by the RIO layer when a message is to be delivered.
	 */
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		printVerbose("received " + Protocol.protocolToString(protocol)
				+ " from RIOLayer, handling");

		MessageType mt = MessageType.ordinalToMessageType(protocol);

		// Verify we have a correct message type
		if (mt == null) {
			throw new RuntimeException("invalid message ordinal " + protocol
					+ " received");
		}

		// TODO: If manager but not managing, send MANAGERIS to client

		DFSPacket packet = DFSPacket.unpack(from, mt, msg);

		/*
		 * TOOD: HIGH: figure out where to put the packet
		 * 
		 * non blocking packets -> packetQueue
		 * 
		 * blocking-response packets -> expectedPackets by name for client to
		 * find... maybe this should just be a hand off instead of a map
		 */
		
		
		// TODO: Dynamic Dispatch should probably move into DFSThread
		
		// Find the instance to handle this message type
		Object instance = null;
		switch (mt.handlingClass) {
		case DFSNode:
			instance = this;
			break;
		case ClientNode:
			instance = clientFunctions;
			break;
		case ManagerNode:
			instance = managerFunctions;
			break;
		}

		// Invalid message type for my node type
		// (manager got client-only, etc)
		if (instance == null) {
			throw new RuntimeException("unhandled message type " + mt + " received");
		}

		// route message
		try {
			Class<?> handlingClass = instance.getClass();
			Class<?>[] paramTypes = { int.class, String.class };
			Method handler = handlingClass.getMethod("receive" + mt.name(),
					paramTypes);
			Object[] args = { from, msgString };
			handler.invoke(instance, args);
		} catch (Exception e) {
			printError(e);
		}

		/*
		 * TODO: HIGH: All errors should be caught and dealt w/ by their
		 * respective receive methods
		 * 
		 * (This might be out of date now)
		 * 
		 * Manager side: respond by sendError(from) or send TX_FAILURE
		 * 
		 * Client side: printError, abort tx - manager might detect failure
		 * first, but maybe good to abort here just in case - if manager removes
		 * the client from it's transacting set an abort shouldn't be necessary
		 * and the manager needs to know what to do with it (ignore it probably)
		 */
	}

	public void receiveNoop(int from, String msg) {
		printInfo("received noop from " + from);
	}

	public void receiveHeartbeat(int from, String msg) {
		printVerbose("received heartbeat from " + from);
	}

	public void receiveManagerIs(int from, String msg) {
		this.clientFunctions.managerAddr = Integer.parseInt(msg);
		printInfo("setting manager address to " + from);
	}

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 */
	public void receiveWD(int from, String msgString) {

		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			clientFunctions.receiveWD(from, filename, contents);
		} else {
			managerFunctions.receiveWD(from, filename, contents);
		}
	}

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 */
	public void receiveRD(int from, String msgString) {
		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			clientFunctions.receiveRD(from, filename, contents);
		} else {
			this.managerFunctions.receiveRD(from, filename, contents);
		}
	}

	/**
	 * TOOD: HIGH: document killNode
	 */
	public void killNode(int destAddr) {
		if (isManager) {
			this.managerFunctions.killNode(destAddr);
		}
	}
}
