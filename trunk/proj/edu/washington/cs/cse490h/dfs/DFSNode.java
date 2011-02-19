package edu.washington.cs.cse490h.dfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

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
	 * Name of the temp file used by write when append is false
	 */
	protected static final String tempFilename = ".temp";

	/**
	 * Name of the log file used by FS transactions
	 */
	protected static final String logFilename = ".log";

	/**
	 * Name of the temp file used when purging the log
	 */
	protected static final String logTempFilename = ".log.temp";

	/**
	 * Purge the log every fsPurgeFrequency commits/aborts
	 * 
	 * TODO: raise for prod
	 */
	protected static final int fsPurgeFrequency = 5;

	/**
	 * The maximum number of commands the client will queue before timing out
	 * and restarting the node
	 */
	protected static int clientMaxWaitingForCommitQueueSize = 10;

	/**
	 * Whether or not this node is a manager node
	 */
	protected boolean isManager;

	/**
	 * Encapsulates manager functionality
	 */
	private ManagerNode managerFunctions;

	/**
	 * Encapsulates paxos functionality
	 */
	private PaxosNode paxosFunctions;
	
	/**
	 * Encapsulates client functionality
	 */
	protected ClientNode clientFunctions;

	/**
	 * FS for this node
	 */
	protected TransactionalFileSystem fs;

	/**
	 * Starts the node as a client
	 */
	@Override
	public void start() {
		// Wipe the server log
		// Logger.eraseLog(this);

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

		nullify();
		this.isManager = false;
		this.clientFunctions = new ClientNode(this,
				clientMaxWaitingForCommitQueueSize);
		restartFS();
	}

	/**
	 * Resets everything to the initial manager state
	 */
	public void restartAsManager() {
		printInfo("(re)starting as manager");

		nullify();
		this.isManager = true;
		this.managerFunctions = new ManagerNode(this);
		this.paxosFunctions = new PaxosNode(this);
		restartFS();

		broadcast(MessageType.ManagerIs,
				Utility.stringToByteArray(this.addr + ""));
	}

	private void nullify() {
		fs = null;
		managerFunctions = null;
		clientFunctions = null;
	}

	/**
	 * Nulls the old fs and tries to create a new one
	 * 
	 * fs instantiation cleans up failed puts and redoes committed transactions
	 * in the log
	 */
	private void restartFS() {
		try {
			fs = new TransactionalFileSystem(this, tempFilename, logFilename,
					logTempFilename, fsPurgeFrequency);
		} catch (IOException e) {
			/*
			 * TODO: for clients, it should be okay to try again here w/
			 * recovery turned off since the files can be recovered from
			 * elsewhere, but managers should stay down here since their FS is
			 * corrupt
			 */
			printError(e);
		}
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

		if (!isManager) {
			clientFunctions.onCommand(line);
		} else {
			// managerFunctions.onCommand(line);
			printError("manager currently supports no commands");
		}
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		// Turn the protocol into a message type
		MessageType mt = MessageType.ordinalToMessageType(protocol);

		printVerbose("received " + mt.name()
				+ " from Universe, giving to RIOLayer");

		// Updates manager address as soon as possible
		if (mt == MessageType.ManagerIs) {
			String msgStr = Utility.byteArrayToString(msg);
			receiveManagerIs(from, msgStr);
			return;
		}

		/*
		 * Manager restarted or is talking to you for the first time - whatever
		 * a client was doing has been abandoned by the manager, so unlock
		 * everything locally
		 * 
		 * TODO: Pull some of the RIO HANDSHAKE handling up here?
		 */
		if (!isManager && mt == MessageType.Handshake) {
			// TODO: restart?
			clientFunctions.abortCurrentTransaction();
		}

		super.onReceive(from, protocol, msg);
	}

	/**
	 * Method called by the RIO layer when a message is to be delivered.
	 */
	public void onRIOReceive(Integer from, MessageType type, byte[] msg) {
		printVerbose("received " + type.name() + " from RIOLayer, handling");

		String msgString = Utility.byteArrayToString(msg);

		// TODO: If manager but not managing, send MANAGERIS to client

		// Find the instance to handle this message type
		Object instance = null;
		switch (type.handlingClass) {
		case DFSNode:
			instance = this;
			break;
		case ClientNode:
			instance = clientFunctions;
			break;
		case ManagerNode:
			instance = managerFunctions;
			break;
		case PaxosNode:
			instance = paxosFunctions;
			break;
		}

		// route message
		try {
			Class<?> handlingClass = instance.getClass();
			Class<?>[] paramTypes = { int.class, String.class };
			Method handler = handlingClass.getMethod("receive" + type.name(),
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
