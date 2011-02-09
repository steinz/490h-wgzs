/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Utility;

/*
 * TODO: HIGH: Verify methods declare that they thow the right exceptions throughout the project
 */

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
public class Client extends RIONode {

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
	 * TODO: LOW/EC: Let clients exchange files w/o sending them to the manager
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
	protected static final String delimiter = " ";

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
	 */
	protected static final int fsPurgeFrequency = 5;

	/**
	 * The maximum number of commands the client will queue before timing out
	 * and restarting the node
	 */
	protected static int clientMaxWaitingForCommitQueueSize = 10;

	/**
	 * Static empty payload for use by messages that don't have payloads
	 */
	protected static final byte[] emptyPayload = new byte[0];

	/**
	 * Whether or not this node is the manager for project 2.
	 */
	protected boolean isManager;

	/**
	 * Encapsulates manager functionality
	 */
	protected ManagerNode managerFunctions;

	/**
	 * Encapsulates client functionality
	 */
	protected ClientNode clientFunctions;

	/**
	 * FS for this node
	 */
	protected TransactionalFileSystem fs;

	/**
	 * Wipe the log and restart the client
	 */
	public void start() {
		// Wipe the server log
		// Logger.eraseLog(this);

		restart();
	}

	/**
	 * fs instantiation cleans up failed puts and redoes committed transactions
	 * in the log
	 * 
	 * Called by start and when things get really messed up; currently when:
	 * 
	 * client fails aborting/committing a transaction (can't write changes to
	 * disk)
	 * 
	 * client doesn't hear a response back from the server after sending a
	 * commit request
	 * 
	 * TODO: HIGH: think about restart
	 * TODO: HIGH: This try/catch pair results in the tfs never being declared if the TFS aborts
	 */
	public void restart() {
		printInfo("CLIENT (RE)STARTING");

		this.isManager = false;

		this.clientFunctions = new ClientNode(this,
				clientMaxWaitingForCommitQueueSize);

		try {
			fs = new TransactionalFileSystem(this, tempFilename, logFilename,
					logTempFilename, fsPurgeFrequency);
		} catch (IOException e) {
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
		printVerbose("received " + Protocol.protocolToString(protocol)
				+ " from Universe, giving to RIOLayer");
		
		if (protocol == Protocol.HANDSHAKE) {
			clientFunctions.unlockAll();
		}
		
		super.onReceive(from, protocol, msg);
	}

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param msg
	 *            The message that was received
	 */
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		printVerbose("received " + Protocol.protocolToString(protocol)
				+ " from RIOLayer, handling");

		String msgString = Utility.byteArrayToString(msg);

		// TODO: HIGH: Replace massive switch w/ dynamic dispatch.

		try {
			switch (protocol) {
			case Protocol.CREATE:
				receiveCreate(from, msgString);
				break;
			case Protocol.DELETE:
				receiveDelete(from, msgString);
				break;
			case Protocol.NOOP:
				printInfo("received noop from " + from);
				break;
			case Protocol.HEARTBEAT:
				printInfo("received heartbeat from " + from);
				break;
			case Protocol.IC:
				receiveIC(from, msgString);
				break;
			case Protocol.IV:
				receiveIV(msgString);
				break;
			case Protocol.WC:
				receiveWC(from, msgString);
				break;
			case Protocol.RC:
				receiveRC(from, msgString);
				break;
			case Protocol.WD:
				receiveWD(from, msgString);
				break;
			case Protocol.RD:
				receiveRD(from, msgString);
				break;
			case Protocol.RQ:
				receiveRQ(from, msgString);
				break;
			case Protocol.WQ:
				receiveWQ(from, msgString);
				break;
			case Protocol.WF:
				receiveWF(msgString);
				break;
			case Protocol.RF:
				receiveRF(msgString);
				break;
			case Protocol.WD_DELETE:
				receiveWD_DELETE(from, msgString);
				break;
			case Protocol.TX_START:
				receiveTX_START(from);
				break;
			case Protocol.TX_ABORT:
				receiveTX_ABORT(from);
				break;
			case Protocol.TX_COMMIT:
				receiveTX_COMMIT(from);
				break;
			case Protocol.TX_SUCCESS:
				receiveTX_SUCCESS();
				break;
			case Protocol.TX_FAILURE:
				receiveTX_FAILURE();
				break;
			case Protocol.ERROR:
				receiveError(from, msgString);
				break;
			case Protocol.SUCCESS:
				receiveSuccessful(from, msgString);
				break;
			default:
				printError("received invalid/deprecated packet type");
			}
		} catch (NotManagerException e) {

			/*
			 * TODO: HIGH: All errors should be caught and dealt w/ by their
			 * respective receive methods
			 * 
			 * Manager side: respond by sendError(from) or send TX_FAILURE
			 * 
			 * Client side: printError, abort tx - manager might detect failure
			 * first, but maybe good to abort here just in case - if manager
			 * removes the client from it's transacting set an abort shouldn't
			 * be necessary and the manager needs to know what to do with it
			 * (ignore it probably)
			 * 
			 * EXCEPT: If someone gets a message type meant only for the other
			 * type of node, the demultiplexing methods below will throw an
			 * exception up here - we can just print an error in this case since
			 * something has to be pretty messed up for this to happen
			 */

			printError(e);
		} catch (NotClientException e) {
			printError(e);
		}
	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

	/**
	 * Create RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 */
	protected void receiveCreate(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveCreate(client, filename);
	}

	/**
	 * Delete RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 * @throws PrivilegeLevelDisagreementException
	 * @throws InconsistentPrivelageLevelsDetectedException
	 */
	protected void receiveDelete(int from, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveDelete(from, filename);
	}

	protected void receiveIC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveIC(client, filename);
	}

	/**
	 * Receives an RC and changes this client's status from IV or RW to RO.
	 * 
	 * @throws NotManagerException
	 */
	protected void receiveRC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveRC(client, filename);

	}

	protected void receiveRQ(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}

		this.managerFunctions.receiveQ(client, filename, Protocol.RQ,
				Protocol.RD, Protocol.RF, true);
	}

	protected void receiveTX_ABORT(int from) throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_ABORT(from);
	}

	protected void receiveTX_COMMIT(int from) throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_COMMIT(from);
	}

	protected void receiveTX_START(int from) throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_START(from);
	}

	/**
	 * Changes the status of this client from IV or RW
	 * 
	 * @throws NotManagerException
	 */
	protected void receiveWC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveWC(client, filename);
	}

	protected void receiveWD_DELETE(int from, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException(
					"WD_DELETE should only be received by the manager");
		}
		this.managerFunctions.receiveWD_DELETE(from, filename);
	}

	protected void receiveWQ(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}

		this.managerFunctions.receiveQ(client, filename, Protocol.WQ,
				Protocol.WD, Protocol.WF, false);
	}

	/*************************************************
	 * end manager-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client-only cache coherency functions
	 ************************************************/

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 * 
	 * @throws NotClientException
	 * @throws UnknownManagerException
	 */
	protected void receiveIV(String msgString) throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveIV(msgString);
	}

	/**
	 * Client receives RF as a request from the server to propagate their
	 * changes.
	 * 
	 * @throws NotClientException
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws PrivilegeLevelDisagreementException
	 */
	protected void receiveRF(String msgString) throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveF(msgString, "RF", Protocol.RD, true);
	}

	/**
	 * Client receives WF as a request from the server to propagate their
	 * changes.
	 * 
	 * @throws NotClientException
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws PrivilegeLevelDisagreementException
	 */
	protected void receiveWF(String msgString) throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveF(msgString, "WF", Protocol.WD, false);
	}

	/**
	 * RPC Error
	 * 
	 * @throws NotClientException
	 */
	protected void receiveError(Integer from, String msgString)
			throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveError(from, msgString);
	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 * 
	 * @throws Exception
	 */
	protected void receiveSuccessful(int from, String msgString)
			throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveSuccessful(from, msgString);
	}

	/**
	 * Transaction succeeded
	 * 
	 * @throws NotClientException
	 * @throws TransactionException
	 * @throws IOException
	 */
	protected void receiveTX_SUCCESS() throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveTX_SUCCESS();
	}

	/**
	 * Transaction failed
	 * 
	 * @throws NotClientException
	 * @throws TransactionException
	 * @throws IOException
	 */
	protected void receiveTX_FAILURE() throws NotClientException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveTX_FAILURE();
	}

	/*************************************************
	 * end client-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client and manager cache coherency functions
	 ************************************************/

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws IllegalConcurrentRequestException
	 * @throws MissingPendingRequestException
	 */
	protected void receiveWD(int from, String msgString) {

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
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	protected void receiveRD(int from, String msgString) {
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

	/*************************************************
	 * end client and manager cache coherency functions
	 ************************************************/


	public void killNode(int destAddr) {
		if (isManager) {
			this.managerFunctions.killNode(destAddr);
		}
	}
}
