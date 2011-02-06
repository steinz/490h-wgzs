/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.NoSuchElementException;
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
 * IMPORTANT: Methods names should not end in Handler unless they are meant to
 * handle commands passed in by onCommand - onCommand dynamically dispatches
 * commands to the method named <cmdName>Handler.
 * 
 * TODO: Managers and Clients are distinct in our implementation. That is, a
 * manager is not also a client. We should change the receive methods so that
 * the manager acts as a client when it should and as manager otherwise. This
 * might require splitting existing messages types that both node types can
 * receive (WD) into two distinct messages types (WD_TO_SERVER and
 * WD_TO_CLIENT).
 * 
 * Event-Drive Framework + Reliable in Order Messaging + Reliable FS + RPC + IVY
 * CacheCoherencey (Extended w/ Create and Delete) + 2PC Transactions
 */
public class Client extends RIONode {

	/*
	 * TODO: ASK: During a tx, consider:
	 * 
	 * put test hello
	 * 
	 * txstart
	 * 
	 * create test
	 * 
	 * put test world
	 * 
	 * get test
	 * 
	 * txcommit
	 * 
	 * Would "get test" return "hello" or "world"? That is, do operations in the
	 * transaction occur in order? If so, we have to either write commands done
	 * during a transaction to disk so the existing code works, or have
	 * fs.getFile check it's in memory data structures for files that could have
	 * been changed by this transaction but haven't been written to disk yet -
	 * checking in memory data structures shouldn't be too hard implement.
	 * 
	 * A similar question is whether or not:
	 * 
	 * txstart
	 * 
	 * create test
	 * 
	 * get test
	 * 
	 * txcommit
	 * 
	 * fails during the get (because test does not exist yet). I think the
	 * general question is "can a client mutate a file twice in a commit?"
	 * 
	 * put test hello
	 * 
	 * txstart
	 * 
	 * append test _world
	 * 
	 * get test -> display to user
	 * 
	 * txcommit
	 * 
	 * The above transaction would not behave as expected if we don't check
	 * logged but not committed files for changes within a transaction.
	 * 
	 * As an aside, does the get call return to the user before the transaction
	 * commits? I'm not sure how that would be implemented, but it seems a
	 * little unintuitive that the client will get feedback from part of their
	 * transaction before committing it.
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
	 * TODO: Separate the Client and Manager code into two modules
	 */

	/*
	 * TODO: LOW/EC: Let clients exchange files w/o sending them to the manager
	 */

	/*
	 * TODO: EC: Batch requests
	 */

	/*
	 * TODO: EC: Only send file diffs for big files
	 */

	/*
	 * TODO: EC: Don't send ACKs for messages that always get responses - for
	 * ex, let WD be WQ's ACK
	 */

	/*
	 * TODO: EC: Multiple TX for clients at the same time
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
	 * Static empty payload for use by messages that don't have payloads
	 */
	protected static final byte[] emptyPayload = new byte[0];

	/**
	 * Whether or not this node is the manager for project 2.
	 */
	protected boolean isManager;

	/**
	 * The address of the manager node.
	 */
	protected int managerAddr;

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
	 * Cleans up failed puts if necessary
	 */
	public void start() {
		this.isManager = false;
		this.managerAddr = -1;

		this.clientFunctions = new ClientNode(this);

		// Wipe the server log
		Logger.eraseLog(this);

		try {
			fs = new TransactionalFileSystem(this, tempFilename, logFilename);
		} catch (IOException e) {
			printError(e);
		}
	}

	/**
	 * TODO: LOW: Associate a unique command_id with every operation to make
	 * Synoptic's trace mapping easier
	 */

	/**
	 * Process a command from user or file. Lowercases the command for further
	 * internal use.
	 */
	public void onCommand(String line) {
		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd = "";
		try {
			cmd = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			printError("no command found in: " + line);
			return;
		}

		if (isManager && !cmd.equals("manager")) {
			printError("unsupported command called on manager (manager is not a client): "
					+ line);
			return;
		}

		/*
		 * TODO: HIGH: process this queue after receiving
		 * TX_{SUCCESSFUL,FAILURE}
		 */
		if (clientFunctions.clientWaitingForCommitSuccess) {
			clientFunctions.clientWaitingForCommitQueue.add(line);
		}

		// Dynamically call <cmd>Command, passing off the tokenizer and the full
		// command string
		try {
			Class<?>[] paramTypes = { StringTokenizer.class, String.class };
			Method handler = clientFunctions.getClass().getMethod(
					cmd + "Handler", paramTypes);
			Object[] args = { tokens, line };
			handler.invoke(clientFunctions, args);
		} catch (NoSuchMethodException e) {
			printError("invalid command:" + line);
		} catch (IllegalArgumentException e) {
			printError("invalid command:" + line);
		} catch (IllegalAccessException e) {
			printError("invalid command:" + line);
		} catch (InvocationTargetException e) {
			/*
			 * TODO: HIGH: Command failed, abort tx if in progress
			 */
			if (clientFunctions.clientTransacting) {

			}

			printError(e);
		}
	}

	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		printVerbose("received " + Protocol.protocolToString(protocol)
				+ " from Universe, giving to RIOLayer");
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

		/*
		 * TODO: HIGH: Replace massive switch w/ dynamic dispatch and
		 * client/manager side receive helpers. Maybe prepend manager only
		 * receive function names "managerReceiveWQ" etc.
		 */

		try {
			switch (protocol) {
			case Protocol.CREATE:
				receiveCreate(from, msgString);
				break;
			case Protocol.DELETE:
				receiveDelete(from, msgString);
				break;
			case Protocol.GET:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.GET) + " packet");
				break;
			case Protocol.PUT:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.PUT) + " packet");
				break;
			case Protocol.APPEND:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.APPEND)
						+ " packet");
				break;
			case Protocol.DATA:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.DATA) + " packet");
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
				printError("received invalid packet");
			}
		} catch (Exception e) {

			/*
			 * TODO: HIGH: All errors should be caught and dealt w/ here
			 * 
			 * Manager side: respond by sendError(from) or send TX_FAILURE
			 * 
			 * Client side: printError, abort tx - manager might detect failure
			 * first, but maybe good to abort here just in case - if manager
			 * removes the client from it's transacting set an abort shouldn't
			 * be necessary and the manager needs to know what to do with it
			 * (ignore it probably)
			 */

			printError(e);
		}
	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

	/*
	 * TODO: HIGH: Are these needed now that the response handlers always send
	 * responses (and handle their own exceptions)?
	 */

	/**
	 * Create RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 */
	protected void receiveCreate(int client, String filename)
			throws NotManagerException, IOException {
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
			throws NotManagerException, IOException,
			PrivilegeLevelDisagreementException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveDelete(from, filename);
	}

	protected void receiveTX_START(int from) throws TransactionException,
			NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_START(from);
	}

	protected void receiveTX_COMMIT(int from) throws TransactionException,
			NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_COMMIT(from);
	}

	protected void receiveTX_ABORT(int from) throws NotManagerException,
			TransactionException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveTX_ABORT(from);
	}

	protected void receiveRQ(int client, String filename)
			throws NotManagerException, IOException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}

		this.managerFunctions.receiveQ(client, filename, Protocol.RQ,
				Protocol.RD, Protocol.RF, true);
	}

	protected void receiveWQ(int client, String filename)
			throws NotManagerException, IOException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}

		this.managerFunctions.receiveQ(client, filename, Protocol.WQ,
				Protocol.WD, Protocol.WF, false);
	}

	/**
	 * Changes the status of this client from IV or RW
	 * 
	 * @param client
	 *            The client to change
	 * @param filename
	 *            The filename
	 * @throws NotManagerException
	 */
	protected void receiveWC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveWC(client, filename);
	}

	/**
	 * Receives an RC and changes this client's status from IV or RW to RO.
	 * 
	 * @param client
	 *            The client to change
	 * @param filename
	 *            The filename
	 * @throws NotManagerException
	 */
	protected void receiveRC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveRC(client, filename);

	}

	protected void receiveIC(int client, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.receiveIC(client, filename);
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
	protected void receiveIV(String msgString) throws NotClientException,
			UnknownManagerException {
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
	protected void receiveRF(String msgString) throws NotClientException,
			UnknownManagerException, IOException,
			PrivilegeLevelDisagreementException {
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
	protected void receiveWF(String msgString) throws NotClientException,
			UnknownManagerException, IOException,
			PrivilegeLevelDisagreementException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveF(msgString, "WF", Protocol.WD, false);
	}

	/*************************************************
	 * end client-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client and manager cache coherency functions
	 ************************************************/

	// TODO: Relocate
	protected void receiveWD_DELETE(int from, String filename)
			throws NotManagerException, IOException,
			MissingPendingRequestException {
		if (!isManager) {
			throw new NotManagerException(
					"WD_DELETE should only be received by the manager");
		}
		this.managerFunctions.receiveWD_DELETE(from, filename);
	}

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws IllegalConcurrentRequestException
	 * @throws MissingPendingRequestException
	 */
	protected void receiveWD(int from, String msgString) throws IOException,
			UnknownManagerException, IllegalConcurrentRequestException,
			MissingPendingRequestException {

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
	protected void receiveRD(int from, String msgString) throws IOException,
			UnknownManagerException {
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
			this.managerFunctions.receiveRD(from, msgString, contents);
		}
	}

	/*************************************************
	 * end client and manager cache coherency functions
	 ************************************************/

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
			throws Exception {
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
	protected void receiveTX_SUCCESS() throws NotClientException, IOException,
			TransactionException {
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
	protected void receiveTX_FAILURE() throws NotClientException,
			TransactionException, IOException {
		if (isManager) {
			throw new NotClientException();
		}
		clientFunctions.receiveTX_FAILURE();
	}
	
	/**
	 * This packet timed out and was a heartbeat packet. It may have been acked, or it may
	 * not have - it's irrelevant from the point of view of the manager.
	 * @param destAddr the destination address for the heartbeat packet
	 * @throws NotManagerException 
	 */
	public void heartbeatTimeout(int destAddr) throws NotManagerException{
		if (!isManager) {
			throw new NotManagerException();
		}
		this.managerFunctions.heartbeatTimeout(destAddr);
	}
	
	public void killNode(int destAddr){
		if (isManager){
			this.managerFunctions.killNode(destAddr);
		}
	}
}
