package edu.washington.cs.cse490h.dfs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * TODO: HIGH: PAXOS!
 * 
 * Manager's only responsibility now is to maintain list of file's in the system and who has
 * RW or ROs on those files.
 * 
 * One primary. Two+ backups.
 * 
 * No TFS needed on Manager!
 * 
 * Primary mutation duplications to backups via in memory RPCs
 * If half managers down, system stalls, but can recover RW/RO lists by querying clients
 *  
 * Leases on primaries - after X rounds, auto elect a new primary via Paxos.
 *  
 * If backups think primary is down , try to elect a new Primary via Paxos between managers:
 * Lowest address known is assumed to be lead.
 * Elect new manager, tell all clients, continue.
 * 
 * Packets received during Paxos? Assume being sent to down manager, so ignore everything
 * until new primary is elected.
 * 
 * Queue to separate class
 */

//NOTE: Implicit transactions are handled by cache coherency!

/*
 * TODO: HIGH: If a client trys to do an operation that requires RW, make sure they have RW and abort 
 * them or send them a failure if they don't (see Writeup 3 > FS Semantics > Paragraph 4 )
 */
// TODO: HIGH: TEST: Need to handle aborts, failures, and successes - every handler code path should end by sending something to the client (except {W,R,I}C) - this comment should persist
// TODO: Replicas - if client <x> goes down, and someone is requesting this file, we should request a copy from its replica
// TODO: I would like to push the manager to a separate subpackage entirely and have separate classes for the node, permission queues, etc.
/**
 * Replica scheme: 1 -> 2 2 -> 3 3 -> 4 4 -> 5 5 -> 1
 */

class ManagerNode {

	/**
	 * A list of locked files (cache coherency)
	 */
	private Map<String, Integer> lockedFiles;

	/**
	 * A map of queued file requests, from filename -> Client request
	 */
	private Map<String, Queue<QueuedFileRequest>> queuedFileRequests;

	/**
	 * Encapsulates the RW and RO caches
	 */
	private static class Cache {
		private Map<String, Integer> RW;
		private Map<String, List<Integer>> RO;
		private RIONode n;

		public Cache(RIONode node) {
			this.n = node;
			this.RW = new HashMap<String, Integer>();
			this.RO = new HashMap<String, List<Integer>>();
		}

		/**
		 * Check if filename is in either cache
		 */
		public boolean contains(String filename) {
			return RW.containsKey(filename) || RO.containsKey(filename);
		}

		/**
		 * Revoke any RO on filename and give RW to addr
		 */
		public void giveRW(int addr, String filename) {
			revokeRO(filename);
			RW.put(filename, addr);
			n.printVerbose("CacheStatus: Giving RW to " + addr + " on "
					+ filename);
		}

		/**
		 * Revoke RW on filename and give RO to addr
		 */
		public void giveRO(int addr, String filename) {
			RW.remove(filename);

			List<Integer> ro = RO.get(filename);
			if (ro == null) {
				ro = new ArrayList<Integer>();
				RO.put(filename, ro);
			}
			if (!ro.contains(addr))
				ro.add(addr);

			n.printVerbose("CacheStatus: Giving RO to " + addr + " on "
					+ filename);
		}

		/**
		 * Return the address of the node w/ RW on filename or null
		 */
		public Integer hasRW(String filename) {
			return RW.get(filename);
		}

		/**
		 * Return the list of addresses of nodes w/ RO on filename or a new
		 * empty list
		 */
		public List<Integer> hasRO(String filename) {
			if (RO.get(filename) == null)
				return new ArrayList<Integer>();
			else
				return RO.get(filename);
		}

		/**
		 * Revoke all RW and RO on filename
		 * 
		 * (Not sure we actually need this one)
		 */
		public void revoke(String filename) {
			RW.remove(filename);
			revokeRO(filename);
			n.printVerbose("CacheStatus: Revoking all access to " + filename);
		}

		/**
		 * Revoke all RO on filename
		 */
		private void revokeRO(String filename) {
			List<Integer> ro = RO.get(filename);
			if (ro != null) {
				ro.clear();
			}
		}
	}

	private DFSNode node;
	/**
	 * List of nodes the manager is waiting for ICs from.
	 */
	private Map<String, List<Integer>> pendingICs;

	/**
	 * Status of who is waiting for read permission for this file
	 */
	private Map<String, Integer> pendingReadPermissionRequests;

	/**
	 * Status of who is waiting for write permission for this file
	 */
	private Map<String, Integer> pendingWritePermissionRequests;

	/**
	 * Status of who is waiting to delete this file via RPC
	 */
	private Map<String, Integer> pendingRPCDeleteRequests;

	/**
	 * Status of who is waiting to create this file via RPC
	 */
	private Map<String, Integer> pendingRPCCreateRequests;

	/**
	 * A set of pending commit requests, used to make sure that commits don't
	 * occur before requests are handled
	 */
	private Map<Integer, QueuedFileRequest> pendingCommitRequests;

	/**
	 * A map detailing who replicates who, for example, replicaNode.get(1) = 2
	 */
	private Map<Integer, Integer> replicaNode;

	private static final Integer TIMEOUT = 10;

	/**
	 * A set of node addresses currently performing transactions
	 */
	private Set<Integer> transactionsInProgress;

	/**
	 * A map from client address to a list of files this client has requested,
	 * in order
	 */
	private Map<Integer, List<String>> transactionTouchedFiles;

	private Cache filePermissionCache;

	public ManagerNode(DFSNode n) {
		this.node = n;
		this.lockedFiles = new HashMap<String, Integer>();
		this.pendingICs = new HashMap<String, List<Integer>>();
		this.queuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
		this.pendingReadPermissionRequests = new HashMap<String, Integer>();
		this.pendingWritePermissionRequests = new HashMap<String, Integer>();
		this.pendingRPCDeleteRequests = new HashMap<String, Integer>();
		this.pendingRPCCreateRequests = new HashMap<String, Integer>();
		this.pendingCommitRequests = new HashMap<Integer, QueuedFileRequest>();
		this.transactionsInProgress = new HashSet<Integer>();
		this.replicaNode = new HashMap<Integer, Integer>();
		this.filePermissionCache = new Cache(node);
		this.transactionTouchedFiles = new HashMap<Integer, List<String>>();

		for (int i = 1; i < 6; i++) {
			replicaNode.put(i, (i % 5 + 1));
		}
	}

	private void createNewFile(String filename, int client) {
		// local create
		try {
			node.fs.createFile(filename);
		} catch (IOException e) {
			sendError(client, filename, e);
			return;
		}
		// give RW to the requester for filename
		node.printVerbose("Changing status of client: " + client
				+ " to RW for file: " + filename);
		filePermissionCache.giveRW(client, filename);

		// send success to requester
		node
				.printVerbose("sending "
						+ Protocol.protocolToString(Protocol.SUCCESS) + " to "
						+ client);
		sendSuccess(client, Protocol.CREATE, filename);
	}

	private void deleteExistingFile(String filename, int client) {
		// delete the file locally
		try {
			node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(client, filename, e);
		}
		// update permissions
		node.printVerbose("marking file " + filename + " as unowned");

		node.printVerbose("Blanking all permissions for file: " + filename);
		filePermissionCache.revoke(filename);

		// no one had permissions, so send success
		sendSuccess(client, Protocol.DELETE, filename);
	}

	private boolean queueRequestIfLocked(int client, int protocol,
			String filename) {
		if (lockedFiles.containsKey(filename)) {

			// Don't care if the file is locked by the requesting client
			if (lockedFiles.get(filename) == client) {
				return false;
			}

			Queue<QueuedFileRequest> requests = queuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
				queuedFileRequests.put(filename, requests);
			}
			node.printVerbose("Queuing " + Protocol.protocolToString(protocol)
					+ " client " + client + " on " + filename);
			requests.add(new QueuedFileRequest(client, protocol, Utility
					.stringToByteArray(filename)));
			return true;
		} else {
			return false;
		}
	}

	public void receiveWDDelete(int client, String filename) {

		// make sure this client actually had RW to begin with
		if (filePermissionCache.hasRW(filename) != client) {
			sendError(client, filename, new TransactionException(
					"Error: client " + client + " does not have RW on file: "
							+ filename));
			return;
		}

		// delete locally
		try {
			if (transactionsInProgress.contains(client))
				try {
					node.fs.deleteFileTX(client, filename);
				} catch (TransactionException e) {
					sendError(client, filename, e);
				} catch (IOException e) {
					sendError(client, filename, e);
				}
			else
				node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(client, filename, e);
		}
		// remove permissions

		node.printVerbose("Blanking permissions for file: " + filename);
		filePermissionCache.revoke(filename);

		// look for pending requests

		// check for a create
		Integer requester = pendingRPCCreateRequests.remove(filename);
		if (requester != null) {
			// create the file which was deleted by the owner
			createNewFile(filename, requester);
		}

		requester = pendingRPCDeleteRequests.remove(filename);
		if (requester != null) {
			sendError(requester, filename, new FileNotFoundException());
		}

		requester = pendingReadPermissionRequests.remove(filename);
		if (requester != null) {
			sendError(requester, filename, new FileNotFoundException());
		}

		requester = pendingWritePermissionRequests.remove(filename);
		if (requester != null) {
			sendError(requester, filename, new FileNotFoundException());
		}

		unlockFile(filename);

	}

	public void receiveRD(int client, String filename, String contents) {

		// make sure this client actually had RW to begin with
		if (filePermissionCache.hasRW(filename) != client) {
			sendError(client, filename, new TransactionException(
					"Error: client " + client + " does not have RW on file: "
							+ filename));
			return;
		}
		// first write the file to save a local copy
		try {
			if (Utility.fileExists(node, filename))
				node.fs.writeFile(filename, contents, false);
			else
				node.fs.createFile(filename);
		} catch (IOException e) {
			sendError(client, filename, e);
			return;
		}

		/*
		 * send out a RD to anyone requesting this - unlike for WD, this
		 * shouldn't be a create or delete (which require RW, so send W{F,D}
		 * messages instead)
		 */
		filePermissionCache.giveRO(client, filename);
		node.printVerbose("Revoking permission on file: " + filename
				+ " for client: " + client);

		Integer destAddr = pendingReadPermissionRequests.remove(filename);
		if (destAddr != null) {
			try {
				sendFile(destAddr, filename, Protocol.RD);
			} catch (IOException e) {
				sendError(client, filename, e);
				return;
			}

			// Add to RO list
			filePermissionCache.giveRO(destAddr, filename);
		}

	}

	public void receiveWD(int client, String filename, String contents) {

		/*
		 * TODO: LOW: No need to do this for deletes or creates (although for
		 * creates it might give us a newer file version, which might be nice)
		 */
		// first write the file to save a local copy

		// make sure this client actually had RW to begin with
		if (filePermissionCache.hasRW(filename) != client) {
			sendError(client, filename, new TransactionException(
					"Error: client " + client + " does not have RW on file: "
							+ filename));
			return;
		}

		// check for blank contents
		if (contents.equals(null))
			contents = "";
		try {
			node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			sendError(client, filename, e);
			return;
		}

		// look for pending request
		boolean foundPendingRequest = false;
		node.printVerbose("Revoking permission on file: " + filename
				+ " for client: " + client);
		filePermissionCache.revoke(filename);

		// check creates
		Integer destAddr = pendingRPCCreateRequests.remove(filename);
		if (destAddr != null) {
			foundPendingRequest = true;
			sendError(destAddr, filename, new FileAlreadyExistsException());
			return;
		}

		// check deletes
		destAddr = pendingRPCDeleteRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(client, filename, new FileNotFoundException());
				return;
			}
			foundPendingRequest = true;
			deleteExistingFile(filename, destAddr);
		}

		// check CC
		destAddr = pendingWritePermissionRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(client, filename, new UnknownManagerException());
				return;
			}
			foundPendingRequest = true;
			try {
				sendFile(destAddr, filename, Protocol.WD);
			} catch (IOException e) {
				sendError(client, filename, e.getMessage());
				return;
			}
		}

		if (!foundPendingRequest) {

			sendError(client, filename, new UnknownManagerException());
			return;
		}

		// update the status of the client who sent the WD
		Integer rw = filePermissionCache.hasRW(filename);
		if (rw == null || rw != client) {
			node.printError("WD received client client w/o RW"); // for now
			sendError(client, filename, "Privilege Disagreement Exception");
			// TODO: Actually implement the exception
			return;
		} else {
			node.printVerbose("Blanking ownership permissions for file: "
					+ filename);
			filePermissionCache.revoke(filename);
		}
	}

	/**
	 * Helper the manager should use to lock a file
	 * 
	 * @param filename
	 */
	private void lockFile(String filename, Integer client) {
		node.printVerbose("manager locking file: " + filename);
		node.logSynopticEvent("MANAGER-LOCK");
		lockedFiles.put(filename, client);

		// add this to the list of touched files for a transaction, abort them
		// if it's out of filename order
		if (transactionsInProgress.contains(client)) {
			List<String> list = transactionTouchedFiles.get(client);
			if (list == null) {
				list = new ArrayList<String>();
				transactionTouchedFiles.put(client, list);
			}
			if (list.contains(filename)) {
				node.printVerbose("Client: " + client + " has touched file: "
						+ filename
						+ " previously, not checking for filename order");
				return;
			}
			if (list.size() == 0) {
				node.printVerbose("Client: " + client
						+ " has now touched file: " + filename);
				list.add(filename);
				return;
			}
			String lastFile = list.get(list.size() - 1);
			if (filename.compareTo(lastFile) < 0) {
				sendError(client, filename, new TransactionException(
						"Error: client: " + client
								+ " requested file out of filename order: "
								+ filename));
			} else {
				node.printVerbose("Client: " + client
						+ " has now touched file: " + filename);
				list.add(filename);
			}
		}
	}

	public void receiveTXStart(int client, String empty) {

		if (transactionsInProgress.contains(client)) {
			sendError(client, "", new TransactionException(client + ""));
			return;
		}

		transactionsInProgress.add(client);
		node.printVerbose("added node " + client
				+ " to list of transactions in progress");

		try {
			node.fs.startTransaction(client);
		} catch (IOException e1) {
			node.printError(e1);
			return;
		}
		// callback setup
		addHeartbeatTimeout(client);

		// If this client has something in transaction touched cache, an error
		// occurred
		if (transactionTouchedFiles.get(client) != null) {
			node
					.printError("ERROR: Manager has cached transaction touched files for client: "
							+ client);
		}
		transactionTouchedFiles.put(client, new ArrayList<String>());

	}

	public void receiveTXCommit(int client, String empty) {
		if (!transactionsInProgress.contains(client)) {
			sendError(client, "", new TransactionException(client + ""));
			return;
		}

		if (!clientHasPendingPermissions(client))
			pendingCommitRequests.put(client, new QueuedFileRequest(client,
					Protocol.TX_COMMIT, DFSNode.emptyPayload));

		transactionsInProgress.remove(client); // remove client tx
		node.printVerbose("removed node " + client
				+ " list of transactions in progress");

		try {
			node.fs.commitTransaction(client);
		} catch (IOException e) {
			sendError(client, "", e);
			return;
		}

		// TODO: This is totally unnecessary now, use transactionTouchedFiles

		// unlock files
		ArrayList<String> filesToUnlock = new ArrayList<String>();

		for (Entry<String, Integer> entry : lockedFiles.entrySet()) {
			if (entry.getValue() == client) {
				filesToUnlock.add(entry.getKey());
			}
		}

		for (int i = 0; i < filesToUnlock.size(); i++) {
			unlockFile(filesToUnlock.get(i));
		}

		node.RIOSend(client, Protocol.TX_SUCCESS, DFSNode.emptyPayload);

		// Clear cache
		if (transactionTouchedFiles.remove(client) == null) {
			node.printError("Manager thinks this client: " + client
					+ " has touched no files!");
		}

	}

	public void receiveTXAbort(int client, String empty) {
		if (!transactionsInProgress.contains(client)) {
			sendError(client, "", new TransactionException(
					"Transaction not in progress on client"));
			return;
		}

		transactionsInProgress.remove(client);
		unlockFilesForClient(client);
		pendingCommitRequests.remove(client);

		node.printVerbose("removed node " + client
				+ " list of transactions in progress");

		try {
			node.fs.abortTransaction(client);
		} catch (IOException e) {
			sendError(client, "", e);
			return;
		}
		// Clear cache
		if (transactionTouchedFiles.remove(client) == null) {
			node.printError("Manager thinks this client: " + client
					+ " has touched no files!");
		}
	}

	/**
	 * Create RPC
	 */
	public void receiveCreate(int client, String filename) {

		if (queueRequestIfLocked(client, Protocol.CREATE, filename)) {
			return;
		}

		// Find out if anyone has RW
		Integer rw = filePermissionCache.hasRW(filename);

		if (rw != null) {
			sendRequest(rw, filename, Protocol.WF);
			pendingRPCCreateRequests.put(filename, client);
			lockFile(filename, client);
		} else if (checkExistence(filename)) {
			// Someone has RO, so throw an error that the file exists already
			sendError(client, filename, new FileAlreadyExistsException());
		} else { // File not in system
			// decide what to do based on transaction status
			if (transactionsInProgress.contains(client)) {
				try {
					node.fs.createFileTX(client, filename);
					sendSuccess(client, Protocol.CREATE, filename);
					filePermissionCache.giveRW(client, filename);
					node.printVerbose("Giving " + client + " RW on file: "
							+ filename);
					lockFile(filename, client);
				} catch (TransactionException e) {
					node.printError(e);
					sendError(client, filename, e.getMessage());
					return;
				} catch (IOException e) {
					node.printError(e);
					sendError(client, filename, e.getMessage());
					return;
				}
			} else {
				createNewFile(filename, client);
			}
		}

	}

	public void receiveDelete(int client, String filename) {
		if (queueRequestIfLocked(client, Protocol.DELETE, filename)) {
			return;
		}

		Integer rw = filePermissionCache.hasRW(filename);
		List<Integer> ro = filePermissionCache.hasRO(filename);

		if (!checkExistence(filename)) {
			// File doesn't exist, send an error to the requester
			sendError(client, filename, new FileNotFoundException());
		}

		boolean waitingForResponses = false;

		// add to pending ICs
		if (rw != null && rw == client) {
			// Requester should have RW
			sendError(client, "", "Got delete request client client with RW");
			return;
		} else if (rw != null && rw != client) {
			// Someone other than the requester has RW status, get updates
			sendRequest(rw, filename, Protocol.WF);
			waitingForResponses = true;
		} else if (ro.size() != 0) {
			pendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO (doesn't include
				 * the requester)
				 */
				sendRequest(i, filename, Protocol.IV);
			}
			waitingForResponses = true;
		}

		if (waitingForResponses) {
			// track pending request
			pendingRPCDeleteRequests.put(filename, client);
			lockFile(filename, client);
		} else {
			if (transactionsInProgress.contains(client))
				try {
					node.fs.deleteFileTX(client, filename);
					node.send(client, Protocol.SUCCESS, DFSNode.emptyPayload);
					node.printVerbose("Giving client: " + client
							+ " RW on file: " + filename);
					filePermissionCache.giveRW(client, filename);
				} catch (TransactionException e) {
					node.printError(e);
				} catch (IOException e) {
					node.printVerbose("IOException on manager for file: "
							+ filename);
				}
			else
				deleteExistingFile(filename, client);
		}
	}

	/**
	 * Checks the existence of this file on the cache first, and the file system
	 * second. If the file exists on the file system but not in the cache,
	 * automatically adds it in.
	 * 
	 * @param filename
	 *            The filename
	 * @return
	 */
	private boolean checkExistence(String filename) {

		if (filePermissionCache.contains(filename))
			return true;
		else if (node.fs.fileExistsTX(node.addr, filename))
			return true;
		else
			return false;
	}

	public void receiveRQ(int client, String filename) {
		receiveQ(client, filename, Protocol.RQ, Protocol.RD, Protocol.RF, true);
	}

	public void receiveWQ(int client, String filename) {
		receiveQ(client, filename, Protocol.WQ, Protocol.WD, Protocol.WF, false);
	}

	private void receiveQ(int client, String filename, int receivedProtocol,
			int responseProtocol, int forwardingProtocol, boolean preserveROs) {

		// check if locked
		if (queueRequestIfLocked(client, receivedProtocol, filename)) {
			return;
		}

		// lock
		lockFile(filename, client);

		// address of node w/ rw or null
		Integer rw = filePermissionCache.hasRW(filename);
		List<Integer> ro = filePermissionCache.hasRO(filename);

		if (!checkExistence(filename)) {
			sendError(client, filename, new FileNotFoundException());
			unlockFile(filename);
			return;
		}

		if (rw != null && ro.size() > 0) {
			String problem = "simultaneous RW (" + rw + ") and ROs ("
					+ ro.toString() + ") detected on file: " + filename;
			node.printError(problem);

			sendError(client, filename, problem);

			unlockFile(filename);
			return;
		}

		// Check RW status
		if (rw != null) {
			// Get updates
			sendRequest(rw, filename, forwardingProtocol);
			if (receivedProtocol == Protocol.RQ) {
				pendingReadPermissionRequests.put(filename, client);
			} else {
				pendingWritePermissionRequests.put(filename, client);
			}
			return;
		}

		// Check RO status
		if (!preserveROs && ro.size() > 0) { // someone(s) have RO
			for (int i : ro) {
				// Invalidate all ROs
				if (i != client) {
					sendRequest(i, filename, Protocol.IV);

					List<Integer> list = pendingICs.get(filename);
					if (list == null) {
						list = new ArrayList<Integer>();
						pendingICs.put(filename, list);
					}
					list.add(i);

				}
			}
			if (receivedProtocol == Protocol.RQ) {
				pendingReadPermissionRequests.put(filename, client);
			} else {
				pendingWritePermissionRequests.put(filename, client);
			}
			return;
		}

		// send file to requester, no one has RW or RO
		try {
			sendFile(client, filename, responseProtocol);
		} catch (IOException e) {
			sendError(client, filename, e.getMessage());
		}

		// unlock and priveleges updated by C message handlers
	}

	/**
	 * 
	 * @param client
	 *            The node this IC was received client.
	 * @param filename
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC client this node for this file
	 */
	public void receiveIC(int client, String filename) {

		int destAddr;
		if (!pendingICs.containsKey(filename)) {
			node
					.printError("Manager has no record of this filename in pending ICs: "
							+ filename);
			sendError(client, filename, new UnknownManagerException());
			return;
		}
		if (!pendingICs.get(filename).contains(client)) {
			node.printError("Manager was not expecting IC client this client: "
					+ client + " for file: " + filename);
			sendError(client, filename, new UnknownManagerException());
			return;
		}

		// update the status of the client who sent the IC
		List<Integer> ro = filePermissionCache.hasRO(filename);
		for (int i = 0; i < ro.size(); i++) {
			if (ro.get(i) == client)
				ro.remove(i);
		}

		node.printVerbose("Changing client: " + client + " to IV");

		List<Integer> waitingForICsFrom = pendingICs.get(filename);

		for (int i = 0; i < waitingForICsFrom.size(); i++) {
			if (waitingForICsFrom.get(i) == client) {
				waitingForICsFrom.remove(i);
			}
		}

		if (waitingForICsFrom.isEmpty()) {
			/*
			 * TODO: this should just clear to prevent reinitialization maybe,
			 * although this way could save some memory... Anyway, check that
			 * whatever assumption is made holds
			 */
			pendingICs.remove(filename);

			if (pendingWritePermissionRequests.containsKey(filename)) {
				destAddr = pendingWritePermissionRequests.remove(filename);
				try {
					sendFile(destAddr, filename, Protocol.WD);
				} catch (IOException e) {
					sendError(client, filename, e.getMessage());
					return;
				}
			} else {
				destAddr = pendingRPCDeleteRequests.remove(filename);
				sendSuccess(destAddr, Protocol.DELETE, filename);
			}
		} else {
			// still waiting for more ICs
			List<Integer> waitingFor = pendingICs.get(filename);
			StringBuilder waiting = new StringBuilder();
			waiting.append("Received IC but waiting for IC from clients : ");
			for (int i : waitingFor) {
				waiting.append(i + " ");
			}
			node.printVerbose(waiting.toString());
		}
	}

	public void receiveRC(int client, String filename) {
		node.printVerbose("Changing client: " + client + " to RO");

		filePermissionCache.giveRO(client, filename);

		// check if someone's in the middle of a transaction with this file. if
		// so, don't do anything.
		if (lockedFiles.containsKey(filename)
				&& !transactionsInProgress.contains(lockedFiles.get(filename)))
			unlockFile(filename);

	}

	public void receiveWC(int client, String filename) {

		node.printVerbose("Changing status of client: " + client
				+ " to RW for file: " + filename);
		filePermissionCache.giveRW(client, filename);

		// check if someone's in the middle of a transaction with this file. if
		// so, don't do anything.
		if (lockedFiles.containsKey(filename)
				&& !transactionsInProgress.contains(lockedFiles.get(filename)))
			unlockFile(filename);
	}

	/**
	 * Helper that sends the contents of filename to to with protocol protocol.
	 * Should only be used by the manager.
	 * 
	 * @throws IOException
	 */
	private void sendFile(int to, String filename, int protocol)
			throws IOException {
		StringBuilder sendMsg = new StringBuilder();

		if (!checkExistence(filename)) {
			// Manager doesn't have the file
			sendError(to, filename, new FileNotFoundException());
			return;
		} else {
			sendMsg.append(filename);
			sendMsg.append(DFSNode.packetDelimiter);
			sendMsg.append(node.fs.getFile(filename));
		}

		byte[] payload = Utility.stringToByteArray(sendMsg.toString());
		node.RIOSend(to, protocol, payload);
	}

	/**
	 * Unlocks filename and checks if there is another request to service
	 */
	private void unlockFile(String filename) {

		node.printVerbose("manager unlocking file: " + filename);
		node.logSynopticEvent("MANAGER-UNLOCK");
		lockedFiles.remove(filename);
		int queuedRequester = -1;

		Queue<QueuedFileRequest> outstandingRequests = queuedFileRequests
				.get(filename);
		while (outstandingRequests != null && outstandingRequests.size() > 0) {
			QueuedFileRequest nextRequest = outstandingRequests.poll();
			if (nextRequest != null) {
				queuedRequester = nextRequest.from;
				node.onRIOReceive(nextRequest.from, nextRequest.protocol,
						nextRequest.msg);
			}
		}

		// Was a node waiting for this file request to process?
		if (queuedRequester != -1
				&& pendingCommitRequests.containsKey(queuedRequester)) {
			if (clientHasPendingPermissions(queuedRequester)) {
				QueuedFileRequest commitRequest = pendingCommitRequests
						.remove(queuedRequester);
				node.onRIOReceive(commitRequest.from, commitRequest.protocol,
						commitRequest.msg);
			}
		}

	}

	/**
	 * Unlocks all files this client has locks on currently. Meant mostly for
	 * the case of tx failures
	 * 
	 * @param client
	 *            the client to unlock all files from
	 */
	private void unlockFilesForClient(int client) {
		ArrayList<String> filesToUnlock = new ArrayList<String>();

		for (Entry<String, Integer> entry : lockedFiles.entrySet()) {
			if (entry.getValue().equals(client))
				filesToUnlock.add(entry.getKey());
		}

		for (int i = 0; i < filesToUnlock.size(); i++)
			unlockFile(filesToUnlock.get(i));
	}

	/**
	 * Checks the pending permission request caches for a client
	 * 
	 * @param client
	 *            The client to check
	 * @return False if this client has no pending permission requests, but True
	 *         if the client has no pending permission requests
	 */
	private boolean clientHasPendingPermissions(int client) {
		return (checkPermissionsHelper(client, pendingWritePermissionRequests) && checkPermissionsHelper(
				client, pendingReadPermissionRequests));

	}

	private boolean checkPermissionsHelper(int client,
			Map<String, Integer> struct) {
		for (Entry<String, Integer> entry : struct.entrySet()) {
			if (entry.getValue() == client)
				return false;
		}
		return true;
	}

	/**
	 * Helper that sends a request for the provided filename to the provided
	 * client using the provided protocol
	 */
	private void sendRequest(int client, String filename, int protocol) {
		byte[] payload = Utility.stringToByteArray(filename);
		node.RIOSend(client, protocol, payload);
	}

	/**
	 * Sends a successful message for the given protocol from the manager to the
	 * client
	 */
	private void sendSuccess(int destAddr, int protocol, String message) {
		String msg = Protocol.protocolToString(protocol)
				+ DFSNode.packetDelimiter + message;
		byte[] payload = Utility.stringToByteArray(msg);
		node.RIOSend(destAddr, Protocol.SUCCESS, payload);
	}

	/**
	 * Send Error method
	 * 
	 * @param destAddr
	 *            Who to send the error code to
	 * @param cause
	 *            The protocol that failed
	 * @param filename
	 *            The filename for the protocol that failed
	 */
	private void sendError(int client, String filename, String message) {
		String msg = filename + DFSNode.packetDelimiter + message;
		byte[] payload = Utility.stringToByteArray(msg);
		node.RIOSend(client, Protocol.ERROR, payload);

		// tx cleanup
		txFailureCleanup(client);
	}

	private void sendError(int client, String filename, Exception e) {
		String msg = filename + DFSNode.packetDelimiter + e.getMessage();
		byte[] payload = Utility.stringToByteArray(msg);
		node.RIOSend(client, Protocol.ERROR, payload);

		// tx cleanup
		txFailureCleanup(client);
	}

	private void txFailureCleanup(int client) {

		node
				.RIOSend(client, Protocol.TX_FAILURE, Utility
						.stringToByteArray(""));
		transactionsInProgress.remove(client);
		unlockFilesForClient(client);
		pendingCommitRequests.remove(client);
		// Clear cache
		if (transactionTouchedFiles.remove(client) == null) {
			node.printError("ERROR: Manager thinks this client: " + client
					+ " has touched no files!");
		}
	}

	/**
	 * This packet timed out and was a packet. It may have been acked, or it may
	 * not have - it's irrelevant from the point of view of the manager.
	 * 
	 * @param destAddr
	 *            the destination address for the heartbeat packet
	 */
	public void heartbeatTimeout(Integer destAddr) {

		if (transactionsInProgress.contains(destAddr)) {
			node.RIOSend(destAddr, Protocol.HEARTBEAT, DFSNode.emptyPayload);
			addHeartbeatTimeout(destAddr);
		}
		// printDebug();
	}

	private void addHeartbeatTimeout(Integer destAddr) {

		Method cbMethod = null;
		try {
			String[] params = { "java.lang.Integer" };
			cbMethod = Callback.getMethod("heartbeatTimeout", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (SecurityException e) {
			node.printError(e);
		} catch (ClassNotFoundException e) {
			node.printError(e);
		} catch (NoSuchMethodException e) {
			node.printError(e);
			e.printStackTrace();
		}
		Integer[] args = { destAddr };
		Callback cb = new Callback(cbMethod, this, args);
		node.addTimeout(cb, TIMEOUT);
	}

	/**
	 * This node didn't respond to a packet even after the maximum number of
	 * tries. If this client was in the middle of the transaction, they're now
	 * aborted and all locks on files they own are released
	 * 
	 * @param destAddr
	 */
	protected void killNode(int destAddr) {
		// might as well send a txabort just in case this node is alive
		node.RIOSend(destAddr, Protocol.TX_FAILURE, Utility
				.stringToByteArray(""));
		transactionsInProgress.remove(destAddr);
		//		
		// // transfer ownership of files
		// for (Entry<String, Integer> entry : filePermissionCache.RW
		// .entrySet()) {
		// Integer newOwner;
		// if (entry.getValue().equals(destAddr)) {
		// String filename = entry.getKey();
		// newOwner = replicaNode.get(destAddr);
		// node.printVerbose("Node: " + destAddr
		// + " failed. Transferring ownership" + " of file: "
		// + filename + " to replica node: " + newOwner);
		// filePermissionCache.giveRW(newOwner, filename);
		// // if someone was waiting for this file, send a WF/RF to the
		// // replica
		// if (pendingWritePermissionRequests.remove(filename) != null) {
		// node.RIOSend(newOwner, Protocol.WF,
		// Utility.stringToByteArray(filename));
		// } else if (pendingReadPermissionRequests.remove(filename) != null) {
		// node.RIOSend(newOwner, Protocol.RF,
		// Utility.stringToByteArray(filename));
		// }
		// }
		// }

		ArrayList<String> filesToUnlock = new ArrayList<String>();

		for (Entry<String, Integer> entry : lockedFiles.entrySet()) {
			if (entry.getValue().equals(destAddr))
				filesToUnlock.add(entry.getKey());
		}

		for (int i = 0; i < filesToUnlock.size(); i++)
			unlockFile(filesToUnlock.get(i));
	}

}
