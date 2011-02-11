import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

//NOTE: Implicit transactions are handled by cache coherency!

/*
 * TODO: HIGH: If a client trys to do an operation that requires RW, make sure they have RW and abort 
 * them or send them a failure if they don't (see Writeup 3 > FS Semantics > Paragraph 4 )
 */

// TODO: HIGH: I vote for sed s/from/client/g

// TODO: HIGH: TEST: If a client tries to write to a file that you locked previously, you should be granted automatic RW. This should work now, but testing...
// TODO: HIGH: TEST: Add to cache status for all files requested
// TODO: HIGH: TEST: Deal with creates and deletes appropriately -  
// 		for create, need to change W_DEL and createReceive to use createfiletx and deletefiletx.
// TODO: HIGH: TEST: Need to handle aborts, failures, and successes - every handler code path should end by sending something to the client (except {W,R,I}C) - this comment should persist
// TODO: HIGH: TEST: Replicas - if client <x> goes down, and someone is requesting this file, we should request a copy from its replica
/**
 * Replica scheme: 1 -> 2 2 -> 3 3 -> 4 4 -> 5 5 -> 1
 */

public class ManagerNode {

	/**
	 * A list of locked files (cache coherency)
	 */
	private Map<String, Integer> lockedFiles;

	/**
	 * A map of queued file requests, from filename -> Client request
	 */
	private Map<String, Queue<QueuedFileRequest>> queuedFileRequests;

	/**
	 * TODO: HIGH: It would be nice if our cache data structures enforced the
	 * constraint that either: one client has RW, some number of clients have
	 * RO, or no clients have RW or RO
	 * 
	 * If we put this in a class we can also log all changes in one place in
	 * that class' wrapped giveRW, giveRO, etc methods
	 * 
	 * I like this kind of thing so here's what I think the class should be:
	 */

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

	private Client node;
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

	private Cache filePermissionCache;

	public ManagerNode(Client n) {
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

		for (int i = 1; i < 6; i++) {
			replicaNode.put(i, (i % 5 + 1));
		}
	}

	private void createNewFile(String filename, int from) {
		// local create
		try {
			node.fs.createFile(filename);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
			return;
		}
		// give RW to the requester for filename
		node.printVerbose("Changing status of client: " + from
				+ " to RW for file: " + filename);
		filePermissionCache.giveRW(from, filename);

		// send success to requester
		node.printVerbose("sending "
				+ Protocol.protocolToString(Protocol.SUCCESS) + " to " + from);
		sendSuccess(from, Protocol.CREATE, filename);
	}

	private void deleteExistingFile(String filename, int from) {
		// delete the file locally
		try {
			node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}
		// update permissions
		node.printVerbose("marking file " + filename + " as unowned");

		node
				.printVerbose("Blanking all permissions for file: " + filename);
		filePermissionCache.revoke(filename);

		// no one had permissions, so send success
		sendSuccess(from, Protocol.DELETE, filename);
	}

	private boolean queueRequestIfLocked(int client, int protocol,
			String filename) {
		if (lockedFiles.containsKey(filename)) {

			// Don't care if the file is locked by the requesting client
			if (lockedFiles.get(filename).equals(client)) {
				return false;
			}

			Queue<QueuedFileRequest> requests = queuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
				queuedFileRequests.put(filename, requests);
			}
			node.printVerbose("Queuing " + Protocol.protocolToString(protocol)
					+ " from " + client + " on " + filename);
			requests.add(new QueuedFileRequest(client, protocol, Utility
					.stringToByteArray(filename)));
			return true;
		} else {
			return false;
		}
	}

	public void receiveWDDelete(int client, String filename) {
		// delete locally
		try {
			if (transactionsInProgress.contains(client))
				try {
					node.fs.deleteFileTX(client, filename);
				} catch (TransactionException e) {
					sendError(client, Protocol.DELETE, filename, ErrorCode.UnknownError);
					return;
				} catch (IOException e) {
					sendError(client, Protocol.DELETE, filename, ErrorCode.UnknownError);
					return;
				}
			else
				node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(client, filename, "IOException on manager for file: " + filename);
			return;
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
			unlockFile(filename);
		}

		requester = pendingRPCDeleteRequests.remove(filename);
		if (requester != null) {
			sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
		}

		requester = pendingReadPermissionRequests.remove(filename);
		if (requester != null) {
			sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
		}
		
		requester = pendingWritePermissionRequests.remove(filename);
		if (requester != null) {
			sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
		}

	}

	public void receiveRD(int from, String filename, String contents) {

		// first write the file to save a local copy
		try {
			node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
			return;
		}

		/*
		 * send out a RD to anyone requesting this - unlike for WD, this
		 * shouldn't be a create or delete (which require RW, so send W{F,D}
		 * messages instead)
		 */
		filePermissionCache.giveRO(from, filename);
		node.printVerbose("Revoking permission on file: " + filename
				+ " for client: " + from);

		Integer destAddr = pendingReadPermissionRequests.remove(filename);
		if (destAddr != null) {
			try {
				sendFile(destAddr, filename, Protocol.RD);
			} catch (IOException e) {
				sendError(from, filename, e.getMessage());
				return;
			}

			// Add to RO list
			filePermissionCache.giveRO(destAddr, filename);
		}

	}

	public void receiveWD(int from, String filename, String contents) {

		/*
		 * TODO: LOW: No need to do this for deletes or creates (although for
		 * creates it might give us a newer file version, which might be nice)
		 */
		// first write the file to save a local copy
		
		// check for blank contents
		if (contents.equals(null))
			contents = "";
		try {
			node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			sendError(from, filename, "IOException in receiveWD");
			return;
		}

		// look for pending request
		boolean foundPendingRequest = false;
		node.printVerbose("Revoking permission on file: " + filename
				+ " for client: " + from);
		filePermissionCache.revoke(filename);

		// check creates
		Integer destAddr = pendingRPCCreateRequests.remove(filename);
		if (destAddr != null) {
			foundPendingRequest = true;
			sendError(destAddr, Protocol.CREATE, filename,
					ErrorCode.FileAlreadyExists);
			return;
		}

		// check deletes
		destAddr = pendingRPCDeleteRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(from, filename, "IllegalConcurrentRequestException!");
				return;
			}
			foundPendingRequest = true;
			deleteExistingFile(filename, destAddr);
		}

		// check CC
		destAddr = pendingWritePermissionRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(from, filename, "IllegalConcurrentRequestException!");
				return;
			}
			foundPendingRequest = true;
			try {
				sendFile(destAddr, filename, Protocol.WD);
			} catch (IOException e) {
				sendError(from, filename, e.getMessage());
				return;
			}
		}

		if (!foundPendingRequest) {

			sendError(from, filename, "MissingPendingRequestException: file: "
					+ filename);
			return;
		}

		// update the status of the client who sent the WD
		Integer rw = filePermissionCache.hasRW(filename);
		if (rw == null || rw != from) {
			node.printError("WD received from client w/o RW"); // for now
			sendError(from, Protocol.DELETE, filename,
					ErrorCode.PrivilegeDisagreement);
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
	private void lockFile(String filename, Integer from) {
		/**
		 * TODO: Detect if client is performing operations out of filename order
		 * during transaction.
		 */

		node.printVerbose("manager locking file: " + filename);
		node.logSynopticEvent("MANAGER-LOCK");
		lockedFiles.put(filename, from);
	}

	public void receiveTXStart(int from, String empty) {

		if (transactionsInProgress.contains(from)) {
			sendError(from, "", "tx already in progress on client");
			return;
		}

		transactionsInProgress.add(from);
		node.printVerbose("added node " + from
				+ " to list of transactions in progress");

		try {
			node.fs.startTransaction(from);
		} catch (IOException e1) {
			node.printError(e1);
			return;
		}
		// callback setup
		addHeartbeatTimeout(from);

	}

	public void receiveTXCommit(int from, String empty) {
		if (!transactionsInProgress.contains(from)) {
			sendError(from, "", "tx not in progress on client");
			return;
		}

		if (!clientHasPendingPermissions(from))
			pendingCommitRequests.put(from, new QueuedFileRequest(from,
					Protocol.TX_COMMIT, Client.emptyPayload));

		transactionsInProgress.remove(from); // remove from tx
		node.printVerbose("removed node " + from
				+ " list of transactions in progress");

		try {
			node.fs.commitTransaction(from);
		} catch (IOException e) {
			sendError(from, "", e.getMessage());
			return;
		}

		/*
		 * TODO: Keep the list of files locked by this client so we don't have
		 * to go through everything that's locked
		 */

		// unlock files
		ArrayList<String> filesToUnlock = new ArrayList<String>();

		for (Entry<String, Integer> entry : lockedFiles.entrySet()) {
			if (entry.getValue() == from) {
				filesToUnlock.add(entry.getKey());
			}
		}

		for (int i = 0; i < filesToUnlock.size(); i++) {
			unlockFile(filesToUnlock.get(i));
		}

		node.RIOSend(from, Protocol.TX_SUCCESS, Client.emptyPayload);

	}

	public void receiveTXAbort(int from, String empty) {
		if (!transactionsInProgress.contains(from)) {
			sendError(from, "", "tx not in progress on client");
			return;
		}


		transactionsInProgress.remove(from);
		unlockFilesForClient(from);
		pendingCommitRequests.remove(from);
		
		node.printVerbose("removed node " + from
				+ " list of transactions in progress");

		try {
			node.fs.abortTransaction(from);
		} catch (IOException e) {
			sendError(from, "", e.getMessage());
			return;
		}
	}

	/**
	 * Create RPC
	 */
	public void receiveCreate(int from, String filename) {

		if (queueRequestIfLocked(from, Protocol.CREATE, filename)) {
			return;
		}

		// Find out if anyone has RW
		Integer rw = filePermissionCache.hasRW(filename);

		if (rw != null) {
			sendRequest(rw, filename, Protocol.WF);
			pendingRPCCreateRequests.put(filename, from);
			lockFile(filename, from);
		} else if (checkExistence(filename)) {
			// Someone has RO, so throw an error that the file exists already
			sendError(from, Protocol.ERROR, filename,
					ErrorCode.FileAlreadyExists);
		} else { // File not in system
			// decide what to do based on transaction status
			if (transactionsInProgress.contains(from)) {
				try {
					node.fs.createFileTX(from, filename);
					sendSuccess(from, Protocol.CREATE, filename);
					filePermissionCache.giveRW(from, filename);
					node.printVerbose("Giving " + from + " RW on file: "
							+ filename);
					lockFile(filename, from);
				} catch (TransactionException e) {
					node.printError(e);
					sendError(from, filename, e.getMessage());
					return;
				} catch (IOException e) {
					node.printError(e);
					sendError(from, filename, e.getMessage());
					return;
				}
			} else {
				createNewFile(filename, from);
			}
		}

	}

	public void receiveDelete(int from, String filename) {
		if (queueRequestIfLocked(from, Protocol.DELETE, filename)) {
			return;
		}

		Integer rw = filePermissionCache.hasRW(filename);
		List<Integer> ro = filePermissionCache.hasRO(filename);

		if (!checkExistence(filename)) {
			// File doesn't exist, send an error to the requester
			sendError(from, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
		}

		boolean waitingForResponses = false;

		// add to pending ICs
		if (rw != null && rw == from) {
			// Requester should have RW
			sendError(from, "", "Got delete request from client with RW");
			return;
		} else if (rw != null && rw != from) {
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
			pendingRPCDeleteRequests.put(filename, from);
			lockFile(filename, from);
		} else {
			if (transactionsInProgress.contains(from))
				try {
					node.fs.deleteFileTX(from, filename);
					node.send(from, Protocol.SUCCESS, Client.emptyPayload);
					node.printVerbose("Giving client: " + from
							+ " RW on file: " + filename);
					filePermissionCache.giveRW(from, filename);
				} catch (TransactionException e) {
					node.printError(e);
				} catch (IOException e) {
					node.printVerbose("IOException on manager for file: "
							+ filename);
				}
			else
				deleteExistingFile(filename, from);
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

	private void receiveQ(int from, String filename, int receivedProtocol,
			int responseProtocol, int forwardingProtocol, boolean preserveROs) {

		// check if locked
		if (queueRequestIfLocked(from, receivedProtocol, filename)) {
			return;
		}

		// lock
		lockFile(filename, from);

		// address of node w/ rw or null
		Integer rw = filePermissionCache.hasRW(filename);
		List<Integer> ro = filePermissionCache.hasRO(filename);

		if (!checkExistence(filename)) {
			sendError(from, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
			unlockFile(filename);
			return;
		}

		if (rw != null && ro.size() > 0) {
			String problem = "simultaneous RW (" + rw + ") and ROs ("
					+ ro.toString() + ") detected on file: " + filename;
			node.printError(problem);

			sendError(from, filename, problem);

			unlockFile(filename);
			return;
		}

		// Check RW status
		if (rw != null) {
			// Get updates
			sendRequest(rw, filename, forwardingProtocol);
			if (receivedProtocol == Protocol.RQ) {
				pendingReadPermissionRequests.put(filename, from);
			} else {
				pendingWritePermissionRequests.put(filename, from);
			}
			return;
		}

		// Check RO status
		if (!preserveROs && ro.size() > 0) { // someone(s) have RO
			for (int i : ro) {
				// Invalidate all ROs
				if (i != from){
					sendRequest(i, filename, Protocol.IV);
					

					List<Integer> list = pendingICs.get(filename);
					if (list == null){
						list = new ArrayList<Integer>();
						pendingICs.put(filename, list);
					}
					list.add(i);

				}
			}
			if (receivedProtocol == Protocol.RQ) {
				pendingReadPermissionRequests.put(filename, from);
			} else {
				pendingWritePermissionRequests.put(filename, from);
			}
			return;
		}

		// send file to requester, no one has RW or RO
		try {
			sendFile(from, filename, responseProtocol);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}

		// unlock and priveleges updated by C message handlers
	}
	
	/**
	 * 
	 * @param from
	 *            The node this IC was received from.
	 * @param filename
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC from this node for this file
	 */
	public void receiveIC(int from, String filename) {

		int destAddr;
		if (!pendingICs.containsKey(filename)) {
			node.printError("Manager has no record of this filename in pending ICs: " + filename);
			sendError(from, Protocol.ERROR, filename, ErrorCode.UnknownError);
			return;
		}
		if (!pendingICs.get(filename).contains(from)){
			node.printError("Manager was not expecting IC from this client: " + from + " for file: " + filename);
			sendError(from, Protocol.ERROR, filename, ErrorCode.UnknownError);
			return;
		} 
			
		// update the status of the client who sent the IC
		List<Integer> ro = filePermissionCache.hasRO(filename);
		for (int i = 0; i < ro.size(); i++){
			if (ro.get(i) == from)
				ro.remove(i);
		}

		node.printVerbose("Changing client: " + from + " to IV");

		List<Integer> waitingForICsFrom = pendingICs.get(filename);

		for (int i = 0; i < waitingForICsFrom.size(); i++){
			if (waitingForICsFrom.get(i) == from){
				waitingForICsFrom.remove(i);
			}
		}
		
		if (waitingForICsFrom.isEmpty()) {
			/*
			 * TODO: this should just clear to prevent reinitialization
			 * maybe, although this way could save some memory... Anyway,
			 * check that whatever assumption is made holds
			 */
			pendingICs.remove(filename);

			if (pendingWritePermissionRequests.containsKey(filename)) {
				destAddr = pendingWritePermissionRequests.remove(filename);
				try {
					sendFile(destAddr, filename, Protocol.WD);
				} catch (IOException e) {
					sendError(from, filename, e.getMessage());
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
	

	public void receiveRC(int from, String filename) {
		node.printVerbose("Changing client: " + from + " to RO");

		filePermissionCache.giveRO(from, filename);

		// check if someone's in the middle of a transaction with this file. if
		// so, don't do anything.
		if (lockedFiles.containsKey(filename)
				&& !transactionsInProgress.contains(lockedFiles.get(filename)))
			unlockFile(filename);

	}

	public void receiveWC(int from, String filename) {

		node.printVerbose("Changing status of client: " + from
				+ " to RW for file: " + filename);
		filePermissionCache.giveRW(from, filename);

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
			sendError(to, Protocol.ERROR, filename, ErrorCode.FileDoesNotExist);
			return;
		} else {
			sendMsg.append(filename);
			sendMsg.append(Client.packetDelimiter);
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
				node.onRIOReceive(commitRequest.from,
						commitRequest.protocol, commitRequest.msg);
			}
		}

	}

	/**
	 * Unlocks all files this client has locks on currently.
	 * Meant mostly for the case of tx failures
	 * @param client the client to unlock all files from
	 */
	private void unlockFilesForClient(int client){
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
	 * @param from
	 *            The client to check
	 * @return False if this client has no pending permission requests, but True
	 *         if the client has no pending permission requests
	 */
	private boolean clientHasPendingPermissions(int from) {
		return (checkPermissionsHelper(from, pendingWritePermissionRequests) && checkPermissionsHelper(
				from, pendingReadPermissionRequests));

	}

	private boolean checkPermissionsHelper(int from, Map<String, Integer> struct) {
		for (Entry<String, Integer> entry : struct.entrySet()) {
			if (entry.getValue() == from)
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

	/*
	 * TODO: Wayne: Comment all send{Success, Error} methods and think about
	 * wheter or not we really need all of them
	 */

	/*
	 * TODO: send successess and errors for all ops done outside of txs, and
	 * tx_successful tx_failure at end of all txs (w/ no op specific responses)?
	 */

	/**
	 * Sends a successful message for the given protocol from the manager to the
	 * client
	 */
	private void sendSuccess(int destAddr, int protocol, String message) {
		String msg = Protocol.protocolToString(protocol)
				+ Client.packetDelimiter + message;
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
	 * @param errorcode
	 *            The error code
	 */
	private void sendError(int destAddr, int cause, String filename,
			int errorcode) {
		String msg = filename + Client.packetDelimiter
				+ Protocol.protocolToString(cause) + Client.packetDelimiter
				+ ErrorCode.lookup(errorcode);
		byte[] payload = Utility.stringToByteArray(msg);
		node.RIOSend(destAddr, Protocol.ERROR, payload);
		
		// tx cleanup
		txFailureCleanup(destAddr);
		
	}

	private void sendError(int from, String filename, String message) {
		String msg = filename + Client.packetDelimiter + message;
		byte[] payload = Utility.stringToByteArray(msg);
		node.RIOSend(from, Protocol.ERROR, payload);
		
		// tx cleanup
		txFailureCleanup(from);
	}
	
	private void txFailureCleanup(int from){

		node.RIOSend(from, Protocol.TX_FAILURE,
				Utility.stringToByteArray(""));
		transactionsInProgress.remove(from);
		unlockFilesForClient(from);
		pendingCommitRequests.remove(from);
	}

	/**
	 * This packet timed out and was a heartbeat packet. It may have been acked,
	 * or it may not have - it's irrelevant from the point of view of the
	 * manager.
	 * 
	 * @param destAddr
	 *            the destination address for the heartbeat packet
	 */
	public void heartbeatTimeout(Integer destAddr) {

		if (transactionsInProgress.contains(destAddr)) {
			node
					.RIOSend(destAddr, Protocol.HEARTBEAT, Client.emptyPayload);
			addHeartbeatTimeout(destAddr);
		}
		//printDebug();
	}

	private void addHeartbeatTimeout(Integer destAddr) {

		Method cbMethod = null;
		try {
			String[] params = { "java.lang.Integer" };
			cbMethod = Callback.getMethod("heartbeatTimeout", this, params);
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
		node.RIOSend(destAddr, Protocol.TX_FAILURE,
				Utility.stringToByteArray(""));
		transactionsInProgress.remove(destAddr);

		// transfer ownership of files
		for (Entry<String, Integer> entry : filePermissionCache.RW
				.entrySet()) {
			Integer newOwner;
			if (entry.getValue().equals(destAddr)) {
				String filename = entry.getKey();
				newOwner = replicaNode.get(destAddr);
				node.printVerbose("Node: " + destAddr
						+ " failed. Transferring ownership" + " of file: "
						+ filename + " to replica node: " + newOwner);
				filePermissionCache.giveRW(newOwner, filename);
				// if someone was waiting for this file, send a WF/RF to the
				// replica
				if (pendingWritePermissionRequests.remove(filename) != null) {
					node.RIOSend(newOwner, Protocol.WF,
							Utility.stringToByteArray(filename));
				} else if (pendingReadPermissionRequests.remove(filename) != null) {
					node.RIOSend(newOwner, Protocol.RF,
							Utility.stringToByteArray(filename));
				}
			}
		}
		ArrayList<String> filesToUnlock = new ArrayList<String>();

		for (Entry<String, Integer> entry : lockedFiles.entrySet()) {
			if (entry.getValue().equals(destAddr))
				filesToUnlock.add(entry.getKey());
		}

		for (int i = 0; i < filesToUnlock.size(); i++)
			unlockFile(filesToUnlock.get(i));
	}

}
